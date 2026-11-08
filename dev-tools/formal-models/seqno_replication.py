# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of sequence-number based replication with primary failover (the "critical write path"):

  * ReplicationOperation on the primary: index locally, sample the global checkpoint *before* the
    replication group, replicate to every tracked copy, mark unavailable in-sync copies as stale,
    fail copies that reject, ack once every pending action completed; "no longer primary" if the
    master disagrees about the term.
  * TransportReplicationAction on a replica: term check ("too old"), primary term bump with engine
    rollback to the global checkpoint, de-duplication by sequence number (planIndexingAsNonPrimary).
  * ReplicationTracker: in-sync / tracked / pendingInSync bookkeeping and computeGlobalCheckpoint.
  * Primary promotion (IndexShard#updateShardState): restoreLocalHistoryFromTranslog, fillSeqNoGaps,
    activatePrimaryMode, primary/replica resync (ops above the global checkpoint + trim of older terms).
  * Master (IndexMetadataUpdater / ShardStateAction): node-left keeps allocation ids in-sync and bumps
    the primary term on primary failure; failShardIfNeeded / markShardCopyAsStale remove ids from the
    in-sync set; shard-started adds them; recovery (initiateTracking, snapshot, markAllocationIdAsInSync).

Copies live in slots; a slot gets a new allocation id (slot, generation) every time it is re-allocated.
Lucene content and translog history are modelled as sets of (seqno, term, is_noop).

Checked properties:
  * acked writes are present on every in-sync copy that is alive, in particular on the current primary
  * no two acked writes share a sequence number; no copy ever applies two different ops with one seqno
  * copies never disagree about an operation below both of their global checkpoints
  * a copy's known global checkpoint never exceeds its local checkpoint
  * quiescence: every write acked or failed, no messages in flight, a primary exists and all live in-sync
    copies hold identical Lucene content
"""

from mc import run_all

UNK = -2  # SequenceNumbers.UNASSIGNED_SEQ_NO
KEYS = ("m_term", "m_primary", "m_in_sync", "m_routing", "m_version", "copies", "writes", "reqs", "resps", "acked", "flags", "used")
# copy = (mode, term, lucene, history, gcp, tracker) ; tracker = (cps, pending, gcp, version, view) ; cps = ((id, lcp, in_sync, tracked), ...)


def lcp_of(lucene):
    seqnos = {o[0] for o in lucene}
    n = -1
    while n + 1 in seqnos:
        n += 1
    return n


def max_seqno(copy):
    ss = [o[0] for o in copy[2]] + [o[0] for o in copy[3]]
    return max(ss) if ss else -1


def compute_gcp(cps, pending, fallback):
    if pending:
        return fallback
    m = None
    for (_, lcp, ins, _) in cps:
        if ins:
            if lcp == UNK:
                return fallback
            m = lcp if m is None else min(m, lcp)
    assert m is not None
    return m


class SeqNoReplicationModel:
    def __init__(self, slots, n_writes, primary_node_left, replica_node_left, recoveries, name=""):
        self.k = slots
        self.n = n_writes
        self.pnl = primary_node_left
        self.rnl = replica_node_left
        self.recoveries = recoveries
        self.name = name or (
            f"SeqNoReplication(copies={slots}, writes={n_writes}, primaryNodeLeft={primary_node_left}, "
            f"replicaNodeLeft={replica_node_left}, recoveries={recoveries})"
        )
        inv = {
            "acked writes present on every live in-sync copy": self.acked_on_in_sync,
            "acked writes have unique sequence numbers": lambda d: len({s for (s, _) in d["acked"]}) == len(d["acked"]),
            "no copy applied two different operations with the same seqno": lambda d: "seqno_collision" not in d["flags"],
            "a primary-mode shard never receives an operation with a newer term": lambda d: "primary_got_newer_term" not in d["flags"],
            "copies agree on every operation below both global checkpoints": self.agree_below_gcp,
            # a copy that is still recovering (tracked but not yet in-sync) may legitimately learn a global checkpoint
            # above its own local checkpoint (ReplicationTracker#updateGlobalCheckpointOnReplica documents lagging/leading
            # knowledge); markAllocationIdAsInSync waits for it to catch up before it becomes in-sync
            "a started copy's global checkpoint never exceeds its local checkpoint": lambda d: all(
                c[0] not in ("R", "P") or c[4] <= lcp_of(c[2]) for c in d["copies"]
            ),
            "primary tracker: global checkpoint <= local checkpoint of every in-sync copy it knows": self.tracker_gcp_ok,
        }
        qui = {
            "every write acked or failed, nothing in flight": lambda d: all(w[0] in ("A", "F") for w in d["writes"])
            and not d["reqs"]
            and not d["resps"],
            "a primary exists": lambda d: d["m_primary"] is not None and d["copies"][d["m_primary"][0]][0] == "P",
            "live in-sync copies converged (identical Lucene content)": self.converged,
        }
        self.invariants = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in inv.items()}
        self.quiescence = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in qui.items()}

    # ------------------------------------------------------------------ state helpers
    def freeze(self, d):
        return tuple(d[k] for k in KEYS)

    def thaw(self, s):
        return dict(zip(KEYS, s))

    def init(self):
        # slot 0 is the started primary, the other slots started replicas, everything in-sync, term 1
        ids = tuple((i, 0) for i in range(self.k))
        cps = tuple(sorted((x, -1, True, True) for x in ids))
        routing = tuple(("S", 0) for _ in range(self.k))
        tracker = (cps, frozenset(), -1, 1, routing)
        copies = [("P", 1, frozenset(), frozenset(), -1, tracker)] + [("R", 1, frozenset(), frozenset(), -1, None) for _ in range(self.k - 1)]
        yield self.freeze(
            dict(m_term=1, m_primary=(0, 0), m_in_sync=frozenset(ids), m_routing=routing, m_version=1, copies=tuple(copies),
                 writes=tuple(("N", None, None, None, frozenset()) for _ in range(self.n)), reqs=frozenset(), resps=frozenset(),
                 acked=frozenset(), flags=frozenset(), used=(0, 0, 0))
        )

    def alive(self, d, cid):
        slot, gen = cid
        return d["m_routing"][slot][1] == gen and d["copies"][slot][0] in ("R", "P", "I")

    def cur_id(self, d, slot):
        return (slot, d["m_routing"][slot][1])

    def set_copy(self, d, slot, **kw):
        c = list(d["copies"][slot])
        for k, v in kw.items():
            c[("mode", "term", "lucene", "history", "gcp", "tracker").index(k)] = v
        cl = list(d["copies"])
        cl[slot] = tuple(c)
        d["copies"] = tuple(cl)

    def set_write(self, d, w, **kw):
        t = list(d["writes"][w])
        for k, v in kw.items():
            t[("status", "primary", "seqno", "term", "outstanding").index(k)] = v
        wl = list(d["writes"])
        wl[w] = tuple(t)
        d["writes"] = tuple(wl)

    def flag(self, d, f):
        d["flags"] = d["flags"] | {f}

    # ------------------------------------------------------------------ properties
    def acked_on_in_sync(self, d):
        for cid in d["m_in_sync"]:
            if self.alive(d, cid):
                hist = d["copies"][cid[0]][3]
                for (s, t) in d["acked"]:
                    if (s, t, False) not in hist:
                        return False
        return True

    def agree_below_gcp(self, d):
        live = [c for c in d["copies"] if c[0] in ("R", "P", "I")]
        for i, c1 in enumerate(live):
            for c2 in live[i + 1 :]:
                bound = min(c1[4], c2[4])
                ops1 = {o[0]: o for o in c1[2] if o[0] <= bound}
                ops2 = {o[0]: o for o in c2[2] if o[0] <= bound}
                for s, o in ops1.items():
                    if s in ops2 and ops2[s] != o:
                        return False
        return True

    def tracker_gcp_ok(self, d):
        for c in d["copies"]:
            if c[0] == "P" and c[5] is not None:
                cps, pending, gcp, _, _ = c[5]
                for (_, lcp, ins, _) in cps:
                    if ins and lcp != UNK and gcp > lcp:
                        return False
        return True

    def converged(self, d):
        contents = {d["copies"][cid[0]][2] for cid in d["m_in_sync"] if self.alive(d, cid)}
        return len(contents) <= 1

    # ------------------------------------------------------------------ tracker helpers (primary side)
    def tracker_update_lcp(self, tr, cid, lcp):
        cps, pending, gcp, ver, view = tr
        new = []
        found = False
        for (x, l, ins, trk) in cps:
            if x == cid:
                found = True
                l2 = max(l, lcp) if l != UNK else lcp
                if x in pending and l2 >= gcp:
                    pending = pending - {x}
                    ins = True
                new.append((x, l2, ins, trk))
            else:
                new.append((x, l, ins, trk))
        if not found:
            return tr
        cps = tuple(sorted(new))
        gcp = compute_gcp(cps, pending, gcp)
        return (cps, pending, gcp, ver, view)

    def tracker_remove(self, tr, cid):
        cps, pending, gcp, ver, view = tr
        cps = tuple(x for x in cps if x[0] != cid)
        pending = pending - {cid}
        gcp = compute_gcp(cps, pending, gcp)
        return (cps, pending, gcp, ver, view)

    def fail_pending_writes(self, d, slot):
        for w, wr in enumerate(d["writes"]):
            if wr[0] == "P" and wr[1] == slot:
                self.set_write(d, w, status="F")

    def master_fail_shard(self, d, p_slot, cid, mark_stale=True):
        """failShardIfNeeded / markShardCopyAsStaleIfNeeded from primary `p_slot` for copy `cid`.
        Returns False when the master no longer considers p the primary for this term ("no longer primary")."""
        p = d["copies"][p_slot]
        if d["m_term"] != p[1] or d["m_primary"] != self.cur_id(d, p_slot):
            return False
        slot, gen = cid
        changed = False
        if d["m_routing"][slot][1] == gen and d["m_routing"][slot][0] != "U":
            rl = list(d["m_routing"])
            rl[slot] = ("U", gen)
            d["m_routing"] = tuple(rl)
            self.set_copy(d, slot, mode="D", tracker=None)
            changed = True
        if mark_stale and cid in d["m_in_sync"]:
            d["m_in_sync"] = d["m_in_sync"] - {cid}
            changed = True
        if changed:
            d["m_version"] += 1
            # the primary learns about the removal when the publication completes
            tr = self.tracker_remove(p[5], cid)
            self.set_copy(d, p_slot, tracker=(tr[0], tr[1], tr[2], d["m_version"], d["m_routing"]))
        return True

    def replication_group(self, tr, pid):
        """ReplicationGroup: targets are tracked copies with a routing entry; in-sync copies without one are 'unavailable'."""
        cps, pending, _, _, view = tr

        def assigned(x):
            return view[x[0]][1] == x[1] and view[x[0]][0] != "U"

        targets = frozenset(x for (x, _, _, trk) in cps if trk and x != pid and assigned(x))
        unavailable = [x for (x, _, ins, _) in cps if ins and not assigned(x)]
        return targets, unavailable

    def ack_if_done(self, d, w):
        wr = d["writes"][w]
        if wr[0] == "P" and not wr[4]:
            self.set_write(d, w, status="A")
            d["acked"] = d["acked"] | {(wr[2], wr[3])}

    # ------------------------------------------------------------------ transitions
    def next(self, s):
        d0 = self.thaw(s)
        yield from self.write_steps(d0)
        yield from self.request_steps(d0)
        yield from self.response_steps(d0)
        yield from self.master_steps(d0)
        yield from self.cluster_state_steps(d0)
        yield from self.recovery_steps(d0)

    def write_steps(self, d0):
        for w, wr in enumerate(d0["writes"]):
            if wr[0] != "N":
                continue
            for slot, c in enumerate(d0["copies"]):
                if c[0] != "P" or c[5] is None:
                    continue
                d = self.thaw(self.freeze(d0))
                pid = self.cur_id(d, slot)
                s_no = max_seqno(c) + 1
                op = (s_no, c[1], False)
                lucene = c[2] | {op}
                tr = self.tracker_update_lcp(c[5], pid, lcp_of(lucene))
                self.set_copy(d, slot, lucene=lucene, history=c[3] | {op}, tracker=tr)
                gcp_snapshot = tr[2]  # primary.computedGlobalCheckpoint() sampled BEFORE the replication group
                targets, unavailable = self.replication_group(tr, pid)
                ok = True
                for x in unavailable:  # markUnavailableShardsAsStale
                    if not self.master_fail_shard(d, slot, x):
                        ok = False
                        break
                if not ok:
                    self.set_copy(d, slot, mode="D", tracker=None)
                    self.fail_pending_writes(d, slot)
                    self.set_write(d, w, status="F", primary=slot, seqno=s_no, term=c[1])
                    yield f"write{w} on {pid}: no longer primary while marking stale copies", self.freeze(d)
                    continue
                reqs = set(d["reqs"])
                for x in targets:
                    reqs.add((x, slot, w, "op", c[1], (op, gcp_snapshot)))
                d["reqs"] = frozenset(reqs)
                self.set_write(d, w, status="P", primary=slot, seqno=s_no, term=c[1], outstanding=targets)
                self.ack_if_done(d, w)
                yield f"write{w} on {pid}: seqno {s_no} term {c[1]} -> {sorted(targets)} gcp={gcp_snapshot}", self.freeze(d)

    def apply_ops(self, d, slot, ops):
        c = d["copies"][slot]
        lucene, history = set(c[2]), set(c[3])
        for op in sorted(ops):
            s_no = op[0]
            if s_no <= lcp_of(lucene):
                continue  # hasBeenProcessedBefore -> processButSkipLucene
            existing = [o for o in lucene if o[0] == s_no]
            if existing:
                if existing[0] != op:
                    self.flag(d, "seqno_collision")
                continue
            lucene.add(op)
            history.add(op)
        self.set_copy(d, slot, lucene=frozenset(lucene), history=frozenset(history))

    def request_steps(self, d0):
        for r in d0["reqs"]:
            target, p_slot, w, kind, rterm, payload = r
            d = self.thaw(self.freeze(d0))
            d["reqs"] = d["reqs"] - {r}
            slot = target[0]
            c = d["copies"][slot]
            resps = set(d["resps"])
            if not self.alive(d, target):
                resps.add((p_slot, w, target, -1, -1, "unavailable"))
                d["resps"] = frozenset(resps)
                yield f"req {kind} w{w} -> {target}: copy gone", self.freeze(d)
                continue
            if c[0] == "P" or rterm < c[1]:
                if c[0] == "P" and rterm > c[1]:
                    self.flag(d, "primary_got_newer_term")
                resps.add((p_slot, w, target, -1, -1, "too_old"))
                d["resps"] = frozenset(resps)
                yield f"req {kind} w{w} -> {target}: rejected (term {rterm} vs {c[1]}, mode {c[0]})", self.freeze(d)
                continue
            if kind == "op":
                op, gcp_req = payload
                ops, trim_above = (op,), None
            else:
                ops, trim_above, gcp_req = payload
            if rterm > c[1]:
                # bumpPrimaryTerm on the replica: updateGlobalCheckpointOnReplica, then rollbackEngineToGlobalCheckpoint
                gcp_c = max(c[4], gcp_req)
                lucene = c[2]
                if gcp_c < max_seqno(c):
                    lucene = frozenset(o for o in c[2] if o[0] <= gcp_c)
                self.set_copy(d, slot, term=rterm, gcp=gcp_c, lucene=lucene)
                c = d["copies"][slot]
            self.set_copy(d, slot, gcp=max(c[4], gcp_req))  # updateGlobalCheckpointOnReplica(globalCheckpoint, "operation")
            self.apply_ops(d, slot, ops)
            c = d["copies"][slot]
            if trim_above is not None:  # trimOperationOfPreviousPrimaryTerms(trimAboveSeqNo)
                self.set_copy(d, slot, history=frozenset(o for o in c[3] if not (o[0] > trim_above and o[1] < c[1])))
                c = d["copies"][slot]
            resps.add((p_slot, w, target, lcp_of(c[2]), c[4], "ok"))
            d["resps"] = frozenset(resps)
            yield f"req {kind} w{w} -> {target}: applied (term {rterm})", self.freeze(d)

    def response_steps(self, d0):
        for r in d0["resps"]:
            p_slot, w, target, lcp, gcp, result = r
            d = self.thaw(self.freeze(d0))
            d["resps"] = d["resps"] - {r}
            p = d["copies"][p_slot]
            if p[0] != "P" or p[5] is None:
                yield f"resp w{w} from {target}: primary gone, dropped", self.freeze(d)
                continue
            is_write = isinstance(w, int)
            if result == "ok":
                self.set_copy(d, p_slot, tracker=self.tracker_update_lcp(p[5], target, lcp))
            else:
                if not self.master_fail_shard(d, p_slot, target):
                    # onNoLongerPrimary: fail the primary shard
                    self.set_copy(d, p_slot, mode="D", tracker=None)
                    self.fail_pending_writes(d, p_slot)
                    yield f"resp w{w} from {target}: {result} -> no longer primary, {p_slot} fails", self.freeze(d)
                    continue
            if is_write:
                wr = d["writes"][w]
                if wr[0] == "P":
                    self.set_write(d, w, outstanding=wr[4] - {target})
                    self.ack_if_done(d, w)
            yield f"resp w{w} from {target}: {result} lcp={lcp}", self.freeze(d)

    def master_steps(self, d0):
        used_p, used_r, used_rec = d0["used"]
        # node holding the primary leaves the cluster: promote an in-sync started replica, bump the term, keep ids in-sync
        if self.pnl and used_p < 1 and d0["m_primary"] is not None:
            p_slot = d0["m_primary"][0]
            cands = [self.cur_id(d0, i) for i in range(self.k) if i != p_slot and d0["m_routing"][i][0] == "S" and self.cur_id(d0, i) in d0["m_in_sync"]]
            for cid in cands:
                d = self.thaw(self.freeze(d0))
                rl = list(d["m_routing"])
                rl[p_slot] = ("U", rl[p_slot][1])
                d["m_routing"] = tuple(rl)
                d["m_primary"] = cid
                d["m_term"] += 1
                d["m_version"] += 1
                d["used"] = (used_p + 1, used_r, used_rec)
                yield f"master: primary node left, promote {cid} with term {d['m_term']}", self.freeze(d)
        # node holding a replica leaves: routing unassigned, id stays in-sync, the copy keeps running until it learns
        if self.rnl and used_r < 1:
            for i in range(self.k):
                if d0["m_routing"][i][0] == "S" and self.cur_id(d0, i) != d0["m_primary"] and d0["copies"][i][0] == "R":
                    d = self.thaw(self.freeze(d0))
                    rl = list(d["m_routing"])
                    rl[i] = ("U", rl[i][1])
                    d["m_routing"] = tuple(rl)
                    d["m_version"] += 1
                    d["used"] = (used_p, used_r + 1, used_rec)
                    yield f"master: replica node {self.cur_id(d0, i)} left", self.freeze(d)
        # (re)allocate an unassigned slot as a fresh initializing replica
        if used_rec < self.recoveries and d0["m_primary"] is not None:
            for i in range(self.k):
                if d0["m_routing"][i][0] == "U" and d0["copies"][i][0] in ("D", "U"):
                    d = self.thaw(self.freeze(d0))
                    gen = d["m_routing"][i][1] + 1
                    rl = list(d["m_routing"])
                    rl[i] = ("I", gen)
                    d["m_routing"] = tuple(rl)
                    d["m_version"] += 1
                    d["used"] = (used_p, used_r, used_rec + 1)
                    self.set_copy(d, i, mode="I", term=0, lucene=frozenset(), history=frozenset(), gcp=-1, tracker=None)
                    yield f"master: allocate {(i, gen)} as initializing replica", self.freeze(d)
        # shard started: the primary marked the recovering copy in-sync
        if d0["m_primary"] is not None:
            p = d0["copies"][d0["m_primary"][0]]
            if p[0] == "P" and p[5] is not None:
                for (x, lcp, ins, trk) in p[5][0]:
                    if ins and x not in p[5][1] and d0["m_routing"][x[0]] == ("I", x[1]) and d0["copies"][x[0]][0] == "I":
                        d = self.thaw(self.freeze(d0))
                        rl = list(d["m_routing"])
                        rl[x[0]] = ("S", x[1])
                        d["m_routing"] = tuple(rl)
                        in_sync = set(d["m_in_sync"]) | {x}
                        if len(in_sync) > self.k:  # IndexMetadataUpdater bounds the set: keep ids with routing entries first
                            assigned = {self.cur_id(d, i) for i in range(self.k) if d["m_routing"][i][0] != "U"}
                            ordered = sorted(in_sync, key=lambda y: (y not in assigned, y))
                            in_sync = set(ordered[: self.k])
                        d["m_in_sync"] = frozenset(in_sync)
                        d["m_version"] += 1
                        yield f"master: shard {x} started, in-sync now {sorted(in_sync)}", self.freeze(d)

    def cluster_state_steps(self, d0):
        for slot, c in enumerate(d0["copies"]):
            if c[0] in ("D", "U"):
                continue
            cid = self.cur_id(d0, slot)
            routing = d0["m_routing"][slot]
            if routing[0] == "U":
                d = self.thaw(self.freeze(d0))
                self.set_copy(d, slot, mode="D", tracker=None)
                self.fail_pending_writes(d, slot)
                yield f"{cid}: applies cluster state, shard removed locally", self.freeze(d)
            elif d0["m_primary"] == cid and c[0] == "R" and c[1] < d0["m_term"]:
                yield from self.promote(d0, slot, cid)
            elif c[0] == "P" and c[5] is not None and c[5][3] < d0["m_version"]:
                d = self.thaw(self.freeze(d0))
                cps, pending, gcp, _, _ = c[5]
                init_ids = {self.cur_id(d, i) for i in range(self.k) if d["m_routing"][i][0] == "I"}
                keep = d["m_in_sync"] | init_ids
                removed = any(x[0] not in keep for x in cps)
                cps = [x for x in cps if x[0] in keep]
                known = {x[0] for x in cps}
                for x in init_ids:
                    if x not in known:
                        cps.append((x, UNK, False, False))
                cps = tuple(sorted(cps))
                pending = frozenset(x for x in pending if x in known)
                if removed:
                    gcp = compute_gcp(cps, pending, gcp)
                self.set_copy(d, slot, tracker=(cps, pending, gcp, d["m_version"], d["m_routing"]))
                yield f"{cid}: updateFromMaster v{d['m_version']}", self.freeze(d)
            elif c[0] == "I" and routing[0] == "S":
                d = self.thaw(self.freeze(d0))
                self.set_copy(d, slot, mode="R")
                yield f"{cid}: started as replica", self.freeze(d)

    def promote(self, d0, slot, cid):
        d = self.thaw(self.freeze(d0))
        c = d["copies"][slot]
        term = d["m_term"]
        lucene = set(c[2]) | set(c[3])  # restoreLocalHistoryFromTranslog
        history = set(c[3])
        mx = max_seqno(c)
        for s_no in range(lcp_of(lucene) + 1, mx + 1):  # fillSeqNoGaps(newPrimaryTerm)
            if all(o[0] != s_no for o in lucene):
                noop = (s_no, term, True)
                lucene.add(noop)
                history.add(noop)
        lucene, history = frozenset(lucene), frozenset(history)
        init_ids = {self.cur_id(d, i) for i in range(self.k) if d["m_routing"][i][0] == "I"}
        cps = []
        for x in sorted(d["m_in_sync"] | init_ids):
            ins = x in d["m_in_sync"]
            cps.append((x, lcp_of(lucene) if x == cid else UNK, ins, ins))
        cps = tuple(cps)
        gcp = compute_gcp(cps, frozenset(), c[4])  # activatePrimaryMode -> updateGlobalCheckpointOnPrimary
        tracker = (cps, frozenset(), gcp, d["m_version"], d["m_routing"])
        self.set_copy(d, slot, mode="P", term=term, lucene=lucene, history=history, tracker=tracker)
        # primary/replica resync (itself a ReplicationOperation): translog ops above the global checkpoint, trim older-term
        # ops above max_seq_no; unavailable in-sync copies (e.g. the old primary) are marked stale on the way
        ops = tuple(sorted(o for o in history if o[0] > gcp))
        targets, unavailable = self.replication_group(tracker, cid)
        for x in unavailable:
            if not self.master_fail_shard(d, slot, x):
                self.set_copy(d, slot, mode="D", tracker=None)
                yield f"{cid}: promoted but no longer primary while marking stale copies", self.freeze(d)
                return
        tr = d["copies"][slot][5]
        reqs = set(d["reqs"])
        for x in targets:
            reqs.add((x, slot, ("resync", cid), "resync", term, (ops, mx, tr[2])))
        d["reqs"] = frozenset(reqs)
        yield f"{cid}: promoted to primary (term {term}), resync ops {ops} -> {sorted(targets)}", self.freeze(d)

    def recovery_steps(self, d0):
        if d0["m_primary"] is None:
            return
        p_slot = d0["m_primary"][0]
        p = d0["copies"][p_slot]
        if p[0] != "P" or p[5] is None:
            return
        cps, pending, gcp, ver, view = p[5]
        for (x, lcp, ins, trk) in cps:
            if x == self.cur_id(d0, p_slot) or not self.alive(d0, x) or d0["copies"][x[0]][0] != "I":
                continue
            if not trk:
                # initiateTracking, then phase 1 + phase 2 copy everything the primary has right now
                d = self.thaw(self.freeze(d0))
                new_cps = tuple(sorted((y, l, i2, True) if y == x else (y, l, i2, t2) for (y, l, i2, t2) in cps))
                self.set_copy(d, p_slot, tracker=(new_cps, pending, gcp, ver, view))
                t = d["copies"][x[0]]
                self.set_copy(d, x[0], lucene=t[2] | p[2], history=t[3] | p[2], term=p[1], gcp=max(t[4], gcp))
                yield f"recovery of {x}: initiateTracking + copy from primary", self.freeze(d)
            elif not ins and x not in pending:
                # markAllocationIdAsInSync(x, targetLocalCheckpoint)
                d = self.thaw(self.freeze(d0))
                tl = lcp_of(d["copies"][x[0]][2])
                new_cps = []
                for (y, l, i2, t2) in cps:
                    if y == x:
                        l2 = max(l, tl) if l != UNK else tl
                        new_cps.append((y, l2, l2 >= gcp, t2))
                    else:
                        new_cps.append((y, l, i2, t2))
                new_cps = tuple(sorted(new_cps))
                new_pending = pending if tl >= gcp else pending | {x}
                new_gcp = compute_gcp(new_cps, new_pending, gcp)
                self.set_copy(d, p_slot, tracker=(new_cps, new_pending, new_gcp, ver, view))
                yield f"recovery of {x}: markAllocationIdAsInSync(lcp={tl}) -> {'in-sync' if tl >= gcp else 'pending'}", self.freeze(d)


def fmt(s):
    d = dict(zip(KEYS, s))
    def cp(c):
        mode, term, lucene, history, gcp, tr = c
        base = f"{mode}/t{term} lucene={sorted((o[0], o[1]) + (('n',) if o[2] else ()) for o in lucene)} gcp={gcp}"
        if tr:
            base += f" tracker[cps={tr[0]} pending={sorted(tr[1])} gcp={tr[2]} v{tr[3]}]"
        return base
    copies = " | ".join(f"c{i}: {cp(c)}" for i, c in enumerate(d["copies"]))
    return (
        f"master[term={d['m_term']} primary={d['m_primary']} in_sync={sorted(d['m_in_sync'])} routing={d['m_routing']} v{d['m_version']}] "
        f"{copies} writes={d['writes']} reqs={sorted(d['reqs'], key=repr)} resps={sorted(d['resps'], key=repr)} acked={sorted(d['acked'])}"
        + (f" FLAGS={sorted(d['flags'])}" if d["flags"] else "")
    )


if __name__ == "__main__":
    run_all(
        [
            SeqNoReplicationModel(2, 1, False, False, 0),
            SeqNoReplicationModel(2, 2, False, False, 0),
            SeqNoReplicationModel(2, 1, True, False, 0),
            SeqNoReplicationModel(2, 2, True, False, 0),
            SeqNoReplicationModel(2, 2, True, True, 0),
            SeqNoReplicationModel(2, 1, True, False, 1),
            SeqNoReplicationModel(3, 1, True, False, 0),
            SeqNoReplicationModel(3, 2, True, False, 0),
            SeqNoReplicationModel(2, 2, True, True, 1),
            SeqNoReplicationModel(3, 2, True, True, 0),
        ],
        fmt=fmt,
    )
