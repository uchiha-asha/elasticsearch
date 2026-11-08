# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``IndexShardOperationPermits`` together with the two IndexShard flows built on it:
``bumpPrimaryTerm`` (primary term transition on promotion) and ``relocated`` (primary relocation
hand-off), plus the ``isPrimaryMode`` check done by ``wrapPrimaryOperationPermitListener``.

Shared state (all fields of the tuple):
  held        number of single permits currently held by operations (semaphore permits taken)
  all_held    True while a blocker holds TOTAL_PERMITS
  waiting     FIFO tuple of blockers parked inside semaphore.tryAcquire(TOTAL_PERMITS, timeout).
              The semaphore is fair, so while this is non-empty a timed tryAcquire(1) by an
              operation returns false, which the Java code turns into an IllegalStateException
              ("failed to obtain permit but operations are not delayed").  The model flags that.
  queued      queuedBlockOperations
  delayed     delayedOperations (tuple of op ids, in order)
  pending     IndexShard.pendingPrimaryTerm
  opterm      ReplicationTracker.operationPrimaryTerm
  primary     ReplicationTracker.primaryMode
  mutex       id of the blocker holding IndexShard.mutex (between delayOperations and the
              assignment of pendingPrimaryTerm), or None
  ops         per operation: (status, term_at_start, term_observed)
              status N=not started, D=delayed, H=holding permit & executing, C=completed,
              F=failed with ShardNotInPrimaryModeException
  blockers    per blocker: (kind, target_term, pc) with kind in {"bump", "reloc", "block"}
              pc: N=not started, S=scheduled on executor, W=waiting in semaphore, A=all permits
              acquired (callback running), R1=semaphore released, R2=done, H=hand-off in progress
  generic     tuple of tuples: each a GENERIC-pool runnable re-acquiring a batch of delayed ops
"""

from mc import run_all

N, D, H, C, F = "N", "D", "H", "C", "F"


class ShardPermitsModel:
    def __init__(self, n_ops, blockers, name_suffix=""):
        self.n_ops = n_ops
        self.blocker_specs = blockers  # list of (kind, term)
        self.name = f"ShardOperationPermits(ops={n_ops}, blockers={blockers}){name_suffix}"
        self.invariants = {
            "all permits held => no operation holds a permit": lambda s: not s["all_held"] or s["held"] == 0,
            "acquire() never fails (fair semaphore has no parked blocker when queued == 0)": lambda s: not s.get("acquire_failed"),
            "queuedBlockOperations >= 0 and delayed ops only exist while blocked": lambda s: s["queued"] >= 0
            and (s["queued"] > 0 or s["delayed"] == ()),
            "operationPrimaryTerm <= pendingPrimaryTerm": lambda s: s["opterm"] <= s["pending"],
            "an executing op observed the term that was pending when it started acquiring": lambda s: all(
                st != H or obs >= start for (st, start, obs) in s["ops"]
            ),
            "an executing op never runs after relocation hand-off completed": lambda s: all(
                st != H for (st, _, _) in s["ops"]
            )
            or s["primary"],
            "primary term never changes while an operation executes": lambda s: all(
                st != H or obs == s["opterm"] for (st, _, obs) in s["ops"]
            ),
        }
        self.quiescence = {
            "all operations finished (none delayed forever)": lambda s: all(st in (C, F) for (st, _, _) in s["ops"]),
            "no blocker stuck, permits released": lambda s: not s["all_held"]
            and s["queued"] == 0
            and all(pc == "R2" for (_, _, pc) in s["blockers"]),
            "operation term caught up with pending term": lambda s: s["opterm"] == s["pending"],
        }

        self.invariants = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in self.invariants.items()}
        self.quiescence = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in self.quiescence.items()}

    def init(self):
        yield self.freeze(
            dict(
                held=0,
                all_held=False,
                waiting=(),
                queued=0,
                delayed=(),
                pending=1,
                opterm=1,
                primary=True,
                mutex=None,
                ops=tuple((N, 0, 0) for _ in range(self.n_ops)),
                blockers=tuple((k, t, N) for (k, t) in self.blocker_specs),
                generic=(),
                acquire_failed=False,
            )
        )

    KEYS = ("held", "all_held", "waiting", "queued", "delayed", "pending", "opterm", "primary", "mutex", "ops", "blockers", "generic", "acquire_failed")

    def freeze(self, d):
        return tuple(d[k] for k in self.KEYS)

    def thaw(self, t):
        return dict(zip(self.KEYS, t))

    # --- IndexShardOperationPermits#innerAcquire for op `o`, run under the permits lock ---
    def inner_acquire(self, d, o):
        d = dict(d)
        ops = list(d["ops"])
        st, start, obs = ops[o]
        if d["queued"] > 0:
            d["delayed"] = d["delayed"] + (o,)
            ops[o] = (D, start, obs)
        else:
            if d["all_held"] or d["waiting"]:
                d["acquire_failed"] = True  # IllegalStateException("failed to obtain permit but operations are not delayed")
                ops[o] = (F, start, obs)
            else:
                d["held"] += 1
                # wrapPrimaryOperationPermitListener: check isPrimaryMode() under the permit
                if d["primary"]:
                    ops[o] = (H, start, d["opterm"])
                else:
                    d["held"] -= 1  # r.close(); l.onFailure(new ShardNotInPrimaryModeException)
                    ops[o] = (F, start, obs)
        d["ops"] = tuple(ops)
        return d

    def next(self, s):
        d = self.thaw(s)
        # ---- operations ----
        for o, (st, start, obs) in enumerate(d["ops"]):
            if st == N:
                nd = dict(d)
                ops = list(nd["ops"])
                ops[o] = (N, nd["pending"], 0)  # ghost: remember the pending term visible when acquire() is called
                nd["ops"] = tuple(ops)
                yield f"op{o}: acquirePrimaryOperationPermit", self.freeze(self.inner_acquire(nd, o))
            elif st == H:
                nd = dict(d)
                nd["held"] -= 1
                ops = list(nd["ops"])
                ops[o] = (C, start, obs)
                nd["ops"] = tuple(ops)
                yield f"op{o}: release permit", self.freeze(nd)
        # ---- GENERIC runnables re-acquiring delayed ops, in order ----
        for gi, batch in enumerate(d["generic"]):
            nd = dict(d)
            o = batch[0]
            restb = batch[1:]
            gens = list(nd["generic"])
            if restb:
                gens[gi] = restb
            else:
                del gens[gi]
            nd["generic"] = tuple(gens)
            yield f"generic: re-acquire op{o}", self.freeze(self.inner_acquire(nd, o))
        # ---- blockers ----
        for b, (kind, term, pc) in enumerate(d["blockers"]):
            def setb(nd, newpc):
                bl = list(nd["blockers"])
                bl[b] = (kind, term, newpc)
                nd["blockers"] = tuple(bl)
                return nd

            if pc == N:
                # bumps are issued under IndexShard.mutex in cluster-state order, so terms strictly increase
                # (asserted by `newPrimaryTerm > pendingPrimaryTerm`); only their async permit acquisitions may reorder
                lower_bumps_started = all(
                    pc2 != N for (k2, t2, pc2) in d["blockers"] if k2 == "bump" and t2 < (term or 0)
                )
                if d["mutex"] is None and (kind != "bump" or (term > d["pending"] and lower_bumps_started)):
                    nd = dict(d)
                    nd["queued"] += 1  # delayOperations()
                    if kind == "bump":
                        nd["mutex"] = b  # still inside bumpPrimaryTerm under IndexShard.mutex
                        yield f"blocker{b}: bumpPrimaryTerm({term}) delayOperations", self.freeze(setb(nd, "S0"))
                    else:
                        yield f"blocker{b}: {kind} delayOperations", self.freeze(setb(nd, "S"))
            elif pc == "S0":
                nd = dict(d)
                nd["pending"] = term  # pendingPrimaryTerm = newPrimaryTerm; termUpdated.countDown()
                nd["mutex"] = None
                yield f"blocker{b}: pendingPrimaryTerm = {term}", self.freeze(setb(nd, "S"))
            elif pc == "S":
                # the executor runs acquireAll(): semaphore.tryAcquire(TOTAL_PERMITS, timeout)
                if d["held"] == 0 and not d["all_held"] and not d["waiting"]:
                    nd = dict(d)
                    nd["all_held"] = True
                    yield f"blocker{b}: acquireAll succeeds immediately", self.freeze(setb(nd, "A"))
                else:
                    nd = dict(d)
                    nd["waiting"] = nd["waiting"] + (b,)
                    yield f"blocker{b}: parks in semaphore (fair queue)", self.freeze(setb(nd, "W"))
            elif pc == "W":
                if d["held"] == 0 and not d["all_held"] and d["waiting"][0] == b:
                    nd = dict(d)
                    nd["waiting"] = nd["waiting"][1:]
                    nd["all_held"] = True
                    yield f"blocker{b}: acquireAll completes after drain", self.freeze(setb(nd, "A"))
            elif pc == "A":
                nd = dict(d)
                if kind == "bump":
                    # onResponse: termUpdated.await(); if (getOperationPrimaryTerm() < newPrimaryTerm) { setOperationPrimaryTerm; onBlocked }
                    if nd["pending"] >= term:
                        if nd["opterm"] < term:
                            nd["opterm"] = term
                        yield f"blocker{b}: term transition callback (opterm={nd['opterm']})", self.freeze(setb(nd, "R1"))
                elif kind == "reloc":
                    # startRelocationHandoff; consumer hands primary context to the target asynchronously
                    yield f"blocker{b}: startRelocationHandoff", self.freeze(setb(nd, "H"))
                else:
                    yield f"blocker{b}: acquireAllPrimaryOperationsPermits callback", self.freeze(setb(nd, "R1"))
            elif pc == "H":
                nd = dict(d)
                nd["primary"] = False  # completeRelocationHandoff() under mutex, then release permits
                yield f"blocker{b}: completeRelocationHandoff", self.freeze(setb(nd, "R1"))
                yield f"blocker{b}: abortRelocationHandoff", self.freeze(setb(dict(d), "R1"))
            elif pc == "R1":
                # Releasables.wrap(releasable, released): first semaphore.release(TOTAL_PERMITS) ...
                nd = dict(d)
                nd["all_held"] = False
                yield f"blocker{b}: semaphore.release(TOTAL_PERMITS)", self.freeze(setb(nd, "R1b"))
            elif pc == "R1b":
                # ... then releaseDelayedOperations()
                nd = dict(d)
                nd["queued"] -= 1
                if nd["queued"] == 0 and nd["delayed"]:
                    nd["generic"] = nd["generic"] + (nd["delayed"],)
                    nd["delayed"] = ()
                yield f"blocker{b}: releaseDelayedOperations", self.freeze(setb(nd, "R2"))


def fmt(s):
    return " ".join(f"{k}={v}" for k, v in zip(ShardPermitsModel.KEYS, s) if k != "acquire_failed")


if __name__ == "__main__":
    run_all(
        [
            ShardPermitsModel(2, [("bump", 2)]),
            ShardPermitsModel(3, [("bump", 2)]),
            ShardPermitsModel(2, [("bump", 2), ("bump", 3)]),
            ShardPermitsModel(2, [("bump", 2), ("block", None)]),
            ShardPermitsModel(2, [("reloc", None)]),
            ShardPermitsModel(2, [("reloc", None), ("block", None)]),
            ShardPermitsModel(3, [("bump", 2), ("bump", 3), ("block", None)]),
        ],
        fmt=fmt,
    )
