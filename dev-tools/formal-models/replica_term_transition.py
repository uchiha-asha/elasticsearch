# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of the replica side of primary-term transitions: ``IndexShard#innerAcquireReplicaOperationPermit``
+ ``bumpPrimaryTerm`` + ``IndexShardOperationPermits``.

Replication operations arrive at a replica carrying the primary term of the primary that sent them.
An operation whose term is newer than ``pendingPrimaryTerm`` triggers a term bump (block all permits,
then ``setOperationPrimaryTerm``); every operation is checked against ``operationPrimaryTerm`` once it
holds its permit ("operation primary term [..] is too old").  Operations that need all permits
(``acquireAllReplicaOperationsPermits``) can be *combined* with the term bump.

Key property checked ("no stale write after a newer one"): the primary terms of the operations that
actually execute never decrease in execution order, and an operation carrying the highest term in the
system is never rejected as too old.

State fields:
  held, all_held, waiting, queued, delayed   -- as in shard_operation_permits.py
  pending, opterm                            -- pendingPrimaryTerm / operationPrimaryTerm
  max_exec                                   -- ghost: highest term among executed operations so far
  ops      per op: (term, all_permits, status)  status N=not arrived, D=delayed, H=executing (single permit),
           X=executing with all permits (combined/all-permits op), C=done, F=rejected as too old
  blockers tuple of (term_or_None, combined_op_or_None, pc); pc S/W/A/R1/R1b/R2 as in the primary model
  generic  batches of delayed ops being re-acquired on the GENERIC pool
"""

from mc import run_all

KEYS = ("held", "all_held", "waiting", "queued", "delayed", "pending", "opterm", "max_exec", "ops", "blockers", "generic")


class ReplicaTermTransitionModel:
    def __init__(self, ops):
        """ops: list of (term, all_permits)"""
        self.spec = ops
        self.tmax = max(t for t, _ in ops)
        self.name = f"ReplicaTermTransition(ops={ops})"
        inv = {
            "all permits held => no single-permit op executing": lambda d: not d["all_held"] or d["held"] == 0,
            "executing ops carry a term >= every previously executed term": lambda d: all(
                st not in ("H", "X") or t >= d["max_exec"] for (t, _, st) in d["ops"]
            ),
            "an executing op's term is never older than operationPrimaryTerm": lambda d: all(
                st not in ("H", "X") or t >= d["opterm"] for (t, _, st) in d["ops"]
            ),
            "an op carrying the highest term is never rejected as too old": lambda d: all(
                not (st == "F" and t == self.tmax) for (t, _, st) in d["ops"]
            ),
            "operationPrimaryTerm <= pendingPrimaryTerm": lambda d: d["opterm"] <= d["pending"],
            "at most one op executes with all permits": lambda d: sum(1 for (_, _, st) in d["ops"] if st == "X") <= 1,
        }
        qui = {
            "all ops finished": lambda d: all(st in ("C", "F") for (_, _, st) in d["ops"]),
            "permits released, nothing delayed, blockers done": lambda d: not d["all_held"]
            and d["queued"] == 0
            and d["delayed"] == ()
            and all(pc == "R2" for (_, _, pc) in d["blockers"]),
            "operationPrimaryTerm caught up with the highest term seen": lambda d: d["opterm"] == d["pending"] == self.tmax,
        }
        self.invariants = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in inv.items()}
        self.quiescence = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in qui.items()}

    def freeze(self, d):
        return tuple(d[k] for k in KEYS)

    def thaw(self, s):
        return dict(zip(KEYS, s))

    def init(self):
        yield self.freeze(
            dict(held=0, all_held=False, waiting=(), queued=0, delayed=(), pending=1, opterm=1, max_exec=0,
                 ops=tuple((t, a, "N") for t, a in self.spec), blockers=(), generic=())
        )

    def set_op(self, d, o, st):
        ops = list(d["ops"])
        t, a, _ = ops[o]
        ops[o] = (t, a, st)
        d["ops"] = tuple(ops)

    def execute(self, d, o, st):
        """operationListener: reject if the op's term is older than operationPrimaryTerm, else execute."""
        t, a, _ = d["ops"][o]
        if t < d["opterm"]:
            self.set_op(d, o, "F")
            return False
        self.set_op(d, o, st)
        d["max_exec"] = max(d["max_exec"], t)
        return True

    def inner_acquire(self, d, o):
        if d["queued"] > 0:
            d["delayed"] = d["delayed"] + (o,)
            self.set_op(d, o, "D")
        else:
            assert not d["all_held"] and not d["waiting"], "fair semaphore invariant (checked by the primary model)"
            d["held"] += 1
            if not self.execute(d, o, "H"):
                d["held"] -= 1

    def bump(self, d, term, combined):
        """bumpPrimaryTerm / asyncBlockOperations under IndexShard.mutex: delayOperations(); schedule block; pendingPrimaryTerm = term."""
        d["queued"] += 1
        d["blockers"] = d["blockers"] + ((term, combined, "S"),)
        if term is not None:
            d["pending"] = term

    def next(self, s):
        d0 = self.thaw(s)
        for o, (t, allp, st) in enumerate(d0["ops"]):
            if st == "N":
                d = dict(d0)
                # requirePrimaryTermUpdate(opPrimaryTerm, allPermits)
                require = t > d["pending"] or (allp and t > d["opterm"])
                if require:
                    if allp:
                        self.bump(d, t, o)  # combined with the operation
                        self.set_op(d, o, "B")
                        yield f"op{o}(term {t}, all): bumpPrimaryTerm combined with op", self.freeze(d)
                    else:
                        self.bump(d, t, None)
                        self.set_op(d, o, "A")  # still has to call indexShardOperationPermits.acquire
                        yield f"op{o}(term {t}): bumpPrimaryTerm({t}) then acquire", self.freeze(d)
                else:
                    if allp:
                        self.bump(d, None, o)  # asyncBlockOperations without term update
                        self.set_op(d, o, "B")
                        yield f"op{o}(term {t}, all): asyncBlockOperations", self.freeze(d)
                    else:
                        self.inner_acquire(d, o)
                        yield f"op{o}(term {t}): acquire permit", self.freeze(d)
            elif st == "A":
                d = dict(d0)
                self.inner_acquire(d, o)
                yield f"op{o}(term {t}): acquire permit after bump", self.freeze(d)
            elif st == "H":
                d = dict(d0)
                d["held"] -= 1
                self.set_op(d, o, "C")
                yield f"op{o}(term {t}): release permit", self.freeze(d)
        for gi, batch in enumerate(d0["generic"]):
            d = dict(d0)
            o = batch[0]
            gens = list(d["generic"])
            if batch[1:]:
                gens[gi] = batch[1:]
            else:
                del gens[gi]
            d["generic"] = tuple(gens)
            self.inner_acquire(d, o)
            yield f"generic: re-acquire op{o}", self.freeze(d)
        for b, (term, combined, pc) in enumerate(d0["blockers"]):
            def setb(d, newpc):
                bl = list(d["blockers"])
                bl[b] = (term, combined, newpc)
                d["blockers"] = tuple(bl)
                return d

            if pc == "S":
                d = dict(d0)
                if d["held"] == 0 and not d["all_held"] and not d["waiting"]:
                    d["all_held"] = True
                    yield f"blocker{b}: acquireAll succeeds immediately", self.freeze(setb(d, "A"))
                else:
                    d["waiting"] = d["waiting"] + (b,)
                    yield f"blocker{b}: parks in semaphore", self.freeze(setb(d, "W"))
            elif pc == "W":
                if d0["held"] == 0 and not d0["all_held"] and d0["waiting"][0] == b:
                    d = dict(d0)
                    d["waiting"] = d["waiting"][1:]
                    d["all_held"] = True
                    yield f"blocker{b}: acquireAll completes after drain", self.freeze(setb(d, "A"))
            elif pc == "A":
                d = dict(d0)
                if term is not None and d["opterm"] < term:
                    d["opterm"] = term  # setOperationPrimaryTerm + onBlocked (rollback / roll translog)
                if combined is not None:
                    # combineWithAction.onResponse(releasable): operationListener runs with all permits
                    if self.execute(d, combined, "X"):
                        yield f"blocker{b}: term callback, combined op{combined} executes", self.freeze(setb(d, "X"))
                        continue
                yield f"blocker{b}: term callback (opterm={d['opterm']})", self.freeze(setb(d, "R1"))
            elif pc == "X":
                d = dict(d0)
                self.set_op(d, combined, "C")
                yield f"blocker{b}: combined op{combined} done, release", self.freeze(setb(d, "R1"))
            elif pc == "R1":
                d = dict(d0)
                d["all_held"] = False
                yield f"blocker{b}: semaphore.release(TOTAL_PERMITS)", self.freeze(setb(d, "R1b"))
            elif pc == "R1b":
                d = dict(d0)
                d["queued"] -= 1
                if d["queued"] == 0 and d["delayed"]:
                    d["generic"] = d["generic"] + (d["delayed"],)
                    d["delayed"] = ()
                yield f"blocker{b}: releaseDelayedOperations", self.freeze(setb(d, "R2"))


def fmt(s):
    d = dict(zip(KEYS, s))
    return " ".join(f"{k}={v}" for k, v in d.items())


if __name__ == "__main__":
    run_all(
        [
            ReplicaTermTransitionModel([(1, False), (2, False)]),
            ReplicaTermTransitionModel([(2, False), (1, False), (2, False)]),
            ReplicaTermTransitionModel([(1, False), (2, False), (3, False)]),
            ReplicaTermTransitionModel([(3, False), (2, False), (1, False)]),
            ReplicaTermTransitionModel([(1, False), (2, True)]),
            ReplicaTermTransitionModel([(2, True), (1, False), (2, False)]),
            ReplicaTermTransitionModel([(1, True), (2, True), (2, False)]),
            ReplicaTermTransitionModel([(1, False), (2, False), (3, True), (2, False)]),
            ReplicaTermTransitionModel([(2, False), (3, True), (3, False), (1, False)]),
        ],
        fmt=fmt,
    )
