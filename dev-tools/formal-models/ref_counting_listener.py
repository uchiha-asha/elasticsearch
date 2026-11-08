# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``org.elasticsearch.action.support.RefCountingListener`` on top of ``RefCountingRunnable``
and ``AbstractRefCounted``.

The owner thread acquires N child listeners (``acquire()`` -> ``refs.acquire()`` -> ``mustIncRef``),
hands them to N worker threads, and then calls ``close()`` (``refs.close()`` -> ``decRef``).  Each
worker eventually calls either ``onResponse`` (``ref.close()`` -> ``decRef``) or ``onFailure``
(``addException`` = ``exceptionPermits.tryAcquire()`` then ``exceptionRef.compareAndExchange``,
then ``decRef``).  ``decRef`` is ``getAndAdd(-1)``; the thread that observes 1 runs ``finish()``
which completes the delegate with the exception recorded in ``exceptionRef`` (or success).

Shared state:
  refcount, closed, status per worker (U=not yet handed out, A=holding ref idle, P=took exception
  permit decision, C=exception recorded, R=released after response, F=released after failure),
  exception_permits, exception_ref (worker id of the first recorded exception or None), dropped,
  delegate ('success' | 'failure' | None), delegate_calls, acquire_failed (mustIncRef threw).
"""

from mc import run_all


class RefCountingListenerModel:
    def __init__(self, n_workers, max_exceptions):
        self.n = n_workers
        self.max_exc = max_exceptions
        self.name = f"RefCountingListener(children={n_workers}, maxExceptions={max_exceptions})"
        self.invariants = {
            "acquire() never hits a closed refcount (mustIncRef would throw)": lambda s: not s["acquire_failed"],
            "refcount >= 0": lambda s: s["refcount"] >= 0,
            "delegate completed at most once": lambda s: s["delegate_calls"] <= 1,
            "delegate completed only after close() and after every child released its ref": lambda s: s["delegate"] is None
            or (s["closed"] and all(st in ("R", "F") for st in s["status"])),
            "delegate fails iff some child failed": lambda s: s["delegate"] is None
            or (s["delegate"] == "failure") == any(st == "F" for st in s["status"]),
            "refcount == owner ref + held child refs": lambda s: s["refcount"]
            == (0 if s["closed"] else 1) + sum(1 for st in s["status"] if st in ("A", "P", "C")),
            "exceptions recorded + dropped == failures that passed addException": lambda s: s["dropped"]
            + (self.max_exc - s["exception_permits"])
            == sum(1 for st in s["status"] if st in ("P", "C", "F")),
        }
        self.quiescence = {
            "delegate completed exactly once": lambda s: s["delegate_calls"] == 1,
        }
        self.invariants = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in self.invariants.items()}
        self.quiescence = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in self.quiescence.items()}

    KEYS = ("refcount", "closed", "status", "exception_permits", "exception_ref", "dropped", "delegate", "delegate_calls", "acquire_failed", "owner_pc")

    def freeze(self, d):
        return tuple(d[k] for k in self.KEYS)

    def thaw(self, t):
        return dict(zip(self.KEYS, t))

    def init(self):
        yield self.freeze(
            dict(
                refcount=1,
                closed=False,
                status=("U",) * self.n,
                exception_permits=self.max_exc,
                exception_ref=None,
                dropped=0,
                delegate=None,
                delegate_calls=0,
                acquire_failed=False,
                owner_pc=0,
            )
        )

    def dec_ref(self, d):
        d = dict(d)
        was = d["refcount"]
        d["refcount"] = was - 1
        if was == 1:
            d["delegate_calls"] += 1
            d["delegate"] = "failure" if d["exception_ref"] is not None else "success"
        return d

    def next(self, s):
        d = self.thaw(s)
        status = d["status"]

        def upd(i, v):
            lst = list(status)
            lst[i] = v
            return tuple(lst)

        # owner: acquire children in order, then close()
        pc = d["owner_pc"]
        if pc < self.n:
            nd = dict(d)
            if nd["refcount"] > 0:
                nd["refcount"] += 1
            else:
                nd["acquire_failed"] = True
            nd["status"] = upd(pc, "A")
            nd["owner_pc"] = pc + 1
            yield f"owner: acquire() child {pc}", self.freeze(nd)
        elif pc == self.n:
            nd = self.dec_ref(d)
            nd["closed"] = True
            nd["owner_pc"] = pc + 1
            yield "owner: close()", self.freeze(nd)

        for i, st in enumerate(status):
            if st == "A":
                nd = self.dec_ref(d)
                nd["status"] = upd(i, "R")
                yield f"child {i}: onResponse -> ref.close()", self.freeze(nd)
                # onFailure: addException step 1: exceptionPermits.tryAcquire()
                nd = dict(d)
                if nd["exception_permits"] > 0:
                    nd["exception_permits"] -= 1
                    nd["status"] = upd(i, "P")
                    yield f"child {i}: onFailure -> exception permit acquired", self.freeze(nd)
                else:
                    nd["dropped"] += 1
                    nd["status"] = upd(i, "C")
                    yield f"child {i}: onFailure -> exception dropped", self.freeze(nd)
            elif st == "P":
                nd = dict(d)
                if nd["exception_ref"] is None:
                    nd["exception_ref"] = i
                nd["status"] = upd(i, "C")
                yield f"child {i}: exceptionRef.compareAndExchange", self.freeze(nd)
            elif st == "C":
                nd = self.dec_ref(d)
                nd["status"] = upd(i, "F")
                yield f"child {i}: ref.close() after failure", self.freeze(nd)


def fmt(s):
    return " ".join(f"{k}={v}" for k, v in zip(RefCountingListenerModel.KEYS, s))


if __name__ == "__main__":
    run_all([RefCountingListenerModel(n, m) for n, m in ((1, 1), (2, 1), (3, 1), (3, 2), (4, 2), (5, 3))], fmt=fmt)
