# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``org.elasticsearch.common.util.concurrent.ThrottledIterator``.

Shared state:
  nxt        -- iterator position (items 0..N-1 are handed out in order)
  permits    -- the ``permits`` AtomicInteger (starts at maxConcurrency)
  refs       -- the ``refs`` AbstractRefCounted (starts at 1 for the incomplete iterator; one more
                per in-flight item; one more per forked continuation); ``onCompletion`` runs when
                it drops to 0
  status     -- per item: U=not handed out, F=in flight (handed to itemConsumer, not released),
                D=released
  done       -- number of ``onCompletion`` invocations
  failures   -- number of ``onContinuationFailure`` invocations (executor rejections)
  pending    -- number of forked continuations accepted by the executor but not yet started
  threads    -- multiset of threads: ('run', pc, after) a thread inside ``run()``; after tells
                what the thread does when run() returns: 'close' (the initial caller, calls
                close() -> refs.decRef) or 'decref' (the finally block of onItemRelease, or the
                onAfter of a forked continuation)
Program counters in run():
  r0 : iterator.hasNext() ? next(), refs.mustIncRef(), itemConsumer.accept() : return
  r1 : permits.decrementAndGet() > 0 ? loop : return
An in-flight item may be released at any time (onItemRelease): permits.getAndIncrement() == 0
decides whether this thread continues with run() (DIRECT executor) or forks it; the item's ref is
dropped in the finally block afterwards.
"""

from mc import run_all


class ThrottledIteratorModel:
    def __init__(self, n_items, max_concurrency, direct, allow_reject):
        self.n = n_items
        self.k = max_concurrency
        self.direct = direct
        self.reject = allow_reject and not direct
        self.name = f"ThrottledIterator(items={n_items}, maxConcurrency={max_concurrency}, direct={direct}, reject={self.reject})"
        self.invariants = {
            "at most maxConcurrency items in flight": lambda s: s[3].count("F") <= self.k,
            "refcount >= 0": lambda s: s[2] >= 0,
            "only one thread inside run() at a time": lambda s: sum(1 for t in s[7] if t[0] == "run") <= 1,
            "onCompletion not called while items in flight or iterator incomplete (unless a continuation was rejected)": lambda s: s[4] == 0
            or s[5] > 0
            or (s[3].count("F") == 0 and s[1] == self.n),
            "onCompletion at most once": lambda s: s[4] <= 1,
            "refcount accounts for iterator + in-flight items + continuations": self.refs_consistent,
        }
        self.quiescence = {
            "every item processed exactly once (unless a continuation was rejected)": lambda s: s[5] > 0
            or all(st == "D" for st in s[3]),
            "onCompletion called exactly once, refcount 0": lambda s: s[4] == 1 and s[2] == 0,
        }

    # state = (closed, nxt, refs, status, done, failures, pending, threads, permits); thread = (kind, pc, after, origin)
    def init(self):
        yield (False, 0, 1, ("U",) * self.n, 0, 0, 0, (("run", "r0", "close", "initial"),), self.k)

    def refs_consistent(self, s):
        closed, nxt, refs, status, done, failures, pending, threads, permits = s
        expected = (0 if closed else 1) + status.count("F") + pending
        # a forked continuation holds its own ref until onAfter; a DIRECT continuation still holds the released
        # item's ref until the finally block of onItemRelease runs after run() returns
        expected += sum(1 for t in threads if t[3] in ("forked", "direct"))
        return refs == expected

    def next(self, s):
        closed, nxt, refs, status, done, failures, pending, threads, permits = s

        def upd(tup, i, v):
            lst = list(tup)
            lst[i] = v
            return tuple(lst)

        def mk(ths, closed=closed, nxt=nxt, refs=refs, status=status, done=done, failures=failures, pending=pending, permits=permits):
            return (closed, nxt, refs, status, done, failures, pending, tuple(sorted(ths)), permits)

        def decref(refs, done):
            refs -= 1
            if refs == 0:
                done += 1
            return refs, done

        for i, th in enumerate(threads):
            rest = threads[:i] + threads[i + 1 :]
            kind, pc, after, origin = th
            if kind == "run":
                if pc == "r0":
                    if nxt < self.n:
                        # mustIncRef() throws if refs == 0 -- modelled as a refcount going negative-ish: flag via invariant
                        yield f"run: next() -> item {nxt}, mustIncRef, itemConsumer.accept", mk(
                            rest + (("run", "r1", after, origin),), nxt=nxt + 1, refs=refs + 1, status=upd(status, nxt, "F")
                        )
                    else:
                        yield "run: iterator exhausted, return", mk(rest + (("after", None, after, origin),))
                elif pc == "r1":
                    if permits - 1 > 0:
                        yield f"run: permits.decrementAndGet() -> {permits - 1}, loop", mk(rest + (("run", "r0", after, origin),), permits=permits - 1)
                    else:
                        yield f"run: permits.decrementAndGet() -> {permits - 1}, return", mk(
                            rest + (("after", None, after, origin),), permits=permits - 1
                        )
            elif kind == "after":
                if after == "close":
                    r, d = decref(refs, done)
                    yield "close(): refs.decRef()", mk(rest, closed=True, refs=r, done=d)
                else:
                    r, d = decref(refs, done)
                    yield f"continuation done ({origin}): refs.decRef()", mk(rest, refs=r, done=d)

        # forked continuation starts on the executor
        if pending > 0:
            yield "executor runs forked continuation", mk(threads + (("run", "r0", "decref", "forked"),), pending=pending - 1)

        # item release
        for item, st in enumerate(status):
            if st == "F":
                ns = upd(status, item, "D")
                if permits == 0:
                    if self.direct:
                        # run() inline on the releasing thread, then finally refs.decRef() (the item's ref)
                        yield f"item {item} released: permits 0->1, run() inline", mk(
                            threads + (("run", "r0", "decref", "direct"),), status=ns, permits=1
                        )
                    else:
                        # refs.mustIncRef(); executor.execute(continuation); finally refs.decRef() (the item's ref)
                        r, d = decref(refs + 1, done)
                        yield f"item {item} released: permits 0->1, fork continuation", mk(
                            threads, status=ns, permits=1, refs=r, done=d, pending=pending + 1
                        )
                        if self.reject:
                            # rejected: onRejection -> onContinuationFailure; onAfter -> refs.decRef; then the item's ref
                            r2, d2 = decref(refs + 1, done)
                            r2, d2 = decref(r2, d2)
                            yield f"item {item} released: permits 0->1, continuation REJECTED", mk(
                                threads, status=ns, permits=1, refs=r2, done=d2, failures=failures + 1
                            )
                else:
                    r, d = decref(refs, done)
                    yield f"item {item} released: permits {permits}->{permits + 1}, refs.decRef()", mk(
                        threads, status=ns, permits=permits + 1, refs=r, done=d
                    )


def fmt(s):
    closed, nxt, refs, status, done, failures, pending, threads, permits = s
    return (
        f"closed={closed} next={nxt} permits={permits} refs={refs} status={''.join(status)} done={done} "
        f"failures={failures} pending={pending} threads={threads}"
    )


if __name__ == "__main__":
    models = []
    for n, k in ((2, 1), (3, 1), (3, 2), (4, 2), (5, 2), (5, 3)):
        for direct in (True, False):
            for rej in (False, True):
                if direct and rej:
                    continue
                models.append(ThrottledIteratorModel(n, k, direct, rej))
    run_all(models, fmt=fmt)
