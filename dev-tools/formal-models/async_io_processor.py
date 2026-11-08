# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``org.elasticsearch.common.util.concurrent.AsyncIOProcessor#put``.

N threads each ``put`` one item into a processor whose queue has capacity Q.  The semaphore
("promise") handoff and the queue are shared; everything else is thread local.

Thread program counters:
  start     : promised = promiseSemaphore.tryAcquire()
  put       : queue.put(item)  (blocks while the queue is full)
  retry     : if (promised || promiseSemaphore.tryAcquire())  -- promised is false here
  drain     : queue.drainTo(candidates); write(candidates)   (semaphore held)
  release   : promiseSemaphore.release(); notifyList(candidates)
  loopcheck : queue.isEmpty() == false ?
  loopacq   : promiseSemaphore.tryAcquire() ?
"""

from mc import run_all

START, PUT, RETRY, DRAIN, RELEASE, LOOPCHECK, LOOPACQ = "start", "put", "retry", "drain", "release", "loopcheck", "loopacq"
HOLDING = (DRAIN, RELEASE)


class AsyncIOProcessorModel:
    def __init__(self, n_items, capacity):
        self.n = n_items
        self.cap = capacity
        self.name = f"AsyncIOProcessor(items={n_items}, queueSize={capacity})"
        self.invariants = {
            "semaphore held iff exactly one thread is draining/writing": self.sem_consistent,
            "queue never exceeds capacity": lambda s: len(s[1]) <= self.cap,
            "no item processed twice": lambda s: all(c <= 1 for c in s[2]),
        }
        self.quiescence = {
            "every item processed and queue empty": lambda s: all(c == 1 for c in s[2]) and s[1] == (),
            "semaphore released": lambda s: s[0] is True,
        }

    # state = (sem_available, queue, processed_counts, threads)
    # thread = (pc, item, promised, candidates)
    def init(self):
        yield (True, (), (0,) * self.n, tuple(sorted((START, i, False, ()) for i in range(self.n))))

    @staticmethod
    def sem_consistent(s):
        sem, queue, processed, threads = s
        holders = sum(1 for th in threads if th[0] in HOLDING)
        return holders <= 1 and (not sem) == (holders == 1)

    def next(self, s):
        sem, queue, processed, threads = s
        for i, th in enumerate(threads):
            rest = threads[:i] + threads[i + 1 :]
            pc, item, promised, cands = th

            def st(new_th, sem=sem, queue=queue, processed=processed):
                ts = rest + ((new_th,) if new_th is not None else ())
                return (sem, queue, processed, tuple(sorted(ts)))

            if pc == START:
                if sem:
                    yield f"t{item}: tryAcquire -> promised", st((DRAIN, item, True, (item,)), sem=False)
                else:
                    yield f"t{item}: tryAcquire -> not promised", st((PUT, item, False, ()))
            elif pc == PUT:
                if len(queue) < self.cap:
                    yield f"t{item}: queue.put({item})", st((RETRY, item, False, ()), queue=queue + (item,))
            elif pc == RETRY:
                if sem:
                    yield f"t{item}: second tryAcquire -> true", st((DRAIN, item, False, ()), sem=False)
                else:
                    yield f"t{item}: second tryAcquire -> false, return", st(None)
            elif pc == DRAIN:
                yield f"t{item}: drainTo + write {cands + queue}", st((RELEASE, item, promised, cands + queue), queue=())
            elif pc == RELEASE:
                newp = list(processed)
                for c in cands:
                    newp[c] += 1
                yield f"t{item}: release + notify {cands}", st((LOOPCHECK, item, promised, ()), sem=True, processed=tuple(newp))
            elif pc == LOOPCHECK:
                if queue:
                    yield f"t{item}: queue not empty", st((LOOPACQ, item, promised, ()))
                else:
                    yield f"t{item}: queue empty, return", st(None)
            elif pc == LOOPACQ:
                if sem:
                    yield f"t{item}: loop tryAcquire -> true", st((DRAIN, item, promised, ()), sem=False)
                else:
                    yield f"t{item}: loop tryAcquire -> false, return", st(None)


def fmt(s):
    sem, queue, processed, threads = s
    return f"sem={'free' if sem else 'HELD'} queue={queue} processed={processed} threads={threads}"


if __name__ == "__main__":
    run_all(
        [AsyncIOProcessorModel(n, q) for n, q in ((2, 1), (3, 1), (3, 2), (4, 1), (4, 2), (5, 2))],
        fmt=fmt,
    )
