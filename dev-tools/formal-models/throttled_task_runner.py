# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``org.elasticsearch.common.util.concurrent.AbstractThrottledTaskRunner`` (``pollAndSpawn``).

Every line of ``pollAndSpawn`` that touches shared state is a separate atomic step so that all
interleavings between concurrent ``enqueueTask`` callers, executor worker threads and task
completions (``releasable.close()``) are explored.

State:
  running  -- the ``runningTasks`` AtomicInteger
  queue    -- the FIFO task queue
  status   -- per task: U=not enqueued, Q=queued, D=dequeued (before executor.execute decision),
              P=handed to executor but not started, R=onResponse called and releasable not yet closed,
              C=completed (releasable closed), X=rejected by the executor
  clp      -- per task: the ``callerLoopProceeded`` flag of its AbstractRunnable
  threads  -- multiset of threads: ('E', t) a caller about to call enqueueTask(t);
              ('P', pc, t) a thread inside pollAndSpawn at program counter pc.

Program counters of pollAndSpawn:
  inc  : incrementRunningTasks() (CAS loop, atomic)         -> poll, or exit the loop
  poll : tasks.poll()                                        -> dec (empty) or exec (got task)
  dec  : runningTasks.decrementAndGet()                      -> peek
  peek : tasks.peek() == null ? break : continue             -> exit or inc
  exec : executor.execute(runnable) (accepted or rejected)   -> flag (accepted) or inc (rejected)
  flag : runnable.callerLoopProceeded = true                 -> inc
"""

import itertools
import sys

from mc import run_all

INC, POLL, DEC, PEEK, EXEC, FLAG = "inc", "poll", "dec", "peek", "exec", "flag"


class ThrottledTaskRunnerModel:
    def __init__(self, n_tasks, max_running, allow_reject, direct_executor):
        self.n = n_tasks
        self.max = max_running
        self.allow_reject = allow_reject
        self.direct = direct_executor
        self.name = (
            f"ThrottledTaskRunner(tasks={n_tasks}, max={max_running}, reject={allow_reject}, "
            f"direct={direct_executor})"
        )
        self.invariants = {
            "0 <= runningTasks <= maxRunningTasks": lambda s: 0 <= s[0] <= self.max,
            "runningTasks == slots actually held": self.slots_consistent,
            "at most maxRunningTasks tasks executing": lambda s: s[2].count("R") <= self.max,
        }
        self.quiescence = {
            "no task left behind (queue empty, every task completed or rejected)": lambda s: all(
                st in ("C", "X") for st in s[2]
            )
            and s[1] == (),
            "runningTasks returns to 0": lambda s: s[0] == 0,
        }

    # state = (running, queue, status, clp, threads)
    def init(self):
        yield (0, (), ("U",) * self.n, (False,) * self.n, tuple(sorted(("E", t, None) for t in range(self.n))))

    @staticmethod
    def slots_consistent(s):
        running, queue, status, clp, threads = s
        slots = sum(1 for th in threads if th[0] == "P" and th[1] in (POLL, DEC))
        slots += sum(1 for st in status if st in ("D", "P", "R"))
        return running == slots

    def next(self, s):
        running, queue, status, clp, threads = s

        def with_threads(new_threads, running=running, queue=queue, status=status, clp=clp):
            return (running, queue, status, clp, tuple(sorted(new_threads)))

        def upd(tup, i, v):
            lst = list(tup)
            lst[i] = v
            return tuple(lst)

        # thread steps
        for i, th in enumerate(threads):
            rest = threads[:i] + threads[i + 1 :]
            kind, a, b = th
            if kind == "E":
                t = a
                # tasks.add(task); pollAndSpawn()
                yield f"enqueueTask({t})", with_threads(rest + (("P", INC, None),), queue=queue + (t,), status=upd(status, t, "Q"))
            elif kind == "P":
                pc, t = a, b
                if pc == INC:
                    if running < self.max:
                        yield "incrementRunningTasks -> true", with_threads(rest + (("P", POLL, None),), running=running + 1)
                    else:
                        yield "incrementRunningTasks -> false (exit loop)", with_threads(rest)
                elif pc == POLL:
                    if queue:
                        t2 = queue[0]
                        yield f"tasks.poll() -> {t2}", with_threads(
                            rest + (("P", EXEC, t2),), queue=queue[1:], status=upd(status, t2, "D")
                        )
                    else:
                        yield "tasks.poll() -> null", with_threads(rest + (("P", DEC, None),))
                elif pc == DEC:
                    yield "runningTasks.decrementAndGet()", with_threads(rest + (("P", PEEK, None),), running=running - 1)
                elif pc == PEEK:
                    if queue:
                        yield "tasks.peek() != null (continue loop)", with_threads(rest + (("P", INC, None),))
                    else:
                        yield "tasks.peek() == null (break)", with_threads(rest)
                elif pc == EXEC:
                    if self.direct:
                        # DIRECT executor: doRun() -> task.onResponse(releasable) happens inside execute()
                        yield f"executor.execute({t}) runs task inline", with_threads(
                            rest + (("P", FLAG, t),), status=upd(status, t, "R")
                        )
                    else:
                        yield f"executor.execute({t}) accepted", with_threads(rest + (("P", FLAG, t),), status=upd(status, t, "P"))
                    if self.allow_reject:
                        # onRejection runs on the calling thread: rejected = true; task.onFailure(e); releasable.close()
                        # -> decrementAndGet, and no pollAndSpawn because rejected == true
                        yield f"executor.execute({t}) rejected", with_threads(
                            rest + (("P", INC, None),), running=running - 1, status=upd(status, t, "X")
                        )
                elif pc == FLAG:
                    yield f"callerLoopProceeded({t}) = true", with_threads(rest + (("P", INC, None),), clp=upd(clp, t, True))

        # executor worker starts a pending runnable
        for t, st in enumerate(status):
            if st == "P":
                yield f"worker runs task {t} (onResponse)", with_threads(threads, status=upd(status, t, "R"))
            if st == "R":
                # task completes: releasable.close() -> decrementAndGet(); if (!rejected && callerLoopProceeded) pollAndSpawn()
                new_threads = threads + ((("P", INC, None),) if clp[t] else ())
                yield f"task {t} closes releasable", with_threads(new_threads, running=running - 1, status=upd(status, t, "C"))


def fmt(s):
    running, queue, status, clp, threads = s
    return f"running={running} queue={queue} status={''.join(status)} clp={''.join('1' if c else '0' for c in clp)} threads={threads}"


if __name__ == "__main__":
    models = []
    for n, mx in ((2, 1), (3, 1), (3, 2), (4, 1), (4, 2)):
        for reject in (False, True):
            for direct in (False, True):
                models.append(ThrottledTaskRunnerModel(n, mx, reject, direct))
    run_all(models, fmt=fmt)
