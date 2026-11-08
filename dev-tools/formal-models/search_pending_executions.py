# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``AbstractSearchAsyncAction.PendingExecutions`` (per-node throttling of shard requests,
``max_concurrent_shard_requests``).

Shared state: the semaphore permit count, the FIFO queue, and per task status
  U = not yet submitted, Q = queued, E = executing (accepted, not yet released), D = done.
Threads: ('S', t) a caller about to submit task t; ('X', pc, task) a thread inside
submit/executeAndRelease/pollNextTaskOrReleasePermit.  Tasks complete asynchronously: a task
in state E may at any time call its Releasable, which runs onDone.onResponse; if the thread that
started it is still inside ``task.accept`` (pc 'acc') the ``onDone.isDone()`` check sees the
completion and that thread continues the loop, otherwise the completing thread runs the
``onDone.addListener`` continuation.

Program counters:
  s0   : submit: semaphore.tryAcquire()  -> acc(task) or s1
  s1   : queue.add(task)                 -> s2
  s2   : semaphore.tryAcquire()          -> pNext or exit
  acc  : inside task.accept(...), task now executing (E); on return checks onDone.isDone()
         -> pNext (already done) or 'sub' (subscribe continuation and return)
  pNext: queue.poll()                    -> acc(next) or rel
  rel  : semaphore.release()             -> peek
  peek : queue.peek() != null ?          -> acq or exit
  acq  : semaphore.tryAcquire()          -> poll2 or exit
  poll2: queue.poll()                    -> acc(next) or rel2
  rel2 : semaphore.release()             -> peek
"""

from mc import run_all


class PendingExecutionsModel:
    def __init__(self, n_tasks, permits):
        self.n = n_tasks
        self.permits = permits
        self.name = f"SearchPendingExecutions(tasks={n_tasks}, permits={permits})"
        self.invariants = {
            "0 <= semaphore <= permits": lambda s: 0 <= s[0] <= self.permits,
            "at most `permits` tasks executing": lambda s: s[2].count("E") <= self.permits,
            "permits held == executing tasks + threads holding a permit without a task": self.permits_consistent,
        }
        self.quiescence = {
            "every submitted task executed (queue empty, all done)": lambda s: all(st == "D" for st in s[2]) and s[1] == (),
            "all permits returned": lambda s: s[0] == self.permits,
        }

    # state = (sem, queue, status, threads); thread = (kind, pc, task)
    def init(self):
        yield (self.permits, (), ("U",) * self.n, tuple(sorted(("S", "s0", t) for t in range(self.n))))

    HOLDING_PCS = ("pNext", "poll2", "rel", "rel2")  # thread holds a permit but no task is executing on its behalf

    def permits_consistent(self, s):
        sem, queue, status, threads = s
        holders = sum(1 for th in threads if th[1] in self.HOLDING_PCS)
        # a thread still inside task.accept() whose task already completed keeps the permit until it polls/releases
        holders += sum(1 for th in threads if th[1] == "acc" and status[th[2]] == "D")
        # otherwise the permit is owned by the executing task (status E) until it completes
        return self.permits - sem == holders + status.count("E")

    def next(self, s):
        sem, queue, status, threads = s

        def upd(tup, i, v):
            lst = list(tup)
            lst[i] = v
            return tuple(lst)

        def mk(rest, new_th, sem=sem, queue=queue, status=status):
            ts = rest + ((new_th,) if new_th else ())
            return (sem, queue, status, tuple(sorted(ts)))

        for i, th in enumerate(threads):
            rest = threads[:i] + threads[i + 1 :]
            kind, pc, t = th
            if pc == "s0":
                if sem > 0:
                    yield f"submit({t}): tryAcquire ok -> executeAndRelease", mk(rest, ("X", "acc", t), sem=sem - 1, status=upd(status, t, "E"))
                else:
                    yield f"submit({t}): tryAcquire failed", mk(rest, ("X", "s1", t))
            elif pc == "s1":
                yield f"submit({t}): queue.add", mk(rest, ("X", "s2", t), queue=queue + (t,), status=upd(status, t, "Q"))
            elif pc == "s2":
                if sem > 0:
                    yield f"submit({t}): second tryAcquire ok", mk(rest, ("X", "pNext", None), sem=sem - 1)
                else:
                    yield f"submit({t}): second tryAcquire failed, return", mk(rest, None)
            elif pc == "acc":
                # task.accept(...) returned; onDone.isDone()?
                if status[t] == "D":
                    yield f"thread: task {t} completed synchronously, continue loop", mk(rest, ("X", "pNext", None))
                else:
                    yield f"thread: task {t} async, subscribe continuation and return", mk(rest, None)
            elif pc in ("pNext", "poll2"):
                if queue:
                    nt = queue[0]
                    yield f"thread: queue.poll -> {nt}, execute", mk(rest, ("X", "acc", nt), queue=queue[1:], status=upd(status, nt, "E"))
                else:
                    yield "thread: queue.poll -> null", mk(rest, ("X", "rel" if pc == "pNext" else "rel2", None))
            elif pc in ("rel", "rel2"):
                yield "thread: semaphore.release", mk(rest, ("X", "peek", None), sem=sem + 1)
            elif pc == "peek":
                if queue:
                    yield "thread: queue.peek != null", mk(rest, ("X", "acq", None))
                else:
                    yield "thread: queue.peek == null, return", mk(rest, None)
            elif pc == "acq":
                if sem > 0:
                    yield "thread: loop tryAcquire ok", mk(rest, ("X", "poll2", None), sem=sem - 1)
                else:
                    yield "thread: loop tryAcquire failed, return", mk(rest, None)

        # asynchronous task completion: releasable -> onDone.onResponse(null)
        for t, st in enumerate(status):
            if st == "E":
                started_by_thread_at_acc = any(th[1] == "acc" and th[2] == t for th in threads)
                if started_by_thread_at_acc:
                    # the starting thread will observe onDone.isDone() == true when accept() returns
                    yield f"task {t} completes (before accept() returned)", mk(threads, None, status=upd(status, t, "D"))
                else:
                    # the subscribed continuation runs pollNextTaskOrReleasePermit on the completing thread
                    yield f"task {t} completes (continuation runs)", mk(threads, ("X", "pNext", None), status=upd(status, t, "D"))


def fmt(s):
    sem, queue, status, threads = s
    return f"sem={sem} queue={queue} status={''.join(status)} threads={threads}"


if __name__ == "__main__":
    run_all([PendingExecutionsModel(n, p) for n, p in ((2, 1), (3, 1), (3, 2), (4, 1), (4, 2), (5, 2))], fmt=fmt)
