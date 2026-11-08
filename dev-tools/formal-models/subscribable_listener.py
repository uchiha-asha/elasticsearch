# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``org.elasticsearch.action.support.SubscribableListener`` (lock-free ``addListener`` /
``setResult``).

``state`` is modelled by content: EMPTY, ('L', id) for a single subscriber, ('CELL', (ids...)) for
the reverse-ordered subscriber chain (head first), or ('DONE', result).  Content equality is a
faithful stand-in for the reference equality used by compareAndExchange because the chain only
ever grows until the terminal DONE state, so no content is ever reachable twice (no ABA).

Each compareAndExchange and each plain volatile read is one atomic step; everything in between is
thread local.
"""

from mc import run_all

EMPTY = "EMPTY"


class SubscribableListenerModel:
    def __init__(self, n_adders, n_completers):
        self.na = n_adders
        self.nc = n_completers
        self.name = f"SubscribableListener(adders={n_adders}, completers={n_completers})"
        self.invariants = {
            "no listener completed twice": lambda s: all(c <= 1 for c in s[1]),
            "a listener is only ever completed with the winning result": lambda s: all(
                r is None or (s[0][0] == "DONE" and r == s[0][1]) for r in s[2]
            ),
        }
        self.quiescence = {
            "listener completed exactly once": lambda s: all(c == 1 for c in s[1]),
            "listener is done": lambda s: s[0][0] == "DONE",
            "published subscribers completed in subscription order": lambda s: tuple(
                l for l in s[4] if l in s[3]
            )
            == s[3],
        }

    # state = (state, completed_counts, result_seen, pub_order, completion_order, threads)
    # adder thread   = ('A', id, pc, cur)       pc in {a0, a1, a2}
    # completer      = ('C', result, pc, cur)   pc in {c0, c1}
    def init(self):
        threads = [("A", i, "a0", None) for i in range(self.na)] + [("C", 100 + j, "c0", None) for j in range(self.nc)]
        yield (EMPTY, (0,) * self.na, (None,) * self.na, (), (), tuple(sorted(threads, key=repr)))

    @staticmethod
    def complete(counts, seen, order, ids, result):
        counts = list(counts)
        seen = list(seen)
        for l in ids:
            counts[l] += 1
            seen[l] = result
            order = order + (l,)
        return tuple(counts), tuple(seen), order

    def next(self, s):
        state, counts, seen, pub, order, threads = s
        for i, th in enumerate(threads):
            rest = threads[:i] + threads[i + 1 :]
            kind, me, pc, cur = th

            def mk(new_th, state=state, counts=counts, seen=seen, pub=pub, order=order):
                ts = rest + ((new_th,) if new_th else ())
                return (state, counts, seen, pub, order, tuple(sorted(ts, key=repr)))

            if kind == "A":
                if pc == "a0":
                    # if (tryComplete(state, listener)) return;
                    if state[0] == "DONE":
                        c, se, o = self.complete(counts, seen, order, (me,), state[1])
                        yield f"add{me}: already done, complete inline", mk(None, counts=c, seen=se, order=o)
                    else:
                        yield f"add{me}: not done", mk(("A", me, "a1", None))
                elif pc == "a1":
                    # currentValue = compareAndExchangeState(EMPTY, wrappedListener)
                    if state == EMPTY:
                        yield f"add{me}: CAS EMPTY -> L{me}", mk(None, state=("L", me), pub=pub + (me,))
                    else:
                        yield f"add{me}: CAS EMPTY failed, witness={state}", mk(("A", me, "a2", state))
                elif pc == "a2":
                    if cur[0] == "DONE":
                        c, se, o = self.complete(counts, seen, order, (me,), cur[1])
                        yield f"add{me}: witness done, complete inline", mk(None, counts=c, seen=se, order=o)
                    elif cur[0] == "L":
                        first = cur[1]
                        tail = ("CELL", (first,))
                        if state == cur:
                            yield f"add{me}: CAS L{first} -> Cell", mk(("A", me, "a2", tail), state=tail)
                        else:
                            yield f"add{me}: CAS L{first} failed", mk(("A", me, "a2", state))
                    elif cur[0] == "CELL":
                        new_cell = ("CELL", (me,) + cur[1])
                        if state == cur:
                            yield f"add{me}: CAS push Cell{me}", mk(None, state=new_cell, pub=pub + (me,))
                        else:
                            yield f"add{me}: CAS push failed", mk(("A", me, "a2", state))
            else:  # completer
                if pc == "c0":
                    yield f"complete{me}: read state", mk(("C", me, "c1", state))
                elif pc == "c1":
                    if cur[0] == "DONE":
                        yield f"complete{me}: already done", mk(None)
                    elif state == cur:
                        done = ("DONE", me)
                        if cur == EMPTY:
                            ids = ()
                        elif cur[0] == "L":
                            ids = (cur[1],)
                        else:
                            ids = tuple(reversed(cur[1]))  # reverse back into subscription order
                        c, se, o = self.complete(counts, seen, order, ids, me)
                        yield f"complete{me}: CAS -> DONE, complete {ids}", mk(None, state=done, counts=c, seen=se, order=o)
                    else:
                        yield f"complete{me}: CAS failed, retry", mk(("C", me, "c1", state))


def fmt(s):
    state, counts, seen, pub, order, threads = s
    return f"state={state} completed={counts} pub={pub} order={order} threads={threads}"


if __name__ == "__main__":
    run_all([SubscribableListenerModel(a, c) for a, c in ((1, 1), (2, 1), (3, 1), (2, 2), (3, 2), (4, 2))], fmt=fmt)
