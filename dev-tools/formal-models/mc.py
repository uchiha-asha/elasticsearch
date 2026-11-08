# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Minimal explicit-state model checker used by the models in this directory.

A model is a Python object with:
  * ``name``        -- a short string
  * ``init()``      -- returns an iterable of initial states (hashable, immutable values)
  * ``next(s)``     -- yields ``(label, s2)`` pairs for every enabled transition of ``s``
  * ``invariants``  -- ``{name: predicate}`` checked on every reachable state
  * ``quiescence``  -- ``{name: predicate}`` checked only on terminal states (states with no
                       enabled transition).  This is how "nothing is left stuck forever" style
                       liveness properties are expressed for the finite systems modelled here:
                       in a terminal state nothing can ever happen again, so e.g. a task that
                       is still queued there is lost for good.
  * ``max_states``  -- optional: stop after exploring this many states (a bounded, incomplete check)

The checker does a breadth-first search so that the first violation found has a shortest trace.
"""

import sys
import time
from collections import deque


class Violation(Exception):
    def __init__(self, kind, prop, trace):
        super().__init__(f"{kind} violated: {prop}")
        self.kind = kind
        self.prop = prop
        self.trace = trace


def _trace(parents, state):
    out = []
    while state is not None:
        parent, label = parents[state]
        out.append((label, state))
        state = parent
    out.reverse()
    return out


def check(model, max_states=None, verbose=True, fmt=None):
    fmt = fmt or (lambda s: repr(s))
    if max_states is None:
        max_states = getattr(model, "max_states", None)  # a model may bound its own exploration
    start = time.time()
    parents = {}
    queue = deque()
    for s in model.init():
        if s not in parents:
            parents[s] = (None, "init")
            queue.append(s)
    explored = 0
    max_depth = 0
    depth = {s: 0 for s in queue}
    try:
        while queue:
            s = queue.popleft()
            explored += 1
            d = depth[s]
            max_depth = max(max_depth, d)
            for name, pred in model.invariants.items():
                if not pred(s):
                    raise Violation("invariant", name, _trace(parents, s))
            n = 0
            for label, s2 in model.next(s):
                n += 1
                if s2 not in parents:
                    parents[s2] = (s, label)
                    depth[s2] = d + 1
                    queue.append(s2)
            if n == 0:
                for name, pred in model.quiescence.items():
                    if not pred(s):
                        raise Violation("quiescence", name, _trace(parents, s))
            if max_states and explored >= max_states:
                if verbose:
                    print(f"[{model.name}] stopped after {explored} states (bound reached)")
                break
    except Violation as v:
        if verbose:
            print(f"[{model.name}] {v.kind.upper()} VIOLATION: {v.prop}")
            print(f"  explored {explored} states in {time.time() - start:.1f}s; trace ({len(v.trace) - 1} steps):")
            for i, (label, st) in enumerate(v.trace):
                print(f"  {i:3d}. {label}")
                print(f"       {fmt(st)}")
        return v
    if verbose:
        print(
            f"[{model.name}] OK: {len(parents)} distinct states, {explored} explored, "
            f"max depth {max_depth}, {time.time() - start:.1f}s"
        )
    return None


def run_all(models, fmt=None):
    failures = 0
    for m in models:
        if check(m, fmt=fmt) is not None:
            failures += 1
    if failures:
        print(f"{failures} model(s) FAILED")
        sys.exit(1)
