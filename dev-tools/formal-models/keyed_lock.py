# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``org.elasticsearch.common.util.concurrent.KeyedLock`` (acquire / tryAcquire / release).

Shared state:
  map      -- the ConcurrentMap: key -> lock id (or absent)
  locks    -- per lock id: (count, holder) where count is the CAS-updated ``KeyLock.count`` and
              holder is the thread holding the ReentrantLock (or None)
  next_id  -- counter used to give every ``new KeyLock()`` a fresh identity (identity matters for
              ``map.remove(key, lock)`` and the ``assert lock == map.get(key)`` in ``release``)
Every volatile read, CAS, ``lock()``/``unlock()``/``tryLock()``, ``putIfAbsent`` and
``remove(key, lock)`` is one atomic step; ``lock()`` is only enabled while the lock is free.

Thread kinds: ('A', key) blocking ``acquire(key)``, ('T', key) ``tryAcquire(key)``.
Program counters (locals carried in the tuple):
  acquire:   get -> (absent) create [new lock, lock(), putIfAbsent]  -> CS / retry
                    (present) readcount -> tryinc (CAS count) -> lock -> CS
  tryAcquire: tget -> create / ttrylock -> tinc loop (CAS) -> CS | tunlock -> done
  critical section CS -> release: dec (CAS count-1) -> unlock -> remove (if count hit 0) -> done
"""

from mc import run_all


class KeyedLockModel:
    def __init__(self, acquirers, try_acquirers, keys=1):
        self.specs = [("A", k % keys) for k in range(acquirers)] + [("T", k % keys) for k in range(try_acquirers)]
        self.keys = keys
        self.name = f"KeyedLock(acquire={acquirers}, tryAcquire={try_acquirers}, keys={keys})"
        self.invariants = {
            "mutual exclusion per key": self.mutex,
            "release sees its own lock in the map": lambda s: not s[3],
            "count >= 0": lambda s: all(c >= 0 for (_, c, _) in s[1]),
            "lock holder is a live thread": lambda s: all(h is None or any(t[0] == h for t in s[4]) for (_, _, h) in s[1]),
        }
        self.quiescence = {
            "all threads done and map empty": lambda s: s[0] == () and s[4] == (),
            "no lock held": lambda s: all(h is None for (_, _, h) in s[1]),
        }

    # state = (map, locks, next_id, assert_failed, threads)
    # map   = tuple of (key, lockid) sorted; locks = tuple of (lockid, count, holder) sorted
    # thread = (tid, kind, key, pc, lockid, local_count)
    def init(self):
        threads = tuple((i, k, key, "get" if k == "A" else "tget", None, None) for i, (k, key) in enumerate(self.specs))
        yield ((), (), 0, False, threads)

    @staticmethod
    def mutex(s):
        in_cs = {}
        for th in s[4]:
            if th[3] == "cs":
                in_cs[th[2]] = in_cs.get(th[2], 0) + 1
        return all(v <= 1 for v in in_cs.values())

    def next(self, s):
        mp, locks, next_id, failed, threads = s
        mapd = dict(mp)
        lockd = {lid: (c, h) for (lid, c, h) in locks}

        def frz(mapd, lockd, next_id, failed, ths):
            # Garbage-collect lock objects that are neither in the map nor referenced by any thread, and
            # renumber the survivors canonically so that the state space stays finite despite retry loops
            # allocating fresh KeyLock objects.
            ths = tuple(sorted(ths))
            order = []
            for lid in [v for _, v in sorted(mapd.items())] + [t[4] for t in ths]:
                if lid is not None and lid not in order:
                    order.append(lid)
            ren = {lid: i for i, lid in enumerate(order)}
            return (
                tuple(sorted((k, ren[v]) for k, v in mapd.items())),
                tuple(sorted((ren[lid], c, h) for lid, (c, h) in lockd.items() if lid in ren)),
                len(order),
                failed,
                tuple((tid, kind, key, pc, ren.get(lid) if lid is not None else None, lc) for (tid, kind, key, pc, lid, lc) in ths),
            )

        for i, th in enumerate(threads):
            rest = threads[:i] + threads[i + 1 :]
            tid, kind, key, pc, lid, lc = th

            def go(pc2, lid2=lid, lc2=lc, mapd=mapd, lockd=lockd, next_id=next_id, failed=failed, done=False):
                ths = rest + (() if done else ((tid, kind, key, pc2, lid2, lc2),))
                return frz(mapd, lockd, next_id, failed, ths)

            if pc in ("get", "tget"):
                cur = mapd.get(key)
                if cur is None:
                    # tryCreateNewLock: new KeyLock() (count=1), lock() held by creator
                    nl = dict(lockd)
                    nl[next_id] = (1, tid)
                    yield f"t{tid}: map.get -> null, new KeyLock#{next_id}.lock()", go("put", lid2=next_id, lockd=nl, next_id=next_id + 1)
                elif pc == "get":
                    yield f"t{tid}: map.get -> lock#{cur}", go("readcount", lid2=cur)
                else:
                    yield f"t{tid}: map.get -> lock#{cur}", go("ttrylock", lid2=cur)
            elif pc == "put":
                if key in mapd:
                    # putIfAbsent fails -> new lock discarded; blocking acquire loops, tryAcquire returns null
                    if kind == "A":
                        yield f"t{tid}: putIfAbsent failed, retry", go("get", lid2=None)
                    else:
                        yield f"t{tid}: putIfAbsent failed, tryAcquire -> null", go(None, done=True)
                else:
                    nm = dict(mapd)
                    nm[key] = lid
                    yield f"t{tid}: putIfAbsent ok -> critical section", go("cs", mapd=nm)
            elif pc == "readcount":
                c, h = lockd[lid]
                if c > 0:
                    yield f"t{tid}: read count={c}", go("tryinc", lc2=c)
                else:
                    yield f"t{tid}: read count=0, retry", go("get", lid2=None)
            elif pc == "tryinc":
                c, h = lockd[lid]
                if c == lc:
                    nl = dict(lockd)
                    nl[lid] = (c + 1, h)
                    yield f"t{tid}: tryIncCount({lc}) ok", go("lock", lockd=nl)
                else:
                    yield f"t{tid}: tryIncCount({lc}) failed, retry", go("get", lid2=None)
            elif pc == "lock":
                c, h = lockd[lid]
                if h is None:
                    nl = dict(lockd)
                    nl[lid] = (c, tid)
                    yield f"t{tid}: lock() acquired -> critical section", go("cs", lockd=nl)
            elif pc == "ttrylock":
                c, h = lockd[lid]
                if h is None:
                    nl = dict(lockd)
                    nl[lid] = (c, tid)
                    yield f"t{tid}: tryLock ok", go("tinc", lockd=nl)
                else:
                    yield f"t{tid}: tryLock failed -> null", go(None, done=True)
            elif pc == "tinc":
                c, h = lockd[lid]
                if c > 0:
                    nl = dict(lockd)
                    nl[lid] = (c + 1, h)
                    yield f"t{tid}: tryIncCount({c}) ok -> critical section", go("cs", lockd=nl)
                    # a competing CAS could also make this fail -- covered by interleaving of other threads
                else:
                    nl = dict(lockd)
                    nl[lid] = (c, None)
                    yield f"t{tid}: count==0, unlock, tryAcquire -> null", go(None, lockd=nl, done=True)
            elif pc == "cs":
                # release(): assert lock == map.get(key); decCountAndGet
                ok = mapd.get(key) == lid
                c, h = lockd[lid]
                nl = dict(lockd)
                nl[lid] = (c - 1, h)
                yield f"t{tid}: release: decCountAndGet -> {c - 1}", go("unlock", lc2=c - 1, lockd=nl, failed=failed or not ok)
            elif pc == "unlock":
                c, h = lockd[lid]
                nl = dict(lockd)
                nl[lid] = (c, None)
                yield f"t{tid}: unlock()", go("remove", lockd=nl)
            elif pc == "remove":
                if lc == 0:
                    nm = dict(mapd)
                    if nm.get(key) == lid:
                        del nm[key]
                    yield f"t{tid}: map.remove(key, lock#{lid})", go(None, mapd=nm, done=True)
                else:
                    yield f"t{tid}: done (count {lc} > 0)", go(None, done=True)


def fmt(s):
    return f"map={s[0]} locks={s[1]} next={s[2]} threads={s[4]}"


if __name__ == "__main__":
    run_all(
        [
            KeyedLockModel(2, 0),
            KeyedLockModel(3, 0),
            KeyedLockModel(2, 1),
            KeyedLockModel(3, 1),
            KeyedLockModel(2, 2),
            KeyedLockModel(4, 0, keys=2),
            KeyedLockModel(4, 1),
        ],
        fmt=fmt,
    )
