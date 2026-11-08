# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of ``org.elasticsearch.transport.ClusterConnectionManager`` (connectToNode / disconnectFromNode /
close) together with the ref-counting of ``Transport.Connection`` (``CloseableConnection`` +
``AbstractRefCounted``) that it relies on.  Single target node.

Shared state:
  closing       the `closing` AtomicBoolean
  cref          connectingRefCounter (AbstractRefCounted, starts at 1; hitting 0 runs pendingConnectionsComplete)
  latch         closeLatch counted down
  map           connectedNodes[node]: a connection id or None
  pending       pendingConnections[node]: (listener id, subscribers) or None
  conns         per connection id: (refcount, closed, registered, managerRefs, removed, waiters)
                  registered  = the handleClose close-listener has been added (putIfAbsent succeeded)
                  managerRefs = the AbstractRefCounted.of(conn::onRemoved) counter (0 = not created yet)
                  removed     = onRemoved() fired
                  waiters     = connectors parked in addRemovedListener waiting to retry
  next_lid/next_cid  fresh ids
  threads       connector threads ('K', id, pc, data, failures), one disconnector ('D', ...), one closer ('M', ...)
  flags         redundant (the `assert false : "redundant connection"` fired), mismatch (the
                `assert future == currentListener` fired), conn_closed_on_user (a user received a connection
                that was already closed), negative refcount

Every ConcurrentMap operation, CAS and listener completion is one atomic step.  Transport connects and
connection validations complete asynchronously (success or failure).  The remote end may close any open
connection at any time.
"""

from mc import run_all

MAX_RETRIES = 3  # the code gives up after 10 concurrent connect/disconnect collisions; 3 keeps the model small

KEYS = ("closing", "cref", "latch", "map", "pending", "conns", "next_lid", "next_cid", "threads", "flags")
CONN_KEYS = ("ref", "closed", "registered", "mrefs", "removed", "waiters")


def conn_get(c, k):
    return c[CONN_KEYS.index(k)]


def conn_set(c, **kw):
    lst = list(c)
    for k, v in kw.items():
        lst[CONN_KEYS.index(k)] = v
    return tuple(lst)


class ClusterConnectionManagerModel:
    def __init__(self, n_connectors, n_disconnects, with_closer, transport_can_fail, remote_can_close):
        self.n = n_connectors
        self.nd = n_disconnects
        self.with_closer = with_closer
        self.tfail = transport_can_fail
        self.rclose = remote_can_close
        self.name = (
            f"ClusterConnectionManager(connectors={n_connectors}, disconnects={n_disconnects}, closer={with_closer}, "
            f"transportFailures={transport_can_fail}, remoteClose={remote_can_close})"
        )
        inv = {
            "no redundant connection (putIfAbsent into connectedNodes never fails)": lambda d: "redundant" not in d["flags"],
            "pendingConnections entry removed is always the current listener": lambda d: "mismatch" not in d["flags"],
            "refcounts never negative": lambda d: "negative" not in d["flags"],
            "registered open connection is in connectedNodes unless being disconnected": self.registered_in_map,
            "connectingRefCounter is 0 only once closing": lambda d: d["cref"] > 0 or d["closing"],
        }
        qui = {
            "every connector completed (success or failure) and no listener left pending": lambda d: d["pending"] is None
            and all(th[0] != "K" or th[2] in ("S", "F") for th in d["threads"]),
            "no connection leaked: all connections closed and connectedNodes empty": lambda d: d["map"] is None
            and all(conn_get(c, "closed") for c in d["conns"].values()),
            "every registered connection fully released (refcount 0)": lambda d: all(
                conn_get(c, "ref") == 0 for c in d["conns"].values() if conn_get(c, "registered")
            ),
            "manager close completed (latch released) if close() was called": lambda d: not d["closing"] or d["latch"],
            "onRemoved fired for every registered connection (no retrying connector left parked)": lambda d: all(
                conn_get(c, "waiters") == () for c in d["conns"].values()
            ),
        }
        self.invariants = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in inv.items()}
        self.quiescence = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in qui.items()}

    @staticmethod
    def registered_in_map(d):
        disconnecting = {th[3] for th in d["threads"] if th[0] == "D" and th[2] == "d1"}
        for cid, c in d["conns"].items():
            if conn_get(c, "registered") and not conn_get(c, "closed") and d["map"] != cid and cid not in disconnecting:
                # the connection may also have been removed from the map by pendingConnectionsComplete (manager closed)
                if not d["closing"]:
                    return False
        return True

    def freeze(self, d):
        conns = tuple(sorted(d["conns"].items()))
        threads = tuple(sorted(d["threads"], key=repr))
        return tuple(conns if k == "conns" else threads if k == "threads" else d[k] for k in KEYS)

    def thaw(self, s):
        d = dict(zip(KEYS, s))
        d["conns"] = dict(d["conns"])
        d["threads"] = list(d["threads"])
        return d

    def init(self):
        threads = [("K", i, "c0", None, 0) for i in range(self.n)]
        threads += [("D", 100 + j, "d0", None, 0) for j in range(self.nd)]
        if self.with_closer:
            threads.append(("M", 200, "m0", None, 0))
        yield self.freeze(
            dict(closing=False, cref=1, latch=False, map=None, pending=None, conns={}, next_lid=0, next_cid=0, threads=threads, flags=())
        )

    # ---------------- helpers that mutate a thawed state dict in place ----------------
    def flag(self, d, f):
        if f not in d["flags"]:
            d["flags"] = tuple(sorted(d["flags"] + (f,)))

    def set_thread(self, d, tid, pc, data=None, failures=None):
        for i, th in enumerate(d["threads"]):
            if th[1] == tid:
                d["threads"][i] = (th[0], tid, pc, data, th[4] if failures is None else failures)
                return
        raise AssertionError(tid)

    def thread(self, d, tid):
        for th in d["threads"]:
            if th[1] == tid:
                return th
        raise AssertionError(tid)

    def on_removed(self, d, cid):
        c = d["conns"][cid]
        if conn_get(c, "removed"):
            return
        waiters = conn_get(c, "waiters")
        d["conns"][cid] = conn_set(c, removed=True, waiters=())
        for w in waiters:  # retry: connectToNodeOrRetry(...)
            self.set_thread(d, w, "c0", None)

    def manager_refs_dec(self, d, cid):
        c = d["conns"][cid]
        m = conn_get(c, "mrefs") - 1
        if m < 0:
            self.flag(d, "negative")
        d["conns"][cid] = conn_set(c, mrefs=m)
        if m == 0:
            self.on_removed(d, cid)

    def close_conn(self, d, cid):
        """CloseableConnection#close(): first close wins and runs the close listeners synchronously."""
        c = d["conns"][cid]
        if conn_get(c, "closed"):
            return
        d["conns"][cid] = conn_set(c, closed=True)
        if conn_get(c, "registered"):
            self.handle_close(d, cid)

    def handle_close(self, d, cid):
        # connectedNodes.remove(node, conn); connectionListener.onNodeDisconnected(node, e); managerRefs.decRef()
        if d["map"] == cid:
            d["map"] = None
        self.manager_refs_dec(d, cid)

    def dec_ref(self, d, cid):
        c = d["conns"][cid]
        r = conn_get(c, "ref") - 1
        if r < 0:
            self.flag(d, "negative")
        d["conns"][cid] = conn_set(c, ref=r)
        if r == 0:
            self.close_conn(d, cid)  # closeInternal() -> close()

    def try_inc_ref(self, d, cid):
        c = d["conns"][cid]
        if conn_get(c, "ref") > 0:
            d["conns"][cid] = conn_set(c, ref=conn_get(c, "ref") + 1)
            return True
        return False

    def cref_dec(self, d):
        d["cref"] -= 1
        if d["cref"] < 0:
            self.flag(d, "negative")
        if d["cref"] == 0:
            # pendingConnectionsComplete(): close every connection in connectedNodes and remove it; count down the latch
            if d["map"] is not None:
                cid = d["map"]
                d["map"] = None
                self.close_conn(d, cid)
            d["latch"] = True

    def complete_pending(self, d, subs, result_cid, failure=False):
        """future.onResponse(conn) / future.onFailure(e): every subscriber's acquiringListener runs."""
        for sid in subs:
            if failure:
                self.set_thread(d, sid, "F", None)
            else:
                self.set_thread(d, sid, "acq", result_cid)

    def remove_pending(self, d, expected_lid):
        """pendingConnections.remove(node); returns the subscribers of the removed listener (None if absent)."""
        p = d["pending"]
        d["pending"] = None
        if p is None:
            return None
        if p[0] != expected_lid:
            self.flag(d, "mismatch")
        return p[1]

    def fail_connection_listener(self, d, lid):
        # failConnectionListener: pendingConnections.remove(node); releaseOnce.run(); future.onFailure(e)
        subs = self.remove_pending(d, lid)
        self.cref_dec(d)
        if subs is not None:
            self.complete_pending(d, subs, None, failure=True)

    # ---------------- transitions ----------------
    def next(self, s):
        d0 = self.thaw(s)

        def fresh():
            d = self.thaw(s)
            return d

        for th in d0["threads"]:
            kind, tid, pc, data, failures = th
            if kind == "K":
                yield from self.connector_steps(fresh, tid, pc, data, failures)
            elif kind == "D":
                if pc == "d0":
                    d = fresh()
                    cid = d["map"]
                    d["map"] = None  # connectedNodes.remove(node)
                    self.set_thread(d, tid, "d1" if cid is not None else "done", cid)
                    yield f"disconnectFromNode: remove -> {cid}", self.freeze(d)
                elif pc == "d1":
                    d = fresh()
                    self.close_conn(d, data)  # nodeChannels.close()
                    self.set_thread(d, tid, "done", None)
                    yield f"disconnectFromNode: close conn{data}", self.freeze(d)
            elif kind == "M":
                if pc == "m0":
                    d = fresh()
                    d["closing"] = True
                    self.cref_dec(d)
                    self.set_thread(d, tid, "done", None)
                    yield "manager.close()", self.freeze(d)

        if self.rclose:
            for cid, c in d0["conns"].items():
                if not conn_get(c, "closed"):
                    d = fresh()
                    self.close_conn(d, cid)
                    yield f"remote closes conn{cid}", self.freeze(d)

    def connector_steps(self, fresh, tid, pc, data, failures):
        if pc == "c0":
            d = fresh()
            if d["closing"] or d["cref"] == 0:  # acquireConnectingRef() == false
                self.set_thread(d, tid, "F", None)
                yield f"K{tid}: manager closed -> onFailure", self.freeze(d)
            else:
                d["cref"] += 1
                self.set_thread(d, tid, "c1", None)
                yield f"K{tid}: acquireConnectingRef", self.freeze(d)
        elif pc == "c1":
            d = fresh()
            if d["map"] is not None:
                cid = d["map"]
                self.cref_dec(d)
                self.set_thread(d, tid, "acq", cid)
                yield f"K{tid}: existing connection conn{cid}", self.freeze(d)
            else:
                self.set_thread(d, tid, "c2", None)
                yield f"K{tid}: connectedNodes.get -> null", self.freeze(d)
        elif pc == "c2":
            d = fresh()
            if d["pending"] is not None:
                lid, subs = d["pending"]
                d["pending"] = (lid, subs + (tid,))  # existingListener.addListener(acquiringListener)
                self.cref_dec(d)
                self.set_thread(d, tid, "wait", None)
                yield f"K{tid}: subscribe to pending L{lid}", self.freeze(d)
            else:
                lid = d["next_lid"]
                d["next_lid"] += 1
                d["pending"] = (lid, (tid,))  # putIfAbsent + currentListener.addListener(acquiringListener)
                self.set_thread(d, tid, "c4", lid)
                yield f"K{tid}: register pending L{lid}", self.freeze(d)
        elif pc == "c4":
            d = fresh()
            lid = data
            if d["map"] is not None:
                cid = d["map"]
                subs = self.remove_pending(d, lid)
                self.cref_dec(d)
                self.complete_pending(d, subs or (), cid)
                yield f"K{tid}: recheck found conn{cid}, complete L{lid}", self.freeze(d)
            else:
                self.set_thread(d, tid, "opening", lid)
                yield f"K{tid}: internalOpenConnection", self.freeze(d)
        elif pc == "opening":
            lid = data
            d = fresh()
            cid = d["next_cid"]
            d["next_cid"] += 1
            d["conns"][cid] = (1, False, False, 0, False, ())
            self.set_thread(d, tid, "opened", (lid, cid))
            yield f"K{tid}: transport opened conn{cid}", self.freeze(d)
            if self.tfail:
                d = fresh()
                self.set_thread(d, tid, "failed_open", lid)  # thread continues in failConnectionListener
                self.fail_connection_listener(d, lid)
                yield f"K{tid}: transport open failed", self.freeze(d)
        elif pc == "opened":
            lid, cid = data
            d = fresh()
            if conn_get(d["conns"][cid], "closed"):
                # "a channel closed while connecting" -> failure path (connection already closed, ref leaks harmlessly)
                self.set_thread(d, tid, "failed_open", lid)
                self.fail_connection_listener(d, lid)
                yield f"K{tid}: conn{cid} closed while connecting", self.freeze(d)
            else:
                self.set_thread(d, tid, "validating", (lid, cid))
                yield f"K{tid}: validating conn{cid}", self.freeze(d)
        elif pc == "failed_open":
            d = fresh()
            # the owner's own acquiringListener was among the subscribers; if it was not (pending already removed by
            # someone else, which would be a mismatch) just finish
            if self.thread(d, tid)[2] == "failed_open":
                self.set_thread(d, tid, "F", None)
            yield f"K{tid}: open failed, done", self.freeze(d)
        elif pc == "validating":
            lid, cid = data
            d = fresh()
            self.set_thread(d, tid, "v1", (lid, cid))
            yield f"K{tid}: validation ok for conn{cid}", self.freeze(d)
            if self.tfail:
                d = fresh()
                self.close_conn(d, cid)  # IOUtils.closeWhileHandlingException(conn)
                self.fail_connection_listener(d, lid)  # completes the subscribers, including this connector's own listener
                d["threads"].append(("K", tid + 1000, "v5", cid, 0))  # ... then runAfter(..., conn::decRef)
                yield f"K{tid}: validation failed for conn{cid}", self.freeze(d)
        elif pc == "v1":
            lid, cid = data
            d = fresh()
            c = d["conns"][cid]
            d["conns"][cid] = conn_set(c, mrefs=1)  # managerRefs = AbstractRefCounted.of(conn::onRemoved)
            if d["map"] is not None:
                self.flag(d, "redundant")
                self.close_conn(d, cid)
            else:
                d["map"] = cid
                c = d["conns"][cid]
                d["conns"][cid] = conn_set(c, mrefs=2, registered=True)  # managerRefs.mustIncRef(); addCloseListener(handleClose)
                if conn_get(c, "closed"):
                    self.handle_close(d, cid)  # listener added to an already closed connection fires immediately
            self.set_thread(d, tid, "v2", (lid, cid))
            yield f"K{tid}: connectedNodes.putIfAbsent(conn{cid})", self.freeze(d)
        elif pc == "v2":
            lid, cid = data
            d = fresh()
            subs = self.remove_pending(d, lid)  # pendingConnections.remove(node)
            self.manager_refs_dec(d, cid)  # managerRefs.decRef()
            self.set_thread(d, tid, "v3", (lid, cid, subs or ()))
            yield f"K{tid}: remove pending L{lid}, managerRefs.decRef", self.freeze(d)
        elif pc == "v3":
            lid, cid, subs = data
            d = fresh()
            self.cref_dec(d)  # releaseOnce.run()
            self.set_thread(d, tid, "v4", (lid, cid, subs))
            yield f"K{tid}: connectingRefCounter.decRef", self.freeze(d)
        elif pc == "v4":
            lid, cid, subs = data
            d = fresh()
            self.complete_pending(d, subs, cid)  # future.onResponse(conn): subscribers (incl. this thread) move to 'acq'
            # this thread still has to run the trailing conn::decRef; remember that in a side thread entry
            d["threads"].append(("K", tid + 1000, "v5", cid, 0))
            yield f"K{tid}: future.onResponse(conn{cid})", self.freeze(d)
        elif pc == "v5":
            cid = data
            d = fresh()
            self.dec_ref(d, cid)  # ActionListener.runAfter(..., conn::decRef)
            d["threads"] = [th for th in d["threads"] if th[1] != tid]
            yield f"K{tid - 1000}: trailing conn{cid}.decRef", self.freeze(d)
        elif pc == "acq":
            cid = data
            d = fresh()
            if self.try_inc_ref(d, cid):
                self.set_thread(d, tid, "user", cid)
                yield f"K{tid}: tryIncRef(conn{cid}) ok -> connected", self.freeze(d)
            else:
                nf = failures + 1
                if nf < MAX_RETRIES:
                    c = d["conns"][cid]
                    if conn_get(c, "removed"):
                        self.set_thread(d, tid, "c0", None, nf)  # removed listener fires immediately -> retry
                    else:
                        d["conns"][cid] = conn_set(c, waiters=conn_get(c, "waiters") + (tid,))
                        self.set_thread(d, tid, "parked", cid, nf)
                    yield f"K{tid}: tryIncRef(conn{cid}) failed, wait for removal (failures={nf})", self.freeze(d)
                else:
                    self.set_thread(d, tid, "F", None, nf)
                    yield f"K{tid}: giving up after {nf} failures", self.freeze(d)
        elif pc == "user":
            cid = data
            d = fresh()
            self.dec_ref(d, cid)  # the caller releases its Releasable
            self.set_thread(d, tid, "S", None)
            yield f"K{tid}: release conn{cid}", self.freeze(d)
        # 'wait', 'parked', 'S', 'F', 'done' have no own steps


def fmt(s):
    d = dict(zip(KEYS, s))
    conns = " ".join(
        f"conn{cid}[ref={c[0]}{' closed' if c[1] else ''}{' reg' if c[2] else ''} mrefs={c[3]}{' removed' if c[4] else ''}"
        f"{' waiters=' + str(c[5]) if c[5] else ''}]"
        for cid, c in d["conns"]
    )
    threads = " ".join(f"{k}{i}:{pc}{'(' + str(data) + ')' if data is not None else ''}{'#' + str(f) if f else ''}" for k, i, pc, data, f in d["threads"])
    return (
        f"closing={d['closing']} cref={d['cref']} map={d['map']} pending={d['pending']} {conns} | {threads}"
        + (f" FLAGS={d['flags']}" if d["flags"] else "")
    )


def bounded(model, max_states):
    """Three concurrent connectors with retries exceed the memory of a workstation; explore them up to a bound."""
    model.max_states = max_states
    model.name += f"[bounded to {max_states} states]"
    return model


if __name__ == "__main__":
    run_all(
        [
            ClusterConnectionManagerModel(1, 1, False, False, False),
            ClusterConnectionManagerModel(2, 1, False, False, False),
            ClusterConnectionManagerModel(2, 1, False, True, False),
            ClusterConnectionManagerModel(2, 1, False, False, True),
            ClusterConnectionManagerModel(2, 1, True, False, False),
            ClusterConnectionManagerModel(2, 1, True, True, True),
            ClusterConnectionManagerModel(2, 2, True, True, True),  # ~1.7M states, about a minute
            bounded(ClusterConnectionManagerModel(3, 1, False, False, True), 2_000_000),
        ],
        fmt=fmt,
    )
