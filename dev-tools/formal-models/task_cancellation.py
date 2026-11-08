# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""
Model of task cancellation with child-task bans: ``TaskCancellationService#doCancelTaskAndDescendants``,
``TaskManager#startBanOnChildTasks`` / ``registerChildConnection`` / ``unregisterChildConnection`` on the
coordinating node, and ``TaskManager#registerCancellableTask`` / ``setBan`` / ``removeBan`` on a child node.

One coordinating node runs cancellable parent task P and sends N child requests to one child node.
At some point P is cancelled (waitForCompletion semantics for the completion listener).

Coordinator state:
  sends       per child request: U=not sent, R=registered (registerChildConnection done, request in flight),
              D=done (response received, unregisterChildConnection done), X=rejected locally (parent banned)
  ban_reason  CancellableTaskHolder.banChildrenReason set (startBan happened)
  child_count childTasksPerConnection[childNode] (0 == absent)
  wait_children  startBan registered onChildTasksCompleted (children were outstanding)
  cancel_pc   progress of doCancelTaskAndDescendants:
              none -> started (startBan + cancel issued) ; refs: ban_children_done, cancel_task_done
              bans_sent (set-ban request in flight) / bans_placed ; completed (completedListener fired) ; unban_sent
  p_finished  parent task holder finished (unregistered)

Child node state:
  banned      bannedParents contains P
  tasks       per child request: '-' not registered, 'P' put into cancellableTasks but ban not yet checked,
              'R' running, 'C' cancelled (task.cancel called, still registered), 'F' finished (unregistered)
  inflight    requests travelling coordinator -> child (tuple of ids), responses child -> coordinator
  ban_msgs    tuple of ban messages in flight to the child ('set' / 'remove'); ban responses back
"""

from mc import run_all

KEYS = (
    "sends", "ban_reason", "child_count", "wait_children", "cancel_pc", "ban_children_done", "cancel_task_done",
    "p_finished", "banned", "tasks", "req_inflight", "resp_inflight", "ban_inflight", "ban_resp_inflight", "removed_ban_count",
)


class TaskCancellationModel:
    def __init__(self, n_children, parent_may_finish_early):
        self.n = n_children
        self.early = parent_may_finish_early
        self.name = f"TaskCancellation(children={n_children}, parentMayFinishEarly={parent_may_finish_early})"
        inv = {
            "while the child node is banned, every registered child task is cancelled or about to be": lambda d: not d["banned"]
            or all(t in ("-", "P", "C", "F") for t in d["tasks"]),
            "child task never runs after ban removal was processed": lambda d: d["removed_ban_count"] == 0
            or all(t in ("-", "F", "X") for t in d["tasks"]) and d["req_inflight"] == (),
            "cancellation reported complete only when no child is registered or in flight": lambda d: d["cancel_pc"]
            not in ("completed", "unban_sent")
            or (all(t in ("-", "F") for t in d["tasks"]) and d["req_inflight"] == () and d["child_count"] == 0),
            "childTasksPerConnection never negative": lambda d: d["child_count"] >= 0,
        }
        qui = {
            "no ban left behind on the child node": lambda d: not d["banned"],
            "cancellation completed (completedListener fired)": lambda d: d["cancel_pc"] == "unban_sent",
            "no child task registered, nothing in flight": lambda d: all(t in ("-", "F") for t in d["tasks"])
            and d["req_inflight"] == ()
            and d["resp_inflight"] == ()
            and d["child_count"] == 0,
            "every child request accounted for (unsent ones only if the parent finished first)": lambda d: all(
                s in ("D", "X") or (s == "U" and d["p_finished"]) for s in d["sends"]
            ),
        }
        self.invariants = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in inv.items()}
        self.quiescence = {k: (lambda s, p=p: p(self.thaw(s))) for k, p in qui.items()}

    def freeze(self, d):
        return tuple(d[k] for k in KEYS)

    def thaw(self, s):
        return dict(zip(KEYS, s))

    def init(self):
        yield self.freeze(
            dict(sends=("U",) * self.n, ban_reason=False, child_count=0, wait_children=False, cancel_pc="none",
                 ban_children_done=False, cancel_task_done=False, p_finished=False, banned=False, tasks=("-",) * self.n,
                 req_inflight=(), resp_inflight=(), ban_inflight=(), ban_resp_inflight=(), removed_ban_count=0)
        )

    @staticmethod
    def upd(t, i, v):
        lst = list(t)
        lst[i] = v
        return tuple(lst)

    def children_completed(self, d):
        """unregisterChildConnection dropped the last child: notify childTaskCompletedListeners (banChildrenRef.close())."""
        if d["child_count"] == 0 and d["wait_children"]:
            d["wait_children"] = False
            d["ban_children_done"] = True
            self.maybe_complete(d)

    def maybe_complete(self, d):
        # RefCountingRunnable: once banChildrenRef and cancelTaskRef are closed, completedListener is chained after
        # setBanListener; completedListener fires once bans are placed as well
        if d["ban_children_done"] and d["cancel_task_done"] and d["cancel_pc"] == "bans_placed":
            d["cancel_pc"] = "completed"

    def next(self, s):
        d0 = self.thaw(s)

        # ---------------- coordinator: send child requests ----------------
        for i, st in enumerate(d0["sends"]):
            if st == "U" and not d0["p_finished"]:
                d = dict(d0)
                if d["ban_reason"]:
                    # registerChildConnection throws TaskCancelledException("parent task was cancelled")
                    d["sends"] = self.upd(d["sends"], i, "X")
                    yield f"coord: child request {i} rejected locally (parent banned)", self.freeze(d)
                else:
                    d["child_count"] += 1
                    d["sends"] = self.upd(d["sends"], i, "R")
                    d["req_inflight"] = d["req_inflight"] + (i,)
                    yield f"coord: registerChildConnection + send child request {i}", self.freeze(d)

        # ---------------- coordinator: response handling ----------------
        for i in d0["resp_inflight"]:
            d = dict(d0)
            d["resp_inflight"] = tuple(x for x in d["resp_inflight"] if x != i)
            d["sends"] = self.upd(d["sends"], i, "D")
            d["child_count"] -= 1  # unregisterChildConnection
            self.children_completed(d)
            yield f"coord: response for child {i}, unregisterChildConnection", self.freeze(d)

        # ---------------- coordinator: cancellation ----------------
        if d0["cancel_pc"] == "none":
            d = dict(d0)
            # startBanOnChildTasks: banChildrenReason = reason; snapshot child connections; register completion listener
            d["ban_reason"] = True
            has_children = d["child_count"] > 0
            if has_children:
                d["wait_children"] = True
            else:
                d["ban_children_done"] = True
            # taskManager.cancel(task, reason, cancelTaskRef::close): listener runs at holder.finish() (or now if finished)
            if d["p_finished"]:
                d["cancel_task_done"] = True
            # setBanOnChildConnections: send set-ban to every snapshotted connection (none if no children)
            if has_children:
                d["cancel_pc"] = "bans_sent"
                d["ban_inflight"] = d["ban_inflight"] + ("set",)
            else:
                d["cancel_pc"] = "bans_placed"
                self.maybe_complete(d)
            yield "coord: doCancelTaskAndDescendants (startBan, cancel, send bans)", self.freeze(d)

        if d0["cancel_pc"] == "completed":
            d = dict(d0)
            d["cancel_pc"] = "unban_sent"
            d["ban_inflight"] = d["ban_inflight"] + ("remove",)
            yield "coord: removeBanOnChildConnections", self.freeze(d)

        for j, _ in enumerate(d0["ban_resp_inflight"]):
            d = dict(d0)
            d["ban_resp_inflight"] = d["ban_resp_inflight"][:j] + d["ban_resp_inflight"][j + 1 :]
            if d["cancel_pc"] == "bans_sent":
                d["cancel_pc"] = "bans_placed"  # CountDownActionListener -> setBanListener
                self.maybe_complete(d)
            yield "coord: ban response received", self.freeze(d)

        # parent task finishes (holder.finish runs the cancellation listeners)
        if not d0["p_finished"] and (self.early or all(st != "U" for st in d0["sends"])):
            d = dict(d0)
            d["p_finished"] = True
            if d["cancel_pc"] != "none":
                d["cancel_task_done"] = True
                self.maybe_complete(d)
            yield "coord: parent task finished (unregister)", self.freeze(d)

        # ---------------- child node: request arrival and task lifecycle ----------------
        for i in d0["req_inflight"]:
            d = dict(d0)
            d["req_inflight"] = tuple(x for x in d["req_inflight"] if x != i)
            d["tasks"] = self.upd(d["tasks"], i, "P")  # cancellableTasks.put(task, holder)
            yield f"child: register task {i} (put)", self.freeze(d)
        for i, t in enumerate(d0["tasks"]):
            if t == "P":
                d = dict(d0)
                if d["banned"]:
                    # ban found before starting: cancel, unregister, throw -> failure response
                    d["tasks"] = self.upd(d["tasks"], i, "F")
                    d["resp_inflight"] = d["resp_inflight"] + (i,)
                    yield f"child: task {i} cancelled before starting", self.freeze(d)
                else:
                    d["tasks"] = self.upd(d["tasks"], i, "R")
                    yield f"child: task {i} starts running", self.freeze(d)
            elif t in ("R", "C"):
                d = dict(d0)
                d["tasks"] = self.upd(d["tasks"], i, "F")  # unregister, then send response
                d["resp_inflight"] = d["resp_inflight"] + (i,)
                yield f"child: task {i} finishes ({'cancelled' if t == 'C' else 'normal'})", self.freeze(d)

        # ---------------- child node: ban messages ----------------
        for j, msg in enumerate(d0["ban_inflight"]):
            d = dict(d0)
            d["ban_inflight"] = d["ban_inflight"][:j] + d["ban_inflight"][j + 1 :]
            if msg == "set":
                d["banned"] = True  # bannedParents.computeIfAbsent
                # then cancellableTasks.getByParent(parent) -> cancel each registered task
                d["tasks"] = tuple("C" if t == "R" else t for t in d["tasks"])
                # NB a task in state 'P' (put, ban not yet checked) is also returned by getByParent and gets cancelled;
                # it will additionally see the ban when it checks -> same outcome, modelled by the 'P' step above
                d["ban_resp_inflight"] = d["ban_resp_inflight"] + ("ok",)
                yield "child: setBan(parent) + cancel children", self.freeze(d)
            else:
                d["banned"] = False
                d["removed_ban_count"] += 1
                yield "child: removeBan(parent)", self.freeze(d)


def fmt(s):
    d = dict(zip(KEYS, s))
    return " ".join(f"{k}={v}" for k, v in d.items())


if __name__ == "__main__":
    run_all([TaskCancellationModel(n, e) for n in (1, 2, 3) for e in (False, True)], fmt=fmt)
