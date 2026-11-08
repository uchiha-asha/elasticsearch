# Formal models of critical Elasticsearch flows

This directory contains executable, finite-state models of concurrency-critical flows in the
server, together with a small explicit-state model checker (`mc.py`). Each model is written
directly against the Java source it names (every shared-memory access or message is one atomic
step), states the safety properties the Java relies on, and is exhaustively checked for all
interleavings of a small number of threads / nodes / operations. The checker reports the shortest
counterexample trace when a property fails.

The models do not depend on anything outside the Python 3 standard library:

```
cd dev-tools/formal-models
python3 throttled_task_runner.py        # one model
for m in *.py; do [ "$m" = mc.py ] || python3 "$m"; done   # all of them (a few minutes)
```

`mc.py` does a breadth-first search over the state space. A model provides `init()`, `next(state)`
(yielding `(label, successor)` pairs), `invariants` (checked on every reachable state) and
`quiescence` predicates (checked on terminal states, i.e. states with no enabled transition; this is
how "nothing is left stuck / leaked forever" properties are expressed for these finite systems).

## Models

| Model | Java under model | Main properties |
|---|---|---|
| `throttled_task_runner.py` | `AbstractThrottledTaskRunner#pollAndSpawn` (+ `ThrottledTaskRunner`) | `runningTasks <= max`; slots held match running/dequeued tasks; no task left in the queue with no worker; every task runs once; covers rejection and the DIRECT executor |
| `search_pending_executions.py` | `AbstractSearchAsyncAction.PendingExecutions` (`max_concurrent_shard_requests` throttle) | at most `permits` tasks executing; permit accounting; no task stranded in the queue; all permits returned |
| `async_io_processor.py` | `AsyncIOProcessor#put` | one writer at a time (promise semaphore); no item left unprocessed; no double processing; semaphore released |
| `subscribable_listener.py` | `SubscribableListener#addListener` / `setResult` (lock-free CAS state machine) | each subscriber completed exactly once with the winning result; subscription order preserved |
| `shard_operation_permits.py` | `IndexShardOperationPermits` + `IndexShard#bumpPrimaryTerm`, `relocated`, `wrapPrimaryOperationPermitListener` | all-permits block excludes operations; delayed ops are released; `acquire()` never fails despite the fair semaphore; ops started after a bump run under the new term; no op executes after relocation hand-off; out-of-order block acquisition for concurrent bumps |
| `replica_term_transition.py` | `IndexShard#innerAcquireReplicaOperationPermit` + `bumpPrimaryTerm` (incl. combined all-permits ops) | executed operation terms never decrease; an op is never older than the current term while executing; an op with the newest term is never rejected as too old; `operationPrimaryTerm` catches up with `pendingPrimaryTerm` |
| `cluster_connection_manager.py` | `ClusterConnectionManager#connectToNode` / `disconnectFromNode` / `close` with `CloseableConnection` + `AbstractRefCounted` | the `redundant connection` and `pending listener mismatch` assertions never fire; refcounts never negative; registered open connections are in `connectedNodes`; every connector's listener completes; no connection leaked; retrying connectors parked in `addRemovedListener` are always woken; `close()` releases the latch |
| `task_cancellation.py` | `TaskCancellationService#doCancelTaskAndDescendants`, `TaskManager#startBanOnChildTasks` / `registerChildConnection` / `setBan` / `removeBan` / `registerCancellableTask` | while banned every registered child is cancelled; no child runs after `removeBan`; completion is only reported when no child is registered or in flight; no ban left behind |
| `seqno_replication.py` | `ReplicationOperation`, `TransportReplicationAction` replica path, `ReplicationTracker`, primary promotion (`restoreLocalHistoryFromTranslog`, `fillSeqNoGaps`, resync + trim), master in-sync rules (`IndexMetadataUpdater`, `ShardStateAction`), peer recovery (`initiateTracking`, `markAllocationIdAsInSync`) | acked writes exist on every live in-sync copy (in particular the new primary after failover); acked seqnos are unique; no copy applies two different ops at one seqno; copies agree below both global checkpoints; started copies' global checkpoint <= local checkpoint; convergence of in-sync copies at quiescence |
| `keyed_lock.py` | `KeyedLock` (acquire / tryAcquire / release with lock-free counts) | per-key mutual exclusion; `release` always sees its own lock in the map; map empty at the end |
| `throttled_iterator.py` | `ThrottledIterator` | at most `maxConcurrency` items in flight; one thread in `run()`; `onCompletion` exactly once after everything, refcount accounting |
| `ref_counting_listener.py` | `RefCountingListener` + `RefCountingRunnable` + `AbstractRefCounted` | delegate completed exactly once, only after `close()` and all children; failure iff some child failed; exception accounting |

Each file's module docstring explains how program counters, threads and state fields map onto the
Java code, and the `__main__` block lists the parameterisations that are checked.

## Results (as of the commit adding this directory)

All models pass all listed parameterisations: no invariant or quiescence violation was found in
any of the twelve flows, i.e. no bug was found in the modelled code. The largest runs explore a few
hundred thousand distinct states (`seqno_replication.py` with three copies, two writes, primary and
replica node-left: ~190k states; `cluster_connection_manager.py` with two connectors, two
disconnects, manager close, transport failures and remote closes: ~1.7M states; the
three-connector configuration of the connection manager is explored up to a 2M-state bound).

To make sure the checker actually detects defects, each model was also run against deliberately
broken variants (kept out of the tree, in the session notes): removing the `tasks.peek()` re-check
in `pollAndSpawn` produces the lost-task race the Java comment warns about in 13 steps; removing the
engine rollback to the global checkpoint on a replica's primary-term bump makes the replication
model report an acked write missing from an in-sync copy after failover in 7 steps.

## Modelling assumptions worth knowing

* Threads are modelled with sequential consistency for `volatile` / CAS / `ConcurrentMap`
  operations, which matches the JMM guarantees for those operations. Plain fields that the Java
  code deliberately reads unsynchronised are modelled as the Java code accesses them.
* `seqno_replication.py` treats the master's response to `failShardIfNeeded` /
  `markShardCopyAsStale` and the primary's application of the resulting cluster state as one step
  (publication completes after the state was applied on the primary's node, barring the publish
  timeout). Node-left keeps allocation ids in the in-sync set and bumps the primary term on primary
  failure, following `IndexMetadataUpdater`; a copy that left the cluster keeps running (and a stale
  primary keeps accepting writes) until it applies the cluster state that removes it. The periodic
  global checkpoint sync and `wait_for_active_shards` are not modelled.
* A copy that is still recovering (tracked but not in-sync) is allowed to learn a global checkpoint
  above its own local checkpoint; `markAllocationIdAsInSync` waits for it to catch up.
* `cluster_connection_manager.py` deliberately does not require that a connection handed to a caller
  is still open: the remote end, `disconnectFromNode` or `close()` may close it at any moment; the
  refcount only guarantees the manager itself never closes it under a caller.
* Retries and event counts are bounded (e.g. three connect/disconnect collisions instead of ten, one
  primary failover, one recovery) to keep the state spaces finite and small.

## Adding a model

Copy the structure of an existing model: immutable tuple states, a `next` generator that yields one
successor per atomic step, `invariants`/`quiescence` dictionaries keyed by a human-readable property
name, a `fmt` function for traces, and a `__main__` that runs a handful of small parameterisations
through `run_all`. Keep every step at the granularity of one shared-memory access or one message so
that every real interleaving is covered.
