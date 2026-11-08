# Verification status

Date: 2026-11-08 UTC. Commit under test: 6391cf6f2 (branch main at 6391cf6f2).

Result: all 12 models pass; 101 parameterisations checked, 0 invariant violations, 0 quiescence violations, 0 bugs found.

Negative controls (not committed): removing the tasks.peek() re-check in pollAndSpawn -> lost task in 13 steps; removing the replica engine rollback on term bump -> acked write missing from an in-sync copy in 7 steps.

## Raw output of the final pass

```
[ThrottledTaskRunner(tasks=2, max=1, reject=False, direct=False)] OK: 265 distinct states, 265 explored, max depth 26, 0.0s
[ThrottledTaskRunner(tasks=2, max=1, reject=False, direct=True)] OK: 213 distinct states, 213 explored, max depth 24, 0.0s
[ThrottledTaskRunner(tasks=2, max=1, reject=True, direct=False)] OK: 397 distinct states, 397 explored, max depth 26, 0.0s
[ThrottledTaskRunner(tasks=2, max=1, reject=True, direct=True)] OK: 329 distinct states, 329 explored, max depth 24, 0.0s
[ThrottledTaskRunner(tasks=3, max=1, reject=False, direct=False)] OK: 2400 distinct states, 2400 explored, max depth 39, 0.0s
[ThrottledTaskRunner(tasks=3, max=1, reject=False, direct=True)] OK: 1851 distinct states, 1851 explored, max depth 36, 0.0s
[ThrottledTaskRunner(tasks=3, max=1, reject=True, direct=False)] OK: 4381 distinct states, 4381 explored, max depth 39, 0.0s
[ThrottledTaskRunner(tasks=3, max=1, reject=True, direct=True)] OK: 3475 distinct states, 3475 explored, max depth 36, 0.0s
[ThrottledTaskRunner(tasks=3, max=2, reject=False, direct=False)] OK: 5874 distinct states, 5874 explored, max depth 39, 0.0s
[ThrottledTaskRunner(tasks=3, max=2, reject=False, direct=True)] OK: 3651 distinct states, 3651 explored, max depth 36, 0.0s
[ThrottledTaskRunner(tasks=3, max=2, reject=True, direct=False)] OK: 9703 distinct states, 9703 explored, max depth 39, 0.0s
[ThrottledTaskRunner(tasks=3, max=2, reject=True, direct=True)] OK: 6394 distinct states, 6394 explored, max depth 36, 0.0s
[ThrottledTaskRunner(tasks=4, max=1, reject=False, direct=False)] OK: 19861 distinct states, 19861 explored, max depth 52, 0.1s
[ThrottledTaskRunner(tasks=4, max=1, reject=False, direct=True)] OK: 14965 distinct states, 14965 explored, max depth 48, 0.1s
[ThrottledTaskRunner(tasks=4, max=1, reject=True, direct=False)] OK: 43172 distinct states, 43172 explored, max depth 52, 0.3s
[ThrottledTaskRunner(tasks=4, max=1, reject=True, direct=True)] OK: 33308 distinct states, 33308 explored, max depth 48, 0.2s
[ThrottledTaskRunner(tasks=4, max=2, reject=False, direct=False)] OK: 58303 distinct states, 58303 explored, max depth 52, 0.4s
[ThrottledTaskRunner(tasks=4, max=2, reject=False, direct=True)] OK: 34111 distinct states, 34111 explored, max depth 48, 0.2s
[ThrottledTaskRunner(tasks=4, max=2, reject=True, direct=False)] OK: 114206 distinct states, 114206 explored, max depth 52, 0.9s
[ThrottledTaskRunner(tasks=4, max=2, reject=True, direct=True)] OK: 70598 distinct states, 70598 explored, max depth 48, 0.5s
[SearchPendingExecutions(tasks=2, permits=1)] OK: 114 distinct states, 114 explored, max depth 17, 0.0s
[SearchPendingExecutions(tasks=3, permits=1)] OK: 1442 distinct states, 1442 explored, max depth 23, 0.0s
[SearchPendingExecutions(tasks=3, permits=2)] OK: 1262 distinct states, 1262 explored, max depth 25, 0.0s
[SearchPendingExecutions(tasks=4, permits=1)] OK: 17419 distinct states, 17419 explored, max depth 29, 0.1s
[SearchPendingExecutions(tasks=4, permits=2)] OK: 21756 distinct states, 21756 explored, max depth 31, 0.1s
[SearchPendingExecutions(tasks=5, permits=2)] OK: 331216 distinct states, 331216 explored, max depth 37, 2.9s
[AsyncIOProcessor(items=2, queueSize=1)] OK: 99 distinct states, 99 explored, max depth 12, 0.0s
[AsyncIOProcessor(items=3, queueSize=1)] OK: 1845 distinct states, 1845 explored, max depth 18, 0.0s
[AsyncIOProcessor(items=3, queueSize=2)] OK: 2163 distinct states, 2163 explored, max depth 18, 0.0s
[AsyncIOProcessor(items=4, queueSize=1)] OK: 28679 distinct states, 28679 explored, max depth 24, 0.1s
[AsyncIOProcessor(items=4, queueSize=2)] OK: 39803 distinct states, 39803 explored, max depth 24, 0.2s
[AsyncIOProcessor(items=5, queueSize=2)] OK: 644913 distinct states, 644913 explored, max depth 30, 5.7s
[SubscribableListener(adders=1, completers=1)] OK: 12 distinct states, 12 explored, max depth 4, 0.0s
[SubscribableListener(adders=2, completers=1)] OK: 75 distinct states, 75 explored, max depth 8, 0.0s
[SubscribableListener(adders=3, completers=1)] OK: 604 distinct states, 604 explored, max depth 11, 0.0s
[SubscribableListener(adders=2, completers=2)] OK: 448 distinct states, 448 explored, max depth 10, 0.0s
[SubscribableListener(adders=3, completers=2)] OK: 4163 distinct states, 4163 explored, max depth 13, 0.0s
[SubscribableListener(adders=4, completers=2)] OK: 43424 distinct states, 43424 explored, max depth 16, 0.5s
[ShardOperationPermits(ops=2, blockers=[('bump', 2)])] OK: 189 distinct states, 189 explored, max depth 12, 0.0s
[ShardOperationPermits(ops=3, blockers=[('bump', 2)])] OK: 1214 distinct states, 1214 explored, max depth 15, 0.0s
[ShardOperationPermits(ops=2, blockers=[('bump', 2), ('bump', 3)])] OK: 1825 distinct states, 1825 explored, max depth 18, 0.0s
[ShardOperationPermits(ops=2, blockers=[('bump', 2), ('block', None)])] OK: 1384 distinct states, 1384 explored, max depth 17, 0.0s
[ShardOperationPermits(ops=2, blockers=[('reloc', None)])] OK: 132 distinct states, 132 explored, max depth 10, 0.0s
[ShardOperationPermits(ops=2, blockers=[('reloc', None), ('block', None)])] OK: 967 distinct states, 967 explored, max depth 15, 0.0s
[ShardOperationPermits(ops=3, blockers=[('bump', 2), ('bump', 3), ('block', None)])] OK: 125040 distinct states, 125040 explored, max depth 26, 1.7s
[ReplicaTermTransition(ops=[(1, False), (2, False)])] OK: 54 distinct states, 54 explored, max depth 9, 0.0s
[ReplicaTermTransition(ops=[(2, False), (1, False), (2, False)])] OK: 263 distinct states, 263 explored, max depth 11, 0.0s
[ReplicaTermTransition(ops=[(1, False), (2, False), (3, False)])] OK: 1112 distinct states, 1112 explored, max depth 16, 0.0s
[ReplicaTermTransition(ops=[(3, False), (2, False), (1, False)])] OK: 1112 distinct states, 1112 explored, max depth 16, 0.0s
[ReplicaTermTransition(ops=[(1, False), (2, True)])] OK: 25 distinct states, 25 explored, max depth 8, 0.0s
[ReplicaTermTransition(ops=[(2, True), (1, False), (2, False)])] OK: 585 distinct states, 585 explored, max depth 15, 0.0s
[ReplicaTermTransition(ops=[(1, True), (2, True), (2, False)])] OK: 2692 distinct states, 2692 explored, max depth 19, 0.0s
[ReplicaTermTransition(ops=[(1, False), (2, False), (3, True), (2, False)])] OK: 2421 distinct states, 2421 explored, max depth 17, 0.0s
[ReplicaTermTransition(ops=[(2, False), (3, True), (3, False), (1, False)])] OK: 12157 distinct states, 12157 explored, max depth 22, 0.1s
[TaskCancellation(children=1, parentMayFinishEarly=False)] OK: 51 distinct states, 51 explored, max depth 9, 0.0s
[TaskCancellation(children=1, parentMayFinishEarly=True)] OK: 55 distinct states, 55 explored, max depth 9, 0.0s
[TaskCancellation(children=2, parentMayFinishEarly=False)] OK: 341 distinct states, 341 explored, max depth 14, 0.0s
[TaskCancellation(children=2, parentMayFinishEarly=True)] OK: 399 distinct states, 399 explored, max depth 14, 0.0s
[TaskCancellation(children=3, parentMayFinishEarly=False)] OK: 2381 distinct states, 2381 explored, max depth 18, 0.0s
[TaskCancellation(children=3, parentMayFinishEarly=True)] OK: 2925 distinct states, 2925 explored, max depth 18, 0.0s
[KeyedLock(acquire=2, tryAcquire=0, keys=1)] OK: 75 distinct states, 75 explored, max depth 11, 0.0s
[KeyedLock(acquire=3, tryAcquire=0, keys=1)] OK: 1470 distinct states, 1470 explored, max depth 18, 0.0s
[KeyedLock(acquire=2, tryAcquire=1, keys=1)] OK: 872 distinct states, 872 explored, max depth 18, 0.0s
[KeyedLock(acquire=3, tryAcquire=1, keys=1)] OK: 18770 distinct states, 18770 explored, max depth 25, 0.2s
[KeyedLock(acquire=2, tryAcquire=2, keys=1)] OK: 8259 distinct states, 8259 explored, max depth 23, 0.1s
[KeyedLock(acquire=4, tryAcquire=0, keys=2)] OK: 5625 distinct states, 5625 explored, max depth 22, 0.1s
[KeyedLock(acquire=4, tryAcquire=1, keys=1)] OK: 514819 distinct states, 514819 explored, max depth 32, 10.4s
[ThrottledIterator(items=2, maxConcurrency=1, direct=True, reject=False)] OK: 29 distinct states, 29 explored, max depth 8, 0.0s
[ThrottledIterator(items=2, maxConcurrency=1, direct=False, reject=False)] OK: 35 distinct states, 35 explored, max depth 10, 0.0s
[ThrottledIterator(items=2, maxConcurrency=1, direct=False, reject=True)] OK: 41 distinct states, 41 explored, max depth 10, 0.0s
[ThrottledIterator(items=3, maxConcurrency=1, direct=True, reject=False)] OK: 54 distinct states, 54 explored, max depth 11, 0.0s
[ThrottledIterator(items=3, maxConcurrency=1, direct=False, reject=False)] OK: 66 distinct states, 66 explored, max depth 14, 0.0s
[ThrottledIterator(items=3, maxConcurrency=1, direct=False, reject=True)] OK: 78 distinct states, 78 explored, max depth 14, 0.0s
[ThrottledIterator(items=3, maxConcurrency=2, direct=True, reject=False)] OK: 90 distinct states, 90 explored, max depth 11, 0.0s
[ThrottledIterator(items=3, maxConcurrency=2, direct=False, reject=False)] OK: 112 distinct states, 112 explored, max depth 13, 0.0s
[ThrottledIterator(items=3, maxConcurrency=2, direct=False, reject=True)] OK: 134 distinct states, 134 explored, max depth 13, 0.0s
[ThrottledIterator(items=4, maxConcurrency=2, direct=True, reject=False)] OK: 199 distinct states, 199 explored, max depth 14, 0.0s
[ThrottledIterator(items=4, maxConcurrency=2, direct=False, reject=False)] OK: 251 distinct states, 251 explored, max depth 17, 0.0s
[ThrottledIterator(items=4, maxConcurrency=2, direct=False, reject=True)] OK: 303 distinct states, 303 explored, max depth 17, 0.0s
[ThrottledIterator(items=5, maxConcurrency=2, direct=True, reject=False)] OK: 375 distinct states, 375 explored, max depth 17, 0.0s
[ThrottledIterator(items=5, maxConcurrency=2, direct=False, reject=False)] OK: 475 distinct states, 475 explored, max depth 21, 0.0s
[ThrottledIterator(items=5, maxConcurrency=2, direct=False, reject=True)] OK: 575 distinct states, 575 explored, max depth 21, 0.0s
[ThrottledIterator(items=5, maxConcurrency=3, direct=True, reject=False)] OK: 539 distinct states, 539 explored, max depth 17, 0.0s
[ThrottledIterator(items=5, maxConcurrency=3, direct=False, reject=False)] OK: 693 distinct states, 693 explored, max depth 20, 0.0s
[ThrottledIterator(items=5, maxConcurrency=3, direct=False, reject=True)] OK: 847 distinct states, 847 explored, max depth 20, 0.0s
[RefCountingListener(children=1, maxExceptions=1)] OK: 11 distinct states, 11 explored, max depth 5, 0.0s
[RefCountingListener(children=2, maxExceptions=1)] OK: 62 distinct states, 62 explored, max depth 8, 0.0s
[RefCountingListener(children=3, maxExceptions=1)] OK: 338 distinct states, 338 explored, max depth 11, 0.0s
[RefCountingListener(children=3, maxExceptions=2)] OK: 387 distinct states, 387 explored, max depth 12, 0.0s
[RefCountingListener(children=4, maxExceptions=2)] OK: 2291 distinct states, 2291 explored, max depth 15, 0.0s
[RefCountingListener(children=5, maxExceptions=3)] OK: 14156 distinct states, 14156 explored, max depth 19, 0.1s
[SeqNoReplication(copies=2, writes=1, primaryNodeLeft=False, replicaNodeLeft=False, recoveries=0)] OK: 4 distinct states, 4 explored, max depth 3, 0.0s
[SeqNoReplication(copies=2, writes=2, primaryNodeLeft=False, replicaNodeLeft=False, recoveries=0)] OK: 37 distinct states, 37 explored, max depth 6, 0.0s
[SeqNoReplication(copies=2, writes=1, primaryNodeLeft=True, replicaNodeLeft=False, recoveries=0)] OK: 27 distinct states, 27 explored, max depth 6, 0.0s
[SeqNoReplication(copies=2, writes=2, primaryNodeLeft=True, replicaNodeLeft=False, recoveries=0)] OK: 317 distinct states, 317 explored, max depth 9, 0.0s
[SeqNoReplication(copies=2, writes=2, primaryNodeLeft=True, replicaNodeLeft=True, recoveries=0)] OK: 527 distinct states, 527 explored, max depth 9, 0.0s
[SeqNoReplication(copies=2, writes=1, primaryNodeLeft=True, replicaNodeLeft=False, recoveries=1)] OK: 121 distinct states, 121 explored, max depth 12, 0.0s
[SeqNoReplication(copies=3, writes=1, primaryNodeLeft=True, replicaNodeLeft=False, recoveries=0)] OK: 380 distinct states, 380 explored, max depth 10, 0.0s
[SeqNoReplication(copies=3, writes=2, primaryNodeLeft=True, replicaNodeLeft=False, recoveries=0)] OK: 24505 distinct states, 24505 explored, max depth 15, 0.9s
[SeqNoReplication(copies=2, writes=2, primaryNodeLeft=True, replicaNodeLeft=True, recoveries=1)] OK: 8258 distinct states, 8258 explored, max depth 17, 0.2s
[SeqNoReplication(copies=3, writes=2, primaryNodeLeft=True, replicaNodeLeft=True, recoveries=0)] OK: 190621 distinct states, 190621 explored, max depth 18, 7.6s
[ClusterConnectionManager(connectors=1, disconnects=1, closer=False, transportFailures=False, remoteClose=False)] OK: 206 distinct states, 206 explored, max depth 41, 0.0s
[ClusterConnectionManager(connectors=2, disconnects=1, closer=False, transportFailures=False, remoteClose=False)] OK: 46636 distinct states, 46636 explored, max depth 81, 0.9s
[ClusterConnectionManager(connectors=2, disconnects=1, closer=False, transportFailures=True, remoteClose=False)] OK: 67816 distinct states, 67816 explored, max depth 81, 1.4s
[ClusterConnectionManager(connectors=2, disconnects=1, closer=False, transportFailures=False, remoteClose=True)] OK: 135192 distinct states, 135192 explored, max depth 81, 3.4s
[ClusterConnectionManager(connectors=2, disconnects=1, closer=True, transportFailures=False, remoteClose=False)] OK: 95312 distinct states, 95312 explored, max depth 82, 2.3s
[ClusterConnectionManager(connectors=2, disconnects=1, closer=True, transportFailures=True, remoteClose=True)] OK: 318894 distinct states, 318894 explored, max depth 82, 9.1s
[ClusterConnectionManager(connectors=3, disconnects=1, closer=False, transportFailures=False, remoteClose=True)] stopped after 2000000 states (bound reached)
[ClusterConnectionManager(connectors=3, disconnects=1, closer=False, transportFailures=False, remoteClose=True)] OK: 2294814 distinct states, 2000000 explored, max depth 46, 79.4s
```
