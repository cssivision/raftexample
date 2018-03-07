# raftexample
A simple key-value store based on raft consensus algorithm.

## raft thesis
serveral novel features:
- ``Strong leader``: Raft uses a stronger form of leadership than other consensus algorithms. For example, log entries only flow from the leader to other servers.
- ``Leader election``: Raft uses randomized timers to elect leaders. This adds only a small amount of mechanism to the heartbeats already required for any consensus algorithm, while resolving conflicts simply and rapidly.
- ``Membership changes``: Raft’s mechanism for changing the set of servers in the cluster uses a new joint consensus approach where the majorities of two different configurations overlap during transitions. This allows the cluster to continue operating normally during configuration changes.