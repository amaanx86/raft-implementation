<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/1/1b/Raft_Consensus_Algorithm_Mascot_on_transparent_background.svg" alt="Raft consensus algorithm mascot" width="160">
</p>

<h1 align="center">Raft Implementation</h1>

<p align="center">
  A minimal Proof of Concept of the Raft consensus algorithm in Go, with a small in-memory Key/Value store sitting on top of it. Built for learning, not production.
</p>

---

In Scope: Leader election, log replication, safety rules, state machine apply.

Out of Scope: Disk persistence, snapshots, membership changes, pre-vote, log compaction, client redirects.

## How Raft Works

Raft elects a single Leader to serialize all writes. Clients send commands to the Leader, the Leader appends them to its log, replicates the entries to the Followers, and once a majority has stored an entry it is committed and applied to the state machine on every node.

<p align="center">
  <img src="docs/cluster-topology.png" alt="Cluster topology: client talks to the leader, leader replicates to followers" width="640">
</p>

Every node is in one of three states. A Follower that hears nothing from the Leader for long enough times out and becomes a Candidate, asks peers for votes, and either wins the term (becomes Leader), loses to another Candidate (steps back to Follower), or times out again and retries.

<p align="center">
  <img src="docs/state-diagram.png" alt="Raft state transitions between Follower, Candidate and Leader" width="640">
</p>

This maps directly onto the code: `Follower`, `Candidate`, `Leader` in `raft/raft.go`, with `startElection`, `becomeLeaderLocked`, and `becomeFollowerLocked` driving the transitions.

## Layout

```text
cmd/raftkv/   Runnable node binary with a tiny HTTP API
raft/         Raft node, RPC types, replication and election logic
kv/           In-memory Key/Value state machine
Makefile      Lab workflows: build, cluster up/down, status, demo
```

## Quick Start

```bash
make cluster   # Build and start a 3-node cluster in the background
make demo      # Write a few keys via the current leader
make status    # See who is leader and where the log is
make stop      # Shut the cluster down
```

Run `make help` to list every target with a one-line description.

## Use the KV Store Directly

Writes only succeed on the current leader. Find it via `make status`.

```bash
curl -X POST -d '{"key":"hello","value":"world"}' http://127.0.0.1:9002/set
curl "http://127.0.0.1:9002/get?key=hello"
curl "http://127.0.0.1:9002/del?key=hello"
```

## Try a Leader Failure

Kill the leader process. Within a second the remaining two nodes elect a new leader for a higher term, and the committed data is still readable from the new leader.
