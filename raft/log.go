package raft

// command is what gets replicated through the raft log.
// it describes a single mutation to apply on the state machine.
type Command struct {
	Op    string // "set" or "del"
	Key   string
	Value string // empty for "del"
}

// logentry is one record in the raft log.
// term is the leader term that created the entry and is used
// to detect stale or conflicting entries during replication.
type LogEntry struct {
	Term    int
	Command Command
}
