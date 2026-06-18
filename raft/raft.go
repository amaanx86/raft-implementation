// package raft is a minimal implementation of the raft consensus
// algorithm intended for learning. it focuses on the three core
// pieces of raft: leader election, log replication, and safety.
//
// non-goals: persistence to disk, snapshots, membership changes,
// pre-vote, or any production hardening. the in-memory log is lost
// when a node restarts.
package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/amaanx86/raft-implementation/kv"
)

// state of a raft node at any moment.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// string makes state values readable in logs.
func (s State) String() string {
	return [...]string{"follower", "candidate", "leader"}[s]
}

// timing constants. kept long-ish so cluster activity is easy to read.
const (
	heartbeatInterval = 100 * time.Millisecond
	electionMin       = 400 * time.Millisecond
	electionMax       = 800 * time.Millisecond
	tickInterval      = 10 * time.Millisecond
)

// node is a single raft server. it owns its raft state, the state
// machine, and the rpc plumbing to talk to peers.
type Node struct {
	mu sync.Mutex

	id    int
	peers map[int]string // peer id -> tcp address

	// volatile role
	state State

	// persistent state in the paper, in-memory here
	currentTerm int
	votedFor    int        // -1 when no vote cast this term
	entries     []LogEntry // index 0 is a dummy entry so real entries start at 1

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	// last time we heard from a leader or granted a vote, used to
	// detect election timeouts.
	lastContact time.Time
	// random per-node election timeout, rerolled on every reset.
	electionTimeout time.Duration

	// listeners that wait for a particular log index to be applied,
	// used by submit so clients can wait for their write to commit.
	applyNotifs map[int]chan struct{}

	// rpc plumbing
	listener  net.Listener
	clients   map[int]*rpc.Client
	clientsMu sync.Mutex

	// state machine
	kv *kv.Store

	// lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup
	rng    *rand.Rand
	logger *log.Logger
}

// newnode constructs a follower with an empty log and no leader.
// peers must not include the node's own id.
func NewNode(id int, peers map[int]string) *Node {
	n := &Node{
		id:          id,
		peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		// the dummy entry simplifies prev-index lookups
		entries:     []LogEntry{{Term: 0}},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   map[int]int{},
		matchIndex:  map[int]int{},
		applyNotifs: map[int]chan struct{}{},
		clients:     map[int]*rpc.Client{},
		kv:          kv.New(),
		stopCh:      make(chan struct{}),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(id))),
		logger:      log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
	}
	n.resetElectionTimerLocked()
	return n
}

// start kicks off the background event loop. the rpc server must be
// started separately via startrpc.
func (n *Node) Start() {
	n.wg.Add(1)
	go n.eventLoop()
}

// stop tears the node down. it closes the listener, drops rpc
// clients, and waits for the event loop to exit.
func (n *Node) Stop() {
	close(n.stopCh)
	if n.listener != nil {
		n.listener.Close()
	}
	n.clientsMu.Lock()
	for _, c := range n.clients {
		c.Close()
	}
	n.clients = map[int]*rpc.Client{}
	n.clientsMu.Unlock()
	n.wg.Wait()
}

// eventloop drives the node's time-based behaviour: candidates and
// followers check for election timeouts, leaders send heartbeats.
func (n *Node) eventLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	var lastHeartbeat time.Time
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			state := n.state
			n.mu.Unlock()
			switch state {
			case Follower, Candidate:
				if n.electionTimedOut() {
					n.startElection()
				}
			case Leader:
				if time.Since(lastHeartbeat) >= heartbeatInterval {
					n.broadcastAppendEntries()
					lastHeartbeat = time.Now()
				}
			}
		}
	}
}

// electiontimedout returns true if no contact has been seen within
// the random election timeout.
func (n *Node) electionTimedOut() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Since(n.lastContact) >= n.electionTimeout
}

// resetelectiontimerlocked refreshes the deadline and picks a new
// random timeout. spread reduces the chance of split votes.
func (n *Node) resetElectionTimerLocked() {
	n.lastContact = time.Now()
	span := electionMax - electionMin
	n.electionTimeout = electionMin + time.Duration(n.rng.Int63n(int64(span)))
}

// becomefollowerlocked transitions to follower at a possibly higher
// term and clears any vote from the previous term.
func (n *Node) becomeFollowerLocked(term int) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1
	}
	n.state = Follower
	n.resetElectionTimerLocked()
}

// becomeleaderlocked initialises per-peer replication state and
// declares this node leader for the current term.
func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	last := len(n.entries) - 1
	for peerID := range n.peers {
		// optimistic: assume peer is up to date, will back off on rejection
		n.nextIndex[peerID] = last + 1
		n.matchIndex[peerID] = 0
	}
	n.logger.Printf("node %d became leader for term %d", n.id, n.currentTerm)
}

// startelection bumps the term, votes for self, and asks peers to
// vote. on receiving a majority it becomes leader.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimerLocked()
	term := n.currentTerm
	lastIdx := len(n.entries) - 1
	lastTerm := n.entries[lastIdx].Term
	peerIDs := make([]int, 0, len(n.peers))
	for id := range n.peers {
		peerIDs = append(peerIDs, id)
	}
	n.logger.Printf("node %d starting election for term %d", n.id, term)
	n.mu.Unlock()

	votes := 1
	needed := (len(peerIDs)+1)/2 + 1
	for _, peerID := range peerIDs {
		go func(peerID int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if err := n.callPeer(peerID, "Raft.RequestVote", args, reply); err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			// ignore late replies once we have moved on
			if n.state != Candidate || n.currentTerm != term {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= needed {
					n.becomeLeaderLocked()
				}
			}
		}(peerID)
	}
}

// handlerequestvote is invoked by the rpc server for incoming votes.
// it follows the rules in figure 2 of the raft paper.
func (n *Node) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		return
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	lastIdx := len(n.entries) - 1
	lastTerm := n.entries[lastIdx].Term
	// candidate must be at least as up-to-date as us
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimerLocked()
	}
	reply.Term = n.currentTerm
}

// handleappendentries is invoked for incoming heartbeats or
// replication batches from a leader.
func (n *Node) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Success = false
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		return
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	// any valid leader contact means we step down to follower
	n.state = Follower
	n.resetElectionTimerLocked()

	// reject if our log is too short or the prev entry doesn't match
	if args.PrevLogIndex >= len(n.entries) {
		reply.Term = n.currentTerm
		return
	}
	if n.entries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = n.currentTerm
		return
	}

	// merge incoming entries, truncating any conflicting suffix
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(n.entries) {
			if n.entries[idx].Term != e.Term {
				n.entries = append(n.entries[:idx], args.Entries[i:]...)
				break
			}
			continue
		}
		n.entries = append(n.entries, args.Entries[i:]...)
		break
	}
	if len(args.Entries) > 0 {
		n.logger.Printf("node %d follower: appended %d entries from leader %d prev=%d (log_len=%d)",
			n.id, len(args.Entries), args.LeaderID, args.PrevLogIndex, len(n.entries)-1)
	}

	// advance commit index, but never beyond what we have locally
	if args.LeaderCommit > n.commitIndex {
		last := len(n.entries) - 1
		prevCommit := n.commitIndex
		n.commitIndex = min(args.LeaderCommit, last)
		if n.commitIndex > prevCommit {
			n.logger.Printf("node %d follower: commit advanced %d -> %d", n.id, prevCommit, n.commitIndex)
		}
		n.applyCommittedLocked()
	}

	reply.Term = n.currentTerm
	reply.Success = true
}

// broadcastappendentries fans out replication or heartbeats to all peers.
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peerIDs := make([]int, 0, len(n.peers))
	for id := range n.peers {
		peerIDs = append(peerIDs, id)
	}
	n.mu.Unlock()

	for _, peerID := range peerIDs {
		go n.replicateTo(peerID, term)
	}
}

// replicateto sends one appendentries to a single peer and processes
// the result. on rejection it backs off nextindex by one entry.
func (n *Node) replicateTo(peerID, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := max(n.nextIndex[peerID], 1)
	prevIdx := nextIdx - 1
	prevTerm := n.entries[prevIdx].Term
	// copy so the leader can keep appending without racing readers
	entries := append([]LogEntry(nil), n.entries[nextIdx:]...)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	// only log real replication, not empty heartbeats, so the trace stays useful
	if len(entries) > 0 {
		n.logger.Printf("node %d leader: replicate to peer %d entries=%d prev=%d commit=%d",
			n.id, peerID, len(entries), prevIdx, n.commitIndex)
	}
	n.mu.Unlock()

	reply := &AppendEntriesReply{}
	if err := n.callPeer(peerID, "Raft.AppendEntries", args, reply); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader || n.currentTerm != term {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if reply.Success {
		n.nextIndex[peerID] = nextIdx + len(entries)
		n.matchIndex[peerID] = n.nextIndex[peerID] - 1
		n.advanceCommitLocked()
		return
	}
	// follower rejected, step back and retry on next heartbeat
	if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
	n.logger.Printf("node %d leader: peer %d rejected, nextIndex backed off to %d",
		n.id, peerID, n.nextIndex[peerID])
}

// advancecommitlocked moves commitindex forward to the largest index
// replicated to a majority, but only for entries in the current term.
func (n *Node) advanceCommitLocked() {
	last := len(n.entries) - 1
	majority := (len(n.peers)+1)/2 + 1
	prevCommit := n.commitIndex
	for N := n.commitIndex + 1; N <= last; N++ {
		// safety rule: never commit entries from prior terms by counting
		if n.entries[N].Term != n.currentTerm {
			continue
		}
		count := 1 // leader has it
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
		}
	}
	if n.commitIndex > prevCommit {
		n.logger.Printf("node %d leader: commit advanced %d -> %d", n.id, prevCommit, n.commitIndex)
	}
	n.applyCommittedLocked()
}

// applycommittedlocked feeds committed log entries into the state
// machine in order and wakes any waiters. it logs every mutation
// so the lab demo shows the full create/update/delete trace.
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		cmd := n.entries[n.lastApplied].Command
		prev, existed := n.kv.Apply(cmd.Op, cmd.Key, cmd.Value)
		n.logger.Printf("node %d apply idx=%d %s", n.id, n.lastApplied, describeApply(cmd, prev, existed))
		if ch, ok := n.applyNotifs[n.lastApplied]; ok {
			close(ch)
			delete(n.applyNotifs, n.lastApplied)
		}
	}
}

// describeapply turns a committed command and the prior store state
// into a short human-readable phrase like `created key="foo" value="bar"`.
func describeApply(cmd Command, prev string, existed bool) string {
	switch cmd.Op {
	case "set":
		if existed {
			return fmt.Sprintf("updated key=%q value=%q (was %q)", cmd.Key, cmd.Value, prev)
		}
		return fmt.Sprintf("created key=%q value=%q", cmd.Key, cmd.Value)
	case "del":
		if existed {
			return fmt.Sprintf("deleted key=%q (was %q)", cmd.Key, prev)
		}
		return fmt.Sprintf("delete miss key=%q", cmd.Key)
	}
	return fmt.Sprintf("unknown op=%q key=%q", cmd.Op, cmd.Key)
}

// submit appends a command to the leader's log. it returns the log
// index assigned and true on success, or (0, false) if not the leader.
func (n *Node) Submit(cmd Command) (int, bool) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, false
	}
	n.entries = append(n.entries, LogEntry{Term: n.currentTerm, Command: cmd})
	idx := len(n.entries) - 1
	term := n.currentTerm
	peerIDs := make([]int, 0, len(n.peers))
	for id := range n.peers {
		peerIDs = append(peerIDs, id)
	}
	n.logger.Printf("node %d leader: append idx=%d op=%s key=%q", n.id, idx, cmd.Op, cmd.Key)
	n.mu.Unlock()

	// nudge replication right away rather than waiting for the next heartbeat
	for _, peerID := range peerIDs {
		go n.replicateTo(peerID, term)
	}
	return idx, true
}

// waitapply blocks until the given log index has been applied to the
// state machine, or until the timeout fires.
func (n *Node) WaitApply(index int, timeout time.Duration) bool {
	n.mu.Lock()
	if n.lastApplied >= index {
		n.mu.Unlock()
		return true
	}
	ch, ok := n.applyNotifs[index]
	if !ok {
		ch = make(chan struct{})
		n.applyNotifs[index] = ch
	}
	n.mu.Unlock()

	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// get reads a key. it only serves reads on the leader to keep the
// guarantees simple. for stronger linearizability you would also
// confirm leadership via a quorum read.
func (n *Node) Get(key string) (value string, found, isLeader bool) {
	n.mu.Lock()
	leader := n.state == Leader
	n.mu.Unlock()
	if !leader {
		n.logger.Printf("node %d read key=%q rejected (not leader)", n.id, key)
		return "", false, false
	}
	v, ok := n.kv.Get(key)
	n.logger.Printf("node %d leader: read key=%q found=%v", n.id, key, ok)
	return v, ok, true
}

// status is a small snapshot of node state useful for /status and tests.
type Status struct {
	ID          int    `json:"id"`
	State       string `json:"state"`
	Term        int    `json:"term"`
	VotedFor    int    `json:"voted_for"`
	LogLen      int    `json:"log_len"`
	CommitIndex int    `json:"commit_index"`
	LastApplied int    `json:"last_applied"`
}

// status returns a copy of key fields for inspection.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		State:       n.state.String(),
		Term:        n.currentTerm,
		VotedFor:    n.votedFor,
		LogLen:      len(n.entries) - 1,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
}
