package raft

import (
	"net"
	"net/rpc"
)

// requestvoteargs is sent by a candidate to ask a peer for its vote.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// requestvotereply is the answer to a vote request.
type RequestVoteReply struct {
	Term        int  // current term of the peer, for the candidate to update itself
	VoteGranted bool // true if vote was given to the candidate
}

// appendentriesargs is sent by the leader to replicate log entries
// and also serves as a heartbeat when entries is empty.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so followers can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prev log index
	Entries      []LogEntry // log entries to store, empty for heartbeats
	LeaderCommit int        // leader's commitindex
}

// appendentriesreply is the follower's response.
type AppendEntriesReply struct {
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained matching prev entry
}

// rpcserver wraps a node and exposes its handlers in the shape
// that net/rpc expects: exported methods with two pointer args
// and an error return.
type rpcServer struct {
	node *Node
}

// requestvote is the net/rpc entrypoint for the vote handler.
func (s *rpcServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.node.handleRequestVote(args, reply)
	return nil
}

// appendentries is the net/rpc entrypoint for the replication handler.
func (s *rpcServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.node.handleAppendEntries(args, reply)
	return nil
}

// startrpc registers the raft service and starts accepting tcp connections.
// it returns immediately after the listener is ready.
func (n *Node) StartRPC(addr string) error {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &rpcServer{node: n}); err != nil {
		return err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	n.listener = l
	go n.acceptLoop(server, l)
	return nil
}

// acceptloop accepts connections until the listener is closed.
func (n *Node) acceptLoop(server *rpc.Server, l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			// listener closed during shutdown, exit cleanly
			return
		}
		go server.ServeConn(conn)
	}
}

// callpeer dials a peer if needed and invokes the rpc method.
// clients are cached and reconnected on error.
func (n *Node) callPeer(peerID int, method string, args, reply any) error {
	client, err := n.clientFor(peerID)
	if err != nil {
		return err
	}
	if err := client.Call(method, args, reply); err != nil {
		// drop the broken client so the next call redials
		n.clientsMu.Lock()
		if n.clients[peerID] == client {
			delete(n.clients, peerID)
		}
		n.clientsMu.Unlock()
		client.Close()
		return err
	}
	return nil
}

// clientfor returns a cached rpc client or dials a new one.
func (n *Node) clientFor(peerID int) (*rpc.Client, error) {
	n.clientsMu.Lock()
	if c, ok := n.clients[peerID]; ok {
		n.clientsMu.Unlock()
		return c, nil
	}
	addr := n.peers[peerID]
	n.clientsMu.Unlock()

	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	n.clientsMu.Lock()
	// another goroutine may have dialed concurrently, keep one
	if existing, ok := n.clients[peerID]; ok {
		n.clientsMu.Unlock()
		c.Close()
		return existing, nil
	}
	n.clients[peerID] = c
	n.clientsMu.Unlock()
	return c, nil
}
