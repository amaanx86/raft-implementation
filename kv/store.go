// package kv is the state machine that sits on top of the raft log.
// only committed log entries are applied here.
package kv

import (
	"maps"
	"sync"
)

// store is a simple in-memory key/value map guarded by a mutex.
type Store struct {
	mu   sync.Mutex
	data map[string]string
}

// new returns an empty store.
func New() *Store {
	return &Store{data: map[string]string{}}
}

// get reads a key. ok is false when the key is missing.
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

// apply mutates the store based on a committed command.
// raft guarantees this is called in log order on every node.
func (s *Store) Apply(op, key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op {
	case "set":
		s.data[key] = value
	case "del":
		delete(s.data, key)
	}
}

// snapshot returns a copy of the current data, useful for debugging.
func (s *Store) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data))
	maps.Copy(out, s.data)
	return out
}
