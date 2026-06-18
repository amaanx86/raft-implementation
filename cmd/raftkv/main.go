// raftkv is a single-node binary that joins a raft cluster and
// serves an http key/value api. start one process per node.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/amaanx86/raft-implementation/raft"
)

func main() {
	var (
		id       = flag.Int("id", 0, "this node's id")
		rpcAddr  = flag.String("rpc", "127.0.0.1:8000", "rpc bind address for raft peer traffic")
		httpAddr = flag.String("http", "127.0.0.1:9000", "http bind address for the client api")
		peersStr = flag.String("peers", "", "comma-separated peers as id=host:port, excluding self")
	)
	flag.Parse()

	peers, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("invalid peers flag: %v", err)
	}

	node := raft.NewNode(*id, peers)
	if err := node.StartRPC(*rpcAddr); err != nil {
		log.Fatalf("starting rpc: %v", err)
	}
	node.Start()

	mux := buildHTTP(node)
	srv := &http.Server{Addr: *httpAddr, Handler: mux}

	go func() {
		log.Printf("node %d listening: rpc=%s http=%s peers=%v", *id, *rpcAddr, *httpAddr, peers)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	// block until interrupted, then shut down cleanly
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("node %d shutting down", *id)
	srv.Close()
	node.Stop()
}

// parsepeers turns "1=127.0.0.1:8001,2=127.0.0.1:8002" into a map.
func parsePeers(s string) (map[int]string, error) {
	out := map[int]string{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for item := range strings.SplitSeq(s, ",") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("expected id=addr, got %q", item)
		}
		id, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("bad id in %q: %w", item, err)
		}
		out[id] = strings.TrimSpace(parts[1])
	}
	return out, nil
}

// buildhttp wires the small client api. all writes go through raft;
// reads only succeed on the current leader so stale data is rare.
func buildHTTP(node *raft.Node) *http.ServeMux {
	mux := http.NewServeMux()

	// /get?key=foo returns the value or 404 if missing.
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		val, found, isLeader := node.Get(key)
		if !isLeader {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		if !found {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		fmt.Fprintln(w, val)
	})

	// /set takes a json body {"key": "...", "value": "..."} and
	// waits until the entry has been committed and applied.
	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.Key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		idx, ok := node.Submit(raft.Command{Op: "set", Key: body.Key, Value: body.Value})
		if !ok {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		if !node.WaitApply(idx, 3*time.Second) {
			http.Error(w, "timed out waiting for commit", http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok\n")
	})

	// /del?key=foo removes a key. blocks until committed.
	mux.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		idx, ok := node.Submit(raft.Command{Op: "del", Key: key})
		if !ok {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		if !node.WaitApply(idx, 3*time.Second) {
			http.Error(w, "timed out waiting for commit", http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok\n")
	})

	// /status returns this node's view of the cluster.
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(node.Status())
	})

	return mux
}
