# raft-implementation makefile.

BIN := bin/raftkv
LOG_DIR := .run

# peers for each node, excluding self.
PEERS_1 := 2=127.0.0.1:8002,3=127.0.0.1:8003
PEERS_2 := 1=127.0.0.1:8001,3=127.0.0.1:8003
PEERS_3 := 1=127.0.0.1:8001,2=127.0.0.1:8002

HTTP_PORTS := 9001 9002 9003

.PHONY: help build node-1 node-2 node-3 cluster stop status logs vet clean

help: ## show available targets
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## compile the raftkv binary into bin/
	@mkdir -p bin
	go build -o $(BIN) ./cmd/raftkv

vet:
	go vet ./...

# foreground runs
node-1: build ## run node 1 in the foreground
	$(BIN) -id=1 -rpc=127.0.0.1:8001 -http=127.0.0.1:9001 -peers="$(PEERS_1)"

node-2: build ## run node 2 in the foreground
	$(BIN) -id=2 -rpc=127.0.0.1:8002 -http=127.0.0.1:9002 -peers="$(PEERS_2)"

node-3: build ## run node 3 in the foreground
	$(BIN) -id=3 -rpc=127.0.0.1:8003 -http=127.0.0.1:9003 -peers="$(PEERS_3)"

# background run - all three nodes detached
cluster: build ## start a 3-node cluster in the background
	@mkdir -p $(LOG_DIR)
	@$(BIN) -id=1 -rpc=127.0.0.1:8001 -http=127.0.0.1:9001 -peers="$(PEERS_1)" > $(LOG_DIR)/n1.log 2>&1 & echo $$! > $(LOG_DIR)/n1.pid
	@$(BIN) -id=2 -rpc=127.0.0.1:8002 -http=127.0.0.1:9002 -peers="$(PEERS_2)" > $(LOG_DIR)/n2.log 2>&1 & echo $$! > $(LOG_DIR)/n2.pid
	@$(BIN) -id=3 -rpc=127.0.0.1:8003 -http=127.0.0.1:9003 -peers="$(PEERS_3)" > $(LOG_DIR)/n3.log 2>&1 & echo $$! > $(LOG_DIR)/n3.pid
	@sleep 1
	@echo "cluster started, logs in $(LOG_DIR)/"
	@$(MAKE) -s status

stop: ## kill every running raftkv process
	@for f in $(LOG_DIR)/n1.pid $(LOG_DIR)/n2.pid $(LOG_DIR)/n3.pid; do \
		if [ -f $$f ]; then kill $$(cat $$f) 2>/dev/null || true; rm -f $$f; fi; \
	done
	@pkill -TERM -f "$(BIN)" 2>/dev/null || true
	@echo "stopped"

status: ## print the status of each running node
	@for p in $(HTTP_PORTS); do \
		printf "node on %s: " $$p; \
		curl -s --max-time 1 http://127.0.0.1:$$p/status || echo "(unreachable)"; \
		echo; \
	done

logs: ## tail the background cluster logs as one merged stream
	@tail -q -F $(LOG_DIR)/n1.log $(LOG_DIR)/n2.log $(LOG_DIR)/n3.log

# demo writes a couple of keys to the current leader. it asks every
# node who they are, then sends writes to whichever one is leader.
demo: ## write a couple of keys via the current leader
	@leader=""; \
	for p in $(HTTP_PORTS); do \
		st=$$(curl -s --max-time 1 http://127.0.0.1:$$p/status); \
		case "$$st" in *'"state":"leader"'*) leader=$$p; break;; esac; \
	done; \
	if [ -z "$$leader" ]; then echo "no leader found, is the cluster up?"; exit 1; fi; \
	echo "leader is on $$leader"; \
	curl -s -X POST -d '{"key":"hello","value":"world"}' http://127.0.0.1:$$leader/set; \
	curl -s -X POST -d '{"key":"foo","value":"bar"}'     http://127.0.0.1:$$leader/set; \
	echo "hello -> $$(curl -s http://127.0.0.1:$$leader/get?key=hello)"; \
	echo "foo   -> $$(curl -s http://127.0.0.1:$$leader/get?key=foo)"

clean: stop ## stop the cluster and remove built artefacts and logs
	rm -rf bin $(LOG_DIR)
