.PHONY: build test test-race clean lint smoke-test smoke-test-sim integration-test install

BINARY_NAME=pg_workload
GO=go

# Build binary
build:
	$(GO) build -o $(BINARY_NAME) ./cmd/pg_workload

# Install to GOPATH/bin
install:
	$(GO) install ./cmd/pg_workload

# Run unit tests
test:
	$(GO) test -v ./...

# Run tests with race detector
test-race:
	$(GO) test -race -v ./...

# Run linter
lint:
	golangci-lint run ./...

# Clean build artifacts
clean:
	$(GO) clean
	rm -f $(BINARY_NAME)
	rm -f report.json sim_report.json sim_report_quick.json
	rm -f timeline.csv
	rm -rf /tmp/pg_workload_sim

# Smoke test: 1 minute run (requires PostgreSQL)
# Usage: make smoke-test PGHOST=localhost PGUSER=postgres
smoke-test: build
	@echo "Running smoke test (1 minute)..."
	@echo "Ensure PostgreSQL is running and accessible"
	./$(BINARY_NAME) run \
		--duration 1m \
		--warmup 10s \
		--cooldown 10s \
		--workers 2 \
		--connections 4 \
		--output report.json
	@echo "Smoke test complete. Report saved to report.json"
	@cat report.json | head -50

# Simulation smoke test: 5 minute simulation (requires PostgreSQL)
# Simulates 1 hour of workload in 5 minutes (12x time scale)
# Usage: make smoke-test-sim PGHOST=localhost PGUSER=postgres
smoke-test-sim: build
	@echo "Running simulation smoke test (5 minutes real time, simulating 1 hour)..."
	@echo "Ensure PostgreSQL is running and accessible"
	./$(BINARY_NAME) run \
		--mode simulation \
		--duration 1h \
		--time-scale 12 \
		--workers 4 \
		--connections 8 \
		--aggregate-interval 1m \
		--timeline-output timeline.csv \
		--output sim_report.json
	@echo "Simulation smoke test complete."
	@echo "Report saved to sim_report.json"
	@echo "Timeline saved to timeline.csv"
	@cat sim_report.json | head -80

# Quick simulation smoke test: 2 minute simulation (requires PostgreSQL)
# Simulates 24 minutes of workload in 2 minutes real time (12x time scale)
# Usage: make smoke-test-sim-quick PGHOST=localhost PGUSER=postgres
smoke-test-sim-quick: build
	@echo "Running quick simulation smoke test (2 minutes real time)..."
	@echo "Ensure PostgreSQL is running and accessible"
	./$(BINARY_NAME) run \
		--mode simulation \
		--duration 24m \
		--time-scale 12 \
		--workers 2 \
		--connections 4 \
		--aggregate-interval 30s \
		--output sim_report_quick.json
	@echo "Quick simulation smoke test complete."
	@cat sim_report_quick.json | head -50

# Integration tests (requires PostgreSQL)
integration-test: build
	PGHOST=$${PGHOST:-localhost} PG_TEST=1 $(GO) test -v -race ./test/integration/...

# Quick validation (no PostgreSQL required)
validate: build test
	./$(BINARY_NAME) --help
	./$(BINARY_NAME) version
	./$(BINARY_NAME) run --help
	@echo "Validation complete"

.DEFAULT_GOAL := build
