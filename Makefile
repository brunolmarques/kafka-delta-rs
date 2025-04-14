.PHONY: all fmt lint test test-integration build clean

# Default target
all: fmt lint test build

# Format code using rustfmt
fmt:
	@echo "Formatting code..."
	cargo fmt --all

# Run clippy for linting
lint:
	@echo "Running linter..."
	cargo clippy -- -D warnings

# Run unit tests
test:
	@echo "Running unit tests..."
	cargo test --package kafka-delta-rs -- --nocapture

# Run unit tests for specific modules
test-modules:
	@echo "Running unit tests for specific modules..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs -- --nocapture --test-threads=1

# Run tests for a specific module
test-config:
	@echo "Running tests for config module..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs config -- --nocapture

test-kafka:
	@echo "Running tests for kafka module..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs kafka -- --nocapture

test-utils:
	@echo "Running tests for utils module..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs utils -- --nocapture

test-handlers:
	@echo "Running tests for handlers module..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs handlers -- --nocapture

test-monitoring:
	@echo "Running tests for monitoring module..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs monitoring -- --nocapture

test-pipeline:
	@echo "Running tests for pipeline module..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs pipeline -- --nocapture

test-logging:
	@echo "Running tests for logging module..."
	RUSTFLAGS=-Awarnings cargo test --package kafka-delta-rs logging -- --nocapture

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	RUSTFLAGS=-Awarnings cargo test integration

# Build the project
build:
	@echo "Building project..."
	cargo build

# Build the project in release mode
release:
	@echo "Building project in release mode..."
	cargo build --release

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean

# Run all tests (unit and integration)
test-all: 
	@echo "Running all tests..."
	cargo test --package kafka-delta-rs -- --nocapture --quiet

# Check if the project compiles without generating artifacts
check:
	@echo "Checking if project compiles..."
	cargo check

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	cargo bench

# Run documentation tests
doc-test:
	@echo "Running documentation tests..."
	cargo test --doc

# Generate documentation
doc:
	@echo "Generating documentation..."
	cargo doc --no-deps --open

# Help target
help:
	@echo "Available targets:"
	@echo "  all            - Run fmt, lint, test, and build"
	@echo "  fmt            - Format code using rustfmt"
	@echo "  lint           - Run clippy for linting"
	@echo "  test           - Run unit tests"
	@echo "  test-modules   - Run unit tests for all modules"
	@echo "  test-config    - Run tests for config module"
	@echo "  test-kafka     - Run tests for kafka module"
	@echo "  test-utils     - Run tests for utils module"
	@echo "  test-handlers  - Run tests for handlers module"
	@echo "  test-monitoring - Run tests for monitoring module"
	@echo "  test-pipeline  - Run tests for pipeline module"
	@echo "  test-logging   - Run tests for logging module"
	@echo "  test-integration - Run integration tests"
	@echo "  test-all       - Run all tests (unit and integration)"
	@echo "  build          - Build the project"
	@echo "  release        - Build the project in release mode"
	@echo "  clean          - Clean build artifacts"
	@echo "  check          - Check if the project compiles"
	@echo "  bench          - Run benchmarks"
	@echo "  doc-test       - Run documentation tests"
	@echo "  doc            - Generate documentation"
	@echo "  help           - Show this help message" 