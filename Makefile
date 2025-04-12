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
	cargo test --lib

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	cargo test --test '*'

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
test-all: test test-integration

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