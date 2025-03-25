# Kafka-Delta-Rust

Welcome to **Kafka-Delta-Rust**: a Rust-based application that reads data from a Kafka topic, consolidates and deduplicates records in parallel, and writes them daily into an existing Delta Lake table—partitioned by a specific column. This project is designed with extensibility, reliability, and performance in mind, using **Tokio** for concurrency, **Rayon** for parallelization, and **deltalake** for Delta Lake operations.

---

## Proposed Project Structure

Below is a high-level overview of how the project is structured. **Each module** in the `src/` directory handles a logical slice of functionality. Traits related to each module are defined in that module’s `mod.rs`, and integration tests live under an `tests/` directory.

```
kafka-delta-rust/
├── Cargo.toml
├── Dockerfile
├── .dockerignore
├── README.md
├── config.yaml                # Example YAML configuration
├── src/
│   ├── main.rs                # Entry point (handles CLI arguments, config parsing)
│   ├── config/
│   │   ├── mod.rs             # Traits and shared items for config
│   │   └── loader.rs          # YAML parsing and config struct definitions
│   ├── kafka/
│   │   ├── mod.rs             # Traits relevant to Kafka interaction
│   │   └── consumer.rs        # Kafka consumer implementation
│   ├── delta/
│   │   ├── mod.rs             # Traits for Delta Lake
│   │   └── writer.rs          # Logic for writing/merging data into Delta tables
│   ├── pipeline/
│   │   ├── mod.rs             # Traits for pipeline steps
│   │   └── processor.rs       # Consolidation and deduplication logic
│   ├── handlers/
│   │   ├── mod.rs             # Traits for error handling
│   │   └── types.rs           # Custom error types
│   ├── logging/
│   │   ├── mod.rs             # Traits for logging
│   │   └── prometheus.rs      # Prometheus integration
│   └── utils.rs               # Utility functions, if needed
└── tests/
    ├── integration.rs         # Integration tests
    └── ...                    # Additional integration test files
```

---

## Table of Contents

1. [Key Features](#key-features)
2. [Architecture Overview](#architecture-overview)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Usage](#usage)
6. [Logging & Monitoring](#logging--monitoring)
7. [Testing](#testing)
8. [Error Handling & Recovery](#error-handling--recovery)
9. [Contributing](#contributing)
10. [License](#license)

---

## Key Features

- **Multi-topic Kafka ingestion**: Reads from multiple Kafka topics (with possible replicas), using **Tokio** to handle asynchronous consumers.
- **Parallel processing**: Uses **Rayon** for CPU-bound tasks (data consolidation, deduplication) and guards against data races.
- **Delta Lake integration**: Writes data (daily partitions) to an existing Delta Lake table using **delta-rs**. Operations can be **INSERT** or **MERGE** for updates.
- **Idempotent and Atomic**: Ensures that if the process crashes mid-operation, it can resume from the last successful checkpoint without data corruption or duplication.
- **Configuration-as-code**: YAML-based configuration that can also be overridden via CLI arguments.
- **Logging & Monitoring**: Integrates with Prometheus for metrics and leverages standard Rust logging for runtime insights.
- **Dockerized**: Provides a multi-stage `Dockerfile` to build and deploy minimal images.
- **Extensible**: Adding support for new input sources (Flink, Spark, CDCs) and output targets (Parquet, SQL DBs) is straightforward thanks to a modular design.
- **Unit & Integration Tests**: Comprehensive testing strategy, with unit tests inside each module file, and integration tests in a separate `tests/` folder.

---

## Architecture Overview

The general data flow is as follows:

1. **Kafka Consumer**: A pool of async tasks (`Tokio`) reads from Kafka topics. Each task streams messages in real time.
2. **Buffering & Parallel Processing**:
   - Messages are batched, consolidated, and deduplicated in parallel using **Rayon**.
   - Data race conditions are prevented with robust synchronization primitives (e.g., locks or channels).
3. **Daily Write to Delta Lake**:
   - Once a day (or at a configured interval), the application writes the consolidated data to a partitioned Delta table.
   - Only the partition corresponding to the current day is affected, avoiding expensive full-table scans.
4. **Logging & Metrics**:
   - Logs are streamed to standard output or file (depending on config), and Prometheus metrics are published for monitoring.
5. **Recovery**:
   - If an interruption occurs, the application uses checkpoints (e.g., offsets, partial data files) to ensure the write is idempotent and consistent upon restart.

---

## Installation

### Prerequisites

- **Rust** (1.60 or later recommended).
- **Kafka** cluster accessible (with topics already set up).
- **Delta Lake** environment (the application must have permission to write/merge data).
- **Docker** (if you want to use containerization).

### Steps

1. **Clone this repository**:

   ```bash
   git clone https://github.com/brunolmarques/kafka-delta-rs.git
   cd kafka-delta-rust
   ```

2. **Build the application**:

   ```bash
   cargo build --release
   ```

   The compiled binary will be located in `target/release/kafka-delta-rust`.

3. **Optionally, build the Docker image**:

   ```bash
   docker build -t kafka-delta-rust:latest .
   ```

---

## Configuration

### YAML Configuration

A sample `config.yaml` is provided in the repository. Here’s a snippet:

```yaml
kafka:
  broker: "localhost:9092"
  topic: "topic_a"
  group_id: "kafka-delta-consumer-group"
  max_poll_records: 5000

delta:
  table_path: "/path/to/delta-table"
  partition_column: "date_col"
  mode: "INSERT"  # or "MERGE"

logging:
  level: "INFO"

monitoring:
  host: "localhost"
  port: 9090
  endpoint: "/metrics"

concurrency:
  num_threads: 4
  retry_attempts: 3

batch:
  batch_interval: 100,           # In seconds
  batch_size: 1000               # Max number of events

credentials:
  kafka_username: "user"
  kafka_password: "pass"
  delta_credentials: "gcp"       # Credentials to the cloud/local storage

# Additional config for advanced use cases ...
```

This file can be overridden at runtime by specifying an alternative path via a CLI argument (e.g., `--config ./my_config.yaml`).

### CLI Arguments

- `--config <FILE_PATH>`: Path to the YAML configuration file (default `./config.yaml`).
- Other arguments can be added as needed to override top-level or nested config fields.

---

## Usage

1. **Run the binary directly**:

   ```bash
   ./target/release/kafka-delta-rust --config ./config.yaml
   ```

2. **Run via Docker**:

   ```bash
   docker run \
       -v /path/to/config.yaml:/app/config.yaml \
       -v /path/to/delta-table:/delta-lake \
       kafka-delta-rust:latest \
       --config /app/config.yaml
   ```

3. **Runtime Behavior**:
   - The application will connect to the specified Kafka brokers, subscribe to each listed topic, and continuously pull records.
   - Data is consolidated in memory (according to your concurrency settings), deduplicated, and stored temporarily.
   - At the configured interval (e.g., daily at midnight), data is written to the Delta table in an **atomic**, partitioned manner.

---

## Logging & Monitoring

- **Rust Logger**: Configurable log levels in `config.yaml` (e.g., `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`). Logs are printed to stdout by default.
- **Prometheus**:
  - Exposes metrics at a configurable port (e.g., `localhost:9090/metrics`).
  - Tracks application health (e.g., processed record counts, offsets, error rates, memory usage).

Integrate these metrics into your existing Prometheus + Grafana stack to visualize pipeline throughput, latency, and error trends.

---

## Testing

1. **Unit Tests**:
   - Each module has corresponding **unit tests** in the same `.rs` file.
   - Run all tests:
   ```
   cargo test
   ```
   - Run tests for a specific module (e.g., `config`):
   ```
   cargo test config
   ```
   - Run a specific test function (e.g., `test_load_config` in module `config`):
   ```
   cargo test config::tests::test_load_config
   ```

2. **Integration Tests**:
   - Found in the `tests/` directory, focusing on end-to-end scenarios (Kafka ingestion + Delta write).
   - Run with:
   - (Placeholder: add integration test examples here.)
     ```bash
     cargo test --test integration_test
     ```

3. **Continuous Integration**:
   This project uses GitHub Actions for continuous integration. Everytime a new PR is pushed, the CI workflow will:

   - Check code formatting using `cargo fmt -- --check`
   - Lint the code using `cargo clippy`
   - Execute tests using `cargo test`

   To run these commands locally, use:

   ```
   cargo fmt -- --check
   cargo clippy -- -D warnings
   cargo test
   ```


---

## Error Handling & Recovery

- **Custom Errors**: Located in `src/errors/types.rs` for various categories (source connection, destination, network, logic, etc.).
- **Retries**: Built-in retry logic (configurable attempts) for transient errors like network failures.
- **Atomic & Idempotent Writes**: If the process crashes mid-write, the application picks up on restart using offsets and partial data. A **MERGE** or **INSERT**-only approach ensures no duplications occur.
- **Logging**: Errors are logged with relevant details. Non-recoverable errors produce stack traces before an orderly shutdown.

---

## Contributing

We welcome contributions! Please follow these steps:

1. **Fork & Clone**: Fork the repository, clone your fork locally.
2. **Create a Feature Branch**: For new features or bug fixes.
3. **Coding Standards**:
   - Keep lines under 100 characters where possible.
   - Use Rust best practices for error handling and concurrency.
   - Limit **dynamic dispatch** in favor of static dispatch for performance.
4. **Testing**: Add or update unit/integration tests.
5. **Pull Request**: Submit a PR with a clear description. Our CI pipeline will run automatically.

---

## License

This project is licensed under the [MIT License](./LICENSE).

---

**Thank you for using Kafka-Delta-Rust!** If you have any questions, feel free to open an issue or submit a pull request. We aim to create a robust, extensible pipeline for near-real-time data processing from Kafka to Delta Lake in Rust.
