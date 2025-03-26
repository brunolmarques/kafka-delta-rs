// Entry point for the application.
// Parses command line arguments (e.g., using clap or structopt), loads the YAML configuration,
// initializes logging and monitoring, then orchestrates the Kafka consumption, consolidation,
// and Delta table writing in a multi-threaded fashion (using Tokio for async and Rayon for parallel tasks).

mod config;
mod delta;
mod handlers;
mod kafka;
mod logging;
mod monitoring;
mod pipeline;

use crate::config::load_config;
use crate::delta::{DeltaRsWriter, DeltaWriter};
use crate::kafka::{KafkaConsumer, RDKafkaConsumer};
use crate::logging::init_logging;
use crate::monitoring::init_monitoring;
use crate::pipeline::consolidate_data;
use std::env;

#[tokio::main]
async fn main() {
    // Parse command line arguments (example: config file path, override topic, table destination, thread pool size)
    let args: Vec<String> = env::args().collect();
    let config_path = args.get(1).unwrap_or(&"config.yaml".to_string()).clone();

    // Load configuration from YAML file
    let config = load_config(&config_path).expect("Failed to load configuration");

    // Initialize logging and monitoring
    init_logging(&config.logging);
    init_monitoring(config.monitoring.port);

    // Initialize Kafka consumer (this example uses a custom Kafka consumer implementation)
    let consumer = RDKafkaConsumer {
        broker: config.kafka.broker,
        topic: config.kafka.topic,
        group_id: config.kafka.group_id,
    };

    // Consume messages (in production, you might spawn tasks for each topic and perform retries)
    let records = consumer.consume().expect("Failed to consume messages");

    // Consolidate and deduplicate records (parallelized using Rayon inside the function)
    let consolidated_records = pipeline::consolidate_data(records);

    // Write the consolidated data to the Delta table (atomic operation)
    let writer = DeltaRsWriter {
        table_path: config.delta.table_path,
        partition: config.delta.partition,
    };
    writer
        .insert(&consolidated_records)
        .expect("Delta insert failed");

    // The application may log successes, update Prometheus metrics, and schedule daily operations.
    println!("Operation completed successfully.");
}
