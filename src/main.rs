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

use std::env;

use crate::handlers::{AppError, AppResult};
use crate::config::AppConfig;
use crate::pipeline::Pipeline;
use crate::kafka::KafkaConsumer;
use std::sync::Arc;
use tokio::main;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Application error: {:?}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), AppError> {
    let args: Vec<String> = env::args().collect();
    let config_file = args.get(1).map(|s| s.as_str()).unwrap_or("my_config.yaml");

    // 1) Load config
    let app_config = AppConfig::load_config(config_file)?;


    // Initialize logging and monitoring
    init_logging(&config.logging);
    init_monitoring(config.monitoring.port);

    // Initialize Kafka consumer (this example uses a custom Kafka consumer implementation)
    let consumer = RDKafkaConsumer {
        broker: config.kafka.broker,
        topic: config.kafka.topics,
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

    Ok(())
}
