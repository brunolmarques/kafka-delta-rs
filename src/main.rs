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
mod model;
mod util;

use std::env;
use std::sync::Arc;
use tokio::main;

use crate::config::AppConfig;
use crate::handlers::{AppError, AppResult};
use crate::kafka::KafkaConsumer;
use crate::logging::init_logging;
use crate::monitoring::Monitoring;
use crate::pipeline::Pipeline;

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

    // 2) Initialize logging and monitoring
    init_logging(&app_config.logging);
    let monitoring = Monitoring::init(&app_config.monitoring)?;

    // 3) Initialize the pipeline
    let pipeline = Arc::new(Pipeline::new(&app_config.delta, Some(&monitoring)));

    // 4) Initialize Kafka consumer
    let consumer = KafkaConsumer::new(&app_config, pipeline, Some(&monitoring))?;

    // TODO: finish implementing the run method

    // The application may log successes, update Prometheus metrics, and schedule daily operations.
    println!("Operation completed successfully.");

    Ok(())
}
