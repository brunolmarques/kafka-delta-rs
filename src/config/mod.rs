// Responsible for parsing a YAML configuration file and merging with CLI arguments
// Dependencies: serde, serde_yaml, structopt/clap

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub delta: DeltaConfig,
    pub logging: LoggingConfig,
    pub monitoring: MonitoringConfig,
    pub concurrency: ConcurrencyConfig,
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub broker: String,
    pub topic: String,
    pub group_id: String,
}

#[derive(Debug, Deserialize)]
pub struct DeltaConfig {
    pub table_path: String,
    pub partition: String,
    pub mode: String,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct MonitoringConfig {
    pub host: String,
    pub port: u16,
    pub endpoint: String, // e.g., "/metrics"
}

#[derive(Debug, Deserialize)]
pub struct ConcurrencyConfig {
    pub thread_pool_size: Option<usize>, // None means unlimited
    pub retry_attempts: Option<usize>,   // None means no retries
}

#[derive(Debug, Deserialize)]
pub struct BatchConfig {
    pub batch_interval: u64,           // in seconds
    pub batch_size: usize,             // max number of events in a batch
}

// Function to load YAML config file
pub fn load_config(path: &str) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let config: AppConfig = serde_yaml::from_str(&content)?;
    Ok(config)
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config() {
        // This test should load a sample YAML config and verify required fields are populated.
        let yaml = r#"
kafka:
  broker: "localhost:9092"
  topic: "topic1"
  group_id: "my_group"
delta:
  table_path: "/data/delta/table"
  partition: "day-time"
  mode: "INSERT"
logging:
  level: "INFO"
monitoring:
  host: "localhost"
  port: 9090
  endpoint: "/metrics"
concurrency:
  thread_pool_size: 4
  retry_attempts: 3
batch-config:
  batch_size: 1000
  batch_interval: 1000
        "#;
        let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.kafka.broker, "localhost:9092");
    }
}
