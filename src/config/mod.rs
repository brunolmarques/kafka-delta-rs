// Responsible for parsing a YAML configuration file and merging with CLI arguments
// Dependencies: serde, serde_yaml, structopt/clap
use serde::Deserialize;
use std::path::Path;

use crate::handlers::{AppResult, ConfigError};

#[allow(dead_code)] // TODO: remove
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub delta: DeltaConfig,
    pub logging: LoggingConfig,
    pub monitoring: MonitoringConfig,
    pub concurrency: ConcurrencyConfig,
    pub pipeline: PipelineConfig,
    pub credentials: CredentialsConfig,
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub broker: String,
    pub topics: Vec<String>,
    pub dead_letter_topic: Option<String>, // Optional: for dead letter queue
    pub group_id: String,
    pub timeout: Option<u64>, // Optional: timeout for Kafka operations, default is 5000ms
}

#[allow(dead_code)] // TODO: remove 
#[derive(Debug, Deserialize)]
pub struct DeltaConfig {
    /// Path to the Delta table
    pub table_path: String,
    /// Write mode for Delta table. Supported modes: "UPSERT" or "INSERT"
    pub mode: DeltaWriteMode,
    /// Message format (JSON or gRPC)
    pub message_format: MessageFormat,
    /// Buffer size for batching messages before writing to Delta
    pub buffer_size: Option<usize>,
}

/// Message format for parsing Kafka messages
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MessageFormat {
    /// JSON format
    Json,
    /// gRPC format
    Grpc,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "PascalCase")]
pub enum DeltaWriteMode {
    Insert,
    Upsert,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String, // Supported levels: "OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"
}

#[derive(Debug, Deserialize)]
pub struct MonitoringConfig {
    /// Whether or not monitoring is enabled
    pub enabled: bool,
    pub service_name: String,
    /// Pull-based endpoint for metrics
    pub endpoint: String, // e.g., "/metrics"
}

#[derive(Debug, Deserialize)]
pub struct ConcurrencyConfig {
    #[allow(dead_code)] // TODO: remove
    pub thread_pool_size: Option<usize>, // None means unlimited
    #[allow(dead_code)] // TODO: remove
    pub retry_attempts: Option<usize>, // None means no retries
}

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub max_buffer_size: Option<usize>, // Optional max buffer size for collecting messages then batch process
    pub max_wait_secs: Option<u64>, // Optional max wait time threshold between each batch processing
}

#[allow(dead_code)] // TODO: remove
#[derive(Debug, Deserialize)]
pub struct CredentialsConfig {
    pub kafka_username: String,
    pub kafka_password: String,
    pub delta_credentials: String,
}

// Methods to load YAML config file
impl AppConfig {
    pub fn load_config<P: AsRef<Path>>(path: P) -> AppResult<Self> {
        log::info!("Loading configuration file: {:?}", path.as_ref());
        let content = std::fs::read_to_string(&path).map_err(|e| {
            log::error!(
                "Failed to read configuration file {:?}: {}",
                path.as_ref(),
                e
            );
            ConfigError::ReadError(format!(
                "Failed to read configuration file {:?}: {}",
                path.as_ref(),
                e
            ))
        })?;

        let mut config: AppConfig = serde_yaml::from_str(&content).map_err(|e| {
            log::error!("YAML parse error in {:?}: {}", path.as_ref(), e);
            ConfigError::ReadError(format!("YAML parse error in {:?}: {}", path.as_ref(), e))
        })?;

        // Validate mandatory fields
        if config.kafka.broker.trim().is_empty() {
            log::error!("Invalid config: kafka.broker is empty");
            return Err(ConfigError::InvalidField("kafka.broker is empty".to_string()).into());
        }

        if config.kafka.topics.is_empty() {
            log::error!("Invalid config: kafka.topics should have at least one topic");
            return Err(ConfigError::InvalidField(
                "kafka.topics should have at least one topic".to_string(),
            )
            .into());
        }

        if config.kafka.group_id.trim().is_empty() {
            log::error!("Invalid config: kafka.group_id is empty");
            return Err(ConfigError::InvalidField("kafka.group_id is empty".to_string()).into());
        }

        if config.delta.table_path.trim().is_empty() {
            log::error!("Invalid config: delta.table_path is empty");
            return Err(ConfigError::InvalidField("delta.table_path is empty".to_string()).into());
        }

        if config.pipeline.max_buffer_size.is_none() || config.pipeline.max_buffer_size.unwrap() < 1
        {
            log::warn!(
                "Warning: max_buffer_size is not set or is less than 1. Defaulting to 10000."
            );
        }

        if config.pipeline.max_wait_secs.is_none() || config.pipeline.max_wait_secs.unwrap() < 1 {
            log::warn!(
                "Warning: max_wait_secs is not set or is less than 1. Defaulting to 360 seconds."
            );
        }

        if config.kafka.timeout.is_none() {
            log::warn!("Warning: kafka.timeout is not set. Defaulting to 5000ms.");
        }

        if config.monitoring.enabled && config.monitoring.endpoint.is_empty() {
            log::error!("Invalid config: monitoring.endpoint is empty");
            return Err(
                ConfigError::InvalidField("monitoring.endpoint is empty".to_string()).into(),
            );
        }

        // Validate the table path and store the parsed path
        let parsed_path = deltalake::Path::parse(&config.delta.table_path).map_err(|e| {
            log::error!("Invalid Delta table path: {e}");
            ConfigError::InvalidField(format!("Invalid Delta table path: {e}"))
        })?;

        // Store the validated path back in the config
        config.delta.table_path = parsed_path.to_string();

        log::info!("Configuration file loaded successfully");
        Ok(config)
    }
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    // Updated helper to generate a dummy valid YAML config for tests.
    fn dummy_yaml_config() -> String {
        r#"
kafka:
  broker: "localhost:9092"
  topics: ["topic1"]
  group_id: "my_group"
  timeout: 5000
delta:
  table_path: "/data/delta/table"
  mode: Insert
  message_format: json
  buffer_size: 10
logging:
  level: "INFO"
monitoring:
  enabled: true
  service_name: "monitor_service"
  endpoint: "/metrics"
concurrency:
  thread_pool_size: 4
  retry_attempts: 3
pipeline:
  max_buffer_size: 1000
  max_wait_secs: 100
credentials:
  kafka_username: "user"
  kafka_password: "pass"
  delta_credentials: "cred"
        "#
        .to_string()
    }

    // Helper to load config from a YAML string using a temporary file.
    fn load_config_from_str(yaml: &str) -> AppResult<AppConfig> {
        let random_number: u64 = rand::random();
        let tmp_path: PathBuf =
            std::env::temp_dir().join(format!("temp_config_{random_number}.yaml"));
        fs::write(&tmp_path, yaml).unwrap();
        let config = AppConfig::load_config(&tmp_path);
        fs::remove_file(&tmp_path).unwrap_or(());
        config
    }

    #[test]
    fn test_load_config() {
        let yaml = dummy_yaml_config();
        let config: AppConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(config.kafka.broker, "localhost:9092");
    }

    #[test]
    fn test_missing_kafka_broker() {
        let yaml = dummy_yaml_config().replace("localhost:9092", "");
        let res = load_config_from_str(&yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("kafka.broker is empty"));
    }

    #[test]
    fn test_missing_kafka_topics() {
        let yaml = dummy_yaml_config().replace("[\"topic1\"]", "[]");
        let res = load_config_from_str(&yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("kafka.topics should have at least one topic"));
    }

    #[test]
    fn test_missing_kafka_group_id() {
        let yaml = dummy_yaml_config().replace("my_group", "");
        let res = load_config_from_str(&yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("kafka.group_id is empty"));
    }

    #[test]
    fn test_missing_delta_table_path() {
        let yaml = dummy_yaml_config().replace("/data/delta/table", "");
        let res = load_config_from_str(&yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("delta.table_path is empty"));
    }

    #[test]
    fn test_valid_config() {
        let yaml = dummy_yaml_config();
        let config = load_config_from_str(&yaml).unwrap();
        assert_eq!(config.kafka.broker, "localhost:9092");
    }

    #[test]
    fn test_nonexistent_yaml() {
        let path: PathBuf = std::env::temp_dir().join("nonexistent_file.yaml");
        let res = AppConfig::load_config(&path);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("Failed to read configuration file"));
    }
}
