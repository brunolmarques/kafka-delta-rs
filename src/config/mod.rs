// Responsible for parsing a YAML configuration file and merging with CLI arguments
// Dependencies: serde, serde_yaml, structopt/clap

use serde::Deserialize;
use std::path::Path;

use crate::delta::DeltaWriteMode;
use crate::handlers::{AppResult, ConfigError};
use crate::model::FieldConfig;
use crate::model::FieldType;

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

#[derive(Debug, Deserialize)]
pub struct DeltaConfig {
    pub table_path: String,
    pub partition: String,
    pub mode: DeltaWriteMode, // Supported modes: "UPSERT" or "INSERT"
    pub schema: Option<Vec<FieldConfig>>,
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
    pub thread_pool_size: Option<usize>, // None means unlimited
    pub retry_attempts: Option<usize>,   // None means no retries
}

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub max_buffer_size: Option<usize>, // Optional max buffer size for collecting messages then batch process
    pub max_wait_secs: Option<u64>, // Optional max wait time threshold between each batch processing
}

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

        let config: AppConfig = serde_yaml::from_str(&content).map_err(|e| {
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

        if config.delta.partition.trim().is_empty() {
            log::error!("Invalid config: delta.partition is empty");
            return Err(ConfigError::InvalidField("delta.partition is empty".to_string()).into());
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

        if config.delta.schema.is_none() {
            log::warn!("Warning: delta.schema is not set. All columns will be parsed as strings.");
        }

        // If a schema is present, check each field
        if let Some(ref fields) = config.delta.schema {
            for f in fields {
                f.validate()?;
            }
        }

        log::info!("Configuration file loaded successfully");
        Ok(config)
    }
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rand::random;
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
  partition: "day-time"
  mode: "INSERT"
  schema:
    - field: "id"
      type: "u64"
    - field: "name"
      type: "string"
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
            std::env::temp_dir().join(format!("temp_config_{}.yaml", random_number));
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

        // Check schema fields
        if let Some(schema) = &config.delta.schema {
            assert_eq!(schema.len(), 2);
            assert_eq!(schema[0].field, "id");
            assert_eq!(schema[0].type_name, FieldType::U64);
            assert_eq!(schema[1].field, "name");
            assert_eq!(schema[1].type_name, FieldType::String);
        } else {
            panic!("Schema should be present");
        }
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
    fn test_missing_delta_partition() {
        let yaml = dummy_yaml_config().replace("day-time", "");
        let res = load_config_from_str(&yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("delta.partition is empty"));
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
