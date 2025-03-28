// Responsible for parsing a YAML configuration file and merging with CLI arguments
// Dependencies: serde, serde_yaml, structopt/clap

use serde::Deserialize;
use std::path::Path;

use crate::handlers::{AppResult, ConfigError};

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
    pub group_id: String,
    pub timeout: Option<u64>, // Optional: timeout for Kafka operations, default is 5000ms
}

#[derive(Debug, Deserialize)]
pub struct DeltaConfig {
    pub table_path: String,
    pub partition: String,
    pub mode: String,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String, // Supported levels: "OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"
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
        if config.kafka.broker.is_empty() {
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
        if config.kafka.group_id.is_empty() {
            log::error!("Invalid config: kafka.group_id is empty");
            return Err(ConfigError::InvalidField("kafka.group_id is empty".to_string()).into());
        }
        if config.delta.table_path.is_empty() {
            log::error!("Invalid config: delta.table_path is empty");
            return Err(ConfigError::InvalidField("delta.table_path is empty".to_string()).into());
        }
        if config.delta.partition.is_empty() {
            log::error!("Invalid config: delta.partition is empty");
            return Err(ConfigError::InvalidField("delta.partition is empty".to_string()).into());
        }

        if config.pipeline.max_buffer_size.is_none() || config.pipeline.max_buffer_size.unwrap() < 1 {
            log::warn!("Warning: max_buffer_size is not set or is less than 1. Defaulting to 10000.");
        }

        if config.pipeline.max_wait_secs.is_none() || config.pipeline.max_wait_secs.unwrap() < 1 {
            log::warn!("Warning: max_wait_secs is not set or is less than 1. Defaulting to 360 seconds.");
        }

        if config.kafka.timeout.is_none() {
            log::warn!("Warning: kafka.timeout is not set. Defaulting to 5000ms.");
        }

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

    // Helper function to load config from a YAML string using a temporary file.
    fn load_config_from_str(yaml: &str) -> AppResult<AppConfig> {
        let tmp_path: PathBuf = std::env::temp_dir().join("temp_config.yaml");
        fs::write(&tmp_path, yaml).unwrap();
        let config = AppConfig::load_config(&tmp_path);
        fs::remove_file(&tmp_path).unwrap();
        config
    }

    #[test]
    fn test_load_config() {
        let yaml = r#"
kafka:
  broker: "localhost:9092"
  topics: 
    - "topic1"
  group_id: "my_group"
  timeout: 5000
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
batch:
  max_buffer_size: 1000
  max_wait_secs: 100
        "#;
        let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.kafka.broker, "localhost:9092");
    }

    #[test]
    fn test_missing_kafka_broker() {
        let yaml = r#"
kafka:
  broker: ""
  topics: ["topic1"]
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
batch:
  max_buffer_size: 1000
  max_wait_secs: 100
        "#;
        let res = load_config_from_str(yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("kafka.broker is empty"));
    }

    #[test]
    fn test_missing_kafka_topics() {
        let yaml = r#"
kafka:
  broker: "localhost:9092"
  topics: []
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
batch:
  max_buffer_size: 1000
  max_wait_secs: 100
        "#;
        let res = load_config_from_str(yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("kafka.topics should have at least one topic"));
    }

    #[test]
    fn test_missing_kafka_group_id() {
        let yaml = r#"
kafka:
  broker: "localhost:9092"
  topics: ["topic1"]
  group_id: ""
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
batch:
  max_buffer_size: 1000
  max_wait_secs: 100
        "#;
        let res = load_config_from_str(yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("kafka.group_id is empty"));
    }

    #[test]
    fn test_missing_delta_table_path() {
        let yaml = r#"
kafka:
  broker: "localhost:9092"
  topics: ["topic1"]
  group_id: "my_group"
delta:
  table_path: ""
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
batch:
  max_buffer_size: 1000
  max_wait_secs: 100
        "#;
        let res = load_config_from_str(yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("delta.table_path is empty"));
    }

    #[test]
    fn test_missing_delta_partition() {
        let yaml = r#"
kafka:
  broker: "localhost:9092"
  topics: ["topic1"]
  group_id: "my_group"
delta:
  table_path: "/data/delta/table"
  partition: ""
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
batch:
  max_buffer_size: 1000
  max_wait_secs: 100
        "#;
        let res = load_config_from_str(yaml);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("delta.partition is empty"));
    }

    #[test]
    fn test_valid_config() {
        let yaml = r#"
kafka:
  broker: "localhost:9092"
  topics: ["topic1"]
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
batch:
  max_buffer_size: 1000
  max_wait_secs: 100
        "#;
        let config = load_config_from_str(yaml).unwrap();
        assert_eq!(config.kafka.broker, "localhost:9092");
    }

    #[test]
    fn test_nonexistent_yaml() {
        use std::path::PathBuf;
        let path: PathBuf = std::env::temp_dir().join("nonexistent_file.yaml");
        let res = AppConfig::load_config(&path);
        assert!(res.is_err());
        let err = format!("{:?}", res.err().unwrap());
        assert!(err.contains("Failed to read configuration file"));
    }
}
