// Custom error definitions for the application.
use thiserror::Error;

/// Errors related to configuration (YAML parsing, invalid fields, etc.)
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read or parse configuration file: {0}")]
    ReadError(String),

    #[error("Missing or invalid configuration field: {0}")]
    InvalidField(String),
}

/// Errors related to Kafka operations
#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Error connecting to Kafka broker: {0}")]
    BrokerConnection(String),

    #[error("Error reading from Kafka: {0}")]
    ReadError(String),

    #[error("Kafka communication lost: {0}")]
    CommunicationLost(String),

    #[error("Kafka timeout occurred: {0}")]
    Timeout(String),
}

/// Errors related to the data pipeline/aggregator
#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Failed to insert record into aggregator: {0}")]
    InsertError(String),

    #[error("Failed to flush aggregator: {0}")]
    FlushError(String),
}

/// Errors related to writing or reading data from Delta
#[derive(Error, Debug)]
pub enum DeltaError {
    #[error("Delta I/O error: {0}")]
    IoError(String),

    #[error("Delta table operation error: {0}")]
    TableError(String),
}

/// A top-level application error enum combining sub-errors
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Config error: {0}")]
    Config(#[from] ConfigError),

    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),

    #[error("Pipeline error: {0}")]
    Pipeline(#[from] PipelineError),

    #[error("Delta error: {0}")]
    Delta(#[from] DeltaError),

    #[error("Other error: {0}")]
    Other(String),
}

/// A specialized result type for our application
pub type AppResult<T> = std::result::Result<T, AppError>;

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::ReadError("file not found".into());
        assert_eq!(
            err.to_string(),
            "Failed to read or parse configuration file: file not found"
        );
    }

    #[test]
    fn test_kafka_error_display() {
        let err = KafkaError::BrokerConnection("unable to connect".into());
        assert_eq!(
            err.to_string(),
            "Error connecting to Kafka broker: unable to connect"
        );
    }

    #[test]
    fn test_pipeline_error_display() {
        let err = PipelineError::InsertError("insert failed".into());
        assert_eq!(
            err.to_string(),
            "Failed to insert record into aggregator: insert failed"
        );
    }

    #[test]
    fn test_delta_error_display() {
        let err = DeltaError::IoError("disk error".into());
        assert_eq!(err.to_string(), "Delta I/O error: disk error");
    }

    #[test]
    fn test_app_error_display() {
        // Test the AppError::Other variant.
        let err = AppError::Other("unknown error".into());
        assert_eq!(err.to_string(), "Other error: unknown error");

        // Test wrapping a KafkaError.
        let kafka_err = AppError::Kafka(KafkaError::ReadError("read failed".into()));
        assert_eq!(
            kafka_err.to_string(),
            "Kafka error: Error reading from Kafka: read failed"
        );
    }

}
