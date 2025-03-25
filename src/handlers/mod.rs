// Custom error definitions for the application.
// Use this module to wrap errors from network, connection, and schema issues.
use std::fmt;

#[derive(Debug)]
pub enum AppError {
    NetworkError(String),
    ConnectionError(String),
    SchemaError(String),
    LogicError(String),
}

impl std::error::Error for AppError {}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            AppError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            AppError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            AppError::LogicError(msg) => write!(f, "Logic error: {}", msg),
        }
    }
}


//---------------------------------------- Tests ----------------------------------------


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = AppError::NetworkError("timeout".to_string());
        assert_eq!(err.to_string(), "Network error: timeout");
    }
}
