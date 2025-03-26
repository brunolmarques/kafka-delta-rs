use crate::config::LoggingConfig;
use env_logger;
use log;
use std::str::FromStr; // Import LoggingConfig from config

// Centralizes logging functionality using the log crate and possibly env_logger.
// Initializes log level based on configuration.
pub fn init_logging(config: &LoggingConfig) {
    let level_filter = log::LevelFilter::from_str(&config.level).unwrap_or(log::LevelFilter::Info);
    env_logger::Builder::new().filter_level(level_filter).init();
    log::info!("Logging initialized at level: {}", config.level);
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_init() {
        // Instantiate LoggingConfig (assumes LoggingConfig has a field 'level' of type String)
        let config = LoggingConfig {
            level: "INFO".to_string(),
        };
        init_logging(&config);
        // ...existing test validations...
    }
}
