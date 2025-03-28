// This module handles consuming messages from Kafka topics (with replicas) using a Rust Kafka crate (rdkafka)
// It implements a trait for the Kafka consumer functionality and includes error handling for connection/network issues.
use rdkafka::{config::ClientConfig, consumer::StreamConsumer};
use rdkafka::consumer::Consumer;
use async_trait::async_trait;
use std::sync::Arc;
use anyhow::Result;

use crate::config::AppConfig;
use crate::pipeline::Pipeline;
use crate::{config::KafkaConfig, handlers::AppError};


// Trait for Kafka consumers.
#[async_trait]
pub trait KafkaConsumer {
    // Changed method signature to async.
    async fn consume(&self) -> Result<Vec<String>, AppError>;
}


pub struct KafkaConsumer {
    consumer: StreamConsumer,
    pipeline: Arc<Pipeline>,
    max_wait_secs: u64,
    max_buffer_size: usize,
}

impl KafkaConsumer {
    pub fn new(app_config: &AppConfig) -> Self {
        Self {
            broker: app_config.kafka.broker.clone(),
            topics: app_config.kafka.topics.clone(),
            group_id: app_config.kafka.group_id.clone(),
        }
    }
}

#[async_trait]
impl KafkaConsumer for RDKafkaConsumer {
    async fn consume(&self) -> Result<Vec<String>, AppError> {
        log::info!("Connecting to Kafka broker: {}", self.broker);
        
        // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &self.group_id)
            .set("bootstrap.servers", &self.broker)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")  // Offsets will be committed manually
            .create()
            .map_err(|e| AppError::ConnectionError(e.to_string()))?;
        

        log::info!("Consuming messages from topic: {}", self.topic);
        
        // Simulate asynchronous consumption logic with a timeout.
        consumer
            .subscribe(&[&self.topic])
            .map_err(|e| AppError::ConsumptionError(e.to_string()))?;

        match consumption {
            Ok(Ok(records)) => {
                log::info!("Consumed {} records", records.len());
                Ok(records)
            }
            Ok(Err(e)) => {
                log::error!("Error during consumption: {}", e);
                Err(AppError::ConsumptionError(e))
            }
            Err(_) => {
                log::error!("Timeout reached while consuming messages.");
                Err(AppError::ConnectionError("Timeout reached".to_string()))
            }
        }
    }
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_consume_messages() {
        let consumer = RDKafkaConsumer {
            broker: "localhost:9092".into(),
            topic: "test_topic".into(),
            group_id: "test_group".into(),
        };
        // Await the async consume call.
        let result = consumer.consume().await;
        assert!(result.is_ok());
    }
}
