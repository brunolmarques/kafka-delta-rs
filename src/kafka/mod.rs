// This module handles consuming messages from Kafka topics (with replicas) using a Rust Kafka crate (rdkafka)
// It implements a trait for the Kafka consumer functionality and includes error handling for connection/network issues.
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, message::BorrowedMessage};
use rdkafka::error::KafkaError as RdKafkaError;
use std::sync::Arc;
use std::time::{Duration, Instant};
use futures::StreamExt;

use crate::handlers::{AppResult, AppError, KafkaError};
use crate::config::AppConfig;
use crate::pipeline::Pipeline;


pub struct KafkaConsumer {
    consumer: StreamConsumer,
    pipeline: Arc<Pipeline>,
    max_wait_secs: u64,
    max_buffer_size: usize,
}


impl KafkaConsumer {
    pub fn new(app_config: &AppConfig, pipeline: Arc<Pipeline> ) -> AppResult<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &app_config.kafka.broker)
            .set("group.id", &app_config.kafka.group_id)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", &app_config.kafka.timeout.unwrap_or(5000).to_string())
            .set("enable.auto.commit", "false")
            .create()
            .map_err(|e: RdKafkaError| {
                AppError::Kafka(KafkaError::BrokerConnection(format!(
                    "Could not create StreamConsumer: {e}"
                )))
            })?;

        let topics: Vec<&str> = app_config.kafka.topics.iter().map(|s| s.as_str()).collect();
        
        consumer
            .subscribe(&topics)
            .map_err(|e| KafkaError::BrokerConnection(format!("Subscription failed: {e}")))?;

        let max_wait_secs = app_config.pipeline.max_wait_secs.unwrap_or(360);
        let max_buffer_size = app_config.pipeline.max_buffer_size.unwrap_or(10000);

        Ok(Self {
            consumer,
            pipeline,
            max_wait_secs,
            max_buffer_size,
        })
    }

    pub async fn run(&self) -> AppResult<()> {
        let mut stream = self.consumer.stream();
        let mut last_flush = Instant::now();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Ok(borrowed_msg) => {
                    self.handle_message(borrowed_msg).await?;
                    // For more robust handling, commit only after a successful flush
                    self.consumer
                        .commit_consumer_state(CommitMode::Async)
                        .map_err(|e| {
                            AppError::Kafka(KafkaError::CommunicationLost(format!(
                                "Commit failed: {e}"
                            )))
                        })?;
                }
                Err(e) => {
                    // Turn rdkafka::error::KafkaError into our custom error
                    return Err(AppError::Kafka(KafkaError::ReadError(format!(
                        "Kafka read error: {e}"
                    ))));
                }
            }

            let should_flush_by_size = self.pipeline.aggregator_len() >= self.max_buffer_size;
            let should_flush_by_time = last_flush.elapsed() >= Duration::from_secs(self.max_wait_secs);

            if should_flush_by_size || should_flush_by_time {
                self.pipeline.flush().await?;
                last_flush = Instant::now();
            }
        }

        // Final flush
        self.pipeline.flush().await?;

        Ok(())
    }

    async fn handle_message(&self, msg: BorrowedMessage<'_>) -> AppResult<()> {
        let offset = msg.offset();
        let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
        let payload = match msg.payload_view::<str>() {
            None => "".to_string(),
            Some(Ok(s)) => s.to_string(),
            Some(Err(_)) => "<invalid utf-8>".to_string(),
        };

        self.pipeline.insert_record(offset, key, payload).await?;
        Ok(())
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
