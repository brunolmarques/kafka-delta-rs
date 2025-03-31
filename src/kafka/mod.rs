// This module handles consuming messages from Kafka topics (with replicas) using a Rust Kafka crate (rdkafka)
// It implements a trait for the Kafka consumer functionality and includes error handling for connection/network issues.
use futures::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError as RdKafkaError;
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::AppConfig;
use crate::handlers::{AppError, AppResult, KafkaError};
use crate::pipeline::PipelineTrait;
use crate::monitoring::Monitoring;


pub struct KafkaConsumer<'a, T: PipelineTrait> {
    consumer: StreamConsumer,
    pipeline: Arc<T>,
    max_wait_secs: u64,
    max_buffer_size: usize,
    monitoring: Option<&'a Monitoring>,
}

impl<'a, T: PipelineTrait> KafkaConsumer<'a, T> {
    pub fn new(app_config: &AppConfig, pipeline: Arc<T>, monitoring: Option<&'a Monitoring>) -> AppResult<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &app_config.kafka.broker)
            .set("group.id", &app_config.kafka.group_id)
            .set("enable.partition.eof", "false")
            .set(
                "session.timeout.ms",
                &app_config.kafka.timeout.unwrap_or(5000).to_string(),
            )
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
            monitoring,
        })
    }

    pub async fn run(&self) -> AppResult<()> {
        let mut stream = self.consumer.stream();
        let mut last_flush = Instant::now();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Ok(borrowed_msg) => {
                    let size = &borrowed_msg.payload().map(|p| p.len() as u64).unwrap_or(0);
                    self.handle_message(borrowed_msg).await?;
                    if let Some(monitoring) = &self.monitoring {
                        monitoring.record_kafka_messages_read(1);
                        monitoring.record_kafka_messages_size(*size);
                    };
                },
                Err(e) => {
                    log::error!("Kafka read error: {:?}", e);
                    // Turn rdkafka::error::KafkaError into application custom error
                    return Err(AppError::Kafka(KafkaError::ReadError(format!(
                        "Kafka read error: {e}"
                    ))));
                }
            }

            let should_flush_by_size = self.pipeline.aggregator_len() >= self.max_buffer_size;
            let should_flush_by_time =
                last_flush.elapsed() >= Duration::from_secs(self.max_wait_secs);

            if should_flush_by_size || should_flush_by_time {
                // Flush the aggregator and, if successful, commit the consumer state.
                // This guarantees idempotency in case of failure.
                self.pipeline.flush().await?;
                self.consumer
                    .commit_consumer_state(CommitMode::Async)
                    .map_err(|e| {
                        log::error!("Commit failed: {e}");
                        AppError::Kafka(KafkaError::CommunicationLost(format!(
                            "Commit failed: {e}"
                        )))
                    })?;
                if let Some(monitoring) = &self.monitoring {
                    monitoring.record_kafka_commit();
                }
                log::info!("Flushed and committed successfully.");
                last_flush = Instant::now();
            }
        }

        // Final flush
        self.pipeline.flush().await?;
        self.consumer
            .commit_consumer_state(CommitMode::Async)
            .map_err(|e| {
                log::error!("Commit failed: {e}");
                AppError::Kafka(KafkaError::CommunicationLost(format!("Commit failed: {e}")))
            })?;

        Ok(())
    }

    // Modified to be generic over any M: Message.
    async fn handle_message<M: Message>(&self, msg: M) -> AppResult<()> {
        let offset = msg.offset();
        // Convert key if present.
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
    use rdkafka::message::Message;
    use std::sync::{Arc, Mutex};
    use crate::pipeline::PipelineTrait;
    use async_trait::async_trait;

    // Dummy implementation of Pipeline for testing.
    struct DummyPipeline {
        pub records: Mutex<Vec<(i64, Option<String>, String)>>,
    }

    impl DummyPipeline {
        fn new() -> Self {
            Self {
                records: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl PipelineTrait for DummyPipeline {
        fn aggregator_len(&self) -> usize {
            // dummy implementation for testing.
            0
        }

        async fn flush(&self) -> AppResult<()> {
            Ok(())
        }

        async fn insert_record(
            &self,
            offset: i64,
            key: Option<String>,
            payload: String,
        ) -> AppResult<()> {
            self.records.lock().unwrap().push((offset, key, payload));
            Ok(())
        }
    }

    // DummyMessage for testing handle_message.
    struct DummyMessage {
        offset: i64,
        key: Option<Vec<u8>>,
        payload: Option<String>,
    }

    impl DummyMessage {
        fn new(offset: i64, key: Option<&[u8]>, payload: Option<&str>) -> Self {
            Self {
                offset,
                key: key.map(|k| k.to_vec()),
                payload: payload.map(|s| s.to_string()),
            }
        }
    }

    impl Message for DummyMessage {
        type Headers = rdkafka::message::OwnedHeaders;

        fn payload(&self) -> Option<&[u8]> {
            self.payload.as_deref().map(|s| s.as_bytes())
        }

        unsafe fn payload_mut(&mut self) -> Option<&mut [u8]> {
            None
        }

        fn key(&self) -> Option<&[u8]> {
            self.key.as_deref()
        }

        fn topic(&self) -> &str {
            "dummy_topic"
        }

        fn partition(&self) -> i32 {
            0
        }

        fn offset(&self) -> i64 {
            self.offset
        }

        fn timestamp(&self) -> rdkafka::message::Timestamp {
            rdkafka::message::Timestamp::NotAvailable
        }

        fn headers(&self) -> Option<&<Self as Message>::Headers> { 
            None 
        }

        fn payload_view<'a, T: rdkafka::message::FromBytes + ?Sized>(
            &'a self,
        ) -> Option<Result<&'a T, T::Error>> {
            self.payload.as_ref().map(|s| T::from_bytes(s.as_bytes()))
        }
    }

    #[tokio::test]
    async fn test_handle_message_valid() {
        let dummy_pipeline = Arc::new(DummyPipeline::new());
        // Create a KafkaConsumer with a dummy consumer.
        let consumer = KafkaConsumer {
            // For testing, the consumer field is not used in handle_message.
            consumer: unsafe { std::mem::zeroed() },
            pipeline: dummy_pipeline.clone(),
            max_wait_secs: 360,
            max_buffer_size: 10000,
            monitoring: None,
        };
        let msg = DummyMessage::new(42, Some(b"test_key"), Some("test_payload"));
        consumer.handle_message(msg).await.unwrap();
        let records = dummy_pipeline.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        let (offset, key, payload) = &records[0];
        assert_eq!(*offset, 42);
        assert_eq!(key.as_deref(), Some("test_key"));
        assert_eq!(payload, "test_payload");
    }

    #[tokio::test]
    async fn test_handle_message_no_payload() {
        let dummy_pipeline = Arc::new(DummyPipeline::new());
        let consumer = KafkaConsumer {
            consumer: unsafe { std::mem::zeroed() },
            pipeline: dummy_pipeline.clone(),
            max_wait_secs: 360,
            max_buffer_size: 10000,
            monitoring: None,
        };
        // Simulate a message with no payload.
        let msg = DummyMessage::new(43, None, None);
        consumer.handle_message(msg).await.unwrap();
        let records = dummy_pipeline.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        let (offset, key, payload) = &records[0];
        assert_eq!(*offset, 43);
        assert!(key.is_none());
        assert_eq!(payload, "");
    }
}
