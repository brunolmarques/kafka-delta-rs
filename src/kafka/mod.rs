// This module handles consuming messages from Kafka topics (with replicas) using a Rust Kafka crate (rdkafka)
// It implements a trait for the Kafka consumer functionality and includes error handling for connection/network issues.
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError as RdKafkaError;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::config::{AppConfig, MessageFormat};
use crate::handlers::{AppError, AppResult, KafkaError};
use crate::monitoring::Monitoring;
use crate::pipeline::{Pipeline, PipelineTrait};
use crate::utils::{parse_grpc_object, parse_json_object};

// KafkaProducer struct to handle sending bad messages to the dead letter topics
pub struct KafkaProducer<'a> {
    producer: FutureProducer,
    dead_letter_topic: String,
    monitoring: Option<&'a Monitoring>,
}

impl<'a> KafkaProducer<'a> {
    pub fn new(app_config: &AppConfig, monitoring: Option<&'a Monitoring>) -> AppResult<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &app_config.kafka.broker)
            .create()
            .map_err(|e: RdKafkaError| {
                AppError::Kafka(KafkaError::BrokerConnection(format!(
                    "Could not create FutureProducer: {e}"
                )))
            })?;

        Ok(Self {
            producer,
            dead_letter_topic: app_config
                .kafka
                .dead_letter_topic
                .clone()
                .unwrap_or_default(),
            monitoring,
        })
    }
    pub async fn send_to_dead_letter_topic(
        &self,
        key: Option<String>,
        payload: String,
    ) -> AppResult<()> {
        let mut attempts = 0;
        let max_attempts = 3;
        loop {
            let record = FutureRecord::to(&self.dead_letter_topic)
                .key(key.as_deref().unwrap_or(""))
                .payload(&payload);

            match self.producer.send(record, Duration::from_secs(0)).await {
                Ok(_) => {
                    if let Some(monitoring) = self.monitoring {
                        monitoring.record_dead_letters(1);
                    }
                    log::info!("Successfully sent message to dead letter topic");
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    log::error!("Attempt {attempts}: Failed to send to dead letter topic: {e:?}");
                    if attempts >= max_attempts {
                        log::error!("Max attempts reached. Giving up.");
                        return Err(AppError::Kafka(KafkaError::CommunicationLost(format!(
                            "Failed after {attempts} attempts: {e:?}"
                        ))));
                    }
                    // Delay before retrying
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

pub struct KafkaConsumer<'a> {
    consumer: StreamConsumer,
    pipeline: Arc<Mutex<Pipeline<'a>>>,
    max_wait_secs: u64,
    max_buffer_size: usize,
    dead_letter_producer: Option<KafkaProducer<'a>>,
    monitoring: Option<&'a Monitoring>,
}

/// KafkaConsumer trait implementation to define the interface for Kafka consumers.
/// Example of a Kafka message format:
///
/// Message Metadata
/// - Topic: "my_topic"
/// - Partition: 0
/// - Offset: 12345
///
/// Message Payload
/// ```json
/// {
///   "key": "some_key",
///   "payload": {
///     "field1": "value1",
///     "field2": "value2"
///   }
/// }
/// ```
/// The key is optional and the payload is a JSON object.
/// The offset is used to track the position of the message in the Kafka topic.
impl<'a> KafkaConsumer<'a> {
    pub fn new(
        app_config: &AppConfig,
        pipeline: Arc<Mutex<Pipeline<'a>>>,
        monitoring: Option<&'a Monitoring>,
    ) -> AppResult<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &app_config.kafka.broker)
            .set("group.id", &app_config.kafka.group_id)
            .set("enable.partition.eof", "false")
            .set(
                "session.timeout.ms",
                app_config.kafka.timeout.unwrap_or(5000).to_string(),
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

        let mut dead_letter_producer = None;
        if app_config.kafka.dead_letter_topic.is_some() {
            dead_letter_producer = Some(KafkaProducer::new(app_config, monitoring)?)
        }

        Ok(Self {
            consumer,
            pipeline,
            max_wait_secs,
            max_buffer_size,
            dead_letter_producer,
            monitoring,
        })
    }

    #[allow(dead_code)]
    pub async fn run(&self) -> AppResult<()> {
        let mut stream = self.consumer.stream();
        let mut last_flush = Instant::now();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Ok(borrowed_msg) => {
                    let size = &borrowed_msg.payload().map(|p| p.len() as u64).unwrap_or(0);
                    self.handle_message(&borrowed_msg).await?;
                    if let Some(monitoring) = self.monitoring {
                        monitoring.record_kafka_messages_read(1);
                        monitoring.record_kafka_messages_size(*size);
                    };

                    log::info!(
                        "Successfully processed message from topic '{}' and partition '{}' at offset '{}'",
                        borrowed_msg.topic(),
                        borrowed_msg.partition(),
                        borrowed_msg.offset()
                    );
                }
                Err(e) => {
                    log::error!("Kafka read error: {e:?}");
                    // Turn rdkafka::error::KafkaError into application custom error
                    return Err(AppError::Kafka(KafkaError::ReadError(format!(
                        "Kafka read error: {e:?}"
                    ))));
                }
            }

            let should_flush_by_size =
                self.pipeline.lock().await.aggregator_len() >= self.max_buffer_size;
            let should_flush_by_time =
                last_flush.elapsed() >= Duration::from_secs(self.max_wait_secs);

            if should_flush_by_size || should_flush_by_time {
                // Flush the aggregator and, if successful, commit the consumer state.
                // This guarantees idempotency in case of failure.
                self.pipeline.lock().await.flush().await?;

                let mut attempts = 0;
                let max_attempts = 3;

                loop {
                    match self.consumer.commit_consumer_state(CommitMode::Async) {
                        Ok(_) => {
                            if let Some(monitoring) = self.monitoring {
                                monitoring.record_kafka_commit();
                            }
                            log::info!("Successfully committed consumer state.");
                            break;
                        }
                        Err(e) => {
                            attempts += 1;
                            log::error!("Attempt {attempts}: Commit failed: {e:?}");
                            if attempts >= max_attempts {
                                log::error!("Max attempts reached. Giving up.");
                                return Err(AppError::Kafka(KafkaError::CommunicationLost(
                                    format!("Commit failed after {attempts} attempts: {e:?}"),
                                )));
                            }
                            // Delay before retrying
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
                log::info!("Flushed and committed successfully.");
                last_flush = Instant::now();
            }
        }

        // Final flush
        self.pipeline.lock().await.flush().await?;
        self.consumer
            .commit_consumer_state(CommitMode::Async)
            .map_err(|e| {
                log::error!("Commit failed: {e}");
                AppError::Kafka(KafkaError::CommunicationLost(format!("Commit failed: {e}")))
            })?;

        Ok(())
    }

    #[allow(dead_code)]
    // Modified to be generic over any M: Message.
    async fn handle_message<M: Message>(&self, msg: &M) -> AppResult<()> {
        let offset = msg.offset();
        // Convert key if present.
        let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());

        let pipeline = self.pipeline.lock().await;
        let payload = match pipeline.delta_config.message_format {
            MessageFormat::Json => {
                let payload_str = match msg.payload_view::<str>() {
                    Some(Ok(s)) => s.to_string(),
                    _ => String::new(),
                };
                let json_value =
                    serde_json::from_str(&payload_str).unwrap_or(serde_json::Value::Null);
                parse_json_object(&json_value, &pipeline.delta_schema)
            }
            MessageFormat::Grpc => {
                let payload_bytes = match msg.payload_view::<[u8]>() {
                    Some(Ok(bytes)) => bytes,
                    _ => &[],
                };
                parse_grpc_object(payload_bytes, &pipeline.delta_schema)
            }
        };

        match payload {
            Ok(None) => Ok(()), // Do nothing case payload is None
            Ok(Some(typed_payload)) => {
                pipeline.insert_record(offset, key, typed_payload)?;
                drop(pipeline);
                Ok(())
            }
            Err(e) => {
                if let Some(producer) = &self.dead_letter_producer {
                    // Create a string representation of the error
                    let error_msg = format!(
                        "Error processing message from topic '{}' at offset {}: {:?}",
                        msg.topic(),
                        offset,
                        e
                    );
                    producer.send_to_dead_letter_topic(key, error_msg).await?;
                }
                Ok(())
            }
        }
    }
}

// Kafka can only be tested with a running Kafka instance.
// Check test directory for integration tests.
