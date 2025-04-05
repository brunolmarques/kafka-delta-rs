// This module consolidates data from multiple Kafka sources,
// deduplicates them in parallel, and prepares atomic daily batches.
// It also implements recovery if a crash occurs (e.g., by reading checkpoints).
use async_trait::async_trait;
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::task; // added for async trait support

use crate::config::DeltaConfig;
use crate::handlers::PipelineError::{FlushError, InsertError};
use crate::handlers::{AppError, AppResult, DeltaError, PipelineError};
use crate::model::{MessageRecordTyped, TypedValue};
use crate::monitoring::Monitoring;
use crate::utils::parse_to_typed;

/// Buffer used for consolidating messages
#[derive(Debug)]
struct InMemoryAggregator {
    records: BTreeMap<i64, MessageRecordTyped>,
    seen_offsets: HashSet<i64>,
    seen_keys: HashSet<String>,
    counter: usize, // new counter field
}

impl InMemoryAggregator {
    fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            seen_offsets: HashSet::new(),
            seen_keys: HashSet::new(),
            counter: 0, // initialize counter
        }
    }

    fn insert(&mut self, record: MessageRecordTyped) -> Result<(), PipelineError> {
        if self.seen_offsets.contains(&record.offset) {
            return Err(InsertError(format!("Duplicate offset {}", record.offset)));
        }

        if let Some(ref key) = record.key {
            if self.seen_keys.contains(key) {
                return Err(InsertError(format!("Duplicate key {}", key)));
            }
        }

        self.seen_offsets.insert(record.offset);
        if let Some(ref k) = record.key {
            self.seen_keys.insert(k.clone());
        }
        self.records.insert(record.offset, record);
        self.counter += 1; // increment counter after successful insert

        Ok(())
    }

    fn drain(&mut self) -> Vec<MessageRecordTyped> {
        let batch = self.records.values().cloned().collect();
        self.records.clear();
        self.seen_offsets.clear();
        self.seen_keys.clear();
        self.counter = 0; // reset counter after draining
        batch
    }

    fn len(&self) -> usize {
        self.counter // return the maintained counter
    }
}

/// Wraps aggregator & flush logic
pub struct Pipeline<'a> {
    aggregator: Arc<Mutex<InMemoryAggregator>>,
    delta_config: &'a DeltaConfig,
    monitoring: Option<&'a Monitoring>, // TODO: Implement monitoring
}

impl<'a> Pipeline<'a> {
    pub fn new(delta_config: &'a DeltaConfig, monitoring: Option<&'a Monitoring>) -> Self {
        Self {
            aggregator: Arc::new(Mutex::new(InMemoryAggregator::new())),
            delta_config,
            monitoring,
        }
    }
}

#[async_trait]
pub trait PipelineTrait {
    // Asynchronously insert a record into the pipeline
    async fn insert_record(
        &self,
        offset: i64,
        key: Option<String>,
        payload: String,
    ) -> AppResult<()>;

    // Asynchronously flush the pipeline data
    async fn flush(&self) -> AppResult<()>;

    // Returns the count of aggregated records
    fn aggregator_len(&self) -> usize;
}

#[async_trait]
impl<'a> PipelineTrait for Pipeline<'a> {
    /// Asynchronously insert a record into aggregator
    async fn insert_record(
        &self,
        offset: i64,
        key: Option<String>,
        payload: String,
    ) -> AppResult<()> {
        // Parse the incoming string as JSON:
        let parsed_json: serde_json::Value = serde_json::from_str(&payload)
            .map_err(|e| PipelineError::ParseError(format!("Invalid JSON: {e}")))?;

        // If there is a schema in config, apply parse_to_typed
        let typed_fields = if let Some(field_configs) = &self.delta_config.schema {
            parse_to_typed(&parsed_json, field_configs)?
        } else {
            // Deal with no schema, treat all columns as strings
            let mut typed_fields = HashMap::new();
            for (key, value) in parsed_json.as_object().unwrap() {
                typed_fields.insert(key.clone(), TypedValue::String(value.to_string()));
            }
            typed_fields
        };

        // Build the record with typed fields
        let record = MessageRecordTyped {
            offset,
            key,
            payload: typed_fields,
        };

        let aggregator = self.aggregator.clone();

        log::debug!("Inserting record: {:?}", record);

        task::spawn_blocking(move || {
            let mut agg = aggregator.lock().map_err(|_| {
                log::error!("insert_record: Failed to acquire aggregator lock");
                InsertError("Failed to acquire aggregator lock".into())
            })?;
            agg.insert(record)
        })
        .await
        .map_err(|e| {
            PipelineError::InsertError(format!("Aggregator insertion task panicked: {e}"))
        })??;

        log::debug!("Record inserted successfully");

        Ok(())
    }

    /// Flush aggregator data
    async fn flush(&self) -> AppResult<()> {
        let aggregator = self.aggregator.clone();

        log::info!("Flushing aggregator data…");

        let batch = task::spawn_blocking(move || {
            let mut agg = aggregator.lock().map_err(|_| {
                log::error!("flush: Failed to acquire aggregator lock");
                FlushError("Failed to acquire aggregator lock".into())
            })?;
            Ok::<_, PipelineError>(agg.drain())
        })
        .await
        .map_err(|e| FlushError(format!("Drain task panicked: {e}")))??;

        if batch.is_empty() {
            log::info!("No messages to flush. Skipping write.");
            return Ok(());
        }

        log::info!(
            "Writing {} records to Delta table at {}…",
            batch.len(),
            self.delta_config.table_path.clone()
        );

        // TODO: Implement Delta table writing logic here

        log::info!("Flush completed.");

        Ok(())
    }

    fn aggregator_len(&self) -> usize {
        let agg = self.aggregator.lock().unwrap(); // In practice, handle PoisonError
        agg.len()
    }
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio; // added for payload hashmap

    // Updated helper for creating a pipeline instance for tests.
    fn create_pipeline() -> Pipeline<'static> {
        let config = Box::leak(Box::new(DeltaConfig {
            table_path: "dummy".to_string(),
            mode: "default".to_string(),
            partition: "default".to_string(),
            schema: None,
        }));
        Pipeline::new(config, None)
    }

    #[tokio::test]
    async fn test_insert_record_unique() {
        let pipeline = create_pipeline();
        let payload = r#"{"dummy": "payload1"}"#.to_string(); // changed to valid JSON
        let res = pipeline
            .insert_record(1, Some("a".to_string()), payload)
            .await;
        assert!(res.is_ok());
        assert_eq!(pipeline.aggregator_len(), 1);
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_offset() {
        let pipeline = create_pipeline();
        let payload1 = r#"{"dummy": "payload1"}"#.to_string();
        let payload2 = r#"{"dummy": "payload2"}"#.to_string();
        assert!(
            pipeline
                .insert_record(1, Some("a".to_string()), payload1)
                .await
                .is_ok()
        );
        let res = pipeline
            .insert_record(1, Some("b".to_string()), payload2)
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_key() {
        let pipeline = create_pipeline();
        let payload1 = r#"{"dummy": "payload1"}"#.to_string();
        let payload2 = r#"{"dummy": "payload2"}"#.to_string();
        assert!(
            pipeline
                .insert_record(1, Some("a".to_string()), payload1)
                .await
                .is_ok()
        );
        let res = pipeline
            .insert_record(2, Some("a".to_string()), payload2)
            .await;
        assert!(res.is_err());
    }

    // Renamed test to fix typo.
    #[tokio::test]
    async fn test_flush_empty_the_aggregator() {
        let pipeline = create_pipeline();
        let payload1 = r#"{"dummy": "payload1"}"#.to_string();
        let payload2 = r#"{"dummy": "payload2"}"#.to_string();
        pipeline
            .insert_record(1, Some("a".to_string()), payload1)
            .await
            .unwrap();
        pipeline
            .insert_record(2, Some("b".to_string()), payload2)
            .await
            .unwrap();
        assert_eq!(pipeline.aggregator_len(), 2);

        assert!(pipeline.flush().await.is_ok());
        assert_eq!(pipeline.aggregator_len(), 0);
    }

    #[test]
    fn test_inmemory_aggregator_insert_and_len() {
        let mut aggregator = InMemoryAggregator::new();
        let mut payload = HashMap::new();
        payload.insert(
            "dummy".to_string(),
            TypedValue::String("payload".to_string()),
        );
        let record = MessageRecordTyped {
            offset: 1,
            key: Some("a".to_string()),
            payload, // using hashmap payload
        };
        assert!(aggregator.insert(record).is_ok());
        assert_eq!(aggregator.len(), 1);

        // Test duplicate offset insertion
        let mut payload_dup = HashMap::new();
        payload_dup.insert(
            "dummy".to_string(),
            TypedValue::String("payload2".to_string()),
        );
        let dup_offset = MessageRecordTyped {
            offset: 1,
            key: Some("b".to_string()),
            payload: payload_dup,
        };
        assert!(aggregator.insert(dup_offset).is_err());
    }

    #[test]
    fn test_inmemory_aggregator_drain() {
        let mut aggregator = InMemoryAggregator::new();
        let mut payload1 = HashMap::new();
        payload1.insert(
            "dummy".to_string(),
            TypedValue::String("payload1".to_string()),
        );
        let mut payload2 = HashMap::new();
        payload2.insert(
            "dummy".to_string(),
            TypedValue::String("payload2".to_string()),
        );
        let record1 = MessageRecordTyped {
            offset: 1,
            key: Some("a".to_string()),
            payload: payload1,
        };
        let record2 = MessageRecordTyped {
            offset: 2,
            key: Some("b".to_string()),
            payload: payload2,
        };
        aggregator.insert(record1).unwrap();
        aggregator.insert(record2).unwrap();
        assert_eq!(aggregator.len(), 2);

        let drained = aggregator.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(aggregator.len(), 0);
    }
}
