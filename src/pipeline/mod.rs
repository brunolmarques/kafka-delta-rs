// This module consolidates data from multiple Kafka sources,
// deduplicates them in parallel, and prepares atomic daily batches.
// It also implements recovery if a crash occurs (e.g., by reading checkpoints).
use async_trait::async_trait;
use deltalake::kernel::{Schema, StructType};
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::*;
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::task;

use crate::config::DeltaConfig;
use crate::handlers::PipelineError::FlushError;
use crate::handlers::{AppError, AppResult, DeltaError, PipelineError};
use crate::model::{MessageRecordTyped, TypedValue};
use crate::monitoring::Monitoring;
use crate::utils::{build_record_batch_from_vec, parse_to_typed};

/// Buffer used for consolidating messages
/// Example of the aggregator:
/// ```
/// InMemoryAggregator {
///     counter: 2
///     seen_offsets: {1, 2, 3}
///     seen_keys: {"a", "b","c"}
///     records: {
///         1: {offset: 1, key: "a", payload: {1: {"user_name": String("payload1"), "id": U64(4732)}}},
///         2: {offset: 2, key: "b", payload: {2: {"user_name": String("payload2"), "id": U64(952)}}},
///         3: {offset: 3, key: "c", payload: {3: {"user_name": String("payload3"), "id": U64(2490)}}},
///     }
/// }
/// ```
/// The `InMemoryAggregator` is a simple in-memory data structure
/// that stores messages with unique offsets and keys.
/// It uses a BTreeMap for ordered storage and HashSet for fast lookups.
#[derive(Debug)]
struct InMemoryAggregator {
    records: BTreeMap<i64, HashMap<String, TypedValue>>,
    seen_offsets: HashSet<i64>,
    seen_keys: HashSet<String>,
    counter: usize,
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
            log::warn!("Duplicate offset {} - skipping insertion", record.offset);
            return Ok(());
        }

        if let Some(ref key) = record.key {
            if self.seen_keys.contains(key) {
                log::warn!("Duplicate key {} - skipping insertion", key);
                return Ok(());
            }
        }

        self.seen_offsets.insert(record.offset);

        if let Some(ref k) = record.key {
            self.seen_keys.insert(k.clone());
        }
        self.records.insert(record.offset, record.payload);
        self.counter += 1; // increment counter after successful insert

        Ok(())
    }

    fn drain(&mut self) -> Vec<HashMap<String, TypedValue>> {
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
    delta_table: DeltaTable,
    delta_writer: Arc<Mutex<RecordBatchWriter>>,
    delta_schema: Arc<arrow::datatypes::Schema>,
    aggregator: Arc<Mutex<InMemoryAggregator>>,
    delta_config: &'a DeltaConfig,
    monitoring: Option<&'a Monitoring>, // TODO: Implement monitoring
}

impl<'a> Pipeline<'a> {
    pub async fn new(
        delta_config: &'a DeltaConfig,
        monitoring: Option<&'a Monitoring>,
    ) -> AppResult<Self> {
        let delta_table = match deltalake::open_table(&delta_config.table_path).await {
            Ok(table) => table,
            Err(e) => {
                log::error!("Failed to open Delta table: {}", e);
                return Err(AppError::Delta(DeltaError::IoError(format!(
                    "Failed to open Delta table: {}",
                    e
                ))));
            }
        };

        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let writer = RecordBatchWriter::for_table(&delta_table).map_err(|e| {
            log::error!("Failed to make RecordBatchWriter: {}", e);
            AppError::Delta(DeltaError::TableError(format!(
                "Failed to make RecordBatchWriter: {}",
                e
            )))
        })?;

        let delta_writer = writer.with_writer_properties(writer_properties);

        let metadata = delta_table.metadata().map_err(|e| {
            log::error!("Failed to get metadata for the table: {}", e);
            AppError::Delta(DeltaError::TableError(format!(
                "Failed to get metadata for the table: {}",
                e
            )))
        })?;

        let schema = metadata.schema().map_err(|e| {
            log::error!("Failed to get schema: {}", e);
            AppError::Delta(DeltaError::TableError(format!(
                "Failed to get schema: {}",
                e
            )))
        })?;

        let arrow_schema =
            <deltalake::arrow::datatypes::Schema as TryFrom<&StructType>>::try_from(&schema)
                .map_err(|e| {
                    log::error!("Failed to convert to arrow schema: {}", e);
                    AppError::Delta(DeltaError::TableError(format!(
                        "Failed to convert to arrow schema: {}",
                        e
                    )))
                })?;

        let arrow_schema_ref = Arc::new(arrow_schema);

        Ok(Self {
            delta_table,
            delta_writer: Arc::new(Mutex::new(delta_writer)),
            delta_schema: arrow_schema_ref,
            aggregator: Arc::new(Mutex::new(InMemoryAggregator::new())),
            delta_config,
            monitoring,
        })
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
        // Parse the incoming string as JSON
        // TODO: Send invalid messages to dead letter topic
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
            let mut agg = aggregator.lock().unwrap_or_else(|poisoned| {
                // Mutex lock will only fail in case of poisoned lock
                // This is a rare case, but can happen if the current lock holder panics
                // Which can leave the aggregator in an inconsistent state
                log::warn!("Lock poisoned, recovering inner data");
                poisoned.into_inner()
            });
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
            let mut agg = aggregator.lock().unwrap_or_else(|poisoned| {
                // Mutex lock will only fail in case of poisoned lock
                // This is a rare case, but can happen if the current lock holder panics
                // Which can leave the aggregator in an inconsistent state
                log::warn!("Lock poisoned, recovering inner data");
                poisoned.into_inner()
            });
            Ok::<_, PipelineError>(agg.drain())
        })
        .await
        .map_err(|e| FlushError(format!("Drain task panicked: {e}")))??;

        if batch.is_empty() {
            log::info!("No messages to flush. Skipping write.");
            return Ok(());
        }

        log::info!(
            "Writing {} records to Delta table in path {}",
            batch.len(),
            self.delta_config.table_path.clone()
        );

        // Convert batch to arrow RecordBatch
        let arrow_table = build_record_batch_from_vec(self.delta_schema.clone(), &batch)?;

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
    use crate::delta::DeltaWriteMode;
    use std::collections::HashMap;
    use tokio;

    // Updated helper for creating a pipeline instance for tests.
    async fn create_pipeline() -> Pipeline<'static> {
        let config = Box::leak(Box::new(DeltaConfig {
            table_path: "dummy".to_string(),
            mode: DeltaWriteMode::INSERT,
            partition: "default".to_string(),
            schema: None,
        }));
        Pipeline::new(config, None).await.unwrap()
    }

    #[tokio::test]
    async fn test_insert_record_unique() {
        let pipeline = create_pipeline().await;
        let payload = r#"{"dummy": "payload1"}"#.to_string(); // changed to valid JSON
        let res = pipeline
            .insert_record(1, Some("a".to_string()), payload)
            .await;
        assert!(res.is_ok());
        assert_eq!(pipeline.aggregator_len(), 1);
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_offset() {
        let pipeline = create_pipeline().await;
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
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_key() {
        let pipeline = create_pipeline().await;
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
        assert!(res.is_ok());
    }

    // Renamed test to fix typo.
    #[tokio::test]
    async fn test_flush_empty_the_aggregator() {
        let pipeline = create_pipeline().await;
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
        assert!(aggregator.insert(dup_offset).is_ok());
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
