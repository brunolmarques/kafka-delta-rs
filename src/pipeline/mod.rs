// This module consolidates data from multiple Kafka sources,
// deduplicates them in parallel, and prepares atomic daily batches.
// It also implements recovery if a crash occurs (e.g., by reading checkpoints).
use async_trait::async_trait;
use deltalake::kernel::StructType;
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::writer::WriteMode;
use deltalake::*;
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::config::{DeltaConfig, DeltaWriteMode};
use crate::delta::{DeltaIo, DeltaWriterIo};
use crate::handlers::{AppError, AppResult, DeltaError, PipelineError};
use crate::model::{MessageRecordTyped, TypedValue};
use crate::monitoring::Monitoring;
use crate::utils::build_record_batch_from_vec;

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
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            seen_offsets: HashSet::new(),
            seen_keys: HashSet::new(),
            counter: 0,
        }
    }

    fn insert(&mut self, record: MessageRecordTyped) -> Result<(), PipelineError> {
        if self.seen_offsets.contains(&record.offset) {
            log::warn!("Duplicate offset {} - skipping insertion", record.offset);
            return Ok(());
        }

        if let Some(ref key) = record.key {
            if self.seen_keys.contains(key) {
                log::warn!("Duplicate key {key} - skipping insertion");
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
    delta_io: Box<dyn DeltaIo>,
    pub delta_schema: arrow::datatypes::Schema,
    aggregator: Arc<std::sync::Mutex<InMemoryAggregator>>,
    pub delta_config: &'a DeltaConfig,
    monitoring: Option<&'a Monitoring>,
}

impl<'a> Pipeline<'a> {
    pub async fn new(
        delta_config: &'a DeltaConfig,
        monitoring: Option<&'a Monitoring>,
    ) -> AppResult<Self> {
        let delta_table = match deltalake::open_table(&delta_config.table_path).await {
            Ok(table) => table,
            Err(e) => {
                log::error!("Failed to open Delta table: {e}");
                return Err(AppError::Delta(DeltaError::IoError(format!(
                    "Failed to open Delta table: {e}"
                ))));
            }
        };

        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let metadata = &delta_table.metadata().map_err(|e| {
            log::error!("Failed to get metadata for the table: {e}");
            AppError::Delta(DeltaError::TableError(format!(
                "Failed to get metadata for the table: {e}"
            )))
        })?;

        let schema = metadata.schema().map_err(|e| {
            log::error!("Failed to get schema: {e}");
            AppError::Delta(DeltaError::TableError(
                format!("Failed to get schema: {e}",),
            ))
        })?;

        let arrow_schema =
            <deltalake::arrow::datatypes::Schema as TryFrom<&StructType>>::try_from(&schema)
                .map_err(|e| {
                    log::error!("Failed to convert to arrow schema: {e}");
                    AppError::Delta(DeltaError::TableError(format!(
                        "Failed to convert to arrow schema: {e}"
                    )))
                })?;

        let delta_io = Box::new(DeltaWriterIo::new(
            Arc::new(Mutex::new(delta_table)),
            writer_properties,
        ));

        Ok(Self {
            delta_io,
            delta_schema: arrow_schema,
            aggregator: Arc::new(std::sync::Mutex::new(InMemoryAggregator::new())),
            delta_config,
            monitoring,
        })
    }

    pub fn insert_record(
        &self,
        offset: i64,
        key: Option<String>,
        payload: HashMap<String, TypedValue>,
    ) -> AppResult<()> {
        let record = MessageRecordTyped {
            offset,
            key,
            payload,
        };

        let mut agg = self.aggregator.lock().map_err(|e| {
            log::error!("Failed to acquire aggregator lock: {e}");
            PipelineError::InsertError(format!("Failed to acquire aggregator lock: {e}"))
        })?;

        agg.insert(record).map_err(AppError::Pipeline)
    }

    pub fn aggregator_len(&self) -> usize {
        self.aggregator.lock().map(|agg| agg.len()).unwrap_or(0)
    }
}

#[async_trait]
pub trait PipelineTrait: Send + Sync {
    // Asynchronously flush the pipeline data
    async fn flush(&self) -> AppResult<()>;
}

#[async_trait]
impl PipelineTrait for Pipeline<'_> {
    /// Flush aggregator data
    async fn flush(&self) -> AppResult<()> {
        log::info!("Flushing aggregator data…");

        // Record flush start time
        let flush_start_time = Instant::now();

        let batch = {
            let mut agg = self.aggregator.lock().map_err(|e| {
                log::error!("Failed to acquire aggregator lock: {e}");
                PipelineError::FlushError(format!("Failed to acquire aggregator lock: {e}"))
            })?;
            agg.drain()
        };

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
        let arrow_batch = build_record_batch_from_vec(self.delta_schema.clone(), &batch)?;

        // Determine write mode based on configuration
        let write_mode = match self.delta_config.mode {
            DeltaWriteMode::Insert => WriteMode::Default,
            DeltaWriteMode::Upsert => WriteMode::MergeSchema,
        };

        // Write the batch using the delta_io interface
        self.delta_io.write_batch(arrow_batch, write_mode).await?;

        // Record flush end time
        let flush_end_time = Instant::now();

        // Record flush duration in seconds
        let flush_duration = flush_end_time
            .duration_since(flush_start_time)
            .as_secs_f64();

        // Record the number of messages written to Delta
        if let Some(monitoring) = self.monitoring {
            monitoring.record_delta_write(batch.len() as u64);
            monitoring.observe_delta_flush_time(flush_duration);
        }

        Ok(())
    }
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DeltaConfig, DeltaWriteMode, MessageFormat};
    use crate::delta::MockDeltaIo;
    use crate::model::TypedValue;
    use arrow::datatypes::{DataType, Field, Schema};
    use mockall::predicate;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_mock_config() -> DeltaConfig {
        DeltaConfig {
            table_path: "test_table".to_string(),
            mode: DeltaWriteMode::Insert,
            message_format: MessageFormat::Json,
            buffer_size: Some(100),
        }
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::UInt64, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn create_test_pipeline() -> Pipeline<'static> {
        let config = Box::leak(Box::new(create_mock_config()));
        let schema = create_test_schema();
        let mut mock = MockDeltaIo::new();
        mock.expect_write_batch()
            .with(predicate::always(), predicate::eq(WriteMode::Default))
            .returning(|_, _| Ok(()));

        Pipeline {
            delta_io: Box::new(mock),
            delta_schema: schema,
            aggregator: Arc::new(std::sync::Mutex::new(InMemoryAggregator::new())),
            delta_config: config,
            monitoring: None,
        }
    }

    #[tokio::test]
    async fn test_pipeline_initialization() {
        let pipeline = create_test_pipeline();
        assert_eq!(pipeline.aggregator_len(), 0);
    }

    #[tokio::test]
    async fn test_insert_record() {
        let pipeline = create_test_pipeline();
        let mut data = HashMap::new();
        data.insert("id".to_string(), TypedValue::U64(1));
        data.insert("name".to_string(), TypedValue::Utf8("test".to_string()));

        let result = pipeline.insert_record(1, Some("key1".to_string()), data);
        assert!(result.is_ok());
        assert_eq!(pipeline.aggregator_len(), 1);
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_offset() {
        let pipeline = create_test_pipeline();
        let mut data1 = HashMap::new();
        data1.insert("id".to_string(), TypedValue::U64(1));
        data1.insert("name".to_string(), TypedValue::Utf8("test1".to_string()));

        let mut data2 = HashMap::new();
        data2.insert("id".to_string(), TypedValue::U64(2));
        data2.insert("name".to_string(), TypedValue::Utf8("test2".to_string()));

        let result1 = pipeline.insert_record(1, Some("key1".to_string()), data1);
        assert!(result1.is_ok());

        let result2 = pipeline.insert_record(1, Some("key2".to_string()), data2);
        assert!(result2.is_ok());

        assert_eq!(pipeline.aggregator_len(), 1);
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_key() {
        let pipeline = create_test_pipeline();
        let mut data1 = HashMap::new();
        data1.insert("id".to_string(), TypedValue::U64(1));
        data1.insert("name".to_string(), TypedValue::Utf8("test1".to_string()));

        let mut data2 = HashMap::new();
        data2.insert("id".to_string(), TypedValue::U64(2));
        data2.insert("name".to_string(), TypedValue::Utf8("test2".to_string()));

        let result1 = pipeline.insert_record(1, Some("key1".to_string()), data1);
        assert!(result1.is_ok());

        let result2 = pipeline.insert_record(2, Some("key1".to_string()), data2);
        assert!(result2.is_ok());

        assert_eq!(pipeline.aggregator_len(), 1);
    }

    #[tokio::test]
    async fn test_flush() {
        let pipeline = create_test_pipeline();
        let mut data = HashMap::new();
        data.insert("id".to_string(), TypedValue::U64(1));
        data.insert("name".to_string(), TypedValue::Utf8("test".to_string()));

        let result = pipeline.insert_record(1, Some("key1".to_string()), data);
        assert!(result.is_ok());
        assert_eq!(pipeline.aggregator_len(), 1);

        let flush_result = pipeline.flush().await;
        assert!(flush_result.is_ok());
        assert_eq!(pipeline.aggregator_len(), 0);
    }

    #[tokio::test]
    async fn test_flush_with_nulls() {
        let pipeline = create_test_pipeline();
        let mut data = HashMap::new();
        data.insert("id".to_string(), TypedValue::Null);
        data.insert("name".to_string(), TypedValue::Utf8("test".to_string()));

        let result = pipeline.insert_record(1, Some("key1".to_string()), data);
        assert!(result.is_ok());
        assert_eq!(pipeline.aggregator_len(), 1);

        let flush_result = pipeline.flush().await;
        assert!(flush_result.is_ok());
        assert_eq!(pipeline.aggregator_len(), 0);
    }
}
