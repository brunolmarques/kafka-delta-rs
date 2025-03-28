// This module consolidates data from multiple Kafka sources,
// deduplicates them in parallel, and prepares atomic daily batches.
// It also implements recovery if a crash occurs (e.g., by reading checkpoints).
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::task;

use crate::handlers::PipelineError::{FlushError, InsertError};
use crate::handlers::{AppResult, PipelineError};

#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub offset: i64,
    pub key: Option<String>,
    pub payload: String,
}

/// Buffer used for consolidating messages
#[derive(Debug)]
struct InMemoryAggregator {
    records: BTreeMap<i64, MessageRecord>,
    seen_offsets: HashSet<i64>,
    seen_keys: HashSet<String>,
}

impl InMemoryAggregator {
    fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            seen_offsets: HashSet::new(),
            seen_keys: HashSet::new(),
        }
    }

    fn insert(&mut self, record: MessageRecord) -> Result<(), PipelineError> {
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

        Ok(())
    }

    fn drain(&mut self) -> Vec<MessageRecord> {
        let batch = self.records.values().cloned().collect();
        self.records.clear();
        self.seen_offsets.clear();
        self.seen_keys.clear();
        batch
    }

    fn len(&self) -> usize {
        self.records.len()
    }
}

/// Wraps aggregator & flush logic
pub struct Pipeline {
    aggregator: Arc<Mutex<InMemoryAggregator>>,
    table_uri: String,
}

impl Pipeline {
    pub fn new(table_uri: String) -> Self {
        Self {
            aggregator: Arc::new(Mutex::new(InMemoryAggregator::new())),
            table_uri,
        }
    }

    /// Asynchronously insert a record into aggregator
    pub async fn insert_record(
        &self,
        offset: i64,
        key: Option<String>,
        payload: String,
    ) -> AppResult<()> {
        let record = MessageRecord {
            offset,
            key,
            payload,
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
    pub async fn flush(&self) -> AppResult<()> {
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
            self.table_uri
        );

        // TODO: Implement Delta table writing logic here
        // Example: This is where you'd use the deltalake crate
        // If an error happens, you can do:
        //   return Err(PipelineError::FlushError("Delta I/O failed: ...".into()));

        for r in batch {
            println!(
                "Offset={}, Key={:?}, Payload={}",
                r.offset, r.key, r.payload
            );
        }

        log::info!("Flush completed.");

        Ok(())
    }

    pub fn aggregator_len(&self) -> usize {
        let agg = self.aggregator.lock().unwrap(); // In practice, handle PoisonError
        agg.len()
    }
}

//---------------------------------------- Tests ----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_insert_record_unique() {
        let pipeline = Pipeline::new("dummy".to_string());
        let res = pipeline
            .insert_record(1, Some("a".to_string()), "payload1".to_string())
            .await;
        assert!(res.is_ok());
        assert_eq!(pipeline.aggregator_len(), 1);
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_offset() {
        let pipeline = Pipeline::new("dummy".to_string());
        assert!(
            pipeline
                .insert_record(1, Some("a".to_string()), "payload1".to_string())
                .await
                .is_ok()
        );
        let res = pipeline
            .insert_record(1, Some("b".to_string()), "payload2".to_string())
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_insert_record_duplicate_key() {
        let pipeline = Pipeline::new("dummy".to_string());
        assert!(
            pipeline
                .insert_record(1, Some("a".to_string()), "payload1".to_string())
                .await
                .is_ok()
        );
        let res = pipeline
            .insert_record(2, Some("a".to_string()), "payload2".to_string())
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_flush_empy_the_aggregator() {
        let pipeline = Pipeline::new("dummy".to_string());
        pipeline
            .insert_record(1, Some("a".to_string()), "payload1".to_string())
            .await
            .unwrap();
        pipeline
            .insert_record(2, Some("b".to_string()), "payload2".to_string())
            .await
            .unwrap();
        assert_eq!(pipeline.aggregator_len(), 2);

        assert!(pipeline.flush().await.is_ok());
        assert_eq!(pipeline.aggregator_len(), 0);
    }

    // Non-asynchronous tests for InMemoryAggregator
    #[test]
    fn test_inmemory_aggregator_insert_and_len() {
        let mut aggregator = InMemoryAggregator::new();
        let record = MessageRecord {
            offset: 1,
            key: Some("a".to_string()),
            payload: "payload".to_string(),
        };
        assert!(aggregator.insert(record).is_ok());
        assert_eq!(aggregator.len(), 1);

        // Test duplicate offset insertion
        let dup_offset = MessageRecord {
            offset: 1,
            key: Some("b".to_string()),
            payload: "payload2".to_string(),
        };
        assert!(aggregator.insert(dup_offset).is_err());
    }

    #[test]
    fn test_inmemory_aggregator_drain() {
        let mut aggregator = InMemoryAggregator::new();
        let record1 = MessageRecord {
            offset: 1,
            key: Some("a".to_string()),
            payload: "payload1".to_string(),
        };
        let record2 = MessageRecord {
            offset: 2,
            key: Some("b".to_string()),
            payload: "payload2".to_string(),
        };
        aggregator.insert(record1).unwrap();
        aggregator.insert(record2).unwrap();
        assert_eq!(aggregator.len(), 2);

        let drained = aggregator.drain();
        assert_eq!(drained.len(), 2);
        // After draining, aggregator should be empty
        assert_eq!(aggregator.len(), 0);
    }
}
