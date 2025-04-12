// This module writes consolidated data to an existing Delta table using the deltalake crate.
// It supports atomic operations and idempotent writes, handling schema changes gracefully.
use serde::Deserialize;
use std::collections::HashMap;
use deltalake::DeltaOps;

use crate::handlers::{AppError, DeltaError};
use crate::model::TypedValue;
use crate::config::DeltaConfig;

// Trait for Delta table writing.
pub trait DeltaWriter {
    // Inserts a batch of records into the delta table atomically.
    async fn write(&self, data: &[HashMap<String, TypedValue>]) -> Result<(), AppError>;
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub enum DeltaWriteMode {
    Insert,
    Upsert,
    
}

// Example implementation using deltalake.
pub struct DeltaRsWriter {
    pub table_path: String,
    pub partition: String,
    pub prune_filter: HashMap<String, String>,
    pub write_mode: DeltaWriteMode, 
}

impl DeltaRsWriter {
    pub fn new(delta_config: &DeltaConfig, prune_filter: HashMap<String, String>) -> Self {
        DeltaRsWriter {
            table_path: delta_config.table_path.clone(),
            partition: delta_config.partition.clone(),
            prune_filter: prune_filter,
            write_mode: delta_config.mode,
        }
    }
}

impl DeltaWriter for DeltaRsWriter {
    async fn write(&self, data: &[HashMap<String, TypedValue>]) -> Result<(), AppError> {
        // Pseudocode: Open the delta table, perform an atomic INSERT or MERGE into the partition specified.
        // If a new column is detected, skip it instead of panicking.
        // If there's no data, we can skip
        if data.is_empty() {
            log::warn!("No data to write; skipping.");
            return Ok(());
        }

        // Create a new DeltaOps instance, operating on DeltaTable at given uri.
        let delta_ops = DeltaOps::try_from_uri(&self.table_path)
            .await
            .map_err(|e| {
                log::error!("Failed to create DeltaOps instance: {}", e);
                DeltaError::IoError(format!("Failed to create DeltaOps instance: {}", e))
            })?;
        
        // Load the Delta table.
        let table = delta_ops.load()
            .await
            .map_err(|e| {
                log::error!("Failed to load Delta table: {}", e);
                DeltaError::IoError(format!("Failed to load Delta table: {}", e))
            })?;
        

        // TODO: Implement data convertion to arrow
        // TODO: Implement atomic write operation

        Ok(())
    }
}

//---------------------------------------- Tests ----------------------------------------


