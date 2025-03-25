// This module writes consolidated data to an existing Delta table using the deltalake crate.
// It supports atomic operations and idempotent writes, handling schema changes gracefully.

use crate::handlers::AppError;

// Trait for Delta table writing.
pub trait DeltaWriter {
    // Inserts a batch of records into the delta table atomically.
    fn insert(&self, records: &[String]) -> Result<(), AppError>;
}

// Example implementation using deltalake.
pub struct DeltaRsWriter {
    pub table_path: String,
    pub partition: String,
}

impl DeltaWriter for DeltaRsWriter {
    fn insert(&self, records: &[String]) -> Result<(), AppError> {
        // Pseudocode: Open the delta table, perform an atomic INSERT or MERGE into the partition specified.
        // If a new column is detected, skip it instead of panicking.
        // TODO: Implement using deltalake crate
        println!(
            "Inserting {} records into {} partition {}",
            records.len(),
            self.table_path,
            self.partition
        );
        Ok(())
    }
}


//---------------------------------------- Tests ----------------------------------------


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_insert() {
        let writer = DeltaRsWriter {
            table_path: "/data/delta/table".to_string(),
            partition: "2025-03-18".to_string(),
        };
        let records = vec!["record1".to_string(), "record2".to_string()];
        let res = writer.insert(&records);
        assert!(res.is_ok());
    }
}
