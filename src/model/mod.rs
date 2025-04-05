use std::hash::{Hash, Hasher};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::handlers::{AppResult, ConfigError};


//--------------------------------- Kafka Message -------------------------------------

#[derive(Debug, Clone)]
pub struct MessageRecordTyped {
    pub offset: i64,
    pub key: Option<String>,
    pub payload: HashMap<String, TypedValue>,
}

impl PartialEq for MessageRecordTyped {
    fn eq(&self, other: &Self) -> bool {
        // Decide which fields define "duplicate." 
        // This case uses offset + key combined:
        self.offset == other.offset && self.key == other.key
    }
}

impl Eq for MessageRecordTyped {}

impl Hash for MessageRecordTyped {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Must match the fields used in PartialEq.
        self.offset.hash(state);
        if let Some(ref k) = self.key {
            k.hash(state);
        } else {
            // For None keys, do nothing:
            "".hash(state);
        }
    }
}


//--------------------------------- Delta Schema -------------------------------------


#[derive(Debug, Deserialize)]
pub struct FieldConfig {
    pub field: String,
    #[serde(rename = "type")]
    pub type_name: FieldType, 
}

impl FieldConfig {
    pub fn validate(&self) -> AppResult<()> {
        // Currently, the enum's derive(Deserialize) will fail on unknown variants, so no big checks needed
        // But it is possible to do any custom validations here, e.g. forbid certain field names, etc.
        if self.field.is_empty() {
            log::error!("Invalid field config: field is empty");
            return Err(ConfigError::InvalidField(
                "FieldConfig.field is empty".to_string()).into()
            );
        }

        Ok(())
    }
}

/// Enum restricting the possible field types
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")] 
// ^ optional convenience for YAML like `type: "string"`
pub enum FieldType {
    Null,
    U64,
    I64,
    F64,
    Bool,
    String,
    DateTime,
    Array,
    HashMap,
    // add more if needed (Bool, f64, etc.)
}

/// An enum to hold typed field values
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    Null, // For null values
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
    DateTime(DateTime<Utc>),
    Array(Vec<TypedValue>), // For array of typed values
    Object(HashMap<String, TypedValue>), // For JSON objects
    // More variants as needed (bool, float, etc.)
}
