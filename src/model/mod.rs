use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

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

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum DeltaWriteMode {
    INSERT,
    UPSERT,
}

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
            return Err(ConfigError::InvalidField("FieldConfig.field is empty".to_string()).into());
        }

        Ok(())
    }
}

/// Enum restricting the possible field types
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Null,
    U64,
    I64,
    F64,
    Bool,
    String,
    DateTime,
    Array {
        #[serde(rename = "item_type")]
        item_type: Box<FieldType>,
    },
    HashMap {
        #[serde(rename = "key_type")]
        key_type: Box<KeyFieldType>,
        #[serde(rename = "value_type")]
        value_type: Box<FieldType>,
    },
}

impl Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Field types that are allowed to be used as keys in HashMaps
#[derive(Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum KeyFieldType {
    U64,
    I64,
    Bool,
    String,
    DateTime,
}

/// An enum to hold typed field values
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    Null,
    U64(u64),
    I64(i64),
    F64(f64), // not hashable!
    Bool(bool),
    String(String),
    DateTime(DateTime<Utc>),
    Array(Vec<TypedValue>),
    Object(HashMap<KeyValue, TypedValue>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    U64(u64),
    I64(i64),
    Bool(bool),
    String(String),
    DateTime(DateTime<Utc>),
}
