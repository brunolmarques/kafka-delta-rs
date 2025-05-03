use std::collections::HashMap;
use std::hash::{Hash, Hasher};

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

#[allow(dead_code)]
/// An enum to hold typed field values
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    Null,
    U64(u64),
    Int64(i64),
    Float64(f64), // not hashable!
    Boolean(bool),
    Utf8(String),
    Date(i32),
    Timestamp(i64),
    Array(Vec<TypedValue>),
    Object(HashMap<KeyValue, TypedValue>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    U64(u64),
    Int64(i64),
    Boolean(bool),
    Utf8(String),
    Date(i32),
    Timestamp(i64),
}

impl From<TypedValue> for KeyValue {
    fn from(value: TypedValue) -> Self {
        match value {
            TypedValue::U64(u) => KeyValue::U64(u),
            TypedValue::Int64(i) => KeyValue::Int64(i),
            TypedValue::Boolean(b) => KeyValue::Boolean(b),
            TypedValue::Utf8(s) => KeyValue::Utf8(s),
            TypedValue::Date(d) => KeyValue::Date(d),
            TypedValue::Timestamp(t) => KeyValue::Timestamp(t),
            _ => {
                log::error!("Unsupported TypedValue type: {value:?}");
                panic!("Unsupported TypedValue type: {value:?}");
            }
        }
    }
}
