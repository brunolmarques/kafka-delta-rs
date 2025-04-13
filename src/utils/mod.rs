use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, MapBuilder,
    NullBuilder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::handlers::{AppResult, PipelineError};
use crate::model::{FieldConfig, FieldType};
use crate::model::{KeyFieldType, KeyValue, TypedValue};

//----------------------------------- Macros -------------------------------------

#[macro_export]
macro_rules! parse_json_field {
    ($raw_value:expr, $method:ident, $variant:ident) => {{
        let type_desc = stringify!($variant);
        let value = $raw_value.$method().ok_or_else(|| {
            log::error!("Expected {}, got something else: {}", type_desc, $raw_value);
            PipelineError::ParseError(format!(
                "Expected {}, got something else: {}",
                type_desc, $raw_value
            ))
        })?;
        Ok(TypedValue::$variant(value))
    }};
}

#[macro_export]
macro_rules! append_to_builder {
    ($builder:expr, $arrow_type:ident, $value:expr) => {
        if let Some(b) = $builder.as_any_mut().downcast_mut::<$arrow_type>() {
            b.append_value($value);
        }
    };
}

//------------------------------- Type Parsing -------------------------------------

/// Parse a single JSON Value into a TypedValue, given a FieldType definition.
fn parse_field_value(
    raw_value: &Value,
    field_type: &FieldType,
) -> Result<TypedValue, PipelineError> {
    match field_type {
        FieldType::Null => Ok(TypedValue::Null),
        FieldType::U64 => {
            parse_json_field!(raw_value, as_u64, U64)
        }
        FieldType::I64 => {
            parse_json_field!(raw_value, as_i64, I64)
        }
        FieldType::F64 => {
            parse_json_field!(raw_value, as_f64, F64)
        }
        FieldType::Bool => {
            parse_json_field!(raw_value, as_bool, Bool)
        }
        FieldType::String => {
            let s = raw_value.as_str().ok_or_else(|| {
                log::error!("Expected String, got something else: {}", &raw_value);
                PipelineError::ParseError(format!(
                    "Expected String, got something else: {}",
                    raw_value
                ))
            })?;
            Ok(TypedValue::String(s.to_owned()))
        }
        FieldType::DateTime => {
            let s = raw_value.as_str().ok_or_else(|| {
                log::error!(
                    "Expected DateTime (string), got something else: {}",
                    &raw_value
                );
                PipelineError::ParseError(format!(
                    "Expected DateTime (string), got something else: {}",
                    raw_value
                ))
            })?;
            let dt = DateTime::parse_from_rfc3339(s)
                .map_err(|e| {
                    log::error!("Field '{}' not a valid DateTime: {}", field_type, e);
                    PipelineError::ParseError(format!("Invalid DateTime format: {e}"))
                })?
                .with_timezone(&Utc);
            Ok(TypedValue::DateTime(dt))
        }
        FieldType::Array { item_type } => {
            let arr = raw_value.as_array().ok_or_else(|| {
                log::error!("Expected JSON array, got something else: {}", &raw_value);
                PipelineError::ParseError(format!(
                    "Expected JSON array, got something else: {}",
                    raw_value
                ))
            })?;

            let mut items = Vec::new();
            for elem in arr {
                // Recursively parse each element with `item_type`
                let typed_value = parse_field_value(elem, item_type)?;
                items.push(typed_value);
            }
            Ok(TypedValue::Array(items))
        }
        FieldType::HashMap {
            key_type,
            value_type,
        } => {
            let obj = raw_value.as_object().ok_or_else(|| {
                log::error!("Expected JSON object, got something else: {}", &raw_value);
                PipelineError::ParseError(format!(
                    "Expected JSON object, got something else: {}",
                    raw_value
                ))
            })?;

            // Map key types are limited to types that impl Eq and Hash.
            // Values are enforced when parsing the Config.yaml file.
            let mut map = HashMap::new();
            for (k, v) in obj.iter() {
                // parse the key using `key_type`.
                let typed_key = match &**key_type {
                    KeyFieldType::U64 => {
                        let key_value = k.parse::<u64>().map_err(|e| {
                            log::error!("Failed to parse key as U64: {}", e);
                            PipelineError::ParseError(format!("Failed to parse key as U64: {}", e))
                        })?;
                        KeyValue::U64(key_value)
                    }
                    KeyFieldType::I64 => {
                        let key_value = k.parse::<i64>().map_err(|e| {
                            log::error!("Failed to parse key as I64: {}", e);
                            PipelineError::ParseError(format!("Failed to parse key as I64: {}", e))
                        })?;
                        KeyValue::I64(key_value)
                    }
                    KeyFieldType::Bool => {
                        let key_value = k.parse::<bool>().map_err(|e| {
                            log::error!("Failed to parse key as Bool: {}", e);
                            PipelineError::ParseError(format!("Failed to parse key as Bool: {}", e))
                        })?;
                        KeyValue::Bool(key_value)
                    }
                    KeyFieldType::String => KeyValue::String(k.clone()),
                    KeyFieldType::DateTime => {
                        let key_value = k.parse::<DateTime<Utc>>().map_err(|e| {
                            log::error!("Failed to parse key as DateTime: {}", e);
                            PipelineError::ParseError(format!(
                                "Failed to parse key as DateTime: {}",
                                e
                            ))
                        })?;
                        KeyValue::DateTime(key_value)
                    }
                };

                // parse the value using `value_type`.
                let typed_val = parse_field_value(v, value_type)?;
                map.insert(typed_key, typed_val);
            }
            Ok(TypedValue::Object(map))
        }
    }
}

/// Parse a JSON object according to field configs, returning a HashMap<String, TypedValue>.
pub fn parse_to_typed(
    json_data: &Value,
    field_configs: &[FieldConfig],
) -> AppResult<HashMap<String, TypedValue>> {
    // We expect `json_value` to be an object
    let obj = json_data.as_object().ok_or_else(|| {
        log::error!("Expected JSON object: <invalid utf-8>");
        PipelineError::ParseError("Expected JSON object: <invalid utf-8>".to_string())
    })?;

    let mut typed_map = HashMap::new();

    // For each field config, retrieve and parse
    for fc in field_configs {
        let field_name = &fc.field;
        let field_type = &fc.type_name;

        let raw_value = match obj.get(field_name) {
            Some(val) => val,
            None => {
                log::error!("Missing field '{}'", field_name);
                return Err(
                    PipelineError::ParseError(format!("Missing field '{}'", field_name)).into(),
                );
            }
        };

        let typed_value = parse_field_value(raw_value, field_type).map_err(|e| {
            log::error!("Error parsing field '{}': {}", field_name, e);
            PipelineError::ParseError(format!("Error parsing field '{}': {}", field_name, e))
        })?;

        typed_map.insert(field_name.clone(), typed_value);
    }
    Ok(typed_map)
}

//-------------------------------------------- Arrow Utils ------------------------------------------

fn create_builder_from_data_type(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::UInt64 => Box::new(UInt64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        DataType::List(inner_type) => {
            let inner_builder = create_builder_from_data_type(inner_type.data_type());
            Box::new(ListBuilder::new(inner_builder))
        }
        DataType::Map(entries, _sorted) => {
            let (key_type, value_type) = match entries.data_type() {
                DataType::Struct(fields) => {
                    let key_type = fields.get(0).unwrap().data_type();
                    let value_type = fields.get(1).unwrap().data_type();
                    (key_type, value_type)
                }
                _ => {
                    log::error!("Invalid map entry type: {:?}", entries.data_type());
                    panic!("Invalid map entry type: {:?}", entries.data_type());
                }
            };
            Box::new(MapBuilder::new(
                None,
                create_builder_from_data_type(key_type),
                create_builder_from_data_type(value_type),
            ))
        }
        _ => {
            log::error!("Unsupported data type: {:?}", data_type);
            panic!("Unsupported data type: {:?}", data_type);
        }
    }
}

fn append_value_to_builder(builder: &mut dyn ArrayBuilder, value: &TypedValue) {
    match value {
        TypedValue::Null => {
            // Append a null in whichever builder is found.
            if let Some(b) = builder.as_any_mut().downcast_mut::<NullBuilder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<UInt64Builder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_null();
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
            {
                b.append_null();
            }
        }
        TypedValue::U64(value) => {
            append_to_builder!(builder, UInt64Builder, *value)
        }
        TypedValue::I64(value) => {
            append_to_builder!(builder, Int64Builder, *value)
        }
        TypedValue::F64(value) => {
            append_to_builder!(builder, Float64Builder, *value)
        }
        TypedValue::Bool(value) => {
            append_to_builder!(builder, BooleanBuilder, *value)
        }
        TypedValue::String(value) => {
            append_to_builder!(builder, StringBuilder, value)
        }
        TypedValue::DateTime(value) => {
            // Convert OffsetDateTime to microseconds
            if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
            {
                let nanos = value.timestamp_nanos_opt().unwrap_or(0);
                // Attempt to cast i128 -> i64
                let micros = nanos / 1_000;
                let micros_i64 = match i64::try_from(micros) {
                    Ok(val) => val,
                    Err(_) => {
                        log::error!("DateTime value out of range for i64 microseconds: {value}");
                        // Decide how you want to handle out-of-range datetimes.
                        // For demonstration, we'll just clamp to 0:
                        0
                    }
                };
                b.append_value(micros_i64);
            }
        }
        TypedValue::Array(value) => {
            if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            {
                b.append(true); // start a new list

                let inner_builder: &mut dyn ArrayBuilder = b.values();
                for item in value {
                    append_value_to_builder(inner_builder, item);
                }
            }
        }
        TypedValue::Object(value_map) => {
            if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
            {
                b.append(true).unwrap_or_else(|err| {
                    log::error!("Error appending MapBuilder: {:?}", err);
                });

                for (key, val) in value_map {
                    // Handle the key
                    match key {
                        KeyValue::String(s) => {
                            if let Some(sb) = b.keys().as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                sb.append_value(s);
                            }
                        }
                        KeyValue::U64(n) => {
                            if let Some(ub) = b.keys().as_any_mut().downcast_mut::<UInt64Builder>()
                            {
                                ub.append_value(*n);
                            }
                        }
                        KeyValue::I64(n) => {
                            if let Some(ib) = b.keys().as_any_mut().downcast_mut::<Int64Builder>() {
                                ib.append_value(*n);
                            }
                        }
                        KeyValue::Bool(b_val) => {
                            if let Some(bb) = b.keys().as_any_mut().downcast_mut::<BooleanBuilder>()
                            {
                                bb.append_value(*b_val);
                            }
                        }
                        KeyValue::DateTime(dt) => {
                            if let Some(db) = b
                                .keys()
                                .as_any_mut()
                                .downcast_mut::<TimestampMicrosecondBuilder>()
                            {
                                let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
                                let micros = nanos / 1_000;
                                let micros_i64 = i64::try_from(micros).unwrap_or(0);
                                db.append_value(micros_i64);
                            }
                        }
                        _ => {
                            log::warn!("Unsupported map key type: {:?}", key);
                        }
                    }

                    // Handle the value
                    append_value_to_builder(b.values(), val);
                }
            }
        }
    }
}

pub fn build_record_batch_from_vec(
    arrow_schema: Arc<Schema>,
    data: &[HashMap<String, TypedValue>],
) -> AppResult<RecordBatch> {
    // For each field in the schema, create a corresponding array builder.
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    for field in arrow_schema.fields().iter() {
        let builder: Box<dyn ArrayBuilder> = create_builder_from_data_type(field.data_type());
        builders.push(builder);
    }

    // Iterate over each "row" in the Vec.
    for row_map in data.iter() {
        // For each field in the schema (by index)
        for (field_index, field) in arrow_schema.fields().iter().enumerate() {
            let builder = builders[field_index].as_mut();
            if let Some(value) = row_map.get(field.name()) {
                // Append the value if the field exists in the row.
                append_value_to_builder(builder, value);
            } else {
                // Otherwise, explicitly append a null value.
                append_value_to_builder(builder, &TypedValue::Null);
            }
        }
    }

    // Finish building all arrays.
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(builders.len());
    for builder in builders.iter_mut() {
        arrays.push(builder.finish().into());
    }

    let record_batch = RecordBatch::try_new(arrow_schema, arrays).map_err(|e| {
        log::error!("Error creating RecordBatch: {:?}", e);
        PipelineError::ParseError(format!("Error creating RecordBatch: {:?}", e))
    })?;

    Ok(record_batch)
}

//-------------------------------------------- Tests ------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, UInt64Array};
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};

    // ===== Type Parsing Tests =====
    #[test]
    fn test_parse_to_typed() {
        let field_configs = vec![
            FieldConfig {
                field: "int_field".to_string(),
                type_name: FieldType::U64,
            },
            FieldConfig {
                field: "float_field".to_string(),
                type_name: FieldType::F64,
            },
            FieldConfig {
                field: "bool_field".to_string(),
                type_name: FieldType::Bool,
            },
            FieldConfig {
                field: "string_field".to_string(),
                type_name: FieldType::String,
            },
            FieldConfig {
                field: "datetime_field".to_string(),
                type_name: FieldType::DateTime,
            },
            FieldConfig {
                field: "object_field".to_string(),
                type_name: FieldType::HashMap {
                    key_type: Box::new(KeyFieldType::String),
                    value_type: Box::new(FieldType::String),
                },
            },
        ];

        let datetime_str = "2023-10-12T00:00:00Z";
        let json_obj = json!({
            "int_field": 123,
            "float_field": 45.67,
            "bool_field": true,
            "string_field": "test",
            "datetime_field": datetime_str,
            "object_field": { "nested": "value" }
        });

        let typed = parse_to_typed(&json_obj, &field_configs)
            .expect("Parsing to typed values should succeed");

        assert_eq!(typed.get("int_field"), Some(&TypedValue::U64(123)));
        assert_eq!(typed.get("float_field"), Some(&TypedValue::F64(45.67)));
        assert_eq!(typed.get("bool_field"), Some(&TypedValue::Bool(true)));
        assert_eq!(
            typed.get("string_field"),
            Some(&TypedValue::String("test".to_string()))
        );

        let expected_dt = DateTime::parse_from_rfc3339(datetime_str)
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            typed.get("datetime_field"),
            Some(&TypedValue::DateTime(expected_dt))
        );

        if let Some(TypedValue::Object(map)) = typed.get("object_field") {
            let key = KeyValue::String("nested".to_string());
            assert_eq!(
                map.get(&key),
                Some(&TypedValue::String("value".to_string()))
            );
        } else {
            panic!("Expected 'object_field' to be an Object variant");
        }
    }

    // ===== Record Batch Tests =====
    fn create_mock_schema() -> Arc<Schema> {
        let fields = vec![
            Field::new("id", DataType::UInt64, true),
            Field::new("name", DataType::Utf8, true),
        ];
        Arc::new(Schema::new(Fields::from(fields)))
    }

    #[test]
    fn test_build_record_batch_from_vec() {
        let schema = create_mock_schema();
        let data = vec![
            HashMap::from([
                ("id".to_string(), TypedValue::U64(1)),
                ("name".to_string(), TypedValue::String("test1".to_string())),
            ]),
            HashMap::from([
                ("id".to_string(), TypedValue::U64(2)),
                ("name".to_string(), TypedValue::String("test2".to_string())),
            ]),
        ];

        let batch = build_record_batch_from_vec(schema, &data).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(name_array.value(0), "test1");
        assert_eq!(name_array.value(1), "test2");
    }

    #[test]
    fn test_build_record_batch_with_nulls() {
        let schema = create_mock_schema();
        let data = vec![
            HashMap::from([
                ("id".to_string(), TypedValue::U64(1)),
                ("name".to_string(), TypedValue::Null),
            ]),
            HashMap::from([
                ("id".to_string(), TypedValue::Null),
                ("name".to_string(), TypedValue::String("test2".to_string())),
            ]),
        ];

        let batch = build_record_batch_from_vec(schema, &data).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert!(id_array.is_null(1));
        assert!(name_array.is_null(0));
        assert_eq!(name_array.value(1), "test2");
    }
}
