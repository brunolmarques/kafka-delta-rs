use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, MapBuilder,
    NullBuilder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::handlers::{AppError, AppResult, PipelineError};
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
            let dt = s.parse::<DateTime<Utc>>().map_err(|e| {
                log::error!("Field '{}' not a valid DateTime: {}", field_type, e);
                PipelineError::ParseError(format!("Invalid DateTime format: {e}"))
            })?;
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

            // Map key types are limited to types that implment Eq and Hash.
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

/// Parse a JSON object according to field configs, function uses a complete schema adherence
/// to the field configs. This means that the JSON object must contain all fields defined in the schema.
/// The function will return a HashMap<String, TypedValue> where the keys are the field names
/// and the values are the parsed TypedValue.
/// If a field is missing or has an invalid type, the function will return an error.
/*
 Why Return a HashMap Instead of a Struct?

When the schema is not fully known at compile time, storing the parsed fields in a HashMap<String, TypedValue>
can be a convenient way to preserve some dynamism. For each field in the YAML-configured schema, it's possible
to parse a value from the JSON and store it by name. This means that the code doesn't require a fixed set of fields
or a dedicated Rust struct matching that set. Instead, relying on a "field definition" from the config.

Pros

Easily add/remove fields in the YAML config without modifying the core Rust code or recompiling.
It's simpler to write a generic "pull field X, parse as type Y, store in the map" function than generating/maintaining
dozens of different typed structs.This structure is more flexible for multi-tenant or multi-schema scenarios.

Cons

Lose the type safety of a compile-time struct. Access to a field is always something like record.get("some_field"),
returning an Option<&TypedValue> rather than a strongly typed field. Require handling arrays, nested objects, etc.
case-by-case, often by adding more variants to TypedValue or storing them as JSON again.
*/
/// Output example:
/// ```json
/// {
///     "user_id": U64(123),
///     "user_name": String("John Doe"),
///     "is_active": Bool(true),
///     "created_at": DateTime("2023-10-12T00:00:00Z"),
///     "preferences": Array([
///         String("dark_mode"),
///         String("notifications")
///     ]),
///     "metadata": HashMap({
///         "key1": String("value1"),
///         "key2": U64(42)
///     })
/// }
/// ```
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

// Tests Type Parsing ------------------------------------------

#[cfg(test)]
mod type_parsing_tests {
    use super::*;
    use chrono::Utc;
    use serde_json::{Value, json};

    #[test]
    fn test_parse_field_value() {
        // Test Null
        assert_eq!(
            parse_field_value(&Value::Null, &FieldType::Null),
            Ok(TypedValue::Null)
        );

        // Test Bool
        assert_eq!(
            parse_field_value(&Value::Bool(true), &FieldType::Bool),
            Ok(TypedValue::Bool(true))
        );

        // Test Number (parsed as u64)
        let num = json!(42);
        assert_eq!(
            parse_field_value(&num, &FieldType::U64),
            Ok(TypedValue::U64(42))
        );

        // Test Number (parsed as i64)
        let num = json!(-42);
        assert_eq!(
            parse_field_value(&num, &FieldType::I64),
            Ok(TypedValue::I64(-42))
        );

        // Test Number (parsed as f64)
        let num = json!(42.5);
        assert_eq!(
            parse_field_value(&num, &FieldType::F64),
            Ok(TypedValue::F64(42.5))
        );

        // Test String
        let string = json!("hello");
        assert_eq!(
            parse_field_value(&string, &FieldType::String),
            Ok(TypedValue::String("hello".to_string()))
        );

        // Test DateTime
        let datetime_str = "2023-10-12T00:00:00Z";
        let datetime = json!(datetime_str);
        let expected_dt = DateTime::parse_from_str(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            parse_field_value(&datetime, &FieldType::DateTime),
            Ok(TypedValue::DateTime(expected_dt))
        );

        // Test Array
        let array = json!([false, 3.14, "test"]);
        let item_type = Box::new(FieldType::String);
        let array_type = FieldType::Array { item_type };
        let result = parse_field_value(&array, &array_type);
        match result {
            Ok(TypedValue::Array(items)) => {
                assert_eq!(items.len(), 3);
                // Note: In a real test, you'd check the actual values, but since we're using
                // String as the item_type, the parsing will fail for non-string values
            }
            _ => panic!("Expected Array variant"),
        }

        // Test Object (HashMap)
        let object = json!({"key": "value", "num": 7});
        let key_type = Box::new(KeyFieldType::String);
        let value_type = Box::new(FieldType::String);
        let map_type = FieldType::HashMap {
            key_type,
            value_type,
        };
        let result = parse_field_value(&object, &map_type);
        match result {
            Ok(TypedValue::Object(map)) => {
                assert_eq!(map.len(), 2);
                // Note: In a real test, you'd check the actual key-value pairs
            }
            _ => panic!("Expected Object variant"),
        }
    }

    #[test]
    fn test_parse_to_typed() {
        // Create a dummy field config array.
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

        // Build a JSON object with matching types.
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

        let expected_dt = DateTime::parse_from_str(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            typed.get("datetime_field"),
            Some(&TypedValue::DateTime(expected_dt))
        );

        if let Some(TypedValue::Object(map)) = typed.get("object_field") {
            // Since we're using KeyValue as the key type, we need to create a KeyValue to look up
            let key = KeyValue::String("nested".to_string());
            assert_eq!(
                map.get(&key),
                Some(&TypedValue::String("value".to_string()))
            );
        } else {
            panic!("Expected 'object_field' to be an Object variant");
        }
    }
}

//-------------------------------------------- Arrow Utils ------------------------------------------

// Parse a FieldConfig into a DataType recursively
fn parse_field_config(field_type: &FieldType) -> DataType {
    match field_type {
        FieldType::Null => DataType::Null,
        FieldType::U64 => DataType::UInt64,
        FieldType::I64 => DataType::Int64,
        FieldType::F64 => DataType::Float64,
        FieldType::Bool => DataType::Boolean,
        FieldType::String => DataType::Utf8,
        FieldType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
        FieldType::Array { item_type } => DataType::List(Arc::new(Field::new_list_field(
            parse_field_config(&*item_type),
            true,
        ))),
        FieldType::HashMap {
            key_type,
            value_type,
        } => {
            let parsed_key_type = match **key_type {
                KeyFieldType::U64 => DataType::UInt64,
                KeyFieldType::I64 => DataType::Int64,
                KeyFieldType::Bool => DataType::Boolean,
                KeyFieldType::String => DataType::Utf8,
                KeyFieldType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            };
            DataType::Map(
                Arc::new(Field::new(
                    "map",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", parsed_key_type, false),
                        Field::new("value", parse_field_config(value_type), false),
                    ])),
                    false,
                )),
                true,
            )
        }
    }
}

pub fn build_arrow_schema_from_config(field_configs: &[FieldConfig]) -> Arc<Schema> {
    let fields: Vec<Field> = field_configs
        .iter()
        .map(|fc| Field::new(fc.field.clone(), parse_field_config(&fc.type_name), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn create_builder_from_data_type(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
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

pub fn build_record_batch_from_btreemap(
    arrow_schema: Arc<Schema>,
    data: &BTreeMap<i64, HashMap<String, TypedValue>>,
) -> AppResult<RecordBatch> {
    // For each field in the schema, create a corresponding array builder.
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    for field in arrow_schema.fields().iter() {
        let builder: Box<dyn ArrayBuilder> = create_builder_from_data_type(field.data_type());
        builders.push(builder);
    }

    // Now, iterate over each "row" in the BTreeMap, and for each column,
    // fetch the value from the row's HashMap and append it to the correct builder.
    for (_row_key, row_map) in data.iter() {
        for (field_name, value) in row_map.iter() {
            let field_index = arrow_schema
                .fields()
                .iter()
                .position(|f| f.name() == field_name)
                .unwrap_or_else(|| {
                    log::error!("Field not found: {}", field_name);
                    panic!("Field not found: {}", field_name);
                });

            let builder = builders[field_index].as_mut();
            match value {
                TypedValue::Null => {
                    if let Some(b) = builder.as_any().downcast_mut::<NullBuilder>() {
                        b.append_null();
                    }
                }
                TypedValue::U64(value) => {
                    if let Some(b) = builder.as_any().downcast_mut::<UInt64Builder>() {
                        b.append_value(*value);
                    }
                }
                TypedValue::I64(value) => {
                    if let Some(b) = builder.as_any().downcast_mut::<Int64Builder>() {
                        b.append_value(*value);
                    }
                }
                TypedValue::F64(value) => {
                    if let Some(b) = builder.as_any().downcast_mut::<Float64Builder>() {
                        b.append_value(*value);
                    }
                }
                TypedValue::Bool(value) => {
                    if let Some(b) = builder.as_any().downcast_mut::<BooleanBuilder>() {
                        b.append_value(*value);
                    }
                }
                TypedValue::String(value) => {
                    if let Some(b) = builder.as_any().downcast_mut::<StringBuilder>() {
                        b.append_value(value);
                    }
                }
                TypedValue::DateTime(value) => {
                    if let Some(b) = builder
                        .as_any()
                        .downcast_mut::<TimestampMicrosecondBuilder>()
                    {
                        b.append_value(value.timestamp_micros());
                    }
                }
                TypedValue::Array(value) => {
                    if let Some(b) = builder.as_any().downcast_mut::<ListBuilder<_>>() {
                        // Start a new list
                        b.append(true);

                        // Get the inner builder
                        let inner_builder: &mut dyn ArrayBuilder = b.values();

                        // Append each element
                        // TODO: Check if it's possible to use recursive function here
                        for item in value {
                            match item {
                                TypedValue::String(s) => {
                                    if let Some(sb) =
                                        inner_builder.as_any().downcast_mut::<StringBuilder>()
                                    {
                                        sb.append_value(s);
                                    }
                                }
                                TypedValue::U64(n) => {
                                    if let Some(ub) =
                                        inner_builder.as_any().downcast_mut::<UInt64Builder>()
                                    {
                                        ub.append_value(n);
                                    }
                                }
                                // Add other types as needed
                                _ => {
                                    log::warn!("Unsupported array element type: {:?}", item);
                                }
                            }
                        }
                    }
                }
                TypedValue::Object(value) => {
                    if let Some(b) = builder
                        .as_any()
                        .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
                    {
                        // Start a new map
                        b.append(true);

                        // TODO: Validate this part
                        // Get the key and value builders
                        let key_builder = b.keys();
                        let value_builder = b.values();

                        // Append each key-value pair
                        for (key, val) in value {
                            match key {
                                KeyValue::String(s) => {
                                    if let Some(sb) =
                                        key_builder.as_any().downcast_mut::<StringBuilder>()
                                    {
                                        sb.append_value(s);
                                    }
                                }
                                KeyValue::U64(n) => {
                                    if let Some(ub) =
                                        key_builder.as_any().downcast_mut::<UInt64Builder>()
                                    {
                                        ub.append_value(n);
                                    }
                                }
                                // Add other key types as needed
                                _ => {
                                    log::warn!("Unsupported map key type: {:?}", key);
                                }
                            }

                            match val {
                                TypedValue::String(s) => {
                                    if let Some(sb) =
                                        value_builder.as_any().downcast_mut::<StringBuilder>()
                                    {
                                        sb.append_value(s);
                                    }
                                }
                                TypedValue::U64(n) => {
                                    if let Some(ub) =
                                        value_builder.as_any().downcast_mut::<UInt64Builder>()
                                    {
                                        ub.append_value(n);
                                    }
                                }
                                // Add other value types as needed
                                _ => {
                                    log::warn!("Unsupported map value type: {:?}", val);
                                }
                            }
                        }
                    }
                }
                _ => {
                    log::error!("Unsupported value type: {:?}", value);
                    return Err(PipelineError::ParseError(format!(
                        "Unsupported value type: {:?}",
                        value
                    ))
                    .into());
                }
            }
        }
    }

    // Once all rows have been appended, build the final Arrow arrays
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(builders.len());
    for builder in builders {
        arrays.push(builder.finish().into());
    }

    // Create a RecordBatch from the arrays
    let record_batch = RecordBatch::try_new(arrow_schema, arrays).map_err(|e| {
        log::error!("Error creating RecordBatch: {:?}", e);
        PipelineError::ParseError(format!("Error creating RecordBatch: {:?}", e)).into()
    })?;

    Ok(record_batch)
}

//-------------------------------------------- Arrow Utils Tests ------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_arrow_schema_from_config() {
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
                field: "string_field".to_string(),
                type_name: FieldType::String,
            },
            FieldConfig {
                field: "bool_field".to_string(),
                type_name: FieldType::Bool,
            },
            FieldConfig {
                field: "datetime_field".to_string(),
                type_name: FieldType::DateTime,
            },
        ];

        let schema = build_arrow_schema_from_config(&field_configs).unwrap();

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "int_field");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(1).name(), "float_field");
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(2).name(), "string_field");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).name(), "bool_field");
        assert_eq!(schema.field(3).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(4).name(), "datetime_field");
        assert_eq!(
            schema.field(4).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_convert_records_to_arrow() {
        let field_configs = vec![
            FieldConfig {
                field: "int_field".to_string(),
                type_name: FieldType::U64,
            },
            FieldConfig {
                field: "string_field".to_string(),
                type_name: FieldType::String,
            },
        ];

        let schema = build_arrow_schema_from_config(&field_configs).unwrap();

        let mut records = Vec::new();
        let mut record1 = HashMap::new();
        record1.insert("int_field".to_string(), TypedValue::U64(42));
        record1.insert(
            "string_field".to_string(),
            TypedValue::String("hello".to_string()),
        );
        records.push(record1);

        let mut record2 = HashMap::new();
        record2.insert("int_field".to_string(), TypedValue::U64(123));
        record2.insert(
            "string_field".to_string(),
            TypedValue::String("world".to_string()),
        );
        records.push(record2);

        let batch = convert_records_to_arrow(&records, &schema).unwrap();

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 2);

        // Check int column
        let int_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(1), 123);

        // Check string column
        let string_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "world");
    }

    #[test]
    fn test_convert_records_with_nulls() {
        let field_configs = vec![FieldConfig {
            field: "nullable_int".to_string(),
            type_name: FieldType::U64,
        }];

        let schema = build_arrow_schema_from_config(&field_configs).unwrap();

        let mut records = Vec::new();
        let mut record1 = HashMap::new();
        record1.insert("nullable_int".to_string(), TypedValue::Null);
        records.push(record1);

        let mut record2 = HashMap::new();
        record2.insert("nullable_int".to_string(), TypedValue::U64(42));
        records.push(record2);

        let batch = convert_records_to_arrow(&records, &schema).unwrap();

        let int_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(int_array.is_null(0));
        assert_eq!(int_array.value(1), 42);
    }
}
