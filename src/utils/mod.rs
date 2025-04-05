use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;

use crate::handlers::{AppResult, PipelineError};
use crate::model::TypedValue;
use crate::model::{FieldConfig, FieldType};

//----------------------------------- Macros -------------------------------------

#[macro_export]
macro_rules! parse_json_field {
    ($raw_value:expr, $field_name:expr, $method:ident, $variant:ident) => {{
        let type_desc = stringify!($variant);
        let value = $raw_value.$method().ok_or_else(|| {
            log::error!("Field '{}' not a valid {}", $field_name, type_desc);
            PipelineError::ParseError(format!("Field '{}' not a valid {}", $field_name, type_desc))
        })?;
        TypedValue::$variant(value)
    }};
}

//------------------------------- Type Parsing -------------------------------------

// parse any JSON recursively
pub fn parse_json_value(value: &Value) -> TypedValue {
    match value {
        Value::Null => TypedValue::Null,
        Value::Bool(b) => TypedValue::Bool(*b),
        Value::Number(n) => TypedValue::F64(n.as_f64().unwrap()), // or parse carefully
        Value::String(s) => TypedValue::String(s.clone()),
        Value::Array(arr) => {
            let items = arr.iter().map(parse_json_value).collect();
            TypedValue::Array(items)
        }
        Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj.iter() {
                map.insert(k.clone(), parse_json_value(v));
            }
            TypedValue::Object(map)
        }
    }
}

/// Parse a JSON object according to field configs
/*
 Why Return a Map Instead of a Struct?

When the schema is not fully known at compile time, storing the parsed fields in a Map<String, TypedValue>
can be a convenient way to preserve some dynamism. For each field in the YAML-configured schema, it's possible
to parse a value from the JSON and store it by name. This means that the code doesn’t require a fixed set of fields
or a dedicated Rust struct matching that set. Instead, relying on a “field definition” from the config.

Pros

Easily add/remove fields in the YAML config without modifying the core Rust code or recompiling.
It’s simpler to write a generic “pull field X, parse as type Y, store in the map” function than generating/maintaining
dozens of different typed structs.This structure is more flexible for multi-tenant or multi-schema scenarios.

Cons

Lose the type safety of a compile-time struct. Access to a field is always something like record.get("some_field"),
returning an Option<&TypedValue> rather than a strongly typed field. Require handling arrays, nested objects, etc.
case-by-case, often by adding more variants to TypedValue or storing them as JSON again.
*/
pub fn parse_to_typed(
    json_value: &Value,
    field_configs: &[FieldConfig],
) -> AppResult<HashMap<String, TypedValue>> {
    // We expect `json_value` to be an object
    let obj = json_value.as_object().ok_or_else(|| {
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

        let typed_value = match field_type {
            FieldType::Null => TypedValue::Null,
            FieldType::U64 => {
                parse_json_field!(raw_value, field_name, as_u64, U64)
            }
            FieldType::I64 => {
                parse_json_field!(raw_value, field_name, as_i64, I64)
            }
            FieldType::F64 => {
                parse_json_field!(raw_value, field_name, as_f64, F64)
            }
            FieldType::Bool => {
                parse_json_field!(raw_value, field_name, as_bool, Bool)
            }
            FieldType::String => {
                // Expect the field to be a string
                let s = raw_value.as_str().ok_or_else(|| {
                    log::error!("Field '{}' not a valid string", field_name);
                    PipelineError::ParseError(format!("Field '{}' not a valid string", field_name))
                })?;
                TypedValue::String(s.to_owned())
            }
            FieldType::Array => {
                if !raw_value.is_array() {
                    log::error!("Field '{}' not a valid array", field_name);
                    return Err(PipelineError::ParseError(format!(
                        "Field '{}' not a valid array",
                        field_name
                    ))
                    .into());
                }
                parse_json_value(raw_value)
            }
            FieldType::DateTime => {
                // Expect the field to be a string parseable as a DateTime
                let s = raw_value.as_str().ok_or_else(|| {
                    log::error!("Field '{}' not a valid string for DateTime", field_name);
                    PipelineError::ParseError(format!(
                        "Field '{}' not a valid string for DateTime",
                        field_name
                    ))
                })?;
                let dt = DateTime::parse_from_rfc3339(s).map_err(|e| {
                    log::error!("Field '{}' not a valid DateTime: {}", field_name, e);
                    PipelineError::ParseError(format!(
                        "Field '{}' not a valid DateTime: {}",
                        field_name, e
                    ))
                })?;
                TypedValue::DateTime(dt.with_timezone(&Utc))
            }
            FieldType::HashMap => {
                if !raw_value.is_object() {
                    log::error!("Field '{}' not a valid HashMap", field_name);
                    return Err(PipelineError::ParseError(format!(
                        "Field '{}' not a valid HashMap",
                        field_name
                    ))
                    .into());
                }

                // Parse the JSON object into a TypedValue
                parse_json_value(raw_value)
            }
        };
        typed_map.insert(field_name.clone(), typed_value);
    }
    Ok(typed_map)
}

//-------------------------------------------- Tests ------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::{Value, json};

    #[test]
    fn test_parse_json_value() {
        // Test Null
        assert_eq!(parse_json_value(&Value::Null), TypedValue::Null);
        // Test Bool
        assert_eq!(parse_json_value(&Value::Bool(true)), TypedValue::Bool(true));
        // Test Number (parsed as f64)
        let num = json!(42);
        assert_eq!(parse_json_value(&num), TypedValue::F64(42.0));
        // Test String
        let string = json!("hello");
        assert_eq!(
            parse_json_value(&string),
            TypedValue::String("hello".to_string())
        );
        // Test Array
        let array = json!([false, 3.14, "test"]);
        if let TypedValue::Array(items) = parse_json_value(&array) {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected Array variant");
        }
        // Test Object
        let object = json!({"key": "value", "num": 7});
        if let TypedValue::Object(map) = parse_json_value(&object) {
            assert_eq!(
                map.get("key"),
                Some(&TypedValue::String("value".to_string()))
            );
            assert_eq!(map.get("num"), Some(&TypedValue::F64(7.0)));
        } else {
            panic!("Expected Object variant");
        }
    }

    #[test]
    fn test_parse_to_typed() {
        // Create a dummy field config array.
        // Assuming FieldConfig has fields: 'field' and 'type_name'
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
                type_name: FieldType::HashMap,
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
            assert_eq!(
                map.get("nested"),
                Some(&TypedValue::String("value".to_string()))
            );
        } else {
            panic!("Expected 'object_field' to be an Object variant");
        }
    }
}
