use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::model::{FieldConfig, FieldType};
use crate::handlers::{AppResult, PipelineError};
use crate::model::TypedValue;

/// Parse a JSON object according to field configs
pub fn parse_to_typed(
    json_value: &Value,
    field_configs: &[FieldConfig],
) -> AppResult<HashMap<String, TypedValue>> {
    // We expect `json_value` to be an object
    let obj = json_value.as_object().ok_or_else(|| {
        PipelineError::ParseError("Expected JSON object at top level".to_string())
    })?;

    let mut typed_map = HashMap::new();

    // For each field config, retrieve and parse
    for fc in field_configs {
        let field_name = &fc.field;
        let field_type = &fc.type_name;

        let raw_value = match obj.get(field_name) {
            Some(val) => val,
            None => {
                // You can decide whether to error or skip missing fields
                return Err(PipelineError::ParseError(
                    format!("Missing field '{}'", field_name)).into()
                );
            }
        };

        let typed_value = match field_type {
            FieldType::U64 => {
                // Convert the JSON value to a u64
                let n = raw_value.as_u64().ok_or_else(|| {
                    PipelineError::ParseError(format!(
                        "Field '{}' not a valid u64",
                        field_name
                    ))
                })?;
                TypedValue::U64(n)
            }
            FieldType::String => {
                let s = raw_value.as_str().ok_or_else(|| {
                    PipelineError::ParseError(format!(
                        "Field '{}' not a valid string",
                        field_name
                    ))
                })?;
                TypedValue::Str(s.to_string())
            }
            FieldType::DateTime => {
                // Expect the field to be a string parseable as a DateTime
                let s = raw_value.as_str().ok_or_else(|| {
                    PipelineError::ParseError(format!(
                        "Field '{}' not a valid string for DateTime",
                        field_name
                    ))
                })?;
                let dt = s.parse::<DateTime<Utc>>().map_err(|e| {
                    PipelineError::ParseError(format!(
                        "Field '{}' not a valid DateTime: {}",
                        field_name, e
                    ))
                })?;
                TypedValue::DateTime(dt)
            }
        };

        typed_map.insert(field_name.clone(), typed_value);
    }

    Ok(typed_map)
}