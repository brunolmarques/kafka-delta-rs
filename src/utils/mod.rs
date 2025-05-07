use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder,
    ListBuilder, MapBuilder, NullBuilder, StringBuilder, TimestampMicrosecondBuilder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::handlers::{AppError, AppResult, ParseError};
use crate::model::{KeyValue, TypedValue};

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

/// Parse a single JSON Value into a TypedValue, given an Arrow DataType.
fn json_to_typed(value: &Value, dt: &DataType) -> AppResult<TypedValue> {
    use TypedValue::*;
    match (dt, value) {
        (DataType::Utf8, Value::String(s)) => Ok(Utf8(s.clone())),
        (DataType::Boolean, Value::Bool(b)) => Ok(Boolean(*b)),
        (DataType::Int64, Value::Number(n)) if n.is_i64() => Ok(Int64(n.as_i64().unwrap())),
        (DataType::Float64, Value::Number(n)) => Ok(Float64(n.as_f64().unwrap())),
        (DataType::Date32, Value::String(s)) => {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| ParseError::BadDate(s.clone(), e))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Ok(Date((d - epoch).num_days() as i32))
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), Value::String(s)) => {
            let ts = DateTime::parse_from_rfc3339(s)
                .map_err(|e| ParseError::BadTimestamp(s.clone(), e))?
                .with_timezone(&Utc)
                .timestamp_micros();
            Ok(Timestamp(ts))
        }
        // ---------- ARRAY ----------
        (DataType::List(field), Value::Array(arr)) => {
            let inner_type = field.data_type();
            let mut parsed = Vec::with_capacity(arr.len());
            for v in arr {
                parsed.push(json_to_typed(v, inner_type)?);
            }
            Ok(Array(parsed))
        }
        // ---------- MAP ----------
        (DataType::Map(field_arc, _), Value::Object(obj)) => {
            if let DataType::Struct(fields) = field_arc.data_type() {
                let key_field = fields.iter().find(|f| f.name() == "key").ok_or_else(|| {
                    ParseError::TypeMismatch("map".into(), dt.clone(), value.clone())
                })?;
                let val_field = fields.iter().find(|f| f.name() == "value").ok_or_else(|| {
                    ParseError::TypeMismatch("map".into(), dt.clone(), value.clone())
                })?;

                let key_type = key_field.data_type();
                let val_type = val_field.data_type();

                let mut map = HashMap::with_capacity(obj.len());
                for (k, v) in obj {
                    let key_parsed =
                        KeyValue::from(json_to_typed(&Value::String(k.clone()), key_type)?);
                    let val_parsed = json_to_typed(v, val_type)?;
                    map.insert(key_parsed, val_parsed);
                }

                Ok(TypedValue::Object(map))
            } else {
                log::error!("Invalid map entry type: {:?}", field_arc.data_type());
                Err(AppError::Parse(ParseError::TypeMismatch(
                    "map".into(),
                    dt.clone(),
                    value.clone(),
                )))
            }
        }
        // ---------- NULL ----------
        (_, Value::Null) => Ok(Null),

        // ---------- FALLBACK ----------
        _ => {
            log::error!("Unsupported data type: {dt:?}");
            Err(AppError::Parse(ParseError::TypeMismatch(
                "field".into(),
                dt.clone(),
                value.clone(),
            )))
        }
    }
}

#[allow(dead_code)]
/// Parse a JSON object according to Delta table schema, returning a HashMap<String, TypedValue>.
pub fn parse_json_object(
    json_data: &Value,
    delta_schema: &Schema,
) -> AppResult<Option<HashMap<String, TypedValue>>> {
    // We expect `json_value` to be an object
    let obj = json_data.as_object().ok_or_else(|| {
        log::error!("Expected JSON object: <invalid utf-8>");
        ParseError::BadJsonObject("Expected JSON object".into(), json_data.clone())
    })?;

    if obj.is_empty() {
        return Ok(None);
    }

    let mut typed_map = HashMap::with_capacity(delta_schema.fields().len());

    // For each field in the Delta schema, retrieve and parse
    for field in delta_schema.fields() {
        let field_name = field.name();
        let data_type = field.data_type();

        let raw_value = obj.get(field_name).ok_or_else(|| {
            log::error!("Missing field '{field_name}'");
            ParseError::MissingField(format!("Missing field '{field_name}'"))
        })?;

        let typed_value = json_to_typed(raw_value, data_type)?;

        typed_map.insert(field_name.to_string(), typed_value);
    }
    Ok(Some(typed_map))
}

//--------------------------------------------- GRPC Utils ---------------------------------------------

#[allow(dead_code)]
pub fn parse_grpc_object(
    #[allow(unused)] payload: &[u8],
    #[allow(unused)] delta_schema: &Schema,
) -> AppResult<Option<HashMap<String, TypedValue>>> {
    // TODO: Implement gRPC parsing
    todo!()
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
                    let key_type = fields.first().unwrap().data_type();
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
            log::error!("Unsupported data type: {data_type:?}");
            panic!("Unsupported data type: {data_type:?}");
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
        TypedValue::Int64(value) => {
            append_to_builder!(builder, Int64Builder, *value)
        }
        TypedValue::Float64(value) => {
            append_to_builder!(builder, Float64Builder, *value)
        }
        TypedValue::Boolean(value) => {
            append_to_builder!(builder, BooleanBuilder, *value)
        }
        TypedValue::Utf8(value) => {
            append_to_builder!(builder, StringBuilder, value)
        }
        TypedValue::Timestamp(value) => {
            if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
            {
                // Convert directly from i64 to microseconds
                let micros = *value / 1_000;
                b.append_value(micros);
            }
        }
        TypedValue::Date(value) => {
            // Handle Date type - convert to days since epoch
            if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(*value);
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
                    log::error!("Error appending MapBuilder: {err:?}");
                });

                for (key, val) in value_map {
                    // Handle the key
                    match key {
                        KeyValue::Utf8(s) => {
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
                        KeyValue::Int64(n) => {
                            if let Some(ib) = b.keys().as_any_mut().downcast_mut::<Int64Builder>() {
                                ib.append_value(*n);
                            }
                        }
                        KeyValue::Boolean(b_val) => {
                            if let Some(bb) = b.keys().as_any_mut().downcast_mut::<BooleanBuilder>()
                            {
                                bb.append_value(*b_val);
                            }
                        }
                        KeyValue::Timestamp(dt) => {
                            if let Some(db) = b
                                .keys()
                                .as_any_mut()
                                .downcast_mut::<TimestampMicrosecondBuilder>()
                            {
                                // Convert directly from i64 to microseconds
                                let micros = *dt / 1_000;
                                db.append_value(micros);
                            }
                        }
                        KeyValue::Date(value) => {
                            if let Some(db) = b.keys().as_any_mut().downcast_mut::<Int32Builder>() {
                                db.append_value(*value);
                            }
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
    arrow_schema: Schema,
    data: &[HashMap<String, TypedValue>],
) -> AppResult<RecordBatch> {
    // For each field in the schema, create a corresponding array builder.
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    for field in arrow_schema.fields().iter() {
        log::info!("Building builder for field: {}", field.name());
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
        arrays.push(builder.finish());
    }

    let record_batch = RecordBatch::try_new(Arc::new(arrow_schema), arrays).map_err(|e| {
        log::error!("Error creating RecordBatch: {e:?}");
        ParseError::ArrowBatchError(format!("Error creating RecordBatch: {e:?}"))
    })?;

    Ok(record_batch)
}

//-------------------------------------------- Tests ------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    #[test]
    fn test_build_record_batch_from_vec() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let data = vec![
            HashMap::from([
                ("id".to_string(), TypedValue::U64(1)),
                ("name".to_string(), TypedValue::Utf8("test1".to_string())),
            ]),
            HashMap::from([
                ("id".to_string(), TypedValue::U64(2)),
                ("name".to_string(), TypedValue::Utf8("test2".to_string())),
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
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let data = vec![
            HashMap::from([
                ("id".to_string(), TypedValue::U64(1)),
                ("name".to_string(), TypedValue::Null),
            ]),
            HashMap::from([
                ("id".to_string(), TypedValue::Null),
                ("name".to_string(), TypedValue::Utf8("test2".to_string())),
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
