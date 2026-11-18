//! Conversions between `dggs-stream-core` columnar types and `arrow-rs`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
    UInt64Array, UInt8Array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, UInt64Type};
use datafusion::error::{DataFusionError, Result};
use dggs_stream_core::columnar::{ArrowDataType, ColumnArray, ColumnSchema};
use dggs_stream_core::error::DggsStreamError;

/// Arrow extension name SedonaDB (and GeoArrow readers) use for WKB geometry.
pub const GEOARROW_WKB_EXTENSION: &str = "geoarrow.wkb";

/// GeoArrow extension metadata: H3 cell boundaries are emitted in lon/lat.
pub const GEOARROW_WKB_METADATA: &str = r#"{"crs":"OGC:CRS84"}"#;

/// Name of the WKB polygon column in every `dggs_stream` table schema.
pub(crate) const WKB_COLUMN: &str = "wkb";

pub(crate) fn to_df_error(err: DggsStreamError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

/// A `Binary` field tagged as `geoarrow.wkb` in `OGC:CRS84`.
pub fn geometry_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Binary, nullable).with_metadata(HashMap::from([
        (
            "ARROW:extension:name".to_string(),
            GEOARROW_WKB_EXTENSION.to_string(),
        ),
        (
            "ARROW:extension:metadata".to_string(),
            GEOARROW_WKB_METADATA.to_string(),
        ),
    ]))
}

pub(crate) fn to_arrow_type(dt: ArrowDataType) -> DataType {
    match dt {
        ArrowDataType::UInt64 => DataType::UInt64,
        ArrowDataType::Int64 => DataType::Int64,
        ArrowDataType::UInt8 => DataType::UInt8,
        ArrowDataType::Float64 => DataType::Float64,
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Utf8 => DataType::Utf8,
        ArrowDataType::Binary => DataType::Binary,
        ArrowDataType::ValueHistogram | ArrowDataType::BinHistogram => {
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(histogram_fields(dt == ArrowDataType::BinHistogram)),
                false,
            )))
        }
    }
}

/// The entry fields of a histogram column: `lower, upper, weight` when binned, else
/// `class, weight`.
fn histogram_fields(binned: bool) -> arrow::datatypes::Fields {
    let float = |name: &str| Field::new(name, DataType::Float64, false);
    if binned {
        vec![float("lower"), float("upper"), float("weight")].into()
    } else {
        vec![Field::new("class", DataType::Int64, false), float("weight")].into()
    }
}

/// Arrow schema for a `dggs_stream` table, with the `wkb` column tagged as geometry.
pub(crate) fn to_arrow_schema(schema: &ColumnSchema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields
        .iter()
        .map(|f| {
            if f.name == WKB_COLUMN && f.data_type == ArrowDataType::Binary {
                geometry_field(&f.name, f.nullable)
            } else {
                Field::new(&f.name, to_arrow_type(f.data_type), f.nullable)
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn validity_of(array: &dyn Array) -> Option<Vec<bool>> {
    array.nulls().map(|n| n.iter().collect())
}

/// Convert an argument array (already coerced to `UInt64`, `Int64` or `Utf8`) to a core column.
pub(crate) fn to_column_array(array: &ArrayRef) -> Result<ColumnArray> {
    let validity = validity_of(array.as_ref());
    match array.data_type() {
        DataType::UInt64 => Ok(ColumnArray::UInt64 {
            values: array.as_primitive::<UInt64Type>().values().to_vec(),
            validity,
        }),
        DataType::Int64 => Ok(ColumnArray::Int64 {
            values: array.as_primitive::<Int64Type>().values().to_vec(),
            validity,
        }),
        DataType::Utf8 => Ok(ColumnArray::Utf8 {
            values: array
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap_or_default().to_string())
                .collect(),
            validity,
        }),
        other => Err(DataFusionError::Internal(format!(
            "dggs_stream UDF received an uncoerced argument type {other}"
        ))),
    }
}

fn nulls(validity: Option<Vec<bool>>) -> Option<NullBuffer> {
    validity.map(NullBuffer::from)
}

fn masked<T>(values: Vec<T>, validity: Option<Vec<bool>>) -> Vec<Option<T>> {
    match validity {
        Some(v) => values
            .into_iter()
            .zip(v)
            .map(|(x, ok)| ok.then_some(x))
            .collect(),
        None => values.into_iter().map(Some).collect(),
    }
}

/// Convert a core column produced by a UDF back to an Arrow array.
pub(crate) fn from_column_array(column: ColumnArray) -> ArrayRef {
    match column {
        ColumnArray::UInt64 { values, validity } => {
            Arc::new(UInt64Array::new(values.into(), nulls(validity)))
        }
        ColumnArray::Int64 { values, validity } => {
            Arc::new(Int64Array::new(values.into(), nulls(validity)))
        }
        ColumnArray::UInt8 { values, validity } => {
            Arc::new(UInt8Array::new(values.into(), nulls(validity)))
        }
        ColumnArray::Float64 { values, validity } => {
            Arc::new(Float64Array::new(values.into(), nulls(validity)))
        }
        ColumnArray::Boolean { values, validity } => Arc::new(BooleanArray::new(
            BooleanBuffer::from(values),
            nulls(validity),
        )),
        ColumnArray::Utf8 { values, validity } => {
            Arc::new(StringArray::from_iter(masked(values, validity)))
        }
        ColumnArray::Binary { values, validity } => {
            Arc::new(BinaryArray::from_iter(masked(values, validity)))
        }
        ColumnArray::Histogram(column) => {
            let fields = histogram_fields(column.binned);
            let float = |values: Vec<f64>| Arc::new(Float64Array::from(values)) as ArrayRef;
            let entries: Vec<ArrayRef> = if column.binned {
                vec![
                    float(column.lower),
                    float(column.upper),
                    float(column.weights),
                ]
            } else {
                vec![
                    Arc::new(arrow::array::Int64Array::from(column.classes)),
                    float(column.weights),
                ]
            };
            let item = Arc::new(Field::new("item", DataType::Struct(fields.clone()), false));
            Arc::new(arrow::array::ListArray::new(
                item,
                arrow::buffer::OffsetBuffer::new(column.offsets.into()),
                Arc::new(arrow::array::StructArray::new(fields, entries, None)),
                None,
            ))
        }
    }
}
