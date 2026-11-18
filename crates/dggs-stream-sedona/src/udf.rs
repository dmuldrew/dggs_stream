//! The `dggs_stream` H3 scalar functions as DataFusion `ScalarUDF`s.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::scan::default_scalar_udfs;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use dggs_stream_core::columnar::ArrowDataType;
use dggs_stream_core::grid::GridKind;

use crate::convert::{from_column_array, geometry_field, to_arrow_type, to_column_array};

/// All `dggs_stream` scalar functions, ready for `SessionContext::register_udf`: the
/// [`scan`](crate::scan) cell functions of every grid, plus
/// [`H3PolygonToCells`](crate::H3PolygonToCells) and [`LonLatToCell`] for each grid.
pub fn scalar_udfs() -> Vec<ScalarUDF> {
    default_scalar_udfs()
        .into_iter()
        .map(|inner| ScalarUDF::new_from_impl(H3ScalarUdf::new(inner)))
        .chain(GridKind::ALL.into_iter().flat_map(|grid| {
            [
                ScalarUDF::new_from_impl(crate::polyfill::H3PolygonToCells::for_grid(grid)),
                ScalarUDF::new_from_impl(LonLatToCell::for_grid(grid)),
            ]
        }))
        .collect()
}

/// Adapts a [`scan`](crate::scan) H3 scalar function to DataFusion's `ScalarUDFImpl`.
///
/// Arguments are coerced to the types the core functions take (`UInt64` cells, `Utf8`
/// hex strings, `Int64` resolutions), so SQL integer and string literals work directly.
/// Functions returning WKB return a `geoarrow.wkb` field.
pub struct H3ScalarUdf {
    inner: Arc<dyn crate::scan::H3ScalarFn>,
    aliases: Vec<String>,
    signature: Signature,
}

impl H3ScalarUdf {
    pub fn new(inner: Arc<dyn crate::scan::H3ScalarFn>) -> Self {
        let aliases = inner.aliases().iter().map(|a| a.to_string()).collect();
        Self {
            inner,
            aliases,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn core_return_type(&self, arg_types: &[DataType]) -> Result<ArrowDataType> {
        let core_types = arg_types
            .iter()
            .map(|t| match t {
                DataType::UInt64 => Ok(ArrowDataType::UInt64),
                DataType::Int64 => Ok(ArrowDataType::Int64),
                DataType::Utf8 => Ok(ArrowDataType::Utf8),
                other => plan_err!("{}: unsupported argument type {other}", self.name()),
            })
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .return_type(&core_types)
            .or_else(|e| plan_err!("{e}"))
    }
}

impl PartialEq for H3ScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.inner.name() == other.inner.name()
    }
}

impl Eq for H3ScalarUdf {}

impl Hash for H3ScalarUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.name().hash(state);
    }
}

impl std::fmt::Debug for H3ScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3ScalarUdf")
            .field("name", &self.inner.name())
            .finish_non_exhaustive()
    }
}

impl ScalarUDFImpl for H3ScalarUdf {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let coerced = arg_types
            .iter()
            .enumerate()
            .map(|(i, t)| match t {
                // The first argument is a cell; later integers are resolutions.
                t if t.is_integer() || t.is_null() => Ok(if i == 0 {
                    DataType::UInt64
                } else {
                    DataType::Int64
                }),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok(DataType::Utf8),
                other => plan_err!("{}: unsupported argument type {other}", self.name()),
            })
            .collect::<Result<Vec<_>>>()?;
        // Reject wrong arity / combinations at planning time.
        self.core_return_type(&coerced)?;
        Ok(coerced)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(to_arrow_type(self.core_return_type(arg_types)?))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let return_type = self.return_type(&arg_types)?;
        Ok(Arc::new(if return_type == DataType::Binary {
            geometry_field(self.name(), true)
        } else {
            Field::new(self.name(), return_type, true)
        }))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let columns = arrays
            .iter()
            .map(to_column_array)
            .collect::<Result<Vec<_>>>()?;
        let out = self
            .inner
            .invoke(&columns)
            .map_err(crate::convert::to_df_error)?;
        let out = from_column_array(out);
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// `{grid}_lonlat_to_cell(lon, lat, resolution) -> UInt64`: the cell holding a WGS84 point
/// (degrees). NULL for a NULL argument, a latitude outside `-90..=90`, a non-finite
/// coordinate, or a resolution outside the grid's range; longitudes wrap.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LonLatToCell {
    signature: Signature,
    grid: GridKind,
    name: &'static str,
}

impl LonLatToCell {
    /// `{grid}_lonlat_to_cell`.
    pub fn for_grid(grid: GridKind) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            grid,
            name: match grid {
                GridKind::H3 => "h3_lonlat_to_cell",
                GridKind::A5 => "a5_lonlat_to_cell",
                GridKind::S2 => "s2_lonlat_to_cell",
                GridKind::Healpix => "healpix_lonlat_to_cell",
            },
        }
    }
}

impl ScalarUDFImpl for LonLatToCell {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let name = self.name;
        if arg_types.len() != 3 {
            return plan_err!("{name} expects (lon, lat, resolution)");
        }
        arg_types
            .iter()
            .enumerate()
            .map(|(i, t)| match (i, t) {
                (0 | 1, t) if t.is_numeric() || t.is_null() => Ok(DataType::Float64),
                (2, t) if t.is_integer() || t.is_null() => Ok(DataType::Int64),
                (_, t) => plan_err!(
                    "{name} expects (lon, lat, resolution), got {t} for argument {}",
                    i + 1
                ),
            })
            .collect()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use arrow::array::{Array, AsArray, UInt64Array};
        use arrow::datatypes::{Float64Type, Int64Type};
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let lon = arrays[0].as_primitive::<Float64Type>();
        let lat = arrays[1].as_primitive::<Float64Type>();
        let res = arrays[2].as_primitive::<Int64Type>();
        let out: UInt64Array = (0..lon.len())
            .map(|i| {
                if lon.is_null(i) || lat.is_null(i) || res.is_null(i) {
                    return None;
                }
                dggs_stream_core::scalar_ops::grid_lonlat_to_cell(
                    self.grid,
                    lon.value(i),
                    lat.value(i),
                    res.value(i),
                )
            })
            .collect();
        let out: Arc<dyn Array> = Arc::new(out);
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}
