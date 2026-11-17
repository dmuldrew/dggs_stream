//! Vectorized [`H3ScalarFn`] implementations for H3 cell operations.
//!
//! Operates directly on [`ColumnArray`] batches with full null-validity propagation.

use dggs_stream_core::columnar::{ArrowDataType, ColumnArray};
use dggs_stream_core::encoding::{fast_hex_u64, h3_index_to_wkb, parse_hex_u64, WkbBuf, WKB_BUF_LEN};
use dggs_stream_core::error::{RasterH3Error, Result};
use dggs_stream_core::scalar_ops::{
    h3_cell_to_parent_str, h3_cell_to_parent_u64, h3_get_resolution, h3_is_valid_str,
    h3_is_valid_u64, h3_to_lat, h3_to_lng,
};

/// A vectorized H3 scalar function over [`ColumnArray`]s. [`crate::udf::RasterH3ScalarUdf`]
/// adapts it to DataFusion's `ScalarUDFImpl`.
pub trait H3ScalarFn: Send + Sync {
    /// Canonical function name in SQL.
    fn name(&self) -> &str;

    /// Optional SQL aliases (e.g. `h3_cell_to_geometry` for `h3_to_geometry`).
    fn aliases(&self) -> &[&str] {
        &[]
    }

    /// Resolve return type given input argument types.
    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType>;

    /// Execute the scalar function over a slice of columnar argument arrays.
    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray>;
}

/// `h3_to_string(UBIGINT) -> VARCHAR`
pub struct H3ToStringUdf;

impl H3ScalarFn for H3ToStringUdf {
    fn name(&self) -> &str {
        "h3_to_string"
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != [ArrowDataType::UInt64] {
            return Err(RasterH3Error::InvalidParameter(
                "h3_to_string expects (UInt64)".into(),
            ));
        }
        Ok(ArrowDataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        let Some(ColumnArray::UInt64 { values, validity }) = args.first() else {
            return Err(RasterH3Error::InvalidParameter(
                "h3_to_string requires a UInt64 column".into(),
            ));
        };
        let n = values.len();
        let mut out_vals = Vec::with_capacity(n);
        let mut out_valid = vec![true; n];
        let mut has_null = false;
        let mut buf = [0u8; 16];

        for i in 0..n {
            let is_valid = validity.as_ref().map(|v| v[i]).unwrap_or(true);
            if !is_valid || values[i] == 0 {
                out_vals.push(String::new());
                out_valid[i] = false;
                has_null = true;
            } else {
                let bytes = fast_hex_u64(values[i], &mut buf);
                out_vals.push(std::str::from_utf8(bytes).unwrap_or_default().to_owned());
            }
        }
        Ok(ColumnArray::Utf8 {
            values: out_vals,
            validity: if has_null { Some(out_valid) } else { None },
        })
    }
}

/// `string_to_h3(VARCHAR) -> UBIGINT`
pub struct StringToH3Udf;

impl H3ScalarFn for StringToH3Udf {
    fn name(&self) -> &str {
        "string_to_h3"
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != [ArrowDataType::Utf8] {
            return Err(RasterH3Error::InvalidParameter(
                "string_to_h3 expects (Utf8)".into(),
            ));
        }
        Ok(ArrowDataType::UInt64)
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        let Some(ColumnArray::Utf8 { values, validity }) = args.first() else {
            return Err(RasterH3Error::InvalidParameter(
                "string_to_h3 requires a Utf8 column".into(),
            ));
        };
        let out_vals: Vec<u64> = values
            .iter()
            .map(|s| parse_hex_u64(s).unwrap_or(0))
            .collect();
        Ok(ColumnArray::UInt64 {
            values: out_vals,
            validity: validity.clone(),
        })
    }
}

/// `h3_to_lat(UBIGINT) -> DOUBLE`
pub struct H3ToLatUdf;

impl H3ScalarFn for H3ToLatUdf {
    fn name(&self) -> &str {
        "h3_to_lat"
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != [ArrowDataType::UInt64] {
            return Err(RasterH3Error::InvalidParameter(
                "h3_to_lat expects (UInt64)".into(),
            ));
        }
        Ok(ArrowDataType::Float64)
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        let Some(ColumnArray::UInt64 { values, validity }) = args.first() else {
            return Err(RasterH3Error::InvalidParameter(
                "h3_to_lat requires a UInt64 column".into(),
            ));
        };
        let out_vals: Vec<f64> = values.iter().map(|&c| h3_to_lat(c)).collect();
        Ok(ColumnArray::Float64 {
            values: out_vals,
            validity: validity.clone(),
        })
    }
}

/// `h3_to_lng(UBIGINT) -> DOUBLE`
pub struct H3ToLngUdf;

impl H3ScalarFn for H3ToLngUdf {
    fn name(&self) -> &str {
        "h3_to_lng"
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != [ArrowDataType::UInt64] {
            return Err(RasterH3Error::InvalidParameter(
                "h3_to_lng expects (UInt64)".into(),
            ));
        }
        Ok(ArrowDataType::Float64)
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        let Some(ColumnArray::UInt64 { values, validity }) = args.first() else {
            return Err(RasterH3Error::InvalidParameter(
                "h3_to_lng requires a UInt64 column".into(),
            ));
        };
        let out_vals: Vec<f64> = values.iter().map(|&c| h3_to_lng(c)).collect();
        Ok(ColumnArray::Float64 {
            values: out_vals,
            validity: validity.clone(),
        })
    }
}

/// `h3_get_resolution(UBIGINT) -> BIGINT`
pub struct H3GetResolutionUdf;

impl H3ScalarFn for H3GetResolutionUdf {
    fn name(&self) -> &str {
        "h3_get_resolution"
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != [ArrowDataType::UInt64] {
            return Err(RasterH3Error::InvalidParameter(
                "h3_get_resolution expects (UInt64)".into(),
            ));
        }
        Ok(ArrowDataType::Int64)
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        let Some(ColumnArray::UInt64 { values, validity }) = args.first() else {
            return Err(RasterH3Error::InvalidParameter(
                "h3_get_resolution requires a UInt64 column".into(),
            ));
        };
        let out_vals: Vec<i64> = values.iter().map(|&c| h3_get_resolution(c)).collect();
        Ok(ColumnArray::Int64 {
            values: out_vals,
            validity: validity.clone(),
        })
    }
}

/// `h3_is_valid(UBIGINT | VARCHAR) -> BOOLEAN`
pub struct H3IsValidUdf;

impl H3ScalarFn for H3IsValidUdf {
    fn name(&self) -> &str {
        "h3_is_valid"
    }

    fn aliases(&self) -> &[&str] {
        &["h3_is_valid_cell"]
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        match arg_types {
            [ArrowDataType::UInt64] | [ArrowDataType::Utf8] => Ok(ArrowDataType::Boolean),
            _ => Err(RasterH3Error::InvalidParameter(
                "h3_is_valid expects (UInt64) or (Utf8)".into(),
            )),
        }
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        match args.first() {
            Some(ColumnArray::UInt64 { values, validity }) => {
                let out_vals: Vec<bool> = values.iter().map(|&c| h3_is_valid_u64(c)).collect();
                Ok(ColumnArray::Boolean {
                    values: out_vals,
                    validity: validity.clone(),
                })
            }
            Some(ColumnArray::Utf8 { values, validity }) => {
                let out_vals: Vec<bool> = values.iter().map(|s| h3_is_valid_str(s)).collect();
                Ok(ColumnArray::Boolean {
                    values: out_vals,
                    validity: validity.clone(),
                })
            }
            _ => Err(RasterH3Error::InvalidParameter(
                "h3_is_valid requires a UInt64 or Utf8 column".into(),
            )),
        }
    }
}

/// `h3_to_wkb(UBIGINT | VARCHAR) -> BINARY` (also aliased as `h3_to_geometry`, `h3_cell_to_geometry`)
pub struct H3ToWkbUdf;

impl H3ScalarFn for H3ToWkbUdf {
    fn name(&self) -> &str {
        "h3_to_wkb"
    }

    fn aliases(&self) -> &[&str] {
        &["h3_to_geometry", "h3_cell_to_geometry"]
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        match arg_types {
            [ArrowDataType::UInt64] | [ArrowDataType::Utf8] => Ok(ArrowDataType::Binary),
            _ => Err(RasterH3Error::InvalidParameter(
                "h3_to_wkb expects (UInt64) or (Utf8)".into(),
            )),
        }
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        let mut wkb_buf: WkbBuf = [0u8; WKB_BUF_LEN];
        match args.first() {
            Some(ColumnArray::UInt64 { values, validity }) => {
                let n = values.len();
                let mut out_vals = Vec::with_capacity(n);
                let mut out_valid = vec![true; n];
                let mut has_null = false;
                for i in 0..n {
                    let is_valid = validity.as_ref().map(|v| v[i]).unwrap_or(true);
                    if is_valid {
                        if let Some(len) = h3_index_to_wkb(values[i], &mut wkb_buf) {
                            out_vals.push(wkb_buf[..len].to_vec());
                            continue;
                        }
                    }
                    out_vals.push(Vec::new());
                    out_valid[i] = false;
                    has_null = true;
                }
                Ok(ColumnArray::Binary {
                    values: out_vals,
                    validity: if has_null { Some(out_valid) } else { None },
                })
            }
            Some(ColumnArray::Utf8 { values, validity }) => {
                let n = values.len();
                let mut out_vals = Vec::with_capacity(n);
                let mut out_valid = vec![true; n];
                let mut has_null = false;
                for i in 0..n {
                    let is_valid = validity.as_ref().map(|v| v[i]).unwrap_or(true);
                    if is_valid {
                        if let Some(cell) = parse_hex_u64(&values[i]) {
                            if let Some(len) = h3_index_to_wkb(cell, &mut wkb_buf) {
                                out_vals.push(wkb_buf[..len].to_vec());
                                continue;
                            }
                        }
                    }
                    out_vals.push(Vec::new());
                    out_valid[i] = false;
                    has_null = true;
                }
                Ok(ColumnArray::Binary {
                    values: out_vals,
                    validity: if has_null { Some(out_valid) } else { None },
                })
            }
            _ => Err(RasterH3Error::InvalidParameter(
                "h3_to_wkb requires a UInt64 or Utf8 column".into(),
            )),
        }
    }
}

/// `h3_cell_to_parent(UBIGINT | VARCHAR, BIGINT) -> UBIGINT | VARCHAR`
pub struct H3CellToParentUdf;

impl H3ScalarFn for H3CellToParentUdf {
    fn name(&self) -> &str {
        "h3_cell_to_parent"
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        match arg_types {
            [ArrowDataType::UInt64, ArrowDataType::Int64] => Ok(ArrowDataType::UInt64),
            [ArrowDataType::Utf8, ArrowDataType::Int64] => Ok(ArrowDataType::Utf8),
            _ => Err(RasterH3Error::InvalidParameter(
                "h3_cell_to_parent expects (UInt64, Int64) or (Utf8, Int64)".into(),
            )),
        }
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        if args.len() != 2 {
            return Err(RasterH3Error::InvalidParameter(
                "h3_cell_to_parent requires 2 arguments".into(),
            ));
        }
        let ColumnArray::Int64 {
            values: res_vals,
            validity: res_valid,
        } = &args[1]
        else {
            return Err(RasterH3Error::InvalidParameter(
                "h3_cell_to_parent second argument must be Int64".into(),
            ));
        };

        match &args[0] {
            ColumnArray::UInt64 { values, validity } => {
                let n = values.len();
                let mut out_vals = Vec::with_capacity(n);
                let mut out_valid = vec![true; n];
                let mut has_null = false;
                for i in 0..n {
                    let v0 = validity.as_ref().map(|v| v[i]).unwrap_or(true);
                    let v1 = res_valid.as_ref().map(|v| v[i]).unwrap_or(true);
                    if v0 && v1 {
                        if let Some(p) = h3_cell_to_parent_u64(values[i], res_vals[i]) {
                            out_vals.push(p);
                            continue;
                        }
                    }
                    out_vals.push(0);
                    out_valid[i] = false;
                    has_null = true;
                }
                Ok(ColumnArray::UInt64 {
                    values: out_vals,
                    validity: if has_null { Some(out_valid) } else { None },
                })
            }
            ColumnArray::Utf8 { values, validity } => {
                let n = values.len();
                let mut out_vals = Vec::with_capacity(n);
                let mut out_valid = vec![true; n];
                let mut has_null = false;
                for i in 0..n {
                    let v0 = validity.as_ref().map(|v| v[i]).unwrap_or(true);
                    let v1 = res_valid.as_ref().map(|v| v[i]).unwrap_or(true);
                    if v0 && v1 {
                        if let Some(p) = h3_cell_to_parent_str(&values[i], res_vals[i]) {
                            out_vals.push(p);
                            continue;
                        }
                    }
                    out_vals.push(String::new());
                    out_valid[i] = false;
                    has_null = true;
                }
                Ok(ColumnArray::Utf8 {
                    values: out_vals,
                    validity: if has_null { Some(out_valid) } else { None },
                })
            }
            _ => Err(RasterH3Error::InvalidParameter(
                "h3_cell_to_parent first argument must be UInt64 or Utf8".into(),
            )),
        }
    }
}

// -----------------------------------------------------------------------------
// The same scalar functions for the other grids (A5, S2, HEALPix)
// -----------------------------------------------------------------------------

use dggs_stream_core::encoding::cell_index_to_wkb;
use dggs_stream_core::grid::GridKind;

/// What a [`GridScalarFn`] computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridScalarOp {
    /// `{grid}_to_string(UBIGINT) -> VARCHAR`
    ToString,
    /// `string_to_{grid}(VARCHAR) -> UBIGINT`
    FromString,
    /// `{grid}_to_lat(UBIGINT) -> DOUBLE`
    ToLat,
    /// `{grid}_to_lng(UBIGINT) -> DOUBLE`
    ToLng,
    /// `{grid}_get_resolution(UBIGINT) -> BIGINT`
    GetResolution,
    /// `{grid}_is_valid(UBIGINT | VARCHAR) -> BOOLEAN`
    IsValid,
    /// `{grid}_to_wkb(UBIGINT | VARCHAR) -> BINARY`
    ToWkb,
    /// `{grid}_cell_to_parent(UBIGINT | VARCHAR, BIGINT) -> UBIGINT | VARCHAR`
    CellToParent,
}

/// A scalar function of one grid, named as DuckDB names it (`a5_to_string`,
/// `string_to_s2`, `healpix_cell_to_parent`, ...). Arguments, results and NULL handling
/// are those of the H3 function of the same suffix.
pub struct GridScalarFn {
    grid: GridKind,
    op: GridScalarOp,
    name: &'static str,
    aliases: &'static [&'static str],
}

macro_rules! grid_scalar_fns {
    ($grid:expr, $p:literal) => {
        [
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::ToString,
                name: concat!($p, "_to_string"),
                aliases: &[],
            },
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::FromString,
                name: concat!("string_to_", $p),
                aliases: &[],
            },
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::ToLat,
                name: concat!($p, "_to_lat"),
                aliases: &[],
            },
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::ToLng,
                name: concat!($p, "_to_lng"),
                aliases: &[],
            },
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::GetResolution,
                name: concat!($p, "_get_resolution"),
                aliases: &[],
            },
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::IsValid,
                name: concat!($p, "_is_valid"),
                aliases: &[concat!($p, "_is_valid_cell")],
            },
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::ToWkb,
                name: concat!($p, "_to_wkb"),
                aliases: &[
                    concat!($p, "_to_geometry"),
                    concat!($p, "_cell_to_geometry"),
                ],
            },
            GridScalarFn {
                grid: $grid,
                op: GridScalarOp::CellToParent,
                name: concat!($p, "_cell_to_parent"),
                aliases: &[],
            },
        ]
    };
}

impl GridScalarFn {
    /// The scalar functions of `grid`. H3's are the dedicated `H3*Udf` types, so H3 has none here.
    pub fn all_for(grid: GridKind) -> Vec<Self> {
        match grid {
            GridKind::H3 => Vec::new(),
            GridKind::A5 => grid_scalar_fns!(GridKind::A5, "a5").into(),
            GridKind::S2 => grid_scalar_fns!(GridKind::S2, "s2").into(),
            GridKind::Healpix => grid_scalar_fns!(GridKind::Healpix, "healpix").into(),
        }
    }

    /// The grid this function works on.
    pub fn grid(&self) -> GridKind {
        self.grid
    }

    /// What this function computes.
    pub fn op(&self) -> GridScalarOp {
        self.op
    }

    fn bad_args(&self, expected: &str) -> RasterH3Error {
        RasterH3Error::InvalidParameter(format!("{} expects {expected}", self.name))
    }
}

/// Row `i` of `validity`, where no bitmap means every row is valid.
fn row_valid(validity: &Option<Vec<bool>>, i: usize) -> bool {
    validity.as_ref().is_none_or(|v| v[i])
}

/// A column from per-row options, with a validity bitmap only if some row is `None`.
fn collect_rows<T: Default, C>(
    rows: impl Iterator<Item = Option<T>>,
    make: impl FnOnce(Vec<T>, Option<Vec<bool>>) -> C,
) -> C {
    let (mut values, mut valid) = (Vec::new(), Vec::new());
    for row in rows {
        valid.push(row.is_some());
        values.push(row.unwrap_or_default());
    }
    let validity = valid.contains(&false).then_some(valid);
    make(values, validity)
}

impl H3ScalarFn for GridScalarFn {
    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[&str] {
        self.aliases
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        use ArrowDataType::{Binary, Boolean, Float64, Int64, UInt64, Utf8};
        use GridScalarOp as Op;
        match (self.op, arg_types) {
            (Op::ToString, [UInt64]) => Ok(Utf8),
            (Op::ToString, _) => Err(self.bad_args("(UInt64)")),
            (Op::FromString, [Utf8]) => Ok(UInt64),
            (Op::FromString, _) => Err(self.bad_args("(Utf8)")),
            (Op::ToLat | Op::ToLng, [UInt64]) => Ok(Float64),
            (Op::GetResolution, [UInt64]) => Ok(Int64),
            (Op::ToLat | Op::ToLng | Op::GetResolution, _) => Err(self.bad_args("(UInt64)")),
            (Op::IsValid, [UInt64] | [Utf8]) => Ok(Boolean),
            (Op::ToWkb, [UInt64] | [Utf8]) => Ok(Binary),
            (Op::IsValid | Op::ToWkb, _) => Err(self.bad_args("(UInt64) or (Utf8)")),
            (Op::CellToParent, [UInt64, Int64]) => Ok(UInt64),
            (Op::CellToParent, [Utf8, Int64]) => Ok(Utf8),
            (Op::CellToParent, _) => Err(self.bad_args("(UInt64, Int64) or (Utf8, Int64)")),
        }
    }

    fn invoke(&self, args: &[ColumnArray]) -> Result<ColumnArray> {
        use GridScalarOp as Op;
        let grid = self.grid;
        // A cell argument, as an integer or a hex string; `None` for a NULL row or a
        // string that is not hexadecimal.
        let cell_at = |i: usize| -> Option<u64> {
            match args.first()? {
                ColumnArray::UInt64 { values, validity } => {
                    row_valid(validity, i).then(|| values[i])
                }
                ColumnArray::Utf8 { values, validity } => row_valid(validity, i)
                    .then(|| parse_hex_u64(&values[i]))
                    .flatten(),
                _ => None,
            }
        };
        let (rows, is_hex) = match args.first() {
            Some(c @ ColumnArray::UInt64 { .. }) => (c.len(), false),
            Some(c @ ColumnArray::Utf8 { .. }) => (c.len(), true),
            _ => return Err(self.bad_args("a UInt64 or Utf8 cell column")),
        };
        let input_validity = args[0].validity().map(<[bool]>::to_vec);
        let hex = |cell: u64| {
            let mut buf = [0u8; 16];
            std::str::from_utf8(fast_hex_u64(cell, &mut buf))
                .unwrap_or_default()
                .to_owned()
        };
        match (self.op, is_hex) {
            (Op::ToString, false) => Ok(collect_rows(
                (0..rows).map(|i| cell_at(i).filter(|&c| c != 0).map(hex)),
                |values, validity| ColumnArray::Utf8 { values, validity },
            )),
            (Op::FromString, true) => Ok(ColumnArray::UInt64 {
                values: (0..rows).map(|i| cell_at(i).unwrap_or(0)).collect(),
                validity: input_validity,
            }),
            (Op::ToLat | Op::ToLng, false) => Ok(ColumnArray::Float64 {
                values: (0..rows)
                    .map(|i| {
                        let centre = cell_at(i).and_then(|c| grid.center_lat_lng(c));
                        centre.map_or(
                            f64::NAN,
                            |(lat, lng)| {
                                if self.op == Op::ToLat {
                                    lat
                                } else {
                                    lng
                                }
                            },
                        )
                    })
                    .collect(),
                validity: input_validity,
            }),
            (Op::GetResolution, false) => Ok(ColumnArray::Int64 {
                values: (0..rows)
                    .map(|i| {
                        cell_at(i)
                            .and_then(|c| grid.resolution(c))
                            .map_or(-1, i64::from)
                    })
                    .collect(),
                validity: input_validity,
            }),
            (Op::IsValid, _) => Ok(ColumnArray::Boolean {
                values: (0..rows)
                    .map(|i| cell_at(i).is_some_and(|c| grid.is_valid(c)))
                    .collect(),
                validity: input_validity,
            }),
            (Op::ToWkb, _) => {
                let mut buf: WkbBuf = [0u8; WKB_BUF_LEN];
                Ok(collect_rows(
                    (0..rows).map(|i| {
                        let len = cell_index_to_wkb(grid, cell_at(i)?, &mut buf)?;
                        Some(buf[..len].to_vec())
                    }),
                    |values, validity| ColumnArray::Binary { values, validity },
                ))
            }
            (Op::CellToParent, _) => {
                let Some(ColumnArray::Int64 {
                    values: res,
                    validity: res_valid,
                }) = args.get(1)
                else {
                    return Err(self.bad_args("an Int64 resolution as its second argument"));
                };
                let parent_at = |i: usize| -> Option<u64> {
                    if !row_valid(res_valid, i) {
                        return None;
                    }
                    grid.parent(cell_at(i)?, u8::try_from(res[i]).ok()?)
                };
                Ok(if is_hex {
                    collect_rows(
                        (0..rows).map(|i| parent_at(i).map(hex)),
                        |values, validity| ColumnArray::Utf8 { values, validity },
                    )
                } else {
                    collect_rows((0..rows).map(parent_at), |values, validity| {
                        ColumnArray::UInt64 { values, validity }
                    })
                })
            }
            (Op::ToString | Op::ToLat | Op::ToLng | Op::GetResolution, true) => {
                Err(self.bad_args("a UInt64 column"))
            }
            (Op::FromString, false) => Err(self.bad_args("a Utf8 column")),
        }
    }
}
