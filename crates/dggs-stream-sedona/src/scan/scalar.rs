//! Vectorized [`CellScalarFn`] implementations for cell operations.
//!
//! Operates directly on [`ColumnArray`] batches with full null-validity propagation.

use dggs_stream_core::columnar::{ArrowDataType, ColumnArray};
use dggs_stream_core::encoding::{fast_hex_u64, parse_hex_u64, WkbBuf, WKB_BUF_LEN};
use dggs_stream_core::error::{DggsStreamError, Result};

/// A vectorized cell scalar function over [`ColumnArray`]s. [`crate::udf::CellScalarUdf`]
/// adapts it to DataFusion's `ScalarUDFImpl`.
pub trait CellScalarFn: Send + Sync {
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

// -----------------------------------------------------------------------------
// Grid scalar functions
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
    /// The scalar functions of `grid`.
    pub fn all_for(grid: GridKind) -> Vec<Self> {
        match grid {
            GridKind::H3 => grid_scalar_fns!(GridKind::H3, "h3").into(),
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

    fn bad_args(&self, expected: &str) -> DggsStreamError {
        DggsStreamError::InvalidParameter(format!("{} expects {expected}", self.name))
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

impl CellScalarFn for GridScalarFn {
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
        // Numeric cell bits are meaningful even in rows masked NULL: validity is copied to
        // the output, but computing the masked value preserves the legacy H3 scalar behavior.
        let raw_cell_at = |i: usize| -> Option<u64> {
            match args.first()? {
                ColumnArray::UInt64 { values, .. } => Some(values[i]),
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
                        let centre = raw_cell_at(i).and_then(|c| grid.center_lat_lng(c));
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
                        raw_cell_at(i)
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
