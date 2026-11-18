//! One raster and one set of options through the CLI, the Python bindings' scanner (the
//! code Polars scans run) and DataFusion SQL: the engines must return the same cells
//! with the same statistics, on every grid and in every transfer mode. The DuckDB
//! extension is checked against the CLI the same way in its own test suite
//! (`cross_engine` in `dggs-stream-duckdb`), on a raster with the same pixels.

mod common;

use std::collections::BTreeMap;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{Float64Type, UInt64Type};
use datafusion::prelude::SessionContext;
use dggs_stream_cli::{parse_stream_args, stream_raster_to_writer, StreamOutputFormat};
use dggs_stream_core::aggregator::output_policy::{NodataPolicy, OutputPolicy};
use dggs_stream_core::columnar::ColumnArray;
use dggs_stream_core::grid::GridKind;
use dggs_stream_core::transfer::{Interpolation, TransferMode};
use dggs_stream_python::{PyRasterScanner, PyScanConfig};
use dggs_stream_sedona::register_all;

use common::{collect, gradient_tif, source};

const SIZE: u32 = 48;

/// `cell -> (count, sum, mean)`; a NULL mean is `None`.
type Cells = BTreeMap<u64, (f64, f64, Option<f64>)>;

/// One scan, spelled for each engine.
struct Case {
    name: &'static str,
    /// Offset from the resolution matched to the pixels.
    offset: i32,
    cli: &'static [&'static str],
    sql: &'static [&'static str],
    transfer: TransferMode,
    output: OutputPolicy,
}

fn cases() -> Vec<Case> {
    let emit = OutputPolicy {
        nodata: NodataPolicy::Emit,
        nodata_value: None,
        decimals: Some(2),
    };
    vec![
        Case {
            name: "centres",
            offset: 0,
            cli: &[],
            sql: &[],
            transfer: TransferMode::AssignCenters,
            output: OutputPolicy::default(),
        },
        Case {
            name: "centres, emit, decimals",
            offset: -1,
            cli: &["--nodata-policy", "emit", "--decimals", "2"],
            sql: &["nodata_policy=emit", "decimals=2"],
            transfer: TransferMode::AssignCenters,
            output: emit,
        },
        Case {
            name: "overlay, mass preserving",
            offset: -1,
            cli: &["--transfer", "overlay", "--mass-preserve"],
            sql: &["transfer=overlay", "mass_preserve=true"],
            transfer: TransferMode::Overlay {
                weighted: true,
                mass_preserve: true,
            },
            output: OutputPolicy::default(),
        },
        Case {
            name: "sample, bilinear",
            offset: 1,
            cli: &["--transfer", "interpolate", "--interpolation", "bilinear"],
            sql: &["transfer=interpolate", "interpolation=bilinear"],
            transfer: TransferMode::Interpolate(Interpolation::Bilinear),
            output: OutputPolicy::default(),
        },
        Case {
            name: "sample, bicubic",
            offset: 1,
            cli: &["--transfer", "interpolate", "--interpolation", "bicubic"],
            sql: &["transfer=interpolate", "interpolation=bicubic"],
            transfer: TransferMode::Interpolate(Interpolation::Bicubic),
            output: OutputPolicy::default(),
        },
    ]
}

fn auto(offset: i32) -> String {
    match offset {
        0 => "auto".into(),
        n => format!("auto{n:+}"),
    }
}

fn cli(src: &str, grid: GridKind, case: &Case) -> (u8, Cells) {
    let mut args: Vec<String> = vec![src.into(), "--grid".into(), grid.as_str().into()];
    args.extend(["-r".to_string(), auto(case.offset)]);
    args.extend(case.cli.iter().map(|s| s.to_string()));
    let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl).unwrap();
    let mut out = Vec::new();
    stream_raster_to_writer(&opts, &mut out).unwrap();
    let column = grid.index_column_name();
    let cells = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|line| {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            (
                row[column].as_u64().unwrap(),
                (
                    row["count"].as_f64().unwrap(),
                    row["sum"].as_f64().unwrap(),
                    row["mean"].as_f64(),
                ),
            )
        })
        .collect();
    (opts.resolutions[0], cells)
}

fn python(src: &str, grid: GridKind, resolution: u8, case: &Case) -> Cells {
    let config = PyScanConfig {
        path: src.into(),
        grid,
        resolutions: vec![resolution],
        transfer: case.transfer,
        output: case.output,
        ..Default::default()
    };
    let (schema, batches) = PyRasterScanner::new(config).batch_iter().unwrap();
    let index = |name: &str| schema.index_of(name).unwrap();
    let (cell, count, sum, mean) = (
        index(grid.index_column_name()),
        index("count"),
        index("sum"),
        index("mean"),
    );
    let mut cells = Cells::new();
    for batch in batches {
        let batch = batch.unwrap();
        let ColumnArray::UInt64 { values: ids, .. } = &batch.columns[cell] else {
            panic!("cell column");
        };
        let floats = |i: usize| match &batch.columns[i] {
            ColumnArray::Float64 { values, validity } => (values.clone(), validity.clone()),
            _ => panic!("float column"),
        };
        let (counts, sums, (means, valid)) = (floats(count).0, floats(sum).0, floats(mean));
        for (row, id) in ids.iter().enumerate() {
            let mean = valid.as_ref().is_none_or(|v| v[row]).then_some(means[row]);
            cells.insert(
                *id,
                (counts[row], sums[row], mean.filter(|m| m.is_finite())),
            );
        }
    }
    cells
}

async fn sql(ctx: &SessionContext, src: &str, grid: GridKind, case: &Case) -> Cells {
    let mut arguments = vec![format!("'{src}'"), format!("'{}'", auto(case.offset))];
    arguments.extend(case.sql.iter().map(|option| format!("'{option}'")));
    let batches = collect(
        ctx,
        &format!(
            "SELECT {}, count, sum, mean FROM {grid}_raster_continuous_aggregate({})",
            grid.index_column_name(),
            arguments.join(", ")
        ),
    )
    .await;
    let mut cells = Cells::new();
    for batch in &batches {
        let ids = batch.column(0).as_primitive::<UInt64Type>();
        let column = |i: usize| batch.column(i).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            let mean = column(3).is_valid(row).then(|| column(3).value(row));
            cells.insert(
                ids.value(row),
                (
                    column(1).value(row),
                    column(2).value(row),
                    mean.filter(|m| m.is_finite()),
                ),
            );
        }
    }
    cells
}

/// Equal cells, and statistics equal to within rounding of the last digits.
fn assert_same(what: &str, a: &Cells, b: &Cells) {
    assert_eq!(
        a.keys().collect::<Vec<_>>(),
        b.keys().collect::<Vec<_>>(),
        "{what}: cells differ"
    );
    let close = |x: f64, y: f64| (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0);
    for (cell, (count, sum, mean)) in a {
        let (other_count, other_sum, other_mean) = b[cell];
        assert!(close(*count, other_count), "{what}: count of {cell:x}");
        assert!(close(*sum, other_sum), "{what}: sum of {cell:x}");
        match (mean, other_mean) {
            (Some(x), Some(y)) => assert!(close(*x, y), "{what}: mean of {cell:x}"),
            (None, None) => {}
            other => panic!("{what}: mean of {cell:x}: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn engines_agree_on_every_grid_and_transfer_mode() {
    // Pixel values are the column index: a field linear in longitude.
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    let ctx = SessionContext::new();
    register_all(&ctx);
    let pixels = f64::from(SIZE * SIZE);
    let mass: f64 = f64::from(SIZE) * (0..SIZE).map(f64::from).sum::<f64>();

    for grid in GridKind::ALL {
        for case in cases() {
            let what = format!("{grid}, {}", case.name);
            let (resolution, from_cli) = cli(&src, grid, &case);
            assert!(!from_cli.is_empty(), "{what}");
            assert_same(
                &format!("{what}: CLI vs Python"),
                &from_cli,
                &python(&src, grid, resolution, &case),
            );
            assert_same(
                &format!("{what}: CLI vs SQL"),
                &from_cli,
                &sql(&ctx, &src, grid, &case).await,
            );

            // What the shared result must be.
            let count: f64 = from_cli.values().map(|c| c.0).sum();
            let sum: f64 = from_cli.values().map(|c| c.1).sum();
            match case.transfer {
                TransferMode::AssignCenters => {
                    assert_eq!((count, sum), (pixels, mass), "{what}");
                }
                // `mass_preserve` conserves the raster's total exactly.
                TransferMode::Overlay { .. } => {
                    assert!((sum - mass).abs() < 1e-6 * mass, "{what}: {sum} vs {mass}");
                    assert!((count - pixels).abs() < 1e-6 * pixels, "{what}: {count}");
                }
                // An interpolated linear field is the field itself at each cell centre,
                // away from the raster's edge.
                TransferMode::Interpolate(_) => {
                    let mut interior = 0;
                    for (cell, (_, _, mean)) in &from_cli {
                        let (_, lon) = grid.center_lat_lng(*cell).unwrap();
                        let column = (lon + 122.45) / 0.001 - 0.5;
                        if (2.0..f64::from(SIZE) - 3.0).contains(&column) {
                            let mean = mean.expect("an interior cell has a value");
                            assert!((mean - column).abs() < 1e-3, "{what}: {mean} vs {column}");
                            interior += 1;
                        }
                    }
                    assert!(interior > 100, "{what}: {interior} interior cells");
                }
            }
        }
    }
}
