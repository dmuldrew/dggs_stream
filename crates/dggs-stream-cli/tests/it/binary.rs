//! Runs the built `dggs-stream-cli` binary (`CARGO_BIN_EXE_dggs-stream-cli`) and compares its
//! output with core batch readers and exporters run on the same synthetic raster.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dggs_stream_core::aggregator::multi_horizon::MultiResolutionConfig;
use dggs_stream_core::columnar::ipc::read_ipc_stream;
use dggs_stream_core::columnar::{
    CategoricalBatchReader, CategoricalFormat, ColumnArray, ContinuousBatchReader, RecordBatch,
};
use dggs_stream_core::pmtiles::PmtilesTiler;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use tempfile::NamedTempFile;

use crate::helpers::{corrupt_strip_geotiff, TestGeoTiffBuilder};

const BIN: &str = env!("CARGO_BIN_EXE_dggs-stream-cli");

fn run(args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .output()
        .expect("run dggs-stream-cli")
}

fn run_ok(args: &[&str]) -> Output {
    let out = run(args);
    assert!(
        out.status.success(),
        "dggs-stream-cli {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Asserts a non-zero exit whose stderr mentions `needle`.
fn run_err(args: &[&str], needle: &str) -> Output {
    let out = run(args);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "dggs-stream-cli {args:?} should fail; stdout {} bytes",
        out.stdout.len()
    );
    assert!(
        stderr.contains(needle),
        "stderr of {args:?} should mention '{needle}':\n{stderr}"
    );
    out
}

/// 32x32 f32 gradient with values 1..=10 at 0.001° pixels.
fn gradient_tif() -> (NamedTempFile, PathBuf) {
    TestGeoTiffBuilder::new(32, 32)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| ((c + r) % 10 + 1) as f32)
}

/// 48x48 u8 classes 1..=5 in 4-pixel blocks.
fn classes_tif() -> (NamedTempFile, PathBuf) {
    TestGeoTiffBuilder::new(48, 48)
        .pixel_size(0.001)
        .create_tempfile(|c, r| ((c / 4 + r / 4) % 5 + 1) as u8)
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

/// `h3_index -> (mean, count, sum)` for continuous batches.
fn continuous_map(batches: &[RecordBatch]) -> BTreeMap<u64, (f64, f64, f64)> {
    let mut out = BTreeMap::new();
    for b in batches {
        let col = |name: &str| match b.column_by_name(name).unwrap() {
            ColumnArray::Float64 { values, .. } => values.clone(),
            other => panic!("{name}: {:?}", other.data_type()),
        };
        let ColumnArray::UInt64 { values: cells, .. } = b.column_by_name("h3_index").unwrap()
        else {
            panic!("h3_index must be UInt64");
        };
        let (mean, count, sum) = (col("mean"), col("count"), col("sum"));
        for i in 0..b.num_rows {
            out.insert(cells[i], (mean[i], count[i], sum[i]));
        }
    }
    out
}

fn core_continuous(path: &Path, cfg: &MultiResolutionConfig) -> BTreeMap<u64, (f64, f64, f64)> {
    let reader = ContinuousBatchReader::open(&[path.to_path_buf()], cfg, None, 2048, None).unwrap();
    continuous_map(&reader.map(|b| b.unwrap()).collect::<Vec<_>>())
}

/// `h3_index -> (majority_class, total_count)` from the core categorical reader.
fn core_categorical(path: &Path, cfg: &MultiResolutionConfig) -> BTreeMap<u64, (i64, f64)> {
    let reader = CategoricalBatchReader::open(
        &[path.to_path_buf()],
        cfg,
        CategoricalFormat::Wide,
        None,
        2048,
        None,
    )
    .unwrap();
    let mut out = BTreeMap::new();
    for b in reader.map(|b| b.unwrap()) {
        let (
            ColumnArray::UInt64 { values: cells, .. },
            ColumnArray::Int64 { values: class, .. },
            ColumnArray::Float64 { values: total, .. },
        ) = (
            b.column_by_name("h3_index").unwrap(),
            b.column_by_name("majority_class").unwrap(),
            b.column_by_name("total_count").unwrap(),
        )
        else {
            panic!("unexpected categorical column types");
        };
        for i in 0..b.num_rows {
            out.insert(cells[i], (class[i], total[i]));
        }
    }
    out
}

fn number(field: &Field) -> Option<f64> {
    Some(match *field {
        Field::Int(v) => f64::from(v),
        Field::Long(v) => v as f64,
        Field::UInt(v) => f64::from(v),
        Field::ULong(v) => v as f64,
        Field::Float(v) => f64::from(v),
        Field::Double(v) => v,
        _ => return None,
    })
}

fn ipc_map(stdout: &[u8]) -> BTreeMap<u64, (f64, f64, f64)> {
    let (_schema, batches) = read_ipc_stream(&mut Cursor::new(stdout)).unwrap();
    continuous_map(&batches)
}

fn jsonl_rows(stdout: &[u8]) -> Vec<serde_json::Value> {
    String::from_utf8(stdout.to_vec())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn bbox_narrows_output_to_the_core_bbox_result() {
    let (_tmp, path) = gradient_tif();
    let full = ipc_map(&run_ok(&["ipc", s(&path)]).stdout);
    assert_eq!(
        full,
        core_continuous(&path, &MultiResolutionConfig::new(vec![8]))
    );

    let bbox = [-122.45, 37.78, -122.435, 37.795];
    let out = run_ok(&["ipc", s(&path), "--bbox", "-122.45,37.78,-122.435,37.795"]);
    let narrowed = ipc_map(&out.stdout);
    let mut cfg = MultiResolutionConfig::new(vec![8]);
    cfg.bbox = Some(bbox);
    assert_eq!(narrowed, core_continuous(&path, &cfg));
    assert!(!narrowed.is_empty() && narrowed.len() < full.len());
    let total = |m: &BTreeMap<u64, (f64, f64, f64)>| m.values().map(|v| v.1).sum::<f64>();
    assert!(total(&narrowed) < total(&full));

    run_err(&["ipc", s(&path), "--bbox", "1,2,3"], "--bbox");
}

#[test]
fn nodata_override_matches_core_and_drops_pixels() {
    let (_tmp, path) = gradient_tif();
    let full = ipc_map(&run_ok(&["ipc", s(&path), "--crs", "EPSG:4326"]).stdout);
    let masked = ipc_map(&run_ok(&["ipc", s(&path), "--crs", "EPSG:4326", "--nodata", "5"]).stdout);

    let mut cfg = MultiResolutionConfig::new(vec![8]);
    cfg.custom_crs = Some("EPSG:4326".into());
    cfg.custom_nodata = Some(5.0);
    assert_eq!(masked, core_continuous(&path, &cfg));

    let count = |m: &BTreeMap<u64, (f64, f64, f64)>| m.values().map(|v| v.1).sum::<f64>();
    let fives = (0..32u32)
        .flat_map(|r| (0..32u32).map(move |c| (c + r) % 10 + 1))
        .filter(|&v| v == 5)
        .count() as f64;
    assert_eq!(count(&full) - count(&masked), fives);

    run_err(&["ipc", s(&path), "--nodata", "five"], "--nodata");
}

#[test]
fn limit_caps_rows_on_ipc_and_jsonl() {
    let (_tmp, path) = gradient_tif();
    let core = core_continuous(&path, &MultiResolutionConfig::new(vec![9]));
    assert!(core.len() > 5, "fixture needs more than 5 cells");

    let limited = ipc_map(&run_ok(&["ipc", s(&path), "-r", "9", "--limit", "5"]).stdout);
    assert_eq!(limited.len(), 5);
    for (cell, v) in &limited {
        assert_eq!(core.get(cell), Some(v), "{cell:#x} is not a core row");
    }

    let rows = jsonl_rows(&run_ok(&["jsonl", s(&path), "-r", "9", "--limit", "3"]).stdout);
    assert_eq!(rows.len(), 3);
    for row in &rows {
        let cell = row["h3_index"].as_u64().unwrap();
        assert_eq!(row["mean"].as_f64(), core.get(&cell).map(|v| v.0));
    }

    assert!(run_ok(&["jsonl", s(&path), "--limit", "0"])
        .stdout
        .is_empty());
    run_err(&["ipc", s(&path), "--limit", "-1"], "--limit");
}

#[test]
fn worker_flags_and_ground_area_overlay_match_the_default_scan() {
    let (_tmp, path) = gradient_tif();
    let default = ipc_map(&run_ok(&["ipc", s(&path), "--transfer", "overlay"]).stdout);
    let tuned = ipc_map(
        &run_ok(&[
            "ipc",
            s(&path),
            "--transfer",
            "overlay",
            "--decode-workers",
            "3",
            "--aggregation-workers",
            "1",
        ])
        .stdout,
    );
    assert!(!default.is_empty());
    assert_eq!(tuned, default);

    // Ground-area overlay runs through the CLI and returns rows with data.
    let ground = run_ok(&[
        "jsonl",
        s(&path),
        "--ground-area",
        "--transfer",
        "overlay",
        "--decode-workers",
        "2",
    ]);
    let rows = jsonl_rows(&ground.stdout);
    assert!(!rows.is_empty());
    assert!(rows
        .iter()
        .all(|row| row["retained_area_m2"].as_f64().unwrap() > 0.0));
}

#[test]
fn transfer_auto_runs_the_mode_it_selects() {
    let (_tmp, path) = gradient_tif();
    // 0.001 degree pixels are far smaller than H3 resolution 8 cells: overlay.
    let auto = run_ok(&["ipc", s(&path), "--transfer", "auto", "-r", "8"]);
    assert!(String::from_utf8_lossy(&auto.stderr).contains("selected overlay"));
    let overlay = ipc_map(&run_ok(&["ipc", s(&path), "--transfer", "overlay", "-r", "8"]).stdout);
    assert!(!overlay.is_empty());
    assert_eq!(ipc_map(&auto.stdout), overlay);

    // Resolution 12 cells are smaller than the pixels: interpolate.
    let auto = run_ok(&["ipc", s(&path), "--transfer", "auto", "-r", "12"]);
    assert!(String::from_utf8_lossy(&auto.stderr).contains("selected interpolate"));
    let interpolate =
        ipc_map(&run_ok(&["ipc", s(&path), "--transfer", "interpolate", "-r", "12"]).stdout);
    assert!(!interpolate.is_empty());
    assert_eq!(ipc_map(&auto.stdout), interpolate);
}

#[test]
fn stdout_output_equals_file_output_and_core() {
    let (_tmp, path) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();
    let core = core_continuous(&path, &MultiResolutionConfig::new(vec![8]));

    // No -o and `-o -` both write to stdout; `-o <file>` writes the same bytes to the file.
    let implicit = run_ok(&["ipc", s(&path)]).stdout;
    let dash = run_ok(&["ipc", s(&path), "-o", "-"]).stdout;
    let file = dir.path().join("out.ipc");
    let to_file = run_ok(&["ipc", s(&path), "-o", s(&file)]);
    assert!(to_file.stdout.is_empty());
    assert_eq!(ipc_map(&implicit), core);
    assert_eq!(ipc_map(&dash), core);
    assert_eq!(ipc_map(&std::fs::read(&file).unwrap()), core);

    let rows = jsonl_rows(&run_ok(&["jsonl", s(&path), "--columns", "h3_index,mean,count"]).stdout);
    assert_eq!(rows.len(), core.len());
    for row in &rows {
        let obj = row.as_object().unwrap();
        assert_eq!(obj.len(), 3, "projection keeps only 3 keys: {obj:?}");
        let (mean, count, _) = core[&row["h3_index"].as_u64().unwrap()];
        assert_eq!(row["mean"].as_f64(), Some(mean));
        assert_eq!(row["count"].as_f64(), Some(count));
    }
    let jsonl_file = dir.path().join("out.jsonl");
    run_ok(&[
        "jsonl",
        s(&path),
        "--columns",
        "h3_index,mean,count",
        "-o",
        s(&jsonl_file),
    ]);
    assert_eq!(
        jsonl_rows(&std::fs::read(&jsonl_file).unwrap()).len(),
        core.len()
    );

    // Categorical long format over stdout.
    let (_ctmp, classes) = classes_tif();
    let long = jsonl_rows(&run_ok(&["jsonl", s(&classes), "-c", "--format", "long"]).stdout);
    let cat_core = core_categorical(&classes, &MultiResolutionConfig::new(vec![8]));
    let mut per_cell: BTreeMap<u64, f64> = BTreeMap::new();
    for row in &long {
        *per_cell
            .entry(row["h3_index"].as_u64().unwrap())
            .or_default() += row["count"].as_f64().unwrap();
    }
    assert_eq!(per_cell.len(), cat_core.len());
    for (cell, total) in per_cell {
        assert!((total - cat_core[&cell].1).abs() < 1e-9);
    }
}

#[test]
fn categorical_parquet_matches_core() {
    let (_tmp, path) = classes_tif();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("cat.parquet");
    run_ok(&["parquet", s(&path), "-c", "-r", "8", "-o", s(&out)]);

    let reader = SerializedFileReader::new(File::open(&out).unwrap()).unwrap();
    // Parquet names the core `majority_class` / `total_count` columns `majority` / `pixel_count`.
    let mut got = BTreeMap::new();
    for row in reader.get_row_iter(None).unwrap() {
        let row = row.unwrap();
        let cols: BTreeMap<&str, f64> = row
            .get_column_iter()
            .filter_map(|(name, field)| number(field).map(|v| (name.as_str(), v)))
            .collect();
        let h3 = row
            .get_column_iter()
            .find_map(|(name, field)| match (name.as_str(), field) {
                ("h3_index", Field::ULong(v)) => Some(*v),
                ("h3_index", Field::Long(v)) => Some(*v as u64),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no integer h3_index in {cols:?}"));
        got.insert(h3, (cols["majority"] as i64, cols["pixel_count"]));
    }
    assert_eq!(
        got,
        core_categorical(&path, &MultiResolutionConfig::new(vec![8]))
    );

    // GeoParquet metadata is written by default.
    let kv = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    assert!(kv.iter().any(|k| k.key == "geo"));
}

#[test]
fn categorical_pmtiles_equals_core_export() {
    let (_tmp, path) = classes_tif();
    let dir = tempfile::tempdir().unwrap();
    let cli_out = dir.path().join("cli.pmtiles");
    let core_out = dir.path().join("core.pmtiles");
    run_ok(&["pmtiles", s(&path), "-c", "-r", "7,8", "-o", s(&cli_out)]);

    PmtilesTiler::process_categorical_source_to_pmtiles(
        &path,
        &core_out,
        MultiResolutionConfig::new(vec![7, 8]),
    )
    .unwrap();
    let cli_bytes = std::fs::read(&cli_out).unwrap();
    assert_eq!(&cli_bytes[..7], b"PMTiles");
    assert_eq!(cli_bytes[7], 3, "PMTiles spec version");
    assert_eq!(cli_bytes, std::fs::read(&core_out).unwrap());

    // The continuous archive of the same raster differs (different feature properties).
    let cont_out = dir.path().join("cont.pmtiles");
    run_ok(&["pmtiles", s(&path), "-r", "7,8", "-o", s(&cont_out)]);
    assert_ne!(std::fs::read(&cont_out).unwrap(), cli_bytes);
}

#[test]
fn bad_invocations_exit_non_zero_with_a_message() {
    let (_tmp, path) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();

    run_err(
        &["frobnicate", s(&path)],
        "Unknown subcommand: 'frobnicate'",
    );
    let usage = run(&[]);
    assert!(!usage.status.success());
    assert!(String::from_utf8_lossy(&usage.stderr).contains("Usage:"));
    let help = run(&["--help"]);
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stderr).contains("Usage:"));

    run_err(&["ipc"], "Missing required input raster path");
    run_err(&["ipc", s(&path), "--bogus"], "Unknown option '--bogus'");
    run_err(&["ipc", s(&path), "extra.tif"], "Unexpected argument");
    run_err(&["ipc", s(&path), "-r", "16"], "-r");
    run_err(&["ipc", s(&path), "--format", "tall"], "--format");
    run_err(&["ipc", s(&path), "--columns", "h3_index,nope"], "nope");
    run_err(&["ipc", s(&path), "--crs"], "Missing value for --crs");
    run_err(
        &["parquet", s(&path)],
        "Missing -o / --output path for parquet",
    );
    run_err(
        &["pmtiles", s(&path)],
        "Missing -o / --output path for pmtiles",
    );

    // Files that do not exist, are not TIFFs, or fail to decode part-way through.
    let missing = dir.path().join("missing.tif");
    let not_tiff = dir.path().join("not.tif");
    std::fs::write(&not_tiff, b"definitely not a tiff").unwrap();
    let corrupt = corrupt_strip_geotiff();
    for sub in ["ipc", "jsonl"] {
        run_err(&[sub, s(&missing)], "Error:");
        run_err(&[sub, s(&not_tiff)], "Error:");
        run_err(&[sub, s(corrupt.path()), "--crs", "EPSG:4326"], "Error:");
    }
    let pq = dir.path().join("bad.parquet");
    let pm = dir.path().join("bad.pmtiles");
    run_err(&["parquet", s(&not_tiff), "-o", s(&pq)], "Error:");
    run_err(&["pmtiles", s(&not_tiff), "-o", s(&pm)], "Error:");
    run_err(&["parquet", s(corrupt.path()), "-o", s(&pq)], "Error:");
    assert!(
        !pq.exists(),
        "a failed export must not leave a parquet file"
    );
}

#[test]
fn parquet_to_pmtiles_round_trips_the_cli_parquet_export() {
    let (_tmp, path) = classes_tif();
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("cells.parquet");
    run_ok(&["parquet", s(&path), "-c", "-r", "7,8", "-o", s(&parquet)]);

    let cli_out = dir.path().join("cli.pmtiles");
    let out = run_ok(&["parquet-to-pmtiles", s(&parquet), "-o", s(&cli_out)]);
    assert!(out.stdout.is_empty(), "nothing is written to stdout");
    assert!(
        out.stderr.is_empty(),
        "no rows were skipped: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let core_out = dir.path().join("core.pmtiles");
    let summary = PmtilesTiler::process_parquet_to_pmtiles(&parquet, &core_out, None).unwrap();
    let rows = SerializedFileReader::new(File::open(&parquet).unwrap())
        .unwrap()
        .metadata()
        .file_metadata()
        .num_rows() as usize;
    assert_eq!(summary.valid_features, rows);
    let cli_bytes = std::fs::read(&cli_out).unwrap();
    assert_eq!(&cli_bytes[..7], b"PMTiles");
    assert_eq!(cli_bytes, std::fs::read(&core_out).unwrap());

    // Naming the auto-detected column explicitly (any case) gives the same archive.
    let named = dir.path().join("named.pmtiles");
    run_ok(&[
        "parquet-to-pmtiles",
        s(&parquet),
        "--h3-column",
        "H3_INDEX",
        "-o",
        s(&named),
    ]);
    assert_eq!(std::fs::read(&named).unwrap(), cli_bytes);
}

/// A nullable hex-string column under a custom name, with a null and an invalid value: the
/// two bad rows are skipped and reported on stderr.
#[test]
fn parquet_to_pmtiles_reads_nullable_hex_columns_and_reports_skipped_rows() {
    use parquet::data_type::{ByteArray, ByteArrayType, DoubleType};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("hex.parquet");
    let schema = Arc::new(
        parse_message_type(
            "message schema { OPTIONAL BYTE_ARRAY cell_id (UTF8); REQUIRED DOUBLE value; }",
        )
        .unwrap(),
    );
    let mut writer = SerializedFileWriter::new(
        File::create(&parquet).unwrap(),
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .unwrap();
    let mut rg = writer.next_row_group().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    let cells = ["8828308281fffff", "not-a-cell", "8828308283fffff"].map(ByteArray::from);
    col.typed::<ByteArrayType>()
        .write_batch(&cells, Some(&[1, 1, 0, 1]), None)
        .unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<DoubleType>()
        .write_batch(&[1.0, 2.0, 3.0, 4.0], None, None)
        .unwrap();
    col.close().unwrap();
    rg.close().unwrap();
    writer.close().unwrap();

    let out = dir.path().join("hex.pmtiles");
    let result = run_ok(&[
        "parquet-to-pmtiles",
        s(&parquet),
        "--h3-column",
        "cell_id",
        "-o",
        s(&out),
    ]);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("skipped 2 of 4 rows"), "{stderr}");
    assert_eq!(&std::fs::read(&out).unwrap()[..7], b"PMTiles");
}

#[test]
fn parquet_to_pmtiles_bad_invocations() {
    let (_tmp, path) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("cells.parquet");
    run_ok(&["parquet", s(&path), "-o", s(&parquet)]);
    let out = dir.path().join("out.pmtiles");

    run_err(
        &["parquet-to-pmtiles"],
        "Missing required input Parquet path",
    );
    run_err(
        &["parquet-to-pmtiles", s(&parquet)],
        "Missing -o / --output path for parquet-to-pmtiles",
    );
    run_err(
        &["parquet-to-pmtiles", s(&parquet), "-o", "-"],
        "cannot write to stdout",
    );
    run_err(
        &["parquet-to-pmtiles", s(&parquet), "-o", s(&out), "-r", "8"],
        "Unknown option '-r' for parquet-to-pmtiles",
    );
    run_err(
        &[
            "parquet-to-pmtiles",
            s(&parquet),
            "x.parquet",
            "-o",
            s(&out),
        ],
        "Unexpected argument",
    );
    run_err(
        &["parquet-to-pmtiles", s(&parquet), "--h3-column"],
        "Missing value for --h3-column",
    );
    run_err(
        &[
            "parquet-to-pmtiles",
            s(&parquet),
            "--h3-column",
            "nope",
            "-o",
            s(&out),
        ],
        "Cell column 'nope' not found",
    );
    let missing = dir.path().join("missing.parquet");
    run_err(
        &["parquet-to-pmtiles", s(&missing), "-o", s(&out)],
        "does not exist",
    );
    run_err(&["parquet-to-pmtiles", s(&path), "-o", s(&out)], "Error:");
    assert!(
        !out.exists(),
        "a failed transcode must not leave an archive"
    );
}

#[test]
fn multiband_parquet_decodes_selected_tiles_and_alpha() {
    let dir = tempfile::tempdir().unwrap();
    for (layout, chunks) in [("planar", 12), ("interleaved", 4)] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../dggs-stream-core/tests/fixtures/multiband-12-{layout}.tif"
        ));
        let out = dir.path().join("cells.parquet");
        let result = run_ok(&[
            "parquet",
            s(&path),
            "-r",
            "12",
            "--bands",
            "3,1",
            "--profile",
            "-o",
            s(&out),
        ]);
        let profile = String::from_utf8(result.stderr).unwrap();
        assert!(profile.contains(&format!("chunks {chunks} (")), "{profile}");
        let reader = SerializedFileReader::new(File::open(out).unwrap()).unwrap();
        assert!(reader.metadata().file_metadata().num_rows() > 0);
        let columns = reader.metadata().file_metadata().schema_descr().columns();
        assert_eq!(columns[4].name(), "band_3_mean");
        assert_eq!(columns[10].name(), "band_1_mean");
    }
}

#[test]
fn multiband_parquet_refuses_repeated_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dggs-stream-core/tests/fixtures/multiband-12-planar.tif");
    let out = dir.path().join("cells.parquet");
    run_err(
        &[
            "parquet",
            s(&path),
            "-r",
            "12",
            "--bands",
            "3,1",
            "--columns",
            "wkb,band_3_mean,BAND_3_MEAN",
            "-o",
            s(&out),
        ],
        "duplicate Parquet column 'band_3_mean'",
    );
    assert!(!out.exists());
}

#[test]
fn ground_area_file_output_is_committed_only_after_success() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("cells");
    let (_file, raster) = gradient_tif();
    let corrupt = corrupt_strip_geotiff();
    for sub in ["jsonl", "ipc"] {
        std::fs::write(&output, b"previous result").unwrap();
        for (input, crs) in [(&*raster, "EPSG:3857"), (corrupt.path(), "EPSG:4326")] {
            run_err(
                &[
                    sub,
                    s(input),
                    "--ground-area",
                    "--crs",
                    crs,
                    "-o",
                    s(&output),
                ],
                "Error:",
            );
            assert_eq!(std::fs::read(&output).unwrap(), b"previous result");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
        let result = run_ok(&[
            sub,
            s(&raster),
            "--ground-area",
            "--crs",
            "EPSG:4326",
            "--profile",
            "--limit",
            "1",
            "-o",
            s(&output),
        ]);
        assert!(String::from_utf8_lossy(&result.stderr).contains("rows serialized 1,"));
        let bytes = std::fs::read(&output).unwrap();
        if sub == "jsonl" {
            let row: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(row["retained_area_m2"].as_f64().unwrap() > 0.0);
        } else {
            let (_, batches) = read_ipc_stream(&mut bytes.as_slice()).unwrap();
            assert_eq!(batches[0].num_rows, 1);
        }
    }
}

fn parent_parts(dataset: &Path) -> Vec<PathBuf> {
    let mut parts = Vec::new();
    for folder in dataset.read_dir().unwrap() {
        for file in folder.unwrap().path().read_dir().unwrap() {
            parts.push(file.unwrap().path());
        }
    }
    parts.sort();
    parts
}

fn parquet_rows(path: &Path) -> Vec<parquet::record::Row> {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    assert!(reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .iter()
        .any(|kv| kv.key == "geo"));
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn cell_id(row: &parquet::record::Row) -> h3o::CellIndex {
    let (_, id) = row
        .get_column_iter()
        .find(|(key, _)| *key == "h3_index")
        .unwrap();
    let id = match id {
        Field::Long(v) => *v as u64,
        Field::ULong(v) => *v,
        _ => panic!("integer H3 index expected"),
    };
    h3o::CellIndex::try_from(id).unwrap()
}

#[test]
fn hive_parent_dataset_matches_single_file() {
    let (_tmp, input) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();
    for categorical in [false, true] {
        let set = dir.path().join(format!("dataset-{categorical}"));
        let file = dir.path().join(format!("reference-{categorical}.parquet"));
        let mut args = vec!["parquet", s(&input), "-r", "8,9"];
        if categorical {
            args.push("-c");
        }
        let mut single = args.clone();
        single.extend(["-o", s(&file)]);
        run_ok(&single);
        args.extend(["--partition-by-parent-res", "7", "-o", s(&set)]);
        run_ok(&args);
        let mut want = parquet_rows(&file);
        let mut got = Vec::new();
        let parts = parent_parts(&set);
        assert!(parts.len() > 1);
        for part in parts {
            for row in parquet_rows(&part) {
                let parent = cell_id(&row).parent(h3o::Resolution::Seven).unwrap();
                assert_eq!(
                    part.parent()
                        .unwrap()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    format!("h3_07={parent}")
                );
                got.push(row);
            }
        }
        got.sort_by_key(cell_id);
        want.sort_by_key(cell_id);
        assert_eq!(got, want);
        run_err(&args, "already exists");
    }
}

#[test]
fn hive_parent_wide_projection_omits_internal_routing_key() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dggs-stream-core/tests/fixtures/multiband-12-planar.tif");
    let dir = tempfile::tempdir().unwrap();
    let set = dir.path().join("dataset");
    let file = dir.path().join("single.parquet");
    let args = [
        "parquet",
        s(&path),
        "-r",
        "10",
        "--bands",
        "3,1",
        "--columns",
        "band_3_mean,wkb,band_1_mean",
    ];
    run_ok(&[&args[..], &["-o", s(&file)]].concat());
    run_ok(
        &[
            &args[..],
            &["--partition-by-parent-res", "8", "-o", s(&set)],
        ]
        .concat(),
    );
    let sorted = |rows: Vec<parquet::record::Row>| {
        let mut values = rows
            .into_iter()
            .map(|row| {
                assert_eq!(
                    row.get_column_iter()
                        .map(|(name, _)| name.as_str())
                        .collect::<Vec<_>>(),
                    ["band_3_mean", "wkb", "band_1_mean"]
                );
                format!("{row:?}")
            })
            .collect::<Vec<_>>();
        values.sort();
        values
    };
    let rows = parent_parts(&set)
        .iter()
        .flat_map(|part| parquet_rows(part))
        .collect();
    assert_eq!(sorted(rows), sorted(parquet_rows(&file)));
}

#[test]
fn hive_parent_invalid_or_failed_export_leaves_no_dataset() {
    let (_tmp, input) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();
    let set = dir.path().join("dataset");
    let flag = ["--partition-by-parent-res", "8", "-o", s(&set)];
    run_err(
        &[&["parquet", s(&input), "-r", "7"][..], &flag].concat(),
        "no finer",
    );
    run_err(
        &[&["parquet", s(&input), "--grid", "s2"][..], &flag].concat(),
        "requires H3",
    );
    run_err(
        &[&["ipc", s(&input)][..], &flag].concat(),
        "--partition-by-parent-res",
    );
    let corrupt = corrupt_strip_geotiff();
    run_err(
        &[&["parquet", s(corrupt.path())][..], &flag].concat(),
        "Error:",
    );
    assert_eq!(dir.path().read_dir().unwrap().count(), 0);
}

#[test]
fn hive_parent_wide_export_refuses_repeated_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dggs-stream-core/tests/fixtures/multiband-12-planar.tif");
    let set = dir.path().join("dataset");
    // The names are checked once, before any part file, so a dataset is never started.
    run_err(
        &[
            "parquet",
            s(&path),
            "-r",
            "12",
            "--bands",
            "3,1",
            "--columns",
            "wkb,band_3_mean,BAND_3_MEAN",
            "--partition-by-parent-res",
            "8",
            "-o",
            s(&set),
        ],
        "duplicate Parquet column 'band_3_mean'",
    );
    assert_eq!(dir.path().read_dir().unwrap().count(), 0);
}

#[test]
#[ignore = "requires the DuckDB CLI on PATH"]
fn duckdb_prunes_hive_parent_files() {
    let (_tmp, input) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();
    let set = dir.path().join("dataset");
    let file = dir.path().join("single.parquet");
    run_ok(&["parquet", s(&input), "-r", "9", "-o", s(&file)]);
    run_ok(&[
        "parquet",
        s(&input),
        "-r",
        "9",
        "--partition-by-parent-res",
        "7",
        "-o",
        s(&set),
    ]);
    let parts = parent_parts(&set);
    let parent = cell_id(&parquet_rows(&parts[0])[0])
        .parent(h3o::Resolution::Seven)
        .unwrap();
    let ids = parquet_rows(&file)
        .iter()
        .map(cell_id)
        .filter(|c| c.parent(h3o::Resolution::Seven) == Some(parent))
        .map(|c| u64::from(c).to_string())
        .collect::<Vec<_>>()
        .join(",");
    let sql = |query: &str| {
        let result = Command::new("duckdb")
            .args(["-csv", "-noheader", "-c", query])
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        String::from_utf8(result.stdout).unwrap()
    };
    let dataset = format!(
        "read_parquet('{}/*/*.parquet', hive_partitioning=true)",
        set.display()
    );
    for filtered in [false, true] {
        let mut have = format!("SELECT * EXCLUDE(h3_07) FROM {dataset}");
        let mut want = format!("SELECT * FROM '{}'", file.display());
        if filtered {
            have += &format!(" WHERE h3_07='{parent}'");
            want += &format!(" WHERE h3_index IN ({ids})");
        }
        let query = format!("SELECT count(*) FROM (({want} EXCEPT ALL {have}) UNION ALL ({have} EXCEPT ALL {want}))");
        assert_eq!(sql(&query).trim(), "0");
    }
    let plan = sql(&format!(
        "EXPLAIN ANALYZE SELECT sum(pixel_count) FROM {dataset} WHERE h3_07='{parent}'"
    ));
    assert!(parts.len() > 1);
    assert!(
        plan.contains(&format!("Scanning Files: 1/{}", parts.len())),
        "{plan}"
    );
    assert!(plan.contains("Total Files Read: 1"), "{plan}");
}

#[test]
fn hive_parent_revisited_across_windows_and_empty_scan() {
    use dggs_stream_core::aggregator::multi_horizon::MultiScanHorizonStreamer;
    use dggs_stream_core::parquet::{
        write_parent_records, ContinuousRowGroupBuffer, ParquetExportConfig,
    };
    use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
    let (_tmp, input) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();
    let single = dir.path().join("single.parquet");
    run_ok(&["parquet", s(&input), "-r", "8", "-o", s(&single)]);
    for empty in [false, true] {
        let mut config = MultiResolutionConfig::new(vec![8]);
        if empty {
            config.min_count = Some(1e12);
        }
        let streamer =
            MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(&input).unwrap(), &config)
                .unwrap();
        let dataset = dir.path().join(format!("dataset-{empty}"));
        let opts = ParquetExportConfig {
            row_group_size: 3,
            geoparquet: true,
            ..Default::default()
        };
        let n =
            write_parent_records::<_, ContinuousRowGroupBuffer>(streamer, &dataset, 0, opts, |r| {
                r.cell_id
            })
            .unwrap();
        let parts = parent_parts(&dataset);
        let mut got: Vec<_> = parts.iter().flat_map(|p| parquet_rows(p)).collect();
        assert_eq!(n, got.len());
        if empty {
            assert_eq!(n, 0);
            assert_eq!(parts.len(), 1);
        } else {
            assert!(parts.len() > 1);
            assert!(parts.iter().all(|p| p.parent() == parts[0].parent()));
            let mut want = parquet_rows(&single);
            got.sort_by_key(cell_id);
            want.sort_by_key(cell_id);
            assert_eq!(got, want);
        }
    }
}

#[test]
fn compression_is_written_to_every_export_layout() {
    use parquet::basic::Compression;
    let dir = tempfile::tempdir().unwrap();
    let (_temp, raster) = classes_tif();
    for (i, (codec, expected, bands, partitioned, categorical)) in [
        ("snappy", Compression::SNAPPY, false, false, false),
        (
            "zstd",
            Compression::ZSTD(Default::default()),
            false,
            true,
            true,
        ),
        ("lz4", Compression::LZ4_RAW, true, false, false),
        ("none", Compression::UNCOMPRESSED, true, true, false),
        (
            "gzip",
            Compression::GZIP(Default::default()),
            false,
            false,
            true,
        ),
        (
            "brotli",
            Compression::BROTLI(Default::default()),
            false,
            true,
            false,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let dest = dir.path().join(i.to_string());
        let mut args = vec![
            "parquet",
            s(&raster),
            "-r",
            "8",
            "--compression",
            codec,
            "-o",
            s(&dest),
        ];
        if bands {
            args.extend(["--bands", "1"]);
        }
        if partitioned {
            args.extend(["--partition-by-parent-res", "6"]);
        }
        if categorical {
            args.push("-c");
        }
        run_ok(&args);
        let parts = if partitioned {
            parent_parts(&dest)
        } else {
            vec![dest]
        };
        assert!(!parts.is_empty());
        for part in parts {
            let reader = SerializedFileReader::new(File::open(part).unwrap()).unwrap();
            assert!(reader.num_row_groups() > 0);
            for group in reader.metadata().row_groups() {
                for column in group.columns() {
                    assert_eq!(column.compression(), expected);
                }
            }
        }
    }
    run_err(
        &["parquet", s(&raster), "--compression", "unknown"],
        "--compression",
    );
}

#[test]
fn polyfill_preserves_feature_ids_and_calls_each_grid() {
    use dggs_stream_core::grid::GridKind;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("polygons.geojson");
    let ring = serde_json::json!([[
        [1.234, 2.345],
        [1.234001, 2.345],
        [1.234, 2.345001],
        [1.234, 2.345]
    ]]);
    let collection = serde_json::json!({"type":"FeatureCollection","features":[
        {"type":"Feature","id":"parcel","geometry":{"type":"Polygon","coordinates":ring},"properties":{}},
        {"type":"Feature","geometry":null,"properties":{}},
        {"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[ring]},"properties":{}}
    ]});
    std::fs::write(&input, collection.to_string()).unwrap();
    for grid in [GridKind::H3, GridKind::S2, GridKind::Healpix, GridKind::A5] {
        let out = run_ok(&[
            "polyfill",
            s(&input),
            "--grid",
            grid.as_str(),
            "-r",
            "8",
            "--containment",
            "covers",
        ]);
        let rows: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&out.stdout)
            .into_iter()
            .collect::<Result<_, _>>()
            .unwrap();
        let cell = grid.cell_at(2.345, 1.234, 8).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["feature_id"], "parcel");
        assert_eq!(rows[1]["feature_id"], 2);
        for row in rows {
            assert_eq!(row[grid.index_column_name()], cell);
            assert_eq!(row[grid.hex_column_name()], format!("{cell:x}"));
        }
    }
    let dest = dir.path().join("cells.jsonl");
    std::fs::write(&dest, "previous output").unwrap();
    std::fs::write(&input, r#"{"type":"Point","coordinates":[0,0]}"#).unwrap();
    run_err(&["polyfill", s(&input), "-o", s(&dest)], "Polygon");
    assert_eq!(std::fs::read_to_string(dest).unwrap(), "previous output");
}

#[test]
fn overview_matches_independent_gdal_extraction() {
    // GDAL average pyramid of a 129x65 image with an internal mask.
    // The reference is gdal_translate -ovr 1 of that COG.
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../dggs-stream-core/tests/fixtures");
    let source = fixtures.join("overview-mask.tif");
    let reference = fixtures.join("overview-mask-level2.tif");
    let scan = |file: &Path, res: &str, mode: &str, flags: &[&str]| {
        let mut args = vec![
            "jsonl",
            s(file),
            "-r",
            res,
            "--transfer",
            mode,
            "--columns",
            "h3_index,resolution,count,sum",
        ];
        args.extend_from_slice(flags);
        let result = run_ok(&args);
        let mut rows: Vec<String> = String::from_utf8(result.stdout)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        rows.sort();
        assert!(!rows.is_empty());
        (rows, String::from_utf8(result.stderr).unwrap())
    };
    for mode in ["assign_centers", "interpolate"] {
        let (preview, message) = scan(&source, "7", mode, &["--overview", "auto"]);
        assert!(message.contains("32x16"), "{message}");
        assert_eq!(preview, scan(&reference, "7", mode, &[]).0);
        let native = scan(&source, "7", mode, &[]).0;
        assert_eq!(
            native,
            scan(&source, "7", mode, &["--overview", "native"]).0
        );
        assert_ne!(native, preview);
    }
    for extra in [
        vec![
            "-c",
            "--format",
            "long",
            "--columns",
            "h3_index,category,count",
        ],
        vec![
            "--bands",
            "1",
            "--columns",
            "h3_index,band_1_mean,band_1_count",
        ],
        vec![
            "--bbox",
            "-121.95,38.95,-121.90,38.98",
            "--columns",
            "h3_index,count,sum",
        ],
    ] {
        let compare = |path, policy| {
            let mut args = vec!["jsonl", path, "-r", "7", "--overview", policy];
            args.extend(&extra);
            let mut rows = String::from_utf8(run_ok(&args).stdout)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            rows.sort();
            assert!(!rows.is_empty());
            rows
        };
        assert_eq!(
            compare(s(&source), "auto"),
            compare(s(&reference), "native")
        );
    }
    for selector in ["auto-finer", "auto-coarser"] {
        let resolutions = |policy| {
            scan(&source, selector, "assign_centers", &["--overview", policy])
                .0
                .into_iter()
                .map(|row| {
                    serde_json::from_str::<serde_json::Value>(&row).unwrap()["resolution"]
                        .as_u64()
                        .unwrap()
                })
                .collect::<std::collections::BTreeSet<_>>()
        };
        let native = resolutions("native");
        assert_eq!(native.len(), 1);
        assert_eq!(resolutions("auto"), native, "{selector}");
    }
    let (fine, message) = scan(&source, "7,11", "assign_centers", &["--overview", "auto"]);
    assert!(message.contains("selected IFD 0"));
    assert_eq!(fine, scan(&source, "7,11", "assign_centers", &[]).0);
    assert_eq!(
        scan(&source, "7", "overlay", &["--overview", "auto"]).0,
        scan(&source, "7", "overlay", &[]).0
    );
}
