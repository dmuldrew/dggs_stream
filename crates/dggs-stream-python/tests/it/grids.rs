//! The `grid` option of the `_v2` entry points and the grid-neutral scalar helpers, on
//! every grid: the C-ABI scan equals the core reader configured for that grid.

use std::ffi::{c_char, CStr, CString};
use std::path::{Path, PathBuf};
use std::ptr;

use dggs_stream_core::aggregator::multi_horizon::MultiResolutionConfig;
use dggs_stream_core::columnar::{ColumnArray, ContinuousBatchReader};
use dggs_stream_core::GridKind;
use dggs_stream_python::{
    dggs_stream_py_export_parquet_v2, dggs_stream_py_export_pmtiles_v2, dggs_stream_py_free_cells,
    dggs_stream_py_free_string, dggs_stream_py_grid_cell_to_parent, dggs_stream_py_grid_code,
    dggs_stream_py_grid_get_resolution, dggs_stream_py_grid_is_valid,
    dggs_stream_py_grid_lonlat_to_cell, dggs_stream_py_grid_max_resolution,
    dggs_stream_py_grid_polygon_to_cells, dggs_stream_py_grid_to_lat, dggs_stream_py_grid_to_lng,
    dggs_stream_py_grid_to_wkb, dggs_stream_py_parquet_to_pmtiles_v2, dggs_stream_py_scan_json_v2,
    dggs_stream_py_wkb_buf_len,
};

use crate::c_abi::{gradient_tif, take_err};

/// A resolution of each grid with several cells over the fixture raster.
fn res_of(grid: GridKind) -> u8 {
    match grid {
        GridKind::H3 => 9,
        GridKind::A5 | GridKind::Healpix => 14,
        GridKind::S2 => 15,
    }
}

fn options(grid: GridKind) -> CString {
    CString::new(format!("{{\"grid\": \"{grid}\"}}")).unwrap()
}

/// `dggs_stream_py_scan_json_v2` of `tif` at `res` with an `options` JSON object.
fn scan_json(
    tif: &Path,
    res: &str,
    options: &CStr,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let path = CString::new(tif.to_str().unwrap()).unwrap();
    let res = CString::new(res).unwrap();
    // SAFETY: the string arguments are live `CString`s owned by this frame (or null); the
    // returned JSON string is copied and freed once, and `take_err` frees the error.
    unsafe {
        let mut err: *mut c_char = ptr::null_mut();
        let out = dggs_stream_py_scan_json_v2(
            path.as_ptr(),
            res.as_ptr(),
            0,
            ptr::null(),
            ptr::null(),
            0,
            0.0,
            ptr::null(),
            ptr::null(),
            -1,
            0,
            options.as_ptr(),
            ptr::null_mut(),
            &mut err,
        );
        if out.is_null() {
            return Err(take_err(err).expect("NULL result must set out_err"));
        }
        let text = CStr::from_ptr(out).to_str().unwrap().to_string();
        dggs_stream_py_free_string(out);
        match serde_json::from_str(&text).unwrap() {
            serde_json::Value::Object(m) => Ok(m),
            other => panic!("scan_json must return an object, got {other}"),
        }
    }
}

/// `(cell, count, sum)` of every cell the core reader emits, sorted by cell.
fn core_cells(tif: &Path, grid: GridKind) -> Vec<(u64, f64, f64)> {
    let cfg = MultiResolutionConfig::single(res_of(grid)).with_grid(grid);
    let mut rows = Vec::new();
    for batch in ContinuousBatchReader::open(&[PathBuf::from(tif)], &cfg, None, 2048, None).unwrap()
    {
        let batch = batch.unwrap();
        let col = |name: &str| batch.column_by_name(name).unwrap().clone();
        let (ColumnArray::UInt64 { values: cells, .. }, ColumnArray::Float64 { values: n, .. }) =
            (col(grid.index_column_name()), col("count"))
        else {
            panic!("unexpected column types");
        };
        let ColumnArray::Float64 { values: sums, .. } = col("sum") else {
            panic!("unexpected column type");
        };
        rows.extend((0..batch.num_rows).map(|i| (cells[i], n[i], sums[i])));
    }
    rows.sort_by_key(|r| r.0);
    rows
}

#[test]
fn v2_scans_take_the_grid_from_options() {
    let (_tmp, tif) = gradient_tif();
    for grid in GridKind::ALL {
        let json = scan_json(&tif, &res_of(grid).to_string(), &options(grid)).unwrap();
        let ids = json[grid.index_column_name()].as_array().unwrap();
        let hexes = json[grid.hex_column_name()].as_array().unwrap();
        let mut rows: Vec<(u64, f64, f64)> = (0..ids.len())
            .map(|i| {
                let cell = ids[i].as_u64().unwrap();
                assert_eq!(hexes[i].as_str().unwrap(), format!("{cell:x}"));
                assert_eq!(grid.resolution(cell), Some(res_of(grid)));
                (
                    cell,
                    json["count"][i].as_f64().unwrap(),
                    json["sum"][i].as_f64().unwrap(),
                )
            })
            .collect();
        rows.sort_by_key(|r| r.0);
        assert!(rows.len() > 4, "{grid}");
        assert_eq!(rows, core_cells(&tif, grid), "{grid}");
        // Only this grid's cell columns are present.
        for other in GridKind::ALL.into_iter().filter(|g| *g != grid) {
            assert!(!json.contains_key(other.index_column_name()));
        }
    }
}

#[test]
fn v2_scan_accepts_budgets_and_rejects_invalid_sizes() {
    let (_tmp, tif) = gradient_tif();
    let options = c"{\"aggregation_budget_mb\":1,\"decode_budget_mb\":1}";
    let result = scan_json(&tif, "9", options).unwrap();
    assert_eq!(
        result["count"].as_array().unwrap().len(),
        core_cells(&tif, GridKind::H3).len()
    );
    for name in ["aggregation_budget_mb", "decode_budget_mb"] {
        let invalid = CString::new(format!(r#"{{"{name}":0}}"#)).unwrap();
        assert!(scan_json(&tif, "9", &invalid).unwrap_err().contains(name));
    }
}

#[test]
fn budgets_follow_the_transfer_mode_auto_resolves_to() {
    let (_tmp, tif) = gradient_tif();
    let scan = |options: &str| scan_json(&tif, "9", &CString::new(options).unwrap());

    // The decode budget bounds decoded chunks in every mode: an overlay scan accepts it
    // and gives the same cells as without it.
    let overlay = scan(r#"{"transfer":"overlay"}"#).unwrap();
    let budgeted = scan(r#"{"transfer":"overlay","decode_budget_mb":1}"#).unwrap();
    assert_eq!(budgeted, overlay);

    // The aggregation budget belongs to centre assignment alone.
    let err = scan(r#"{"transfer":"overlay","aggregation_budget_mb":1}"#).unwrap_err();
    assert!(
        err.contains("applies only to transfer='assign_centers'"),
        "{err}"
    );

    // `auto` is resolved before the check: about ten pixels per resolution-9 cell
    // resolves to overlay, so the budget is refused with the mode named...
    let err = scan(r#"{"transfer":"auto","aggregation_budget_mb":1}"#).unwrap_err();
    assert!(err.contains("transfer='auto' chose 'overlay'"), "{err}");
    // ...while sub-pixel sampling makes `auto` assign centres, where the budget applies.
    // A small aggregation budget can change the order cells are emitted and merged in,
    // so compare rows sorted by cell, with numbers equal to rounding.
    let rows = |columns: &serde_json::Map<String, serde_json::Value>| {
        let names: Vec<_> = columns.keys().cloned().collect();
        let len = columns[&names[0]].as_array().unwrap().len();
        let mut rows: Vec<Vec<serde_json::Value>> = (0..len)
            .map(|i| names.iter().map(|n| columns[n][i].clone()).collect())
            .collect();
        let key = names.iter().position(|n| n == "h3_index").unwrap();
        rows.sort_by_key(|row| row[key].as_u64());
        rows
    };
    let auto = scan(r#"{"transfer":"auto","sampling":"rgss","aggregation_budget_mb":1}"#).unwrap();
    let centres = scan(r#"{"sampling":"rgss"}"#).unwrap();
    assert_eq!(
        auto.keys().collect::<Vec<_>>(),
        centres.keys().collect::<Vec<_>>()
    );
    let (auto, centres) = (rows(&auto), rows(&centres));
    assert!(!centres.is_empty());
    assert_eq!(auto.len(), centres.len());
    for (a, c) in auto.iter().flatten().zip(centres.iter().flatten()) {
        match (a.as_f64(), c.as_f64()) {
            (Some(x), Some(y)) => assert!((x - y).abs() <= 1e-12 * y.abs().max(1.0), "{x} {y}"),
            _ => assert_eq!(a, c),
        }
    }
}

#[test]
fn resolutions_are_bounded_by_the_grid() {
    let (_tmp, tif) = gradient_tif();
    for grid in GridKind::ALL {
        let finest = grid.max_resolution();
        let err = scan_json(&tif, &(finest + 1).to_string(), &options(grid)).unwrap_err();
        assert!(
            err.contains(&format!(
                "{} resolutions between 0 and {finest}",
                grid.label()
            )),
            "{err}"
        );
    }
    // 20 is an A5 resolution but not an H3 one.
    assert!(scan_json(&tif, "20", &options(GridKind::A5)).is_ok());
    assert!(scan_json(&tif, "20", c"{}").is_err());
    let err = scan_json(&tif, "9", c"{\"grid\": \"s9\"}").unwrap_err();
    assert!(err.contains("Invalid grid 's9'"), "{err}");
}

#[test]
fn exports_write_the_requested_grid() {
    let (_tmp, tif) = gradient_tif();
    let dir = tempfile::tempdir().unwrap();
    let input = CString::new(tif.to_str().unwrap()).unwrap();
    for grid in GridKind::ALL {
        let expected = core_cells(&tif, grid).len() as i64;
        let res = CString::new(res_of(grid).to_string()).unwrap();
        let opts = options(grid);
        let parquet = dir.path().join(format!("{grid}.parquet"));
        let pmtiles = dir.path().join(format!("{grid}.pmtiles"));
        let renamed = dir.path().join(format!("{grid}-tiles.pmtiles"));
        let c_parquet = CString::new(parquet.to_str().unwrap()).unwrap();
        let c_pmtiles = CString::new(pmtiles.to_str().unwrap()).unwrap();
        let c_renamed = CString::new(renamed.to_str().unwrap()).unwrap();
        let name = CString::new(grid.as_str()).unwrap();
        // SAFETY: every string argument is a live `CString` of this frame (or null) and the
        // out-pointers are writable locals.
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();
            let mut failures = u64::MAX;
            let n = dggs_stream_py_export_parquet_v2(
                input.as_ptr(),
                c_parquet.as_ptr(),
                res.as_ptr(),
                0,
                ptr::null(),
                1,
                opts.as_ptr(),
                &mut failures,
                &mut err,
            );
            assert_eq!((n, failures), (expected, 0), "{grid}: {:?}", take_err(err));
            let n = dggs_stream_py_export_pmtiles_v2(
                input.as_ptr(),
                c_pmtiles.as_ptr(),
                res.as_ptr(),
                0,
                ptr::null(),
                opts.as_ptr(),
                &mut failures,
                &mut err,
            );
            assert_eq!(n, expected, "{grid}: {:?}", take_err(err));
            // Auto-detected from the `{grid}_index` column, and with the grid named.
            for grid_arg in [ptr::null(), name.as_ptr()] {
                let n = dggs_stream_py_parquet_to_pmtiles_v2(
                    c_parquet.as_ptr(),
                    c_renamed.as_ptr(),
                    ptr::null(),
                    grid_arg,
                    &mut err,
                );
                assert_eq!(n, expected, "{grid}: {:?}", take_err(err));
            }
            let n = dggs_stream_py_parquet_to_pmtiles_v2(
                c_parquet.as_ptr(),
                c_renamed.as_ptr(),
                ptr::null(),
                c"hexagons".as_ptr(),
                &mut err,
            );
            assert_eq!(n, -1);
            assert!(take_err(err).unwrap().contains("Invalid grid"));
        }
        assert!(std::fs::read(&pmtiles).unwrap().starts_with(b"PMTiles"));
    }
}

#[test]
fn grid_scalars_match_core() {
    // SAFETY: `dggs_stream_py_grid_code` takes NUL-terminated literals or null.
    unsafe {
        assert_eq!(dggs_stream_py_grid_code(c"H3".as_ptr()), 0);
        assert_eq!(dggs_stream_py_grid_code(c"healpix".as_ptr()), 3);
        assert_eq!(dggs_stream_py_grid_code(c"s9".as_ptr()), -1);
        assert_eq!(dggs_stream_py_grid_code(ptr::null()), -1);
    }
    assert_eq!(dggs_stream_py_grid_max_resolution(9), -1);
    assert_eq!(dggs_stream_py_grid_lonlat_to_cell(9, 0.0, 0.0, 3), 0);
    let cap = dggs_stream_py_wkb_buf_len();
    for grid in GridKind::ALL {
        let code = grid as i32;
        assert_eq!(
            dggs_stream_py_grid_max_resolution(code),
            i32::from(grid.max_resolution())
        );
        // South of -45° at lon 0 an S2 cell is on face 5, whose ids set bit 63.
        for (lon, lat) in [(-122.4194, 37.7749), (179.99, -0.01), (0.0, -89.5)] {
            for res in [0u8, 6, grid.max_resolution() - 1] {
                let cell = dggs_stream_py_grid_lonlat_to_cell(code, lon, lat, i64::from(res));
                assert_eq!(Some(cell), grid.cell_at(lat, lon, res), "{grid}");
                assert_eq!(dggs_stream_py_grid_is_valid(code, cell), 1);
                assert_eq!(
                    dggs_stream_py_grid_get_resolution(code, cell),
                    i64::from(res)
                );
                let (c_lat, c_lng) = grid.center_lat_lng(cell).unwrap();
                assert_eq!(dggs_stream_py_grid_to_lat(code, cell), c_lat);
                assert_eq!(dggs_stream_py_grid_to_lng(code, cell), c_lng);
                let parent = dggs_stream_py_grid_cell_to_parent(code, cell, 0);
                assert_eq!(Some(parent), grid.parent(cell, 0));
                assert_eq!(
                    dggs_stream_py_grid_cell_to_parent(code, cell, i64::from(res) + 1),
                    0
                );
                let mut buf = vec![0u8; cap];
                // SAFETY: `buf` holds `cap` writable bytes.
                let n = unsafe { dggs_stream_py_grid_to_wkb(code, cell, buf.as_mut_ptr(), cap) };
                let wkb = dggs_stream_core::scalar_ops::grid_to_wkb_u64(grid, cell).unwrap();
                assert_eq!(&buf[..n], &wkb[..]);
                // SAFETY: a too-small or null buffer is reported as 0 bytes written.
                unsafe {
                    assert_eq!(
                        dggs_stream_py_grid_to_wkb(code, cell, buf.as_mut_ptr(), 8),
                        0
                    );
                    assert_eq!(
                        dggs_stream_py_grid_to_wkb(code, cell, ptr::null_mut(), cap),
                        0
                    );
                }
            }
        }
        assert_eq!(dggs_stream_py_grid_is_valid(code, 0), 0);
        assert!(dggs_stream_py_grid_to_lat(code, 0).is_nan());
        assert_eq!(dggs_stream_py_grid_get_resolution(code, 0), -1);
    }
    let s2_south = dggs_stream_py_grid_lonlat_to_cell(GridKind::S2 as i32, 0.0, -89.5, 10);
    assert!(s2_south >> 63 == 1, "face 5 ids use the top bit");
}

#[test]
fn grid_polygon_fill_matches_core() {
    let ring = [
        (-122.45, 37.75),
        (-122.40, 37.75),
        (-122.40, 37.80),
        (-122.45, 37.80),
        (-122.45, 37.75),
    ];
    let mut wkb = vec![1u8];
    wkb.extend(3u32.to_le_bytes());
    wkb.extend(1u32.to_le_bytes());
    wkb.extend((ring.len() as u32).to_le_bytes());
    for (x, y) in ring {
        wkb.extend(f64::to_le_bytes(x));
        wkb.extend(f64::to_le_bytes(y));
    }
    for grid in GridKind::ALL {
        let res = i64::from(res_of(grid)) - 2;
        let mut expected = Vec::new();
        dggs_stream_core::scalar_ops::grid_polygon_to_cells_wkb_into(
            grid,
            &wkb,
            res,
            dggs_stream_core::scalar_ops::ContainmentMode::ContainsCentroid,
            &mut expected,
        )
        .unwrap();
        assert!(!expected.is_empty(), "{grid}");
        let fill = |res: i64, mode: *const c_char| -> (i32, Vec<u64>) {
            let mut cells: *mut u64 = ptr::null_mut();
            let mut len = 0usize;
            // SAFETY: `wkb` is readable for its length, `mode` is null or a literal, and
            // the out-pointers are writable; a returned array is copied and freed once.
            unsafe {
                let status = dggs_stream_py_grid_polygon_to_cells(
                    grid as i32,
                    wkb.as_ptr(),
                    wkb.len(),
                    res,
                    mode,
                    &mut cells,
                    &mut len,
                );
                if status != 0 {
                    assert!(cells.is_null());
                    return (status, Vec::new());
                }
                let out = std::slice::from_raw_parts(cells, len).to_vec();
                dggs_stream_py_free_cells(cells, len);
                (status, out)
            }
        };
        assert_eq!(fill(res, ptr::null()), (0, expected));
        assert_eq!(fill(i64::from(grid.max_resolution()) + 1, ptr::null()).0, 1);
        assert_eq!(fill(res, c"sideways".as_ptr()).0, 2);
    }
    let mut cells: *mut u64 = ptr::null_mut();
    let mut len = 0usize;
    // SAFETY: as above; grid code 9 is rejected before the outputs are written.
    let status = unsafe {
        dggs_stream_py_grid_polygon_to_cells(
            9,
            wkb.as_ptr(),
            wkb.len(),
            5,
            ptr::null(),
            &mut cells,
            &mut len,
        )
    };
    assert_eq!(status, 4);
}

/// `(cell, pixel_count, area_m2, weighted_sum, mean)` of the core's ground-area overlay,
/// sorted by cell.
fn core_overlay_cells(
    tif: &Path,
    grid: GridKind,
    coverage: Option<f64>,
) -> Vec<(u64, f64, f64, f64, f64)> {
    use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
    use dggs_stream_core::transfer::{scan_ground_area_overlay, TransferMode};
    let mut cfg = MultiResolutionConfig::single(res_of(grid)).with_grid(grid);
    cfg.transfer = TransferMode::OVERLAY;
    cfg.valid_coverage_threshold = coverage;
    let mut rows: Vec<_> = scan_ground_area_overlay(GeoTiffStreamReader::open(tif).unwrap(), &cfg)
        .unwrap()
        .map(|row| {
            let (_, cell, a) = row.unwrap();
            (
                cell,
                a.pixel_count(),
                a.retained_area_m2(),
                a.area_weighted_sum(),
                a.area_stats().mean(),
            )
        })
        .collect();
    rows.sort_by_key(|r| r.0);
    rows
}

fn floats(json: &serde_json::Map<String, serde_json::Value>, name: &str) -> Vec<f64> {
    json[name]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_f64().unwrap())
        .collect()
}

#[test]
fn v2_ground_area_overlay_matches_the_core_on_every_grid() {
    let (_tmp, tif) = gradient_tif();
    for grid in GridKind::ALL {
        for (coverage, decimals) in [(None, None), (Some(0.5), Some(0))] {
            let expected = core_overlay_cells(&tif, grid, coverage);
            assert!(expected.len() > 1, "{grid}");
            let mut options = serde_json::json!({
                "grid": grid.to_string(),
                "ground_area": true,
                "transfer": "overlay",
            });
            if let Some(coverage) = coverage {
                options["valid_coverage_threshold"] = coverage.into();
            }
            if let Some(decimals) = decimals {
                options["decimals"] = decimals.into();
            }
            let options = CString::new(options.to_string()).unwrap();
            let json = scan_json(&tif, &res_of(grid).to_string(), &options).unwrap();
            let ids: Vec<u64> = json[grid.index_column_name()]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .collect();
            let (counts, areas) = (
                floats(&json, "pixel_count"),
                floats(&json, "retained_area_m2"),
            );
            let (sums, means) = (
                floats(&json, "area_weighted_sum"),
                floats(&json, "area_weighted_mean"),
            );
            let mut rows: Vec<_> = (0..ids.len())
                .map(|i| (ids[i], counts[i], areas[i], sums[i], means[i]))
                .collect();
            rows.sort_by_key(|r| r.0);
            assert_eq!(rows.len(), expected.len(), "{grid}");
            for (got, want) in rows.iter().zip(&expected) {
                assert_eq!(got.0, want.0, "{grid} cells");
                // Fractional pixel counts are never rounded (rounding to 0 decimals would
                // move one by up to 0.5), though the JSON round trip may differ in the
                // last bit; the area statistics are rounded.
                assert!(
                    (got.1 - want.1).abs() <= 1e-12 * want.1.max(1.0),
                    "{grid} {} pixel_count: {} vs {}",
                    got.0,
                    got.1,
                    want.1
                );
                for (a, b, name) in [
                    (got.2, want.2, "area"),
                    (got.3, want.3, "sum"),
                    (got.4, want.4, "mean"),
                ] {
                    // With `decimals: 0` the rounded statistic is within 0.5 of the exact one.
                    let tolerance = if decimals.is_some() {
                        0.5 + 1e-12 * b.abs()
                    } else {
                        1e-12 * b.abs().max(1.0)
                    };
                    assert!(
                        (a - b).abs() <= tolerance,
                        "{grid} {} {name}: {a} vs {b}",
                        got.0
                    );
                }
            }
            // Overlay counts partial pixels, which centre assignment never does.
            assert!(
                rows.iter().any(|r| r.1.fract() != 0.0),
                "{grid}: no fractional counts"
            );
        }
    }
}

#[test]
fn v2_ground_area_overlay_validates_its_options() {
    let (_tmp, tif) = gradient_tif();
    for (options, message) in [
        // A coverage threshold needs weighted overlay and a fraction.
        (
            r#"{"ground_area":true,"valid_coverage_threshold":0.5}"#,
            "valid_coverage_threshold",
        ),
        (
            r#"{"ground_area":true,"transfer":"overlay","valid_coverage_threshold":1.5}"#,
            "valid_coverage_threshold",
        ),
        // Mass-preserving, unweighted and interpolating transfers are not area integrals.
        (
            r#"{"ground_area":true,"transfer":"overlay","mass_preserve":true}"#,
            "ground_area",
        ),
        (
            r#"{"ground_area":true,"transfer":"overlay","overlay_weighted":false}"#,
            "ground_area",
        ),
        (
            r#"{"ground_area":true,"transfer":"interpolate"}"#,
            "ground_area",
        ),
    ] {
        let options = CString::new(options).unwrap();
        let error = scan_json(&tif, "9", &options).unwrap_err();
        assert!(error.contains(message), "{options:?}: {error}");
    }
    // Weighted overlay is accepted.
    let ok = CString::new(r#"{"ground_area":true,"transfer":"overlay"}"#).unwrap();
    assert!(scan_json(&tif, "9", &ok).is_ok());
}
