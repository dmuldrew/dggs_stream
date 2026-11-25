//! Parquet and PMTiles export entry points.

use std::ffi::{c_char, c_int};
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::error::{safe_panic_payload_to_string, DggsStreamError, Result};
use dggs_stream_core::parquet::{ParquetExportConfig, ParquetWriter};
use dggs_stream_core::pmtiles::PmtilesTiler;
use dggs_stream_core::raster::mosaic::{resolve_raster_sources, MosaicReader, OverlapRule};

use dggs_stream_core::grid::GridKind;

use crate::ffi::args::{output_policy_of, parse_c_grid, parse_c_resolutions, parse_grid};
use crate::ffi::{ffi_call, opt_cstr, set_c_err};

/// C-ABI direct Parquet exporter (`h3_raster_to_parquet`). Returns number of hexagons written, or `-1` on error.
///
/// Kept with its original signature for callers built against earlier releases; see
/// [`raster_h3_py_export_parquet_with_stats`] for the transform-failure count.
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_export_parquet(
    input_path: *const c_char,
    output_path: *const c_char,
    resolutions: *const c_char,
    categorical: c_int,
    crs: *const c_char,
    geoparquet: c_int,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: forwards the caller's arguments unchanged (same contract), with a null
    // `out_transform_failures`, which the callee accepts.
    unsafe {
        raster_h3_py_export_parquet_with_stats(
            input_path,
            output_path,
            resolutions,
            categorical,
            crs,
            geoparquet,
            ptr::null_mut(),
            out_err,
        )
    }
}

/// [`raster_h3_py_export_parquet`] that also writes the number of valid sample points
/// dropped by failed coordinate transforms to `out_transform_failures` (when non-null).
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL;
/// `out_transform_failures` must be NULL or point to a writable `u64`.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_export_parquet_with_stats(
    input_path: *const c_char,
    output_path: *const c_char,
    resolutions: *const c_char,
    categorical: c_int,
    crs: *const c_char,
    geoparquet: c_int,
    out_transform_failures: *mut u64,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: forwards the caller's arguments unchanged (same contract) with a null
    // `options`, which the callee accepts.
    unsafe {
        raster_h3_py_export_parquet_v2(
            input_path,
            output_path,
            resolutions,
            categorical,
            crs,
            geoparquet,
            ptr::null(),
            out_transform_failures,
            out_err,
        )
    }
}

/// Options of the `_v2` export entry points, a JSON object. Unknown fields are rejected.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PyExportOptions {
    bands: Option<Vec<usize>>,
    /// `h3` (default), `a5`, `s2` or `healpix`.
    grid: Option<String>,
    /// `omit` (default) or `emit`.
    nodata_policy: Option<String>,
    emit_nodata_value: Option<f64>,
    decimals: Option<i64>,
    /// `assign_centers` (default), `interpolate`, `overlay` or `auto`.
    transfer: Option<String>,
    interpolation: Option<String>,
    overlay_weighted: Option<bool>,
    mass_preserve: Option<bool>,
    valid_coverage_threshold: Option<f64>,
}

impl PyExportOptions {
    fn parse(json: Option<&str>) -> Result<Self> {
        json.map_or(Ok(Self::default()), |json| {
            serde_json::from_str(json).map_err(|e| {
                DggsStreamError::InvalidParameter(format!("Invalid export options: {e}"))
            })
        })
    }

    fn grid(&self) -> Result<GridKind> {
        self.grid.as_deref().map_or(Ok(GridKind::H3), parse_grid)
    }

    /// Copy the NoData policy, NoData value and decimals onto an export's configuration.
    fn apply_output_policy(&self, config: &mut MultiResolutionConfig) -> Result<()> {
        let policy = output_policy_of(
            self.nodata_policy.as_deref(),
            self.emit_nodata_value,
            self.decimals,
        )?;
        config.nodata_policy = policy.nodata;
        config.emit_nodata_value = policy.nodata_value;
        config.decimals = policy.decimals;
        config.valid_coverage_threshold = self.valid_coverage_threshold;
        config.transfer = dggs_stream_core::transfer::TransferMode::from_options(
            self.transfer.as_deref(),
            self.interpolation.as_deref(),
            self.overlay_weighted,
            self.mass_preserve,
        )?;
        Ok(())
    }
}

/// Write the transform-failure count and turn an export's outcome into its C result:
/// the number of cells written, or `-1` with `out_err` set.
///
/// # Safety
/// `out_transform_failures` must be NULL or point to a writable `u64`; `out_err` must be
/// NULL or a writable `char **`.
unsafe fn finish_export(
    what: &str,
    res: std::thread::Result<Result<usize>>,
    failures: &AtomicU64,
    out_transform_failures: *mut u64,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: `out_transform_failures` is null or writable and `out_err` meets
    // `set_c_err`'s contract (caller contract).
    unsafe {
        if !out_transform_failures.is_null() {
            *out_transform_failures = failures.load(Ordering::Relaxed);
        }
        match res {
            Ok(Ok(count)) => count as i64,
            Ok(Err(e)) => {
                set_c_err(out_err, &e.to_string());
                -1
            }
            Err(payload) => {
                let msg = safe_panic_payload_to_string(payload);
                set_c_err(out_err, &format!("Panic in {what}: {msg}"));
                -1
            }
        }
    }
}

/// [`raster_h3_py_export_parquet_with_stats`] with an `options` JSON object (NULL for
/// none): `grid` (`h3`, `a5`, `s2` or `healpix`) selects the grid, which names the cell
/// columns and bounds `resolutions`.
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL;
/// `out_transform_failures` must be NULL or point to a writable `u64`; `out_err` must be
/// NULL or a writable `char **`.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_export_parquet_v2(
    input_path: *const c_char,
    output_path: *const c_char,
    resolutions: *const c_char,
    categorical: c_int,
    crs: *const c_char,
    geoparquet: c_int,
    options: *const c_char,
    out_transform_failures: *mut u64,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: every C string argument is null or NUL-terminated (caller contract), as
    // `opt_cstr` requires; the out-pointers meet `finish_export`'s contract, and error
    // strings are freed by the caller with `raster_h3_py_free_string`.
    unsafe {
        let failures = Arc::new(AtomicU64::new(0));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<usize> {
            let in_str = opt_cstr(input_path)
                .ok_or_else(|| DggsStreamError::InvalidParameter("Missing input_path".into()))?;
            let out_str = opt_cstr(output_path)
                .ok_or_else(|| DggsStreamError::InvalidParameter("Missing output_path".into()))?;
            let opts = PyExportOptions::parse(opt_cstr(options).as_deref())?;
            let grid = opts.grid()?;
            let crs_opt = opt_cstr(crs);
            let paths = resolve_raster_sources(&in_str)?;
            let res_vec = parse_c_resolutions(grid, resolutions)?.resolve_paths(
                grid,
                &paths,
                crs_opt.as_deref(),
            )?;
            let mut config = MultiResolutionConfig::new(res_vec).with_grid(grid);
            opts.apply_output_policy(&mut config)?;
            config.custom_crs = crs_opt.clone();
            config.transform_failure_counter = Some(Arc::clone(&failures));

            if let Some(bands) = &opts.bands {
                if categorical != 0 {
                    return Err(DggsStreamError::InvalidParameter(
                        "bands requires a continuous scan".into(),
                    ));
                }
                let [path] = paths.as_slice() else {
                    return Err(DggsStreamError::InvalidParameter(
                        "bands requires one raster".into(),
                    ));
                };
                let raster = dggs_stream_core::raster::geotiff::GeoTiffStreamReader::open(path)?;
                let mut reader = dggs_stream_core::columnar::multiband::MultiBandBatchReader::open(
                    raster, &config, bands, None, 2048, None,
                )?;
                return dggs_stream_core::parquet::write_multiband_parquet(
                    &mut reader,
                    Path::new(&out_str),
                    geoparquet != 0,
                    ParquetExportConfig::default().row_group_size,
                )
                .map_err(|e| DggsStreamError::StreamFailed(e.to_string()));
            }
            let mosaic = Arc::new(MosaicReader::open(
                &paths,
                None,
                crs_opt.as_deref(),
                OverlapRule::Cutline,
            )?);
            let pq_cfg = ParquetExportConfig {
                geoparquet: geoparquet != 0,
                is_categorical: categorical != 0,
                ..Default::default()
            };

            if categorical == 0 {
                let streamer = MultiScanHorizonStreamer::new_mosaic(mosaic, &config)?;
                ParquetWriter::write_continuous_streamer_to_parquet(
                    streamer,
                    Path::new(&out_str),
                    pq_cfg,
                )
                .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))
            } else {
                let streamer = MultiCategoricalHorizonStreamer::new_mosaic(mosaic, &config)?;
                ParquetWriter::write_categorical_streamer_to_parquet(
                    streamer,
                    Path::new(&out_str),
                    pq_cfg,
                )
                .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))
            }
        }));
        finish_export(
            "export_parquet",
            res,
            &failures,
            out_transform_failures,
            out_err,
        )
    }
}

/// C-ABI direct PMTiles v3 exporter (`h3_raster_to_pmtiles`). Returns number of hexagons written, or `-1` on error.
///
/// Kept with its original signature for callers built against earlier releases; see
/// [`raster_h3_py_export_pmtiles_with_stats`] for the transform-failure count.
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_export_pmtiles(
    input_path: *const c_char,
    output_path: *const c_char,
    resolutions: *const c_char,
    categorical: c_int,
    crs: *const c_char,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: forwards the caller's arguments unchanged (same contract), with a null
    // `out_transform_failures`, which the callee accepts.
    unsafe {
        raster_h3_py_export_pmtiles_with_stats(
            input_path,
            output_path,
            resolutions,
            categorical,
            crs,
            ptr::null_mut(),
            out_err,
        )
    }
}

/// [`raster_h3_py_export_pmtiles`] that also writes the number of valid sample points
/// dropped by failed coordinate transforms to `out_transform_failures` (when non-null).
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL;
/// `out_transform_failures` must be NULL or point to a writable `u64`.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_export_pmtiles_with_stats(
    input_path: *const c_char,
    output_path: *const c_char,
    resolutions: *const c_char,
    categorical: c_int,
    crs: *const c_char,
    out_transform_failures: *mut u64,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: forwards the caller's arguments unchanged (same contract) with a null
    // `options`, which the callee accepts.
    unsafe {
        raster_h3_py_export_pmtiles_v2(
            input_path,
            output_path,
            resolutions,
            categorical,
            crs,
            ptr::null(),
            out_transform_failures,
            out_err,
        )
    }
}

/// [`raster_h3_py_export_pmtiles_with_stats`] with the `options` JSON object of
/// [`raster_h3_py_export_parquet_v2`].
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL;
/// `out_transform_failures` must be NULL or point to a writable `u64`; `out_err` must be
/// NULL or a writable `char **`.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_export_pmtiles_v2(
    input_path: *const c_char,
    output_path: *const c_char,
    resolutions: *const c_char,
    categorical: c_int,
    crs: *const c_char,
    options: *const c_char,
    out_transform_failures: *mut u64,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: every C string argument is null or NUL-terminated (caller contract), as
    // `opt_cstr` requires; the out-pointers meet `finish_export`'s contract, and error
    // strings are freed by the caller with `raster_h3_py_free_string`.
    unsafe {
        let failures = Arc::new(AtomicU64::new(0));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<usize> {
            let in_str = opt_cstr(input_path)
                .ok_or_else(|| DggsStreamError::InvalidParameter("Missing input_path".into()))?;
            let out_str = opt_cstr(output_path)
                .ok_or_else(|| DggsStreamError::InvalidParameter("Missing output_path".into()))?;
            let opts = PyExportOptions::parse(opt_cstr(options).as_deref())?;
            if opts.bands.is_some() {
                return Err(DggsStreamError::InvalidParameter(
                    "bands is unsupported for PMTiles".into(),
                ));
            }
            let grid = opts.grid()?;
            let crs_opt = opt_cstr(crs);
            let res_vec = parse_c_resolutions(grid, resolutions)?.resolve_paths(
                grid,
                &resolve_raster_sources(&in_str)?,
                crs_opt.as_deref(),
            )?;
            let mut config = MultiResolutionConfig::new(res_vec).with_grid(grid);
            opts.apply_output_policy(&mut config)?;
            config.custom_crs = crs_opt;
            config.transform_failure_counter = Some(Arc::clone(&failures));

            if categorical == 0 {
                PmtilesTiler::process_raster_source_to_pmtiles(&in_str, Path::new(&out_str), config)
                    .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))
            } else {
                PmtilesTiler::process_categorical_source_to_pmtiles(
                    &in_str,
                    Path::new(&out_str),
                    config,
                )
                .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))
            }
        }));
        finish_export(
            "export_pmtiles",
            res,
            &failures,
            out_transform_failures,
            out_err,
        )
    }
}

/// Transcode a cell-indexed Parquet file into a PMTiles v3 archive (DuckDB's
/// `h3_parquet_to_pmtiles`). `h3_column` names the cell column (integer or hex string,
/// matched case-insensitively); NULL auto-detects `{h3,a5,s2,healpix}_{index,hex}`, a
/// bare grid name, `cell` or `hex`. The grid is the one the column is named for, H3 if
/// the name does not say. Returns the number of cell features written, or `-1` with
/// `out_err` set.
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL; `out_err` must be
/// NULL or a writable `char **`.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_parquet_to_pmtiles(
    parquet_path: *const c_char,
    pmtiles_path: *const c_char,
    h3_column: *const c_char,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: forwards the caller's arguments unchanged (same contract) with a null
    // `grid`, which the callee accepts.
    unsafe {
        raster_h3_py_parquet_to_pmtiles_v2(
            parquet_path,
            pmtiles_path,
            h3_column,
            ptr::null(),
            out_err,
        )
    }
}

/// [`raster_h3_py_parquet_to_pmtiles`] with the cells' `grid` (`h3`, `a5`, `s2` or
/// `healpix`) given explicitly, for a cell column whose name does not identify it. NULL
/// keeps the auto-detection.
///
/// # Safety
/// Pointer arguments must be valid NUL-terminated C strings or NULL; `out_err` must be
/// NULL or a writable `char **`.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_parquet_to_pmtiles_v2(
    parquet_path: *const c_char,
    pmtiles_path: *const c_char,
    cell_column: *const c_char,
    grid: *const c_char,
    out_err: *mut *mut c_char,
) -> i64 {
    // SAFETY: the C string arguments are null or NUL-terminated and `out_err` is null or a
    // writable `char **` (caller contract), as `opt_cstr` and `ffi_call` require.
    unsafe {
        ffi_call("parquet_to_pmtiles", out_err, || {
            let in_str = opt_cstr(parquet_path)
                .ok_or_else(|| DggsStreamError::InvalidParameter("Missing parquet_path".into()))?;
            let out_str = opt_cstr(pmtiles_path)
                .ok_or_else(|| DggsStreamError::InvalidParameter("Missing pmtiles_path".into()))?;
            let column = opt_cstr(cell_column);
            let grid = match opt_cstr(grid) {
                None => None,
                Some(_) => Some(parse_c_grid(grid)?),
            };
            let summary = PmtilesTiler::process_parquet_to_pmtiles_for(
                Path::new(&in_str),
                Path::new(&out_str),
                column.as_deref(),
                grid,
            )
            .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))?;
            Ok(summary.valid_features as i64)
        })
        .unwrap_or(-1)
    }
}
