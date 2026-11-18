use dggs_stream_core::aggregator::output_policy::{NodataPolicy, OutputPolicy};
use dggs_stream_core::transfer::TransferMode;
use std::ffi::CString;
use std::path::PathBuf;

use crate::aggregator::sampling::SamplingPattern;
use crate::ffi::{
    duckdb_bind_add_result_column, duckdb_bind_get_named_parameter, duckdb_bind_get_parameter,
    duckdb_bind_get_parameter_count, duckdb_bind_info, duckdb_bind_set_error,
    duckdb_create_logical_type, duckdb_destroy_logical_type, duckdb_destroy_value, duckdb_get_bool,
    duckdb_get_double, duckdb_get_int64, duckdb_get_uint64, duckdb_get_varchar,
    duckdb_logical_type, duckdb_value, from_duckdb_string, to_c_string, DuckDBType,
};
use crate::grid::GridKind;
use crate::raster::mosaic::OverlapRule;

use super::parsing::{parse_bbox_str, parse_resolutions_str_for};

/// Conservative WGS84 bounding box `[min_lon, min_lat, max_lon, max_lat]` for an H3 cell.
/// Accounts for high-latitude longitudinal widening, polar containment, and antimeridian
/// crossing (`min_lon > max_lon`), so `is_point_in_bbox` and `rect_intersects_bbox` never
/// clip valid samples belonging to `cell`.
pub fn cell_wgs84_bbox(cell: h3o::CellIndex) -> [f64; 4] {
    cell_wgs84_bbox_for(GridKind::H3, u64::from(cell)).unwrap_or([-180.0, -90.0, 180.0, 90.0])
}

/// Conservative WGS84 bounding box `[min_lon, min_lat, max_lon, max_lat]` for a cell on `grid`
/// ([`GridKind::wgs84_bbox`]).
pub fn cell_wgs84_bbox_for(grid: GridKind, cell_u64: u64) -> Option<[f64; 4]> {
    grid.wgs84_bbox(cell_u64)
}

/// RAII guard ensuring DuckDB allocated value handle is destroyed
///
/// The handle is private so that only this module, which takes it straight
/// from a DuckDB getter, can construct one; `Drop` relies on that.
pub struct OwnedValue(duckdb_value);

impl Drop for OwnedValue {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: a non-null handle was returned by `duckdb_bind_get_*parameter`,
            // which transfers ownership to us, and this guard is its only owner,
            // so it is destroyed exactly once.
            unsafe {
                duckdb_destroy_value(&mut self.0);
            }
        }
    }
}

/// Ergonomic, safe wrapper around DuckDB's `duckdb_bind_info`
///
/// Invariant: `info` is the live bind handle of the bind callback that
/// created the helper (see [`BindHelper::new`]); every safe method relies on it.
pub struct BindHelper {
    info: duckdb_bind_info,
}

impl BindHelper {
    /// # Safety
    /// `info` must be the `duckdb_bind_info` DuckDB passed to the current bind
    /// callback, and the helper must not be used after that callback returns.
    #[inline(always)]
    pub unsafe fn new(info: duckdb_bind_info) -> Self {
        Self { info }
    }

    /// The wrapped bind handle.
    #[inline(always)]
    pub fn info(&self) -> duckdb_bind_info {
        self.info
    }

    /// Number of positional parameters passed to the function
    #[inline(always)]
    pub fn parameter_count(&self) -> usize {
        // SAFETY: `self.info` is the live bind handle (struct invariant).
        unsafe { duckdb_bind_get_parameter_count(self.info) as usize }
    }

    /// Get a positional parameter value by index wrapped in an RAII `OwnedValue`
    #[inline(always)]
    pub fn get_owned_parameter(&self, index: usize) -> Option<OwnedValue> {
        if index >= self.parameter_count() {
            return None;
        }
        // SAFETY: `self.info` is the live bind handle (struct invariant) and `index` is below the
        // parameter count; ownership of the returned value passes to `OwnedValue`.
        let val = unsafe { duckdb_bind_get_parameter(self.info, index as u64) };
        if val.is_null() {
            None
        } else {
            Some(OwnedValue(val))
        }
    }

    /// Get a positional parameter value by index. The caller owns the returned
    /// handle and must release it with `duckdb_destroy_value`.
    #[inline(always)]
    pub fn get_parameter(&self, index: usize) -> duckdb_value {
        // SAFETY: `self.info` is the live bind handle (struct invariant); DuckDB
        // returns null for an out-of-range index rather than reading past the end.
        unsafe { duckdb_bind_get_parameter(self.info, index as u64) }
    }

    /// Extract a positional VARCHAR parameter as a Rust `String`
    pub fn get_string_param(&self, index: usize) -> Option<String> {
        let val = self.get_owned_parameter(index)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; `duckdb_get_varchar` returns a fresh DuckDB-allocated string
        // (or null) that `from_duckdb_string` copies and frees.
        unsafe { from_duckdb_string(duckdb_get_varchar(val.0)) }
    }

    /// Extract a positional BIGINT parameter as `i64`
    pub fn get_int_param(&self, index: usize) -> Option<i64> {
        let val = self.get_owned_parameter(index)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; the getter only reads it.
        unsafe { Some(duckdb_get_int64(val.0)) }
    }

    /// Extract a positional DOUBLE parameter as `f64`
    pub fn get_double_param(&self, index: usize) -> Option<f64> {
        let val = self.get_owned_parameter(index)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; the getter only reads it.
        unsafe { Some(duckdb_get_double(val.0)) }
    }

    /// Get a named parameter by name wrapped in an RAII `OwnedValue`
    #[inline(always)]
    pub fn get_owned_named_parameter(&self, name: &str) -> Option<OwnedValue> {
        let c_name = to_c_string(name);
        // SAFETY: `self.info` is the live bind handle (struct invariant) and `c_name` is NUL-terminated
        // for the call; ownership of the returned value passes to `OwnedValue`.
        let val = unsafe { duckdb_bind_get_named_parameter(self.info, c_name.as_ptr()) };
        if val.is_null() {
            None
        } else {
            Some(OwnedValue(val))
        }
    }

    /// Get a named parameter by name. The caller owns the returned handle and
    /// must release it with `duckdb_destroy_value`.
    pub fn get_named_parameter(&self, name: &str) -> duckdb_value {
        let c_name = to_c_string(name);
        // SAFETY: `self.info` is the live bind handle (struct invariant) and
        // `c_name` is NUL-terminated for the call.
        unsafe { duckdb_bind_get_named_parameter(self.info, c_name.as_ptr()) }
    }

    /// Extract a named VARCHAR parameter as `String`
    pub fn get_named_string(&self, name: &str) -> Option<String> {
        let val = self.get_owned_named_parameter(name)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; `duckdb_get_varchar` returns a fresh DuckDB-allocated string
        // (or null) that `from_duckdb_string` copies and frees.
        unsafe { from_duckdb_string(duckdb_get_varchar(val.0)) }
    }

    /// Extract a named BIGINT parameter as `i64`
    pub fn get_named_int(&self, name: &str) -> Option<i64> {
        let val = self.get_owned_named_parameter(name)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; the getter only reads it.
        unsafe { Some(duckdb_get_int64(val.0)) }
    }

    /// Extract a named UBIGINT parameter as `u64`
    pub fn get_named_uint(&self, name: &str) -> Option<u64> {
        let val = self.get_owned_named_parameter(name)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; the getter only reads it.
        unsafe { Some(duckdb_get_uint64(val.0)) }
    }

    /// Extract a named DOUBLE parameter as `f64`
    pub fn get_named_double(&self, name: &str) -> Option<f64> {
        let val = self.get_owned_named_parameter(name)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; the getter only reads it.
        unsafe { Some(duckdb_get_double(val.0)) }
    }

    /// Extract a named BOOLEAN parameter as `bool`
    pub fn get_named_bool(&self, name: &str) -> Option<bool> {
        let val = self.get_owned_named_parameter(name)?;
        // SAFETY: `val` owns a live `duckdb_value` until it drops at the end of this function; the getter only reads it.
        unsafe { Some(duckdb_get_bool(val.0)) }
    }

    /// Set an error message on the bind context and halt execution
    pub fn set_error(&self, msg: &str) {
        let c_err = CString::new(msg)
            .unwrap_or_else(|_| CString::new("Error in table function bind").unwrap());
        // SAFETY: `self.info` is the live bind handle (struct invariant); DuckDB copies the
        // NUL-terminated message before `c_err` drops.
        unsafe {
            duckdb_bind_set_error(self.info, c_err.as_ptr());
        }
    }

    /// Add a result column to the output schema with automated logical type lifecycle management
    pub fn add_result_column(&self, name: &str, duckdb_type: DuckDBType) {
        // SAFETY: `self.info` is the live bind handle (struct invariant). DuckDB copies the
        // name and logical type, so the type we created is destroyed once here.
        unsafe {
            let col_name = to_c_string(name);
            let mut logical_type = duckdb_create_logical_type(duckdb_type);
            duckdb_bind_add_result_column(self.info, col_name.as_ptr(), logical_type);
            duckdb_destroy_logical_type(&mut logical_type);
        }
    }

    /// Add a custom logical type column (e.g. GEOMETRY)
    ///
    /// # Safety
    /// `logical_type` must be a live DuckDB logical type for the duration of this call.
    pub unsafe fn add_custom_result_column(&self, name: &str, logical_type: duckdb_logical_type) {
        // SAFETY: `self.info` is the live bind handle (struct invariant), `logical_type` is
        // live for the call (caller contract), and DuckDB copies both arguments.
        unsafe {
            let col_name = to_c_string(name);
            duckdb_bind_add_result_column(self.info, col_name.as_ptr(), logical_type);
        }
    }

    // =========================================================================
    // High-Level Standard Raster Parameter Parsers
    // =========================================================================

    /// Parse `grid := 'h3' | 'a5'`, defaulting to `default_grid`.
    pub fn parse_grid(&self, default_grid: GridKind) -> Option<GridKind> {
        match self.get_named_string("grid") {
            Some(s) => match GridKind::parse(&s) {
                Some(g) => Some(g),
                None => {
                    self.set_error(&format!(
                        "Invalid grid parameter '{}': expected {}",
                        s,
                        GridKind::EXPECTED
                    ));
                    None
                }
            },
            None => Some(default_grid),
        }
    }

    /// Parse target H3 resolutions across all supported conventions:
    /// 1. `resolutions := '7,8'` (comma/whitespace separated list)
    /// 2. `min_resolution := 6, max_resolution := 8` (inclusive integer range)
    /// 3. `resolution := 8` (named integer)
    /// 4. Positional fallback parameter at `positional_idx`
    pub fn parse_resolutions(&self, default_res: u8, positional_idx: Option<usize>) -> Vec<u8> {
        self.parse_resolutions_for(GridKind::H3, default_res, positional_idx)
    }

    /// [`Self::parse_resolutions_for`], also accepting `resolutions := 'auto'` (or
    /// `'auto+1'`, `'auto-2'`, ...): the resolution of `grid` whose cells are closest in
    /// area to the pixels of the raster at `file_path`. Reports a malformed `auto` or an
    /// unreadable raster as a binding error and returns `None`.
    pub fn resolve_resolutions_for(
        &self,
        grid: GridKind,
        default_res: u8,
        positional_idx: Option<usize>,
        file_path: &str,
        source_crs: Option<&str>,
    ) -> Option<Vec<u8>> {
        let auto = self
            .get_named_string("resolutions")
            .and_then(|s| dggs_stream_core::resolution::ResolutionSpec::parse_auto(&s));
        let Some(spec) = auto else {
            return Some(self.parse_resolutions_for(grid, default_res, positional_idx));
        };
        // SAFETY: `self.info` is the live bind handle (struct invariant).
        let factory =
            unsafe { crate::functions::config::raster_source_factory_for_bind(self.info) };
        let resolved = spec.and_then(|spec| {
            let paths = crate::raster::mosaic::resolve_raster_sources(file_path)?;
            spec.resolve_paths_with(grid, &paths, source_crs, factory.as_ref())
        });
        match resolved {
            Ok(resolutions) => Some(resolutions),
            Err(e) => {
                self.set_error(&format!("resolutions: {e}"));
                None
            }
        }
    }

    /// Parse target resolutions for `grid` across all supported conventions.
    pub fn parse_resolutions_for(
        &self,
        grid: GridKind,
        default_res: u8,
        positional_idx: Option<usize>,
    ) -> Vec<u8> {
        let max_res = i64::from(grid.max_resolution());
        // 1. Named parameter: resolutions (VARCHAR, e.g. '7,8' or '7, 8, 9')
        if let Some(s) = self.get_named_string("resolutions") {
            let list = parse_resolutions_str_for(grid, &s);
            if !list.is_empty() {
                return list;
            }
        }

        // 2. Named parameters: min_resolution and max_resolution (BIGINT)
        let min_res_val = self.get_named_int("min_resolution");
        let max_res_val = self.get_named_int("max_resolution");
        if let (Some(min_r), Some(max_r)) = (min_res_val, max_res_val) {
            if (0..=max_res).contains(&min_r) && (0..=max_res).contains(&max_r) && min_r <= max_r {
                return ((min_r as u8)..=(max_r as u8)).collect();
            }
        }

        // 3. Named parameter: resolution (BIGINT)
        if let Some(res_int) = self.get_named_int("resolution") {
            if (0..=max_res).contains(&res_int) {
                return vec![res_int as u8];
            }
        }

        // 4. Positional fallback parameter
        if let Some(pos_idx) = positional_idx {
            if let Some(res_int) = self.get_int_param(pos_idx) {
                if (0..=max_res).contains(&res_int) {
                    return vec![res_int as u8];
                }
            }
        }

        vec![default_res]
    }

    /// Parse spatial bounding box from coordinates or H3/A5 cell:
    /// 1. `min_lon, min_lat, max_lon, max_lat` (4 explicit coordinates)
    /// 2. `bbox := 'min_lon,min_lat,max_lon,max_lat'` (list/string)
    /// 3. `h3_cell` (BIGINT), `a5_cell` / `s2_cell` / `healpix_cell` (UBIGINT)
    /// 4. `h3_hex` / `a5_hex` / `s2_hex` / `healpix_hex` (VARCHAR)
    pub fn parse_bbox(&self) -> Option<[f64; 4]> {
        // 1. Explicit min_lon, min_lat, max_lon, max_lat
        let min_lon = self.get_named_double("min_lon");
        let min_lat = self.get_named_double("min_lat");
        let max_lon = self.get_named_double("max_lon");
        let max_lat = self.get_named_double("max_lat");

        if let (Some(min_x), Some(min_y), Some(max_x), Some(max_y)) =
            (min_lon, min_lat, max_lon, max_lat)
        {
            return Some([min_x, min_y, max_x, max_y]);
        }

        // 2. Comma-separated bbox string
        if let Some(s) = self.get_named_string("bbox") {
            if let Some(bbox) = parse_bbox_str(&s) {
                return Some(bbox);
            }
        }

        // 3. H3 cell index (u64 / i64)
        if let Some(cell_u64) = self
            .get_named_uint("h3_cell")
            .or_else(|| self.get_named_int("h3_cell").map(|v| v as u64))
        {
            if let Ok(cell) = h3o::CellIndex::try_from(cell_u64) {
                return Some(cell_wgs84_bbox(cell));
            }
        }

        // 4. H3 hex string
        if let Some(hex_str) = self.get_named_string("h3_hex") {
            if let Ok(cell) = hex_str.trim().parse::<h3o::CellIndex>() {
                return Some(cell_wgs84_bbox(cell));
            }
        }

        // 5. A5, S2 or HEALPix cell index (`a5_cell`, ...) or hex string (`a5_hex`, ...)
        for grid in [GridKind::A5, GridKind::S2, GridKind::Healpix] {
            let cell = self.get_named_uint(&format!("{grid}_cell")).or_else(|| {
                self.get_named_string(grid.hex_column_name())
                    .and_then(|hex| crate::encoding::parse_hex_u64(hex.trim()))
            });
            if let Some(bbox) = cell.and_then(|cell| cell_wgs84_bbox_for(grid, cell)) {
                return Some(bbox);
            }
        }

        None
    }

    /// Parse the sampling preset; report unknown names as binding errors.
    pub fn parse_sampling(&self) -> Option<SamplingPattern> {
        match self.get_named_string("sampling") {
            Some(s) => match SamplingPattern::parse(&s) {
                Ok(pattern) => Some(pattern),
                Err(error) => {
                    self.set_error(&error.to_string());
                    None
                }
            },
            None => Some(SamplingPattern::default()),
        }
    }

    /// Parse mosaic overlap rule (`cutline`, `replace`, `highest_resolution`)
    pub fn parse_overlap_rule(&self) -> OverlapRule {
        if let Some(s) = self.get_named_string("overlap_rule") {
            OverlapRule::parse(&s)
        } else {
            OverlapRule::default()
        }
    }

    /// Parse source coordinate reference system override (`source_crs` or `crs`)
    pub fn parse_source_crs(&self) -> Option<String> {
        self.get_named_string("source_crs")
            .or_else(|| self.get_named_string("crs"))
    }

    /// `scale_offset` (default true): convert stored values to physical
    /// values with each band's GDAL `SCALE`/`OFFSET` metadata.
    pub fn parse_scale_offset(&self) -> bool {
        self.get_named_bool("scale_offset").unwrap_or(true)
    }

    /// Parse the output options: `nodata_policy` (`'omit'` or `'emit'`),
    /// `emit_nodata_value` and `decimals`. Invalid values are binding errors.
    pub fn parse_output_policy(&self) -> Option<OutputPolicy> {
        let mut policy = OutputPolicy::default();
        if let Some(name) = self.get_named_string("nodata_policy") {
            match NodataPolicy::parse(&name) {
                Some(nodata) => policy.nodata = nodata,
                None => {
                    self.set_error(&format!(
                        "nodata_policy must be {}, got '{name}'",
                        NodataPolicy::EXPECTED
                    ));
                    return None;
                }
            }
        }
        if let Some(value) = self.get_named_double("emit_nodata_value") {
            if !value.is_finite() {
                self.set_error("emit_nodata_value must be finite; leave it unset for NULL");
                return None;
            }
            if !policy.emits_nodata() {
                self.set_error("emit_nodata_value needs nodata_policy := 'emit'");
                return None;
            }
            policy.nodata_value = Some(value);
        }
        if let Some(decimals) = self.get_named_int("decimals") {
            let max = i64::from(dggs_stream_core::aggregator::output_policy::MAX_DECIMALS);
            if !(0..=max).contains(&decimals) {
                self.set_error(&format!(
                    "decimals must be between 0 and {max}, got {decimals}"
                ));
                return None;
            }
            policy.decimals = Some(decimals as u8);
        }
        Some(policy)
    }

    /// Parse execution limits: `workers`/`threads` (aggregation),
    /// `decode_workers`, `fetch_workers`, `decode_budget_mb` and
    /// `compaction_budget_mb`, plus `strict_transforms`.
    /// Non-positive values are errors.
    pub fn parse_execution(&self) -> Option<ExecutionParams> {
        let positive = |names: &[&str]| -> Result<Option<usize>, ()> {
            for name in names {
                if let Some(v) = self.get_named_int(name) {
                    if v <= 0 {
                        self.set_error(&format!("{} must be a positive integer", name));
                        return Err(());
                    }
                    return Ok(Some(v as usize));
                }
            }
            Ok(None)
        };
        let output = self.parse_output_policy()?;
        let transfer = match TransferMode::from_options(
            self.get_named_string("transfer").as_deref(),
            self.get_named_string("interpolation").as_deref(),
            self.get_named_bool("overlay_weighted"),
            self.get_named_bool("mass_preserve"),
        ) {
            Ok(transfer) => transfer,
            Err(e) => {
                self.set_error(&e.to_string());
                return None;
            }
        };
        let mut execution = ExecutionParams {
            output,
            transfer,
            profile: self.get_named_bool("profile").unwrap_or(false),
            aggregation_workers: positive(&["workers", "threads"]).ok()?,
            decode_workers: positive(&["decode_workers"]).ok()?,
            fetch_workers: positive(&["fetch_workers"]).ok()?,
            decode_budget_mb: positive(&["decode_budget_mb"]).ok()?,
            compaction_budget_mb: positive(&["compaction_budget_mb"]).ok()?,
            strict_transforms: self.get_named_bool("strict_transforms").unwrap_or(false),
        };
        // Fill unset values from `SET raster_h3_*` session options (v1.5.6
        // build) or `DGGS_STREAM_*` environment fallbacks.
        // SAFETY: `self.info` is the live bind handle (struct invariant).
        unsafe { crate::functions::config::apply_duckdb_config_options(self.info, &mut execution) };
        Some(execution)
    }

    /// Parse common raster parameters shared across continuous and categorical aggregations
    pub fn parse_common_raster_params(&self, func_name: &str) -> Option<CommonRasterParams> {
        self.parse_common_raster_params_for(func_name, GridKind::H3)
    }

    /// Parse common raster parameters shared across continuous and categorical aggregations
    /// using `default_grid` when `grid` is not explicitly specified.
    pub fn parse_common_raster_params_for(
        &self,
        func_name: &str,
        default_grid: GridKind,
    ) -> Option<CommonRasterParams> {
        if self.parameter_count() < 1 {
            self.set_error(&format!(
                "{} requires at least 1 argument: file_path",
                func_name
            ));
            return None;
        }

        let file_path = match self.get_string_param(0) {
            Some(p) => p,
            None => {
                self.set_error("Invalid file_path parameter");
                return None;
            }
        };

        let grid = self.parse_grid(default_grid)?;
        let source_crs = self.parse_source_crs();
        let resolutions =
            self.resolve_resolutions_for(grid, 8, Some(1), &file_path, source_crs.as_deref())?;
        let nodata = self.get_named_double("nodata");
        let scale_offset = self.parse_scale_offset();
        let chunk_size = self
            .get_named_int("chunk_size")
            .filter(|&cs| cs > 0)
            .unwrap_or(512) as u32;
        let band = self.get_named_int("band").filter(|&b| b > 0).unwrap_or(1) as u32;
        let sampling = self.parse_sampling()?;
        let bbox = self.parse_bbox();
        let compact = self.get_named_bool("compact").unwrap_or(false);
        let overlap_rule = self.parse_overlap_rule();
        let emit_geom = self
            .get_named_bool("geom")
            .unwrap_or_else(crate::ffi::is_geometry_available);
        let execution = self.parse_execution()?;

        let resolved_paths = match crate::raster::mosaic::resolve_raster_sources(&file_path) {
            Ok(paths) => paths,
            Err(e) => {
                self.set_error(&format!("Failed to resolve raster source(s): {}", e));
                return None;
            }
        };

        Some(CommonRasterParams {
            file_path,
            resolved_paths,
            grid,
            resolutions,
            source_crs,
            nodata,
            scale_offset,
            chunk_size,
            band,
            sampling,
            bbox,
            compact,
            overlap_rule,
            emit_geom,
            execution,
            // SAFETY: `self.info` is the live bind handle (struct invariant).
            remote_source_factory: unsafe {
                crate::functions::config::raster_source_factory_for_bind(self.info)
            },
        })
    }
}

// =============================================================================
// Common Raster Binding Parameters Struct
// =============================================================================

/// Standard parameters common to all raster aggregation table functions
#[derive(Debug, Clone)]
pub struct CommonRasterParams {
    pub file_path: String,
    pub resolved_paths: Vec<PathBuf>,
    pub grid: GridKind,
    pub resolutions: Vec<u8>,
    pub source_crs: Option<String>,
    pub nodata: Option<f64>,
    /// `scale_offset`: calibrate stored values with GDAL band scale/offset.
    pub scale_offset: bool,
    pub chunk_size: u32,
    pub band: u32,
    pub sampling: SamplingPattern,
    pub bbox: Option<[f64; 4]>,
    pub compact: bool,
    pub overlap_rule: OverlapRule,
    pub emit_geom: bool,
    pub execution: ExecutionParams,
    /// Routes raster byte-range reads through the DuckDB file system when
    /// `dggs_stream_use_duckdb_fs` is set (v1.5.6 build only).
    pub remote_source_factory: Option<dggs_stream_core::raster::http_range::RemoteSourceFactory>,
}

/// SQL execution limits and options. Each `None` keeps the streamer default.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ExecutionParams {
    /// `nodata_policy`, `emit_nodata_value` and `decimals`: what all-NoData cells and
    /// output precision look like.
    pub output: OutputPolicy,
    /// `transfer`, `interpolation`, `overlay_weighted` and `mass_preserve`: how pixels
    /// map onto cells.
    pub transfer: TransferMode,
    /// `profile`: print where the scan's time went on stderr when it ends, and keep the
    /// report as `dggs_stream_core::run_profile::last_report()`.
    pub profile: bool,
    /// `workers` / `threads`: dedicated aggregation pool size.
    pub aggregation_workers: Option<usize>,
    /// `decode_workers`: chunk decoding threads.
    pub decode_workers: Option<usize>,
    /// `fetch_workers`: concurrent remote range requests.
    pub fetch_workers: Option<usize>,
    /// `decode_budget_mb`: decoded chunk memory budget in MiB.
    pub decode_budget_mb: Option<usize>,
    /// `compaction_budget_mb`: opt-in limit on pending compaction groups in MiB.
    pub compaction_budget_mb: Option<usize>,
    /// `strict_transforms`: fail the query when a valid sample point cannot
    /// be transformed to WGS84 instead of silently skipping it.
    pub strict_transforms: bool,
}

impl ExecutionParams {
    /// Apply the requested limits to a streamer configuration.
    pub fn apply(&self, config: &mut crate::aggregator::multi_horizon::MultiResolutionConfig) {
        if self.aggregation_workers.is_some() {
            config.aggregation_workers = self.aggregation_workers;
        }
        if let Some(n) = self.decode_workers {
            config.decode_workers = n;
        }
        if let Some(n) = self.fetch_workers {
            config.fetch_workers = n;
        }
        if let Some(mb) = self.decode_budget_mb {
            config.decode_budget_bytes = mb.saturating_mul(1024 * 1024);
        }
        if let Some(mb) = self.compaction_budget_mb {
            config.compaction_budget_bytes = Some(mb.saturating_mul(1024 * 1024));
        }
        config.strict_transforms = self.strict_transforms;
        config.nodata_policy = self.output.nodata;
        config.emit_nodata_value = self.output.nodata_value;
        config.decimals = self.output.decimals;
        config.transfer = self.transfer;
        if self.profile {
            config.profile = Some(dggs_stream_core::run_profile::RunProfile::announcing());
        }
    }
}

pub type CommonRasterBindParams = CommonRasterParams;

impl CommonRasterParams {
    /// Extract all common raster parameters from bind context
    pub fn extract(bind: &BindHelper, func_name: &str, default_compact: bool) -> Option<Self> {
        let mut params = bind.parse_common_raster_params(func_name)?;
        if bind.get_named_bool("compact").is_none() {
            params.compact = default_compact;
        }
        Some(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owned_value_null_safety() {
        let val = OwnedValue(std::ptr::null_mut());
        // Dropping null value must not panic or segfault
        drop(val);
    }

    #[test]
    fn test_execution_params_override_streamer_defaults() {
        use crate::aggregator::multi_horizon::MultiResolutionConfig;
        let mut config = MultiResolutionConfig::new(vec![8]);
        let defaults = (
            config.aggregation_workers,
            config.decode_workers,
            config.fetch_workers,
        );
        ExecutionParams::default().apply(&mut config);
        assert_eq!(
            (
                config.aggregation_workers,
                config.decode_workers,
                config.fetch_workers
            ),
            defaults
        );
        ExecutionParams {
            aggregation_workers: Some(3),
            decode_workers: Some(2),
            fetch_workers: Some(5),
            decode_budget_mb: Some(32),
            compaction_budget_mb: Some(8),
            strict_transforms: true,
            ..Default::default()
        }
        .apply(&mut config);
        assert_eq!(config.aggregation_workers, Some(3));
        assert_eq!(config.decode_workers, 2);
        assert_eq!(config.fetch_workers, 5);
        assert_eq!(config.decode_budget_bytes, 32 * 1024 * 1024);
        assert_eq!(config.compaction_budget_bytes, Some(8 * 1024 * 1024));
        assert!(config.strict_transforms);
    }
}
