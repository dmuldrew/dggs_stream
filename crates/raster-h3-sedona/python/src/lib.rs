//! SedonaDB Python bindings for `raster_h3`.
//!
//! `raster_h3_sedona.RasterH3Table` wraps [`raster_h3_sedona::RasterH3Table`] and
//! implements SedonaDB's `__sedonadb_table_provider__` protocol: it returns a
//! `sedonadb_table_provider` capsule holding a `SedonaCTableProvider`
//! ([`ExportedTableProvider`]), so any SedonaDB context can scan it with projection,
//! `LIMIT`, threshold and spatial filter pushdown.
//!
//! `raster_h3_sedona.scalar_functions()` exports the `raster_h3` SQL functions (including
//! `h3_polygon_to_cells` and `h3_to_wkb`) as `sedonadb_scalar_kernel` capsules.
//!
//! `raster_h3_sedona.write_pmtiles(data, path)` writes the rows of anything exposing
//! `__arrow_c_stream__` (a SedonaDB DataFrame, a PyArrow table, a Polars DataFrame) as H3
//! hexagon features in a PMTiles v3 archive. SedonaDB's extension ABI cannot register file
//! formats, so this is the Python counterpart of `COPY ... TO 'cells.pmtiles'`.
//!
//! ```python
//! import sedonadb
//! import raster_h3_sedona
//! from raster_h3_sedona import RasterH3Table
//!
//! sd = sedonadb.connect()
//! sd.register(raster_h3_sedona.scalar_functions())
//! sd.create_data_frame(RasterH3Table.continuous("dem.tif", 8)).to_view("dem")
//! sd.sql("SELECT h3_index, mean FROM dem WHERE count >= 4").show()
//! ```

mod functions;

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::RecordBatchReader;
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use raster_h3_sedona::scan::{CategoricalRasterTableProvider, ContinuousRasterTableProvider};
use raster_h3_sedona::{GridKind, NodataPolicy, OutputPolicy, ResolutionSpec};
use sedona_extension::extension::SedonaCTableProvider;
use sedona_extension::runtime::RuntimeHandle;
use sedona_extension::table_provider::ExportedTableProvider;

/// The runtime exported providers plan and execute on, shared by every table.
fn runtime() -> PyResult<Arc<RuntimeHandle>> {
    static RUNTIME: OnceLock<Arc<RuntimeHandle>> = OnceLock::new();
    if let Some(runtime) = RUNTIME.get() {
        return Ok(Arc::clone(runtime));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("raster-h3-sedona")
        .enable_all()
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("cannot start the tokio runtime: {e}")))?;
    Ok(Arc::clone(
        RUNTIME.get_or_init(|| Arc::new(RuntimeHandle::new(runtime))),
    ))
}

/// One H3 resolution or a list of them.
#[derive(FromPyObject)]
enum Resolutions {
    One(u8),
    Many(Vec<u8>),
    /// `"auto"`, `"auto+1"`, `"auto-2"`, ...
    Text(String),
}

impl Resolutions {
    fn into_spec(self, grid: GridKind) -> PyResult<ResolutionSpec> {
        let resolutions = match self {
            Self::One(r) => vec![r],
            Self::Many(rs) => rs,
            Self::Text(text) => return ResolutionSpec::parse(grid, &text).map_err(value_error),
        };
        let finest = grid.max_resolution();
        match resolutions.iter().find(|&&r| r > finest) {
            Some(r) => Err(PyValueError::new_err(format!(
                "{} resolution must be between 0 and {finest}, got {r}",
                grid.label()
            ))),
            None => Ok(ResolutionSpec::Fixed(resolutions)),
        }
    }
}

fn resolutions(resolutions: Option<Resolutions>, grid: GridKind) -> PyResult<ResolutionSpec> {
    resolutions.map_or(Ok(ResolutionSpec::Fixed(Vec::new())), |r| r.into_spec(grid))
}

/// A grid name (`h3`, `a5`, `s2`, `healpix`, case-insensitive); `None` is H3.
fn grid_of(name: Option<&str>) -> PyResult<GridKind> {
    name.map_or(Ok(GridKind::H3), |name| {
        GridKind::parse(name).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown grid {name:?}: expected {}",
                GridKind::EXPECTED
            ))
        })
    })
}

/// Filters reach these tables only through SedonaDB's extension ABI, which strips the
/// `geoarrow.wkb` tag from geometry literals after SedonaDB has checked their CRS.
/// `nodata_policy`, `emit_nodata_value` and `decimals` as an [`OutputPolicy`].
fn output_policy(
    nodata_policy: Option<&str>,
    emit_nodata_value: Option<f64>,
    decimals: Option<u8>,
) -> PyResult<OutputPolicy> {
    let mut policy = OutputPolicy::default();
    if let Some(name) = nodata_policy {
        policy.nodata = NodataPolicy::parse(name).ok_or_else(|| {
            PyValueError::new_err(format!(
                "nodata_policy must be {}, got {name:?}",
                NodataPolicy::EXPECTED
            ))
        })?;
    }
    if let Some(value) = emit_nodata_value {
        if !value.is_finite() || !policy.emits_nodata() {
            return Err(PyValueError::new_err(
                "emit_nodata_value must be finite and needs nodata_policy='emit'",
            ));
        }
        policy.nodata_value = Some(value);
    }
    if decimals.is_some_and(|d| d > 15) {
        return Err(PyValueError::new_err("decimals must be between 0 and 15"));
    }
    policy.decimals = decimals;
    Ok(policy)
}

fn sedona_table(table: raster_h3_sedona::RasterH3Table) -> raster_h3_sedona::RasterH3Table {
    table.with_untagged_wkb_as_lnglat(true)
}

fn value_error(err: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(err.to_string())
}

/// A raster aggregated to H3 cells, scannable by SedonaDB.
///
/// Build one with `RasterH3Table.continuous(...)` or `RasterH3Table.categorical(...)` and
/// pass it to `SedonaContext.create_data_frame`. The table is re-scanned on every query.
#[pyclass(name = "RasterH3Table", module = "raster_h3_sedona", frozen)]
struct PyRasterH3Table {
    inner: Arc<raster_h3_sedona::RasterH3Table>,
    description: String,
}

#[pymethods]
impl PyRasterH3Table {
    /// Continuous statistics (`mean`, `stddev`, `count`, `min`, `max`, `sum`) per cell.
    ///
    /// `source` is a path, glob or URL; `resolutions` one H3 resolution or a list
    /// (default 8). `crs` overrides the raster's CRS, `nodata` its NoData value, `band`
    /// selects a 1-based band, and `bbox` (`[min_lon, min_lat, max_lon, max_lat]`) clips
    /// the samples aggregated.
    #[staticmethod]
    #[pyo3(signature = (source, resolutions=None, *, crs=None, nodata=None, band=None, bbox=None, grid=None, nodata_policy=None, emit_nodata_value=None, decimals=None))]
    #[allow(clippy::too_many_arguments)]
    fn continuous(
        source: &str,
        resolutions: Option<Resolutions>,
        crs: Option<String>,
        nodata: Option<f64>,
        band: Option<usize>,
        bbox: Option<[f64; 4]>,
        grid: Option<&str>,
        nodata_policy: Option<&str>,
        emit_nodata_value: Option<f64>,
        decimals: Option<u8>,
    ) -> PyResult<Self> {
        let grid = grid_of(grid)?;
        let output = output_policy(nodata_policy, emit_nodata_value, decimals)?;
        let mut provider = ContinuousRasterTableProvider::try_new_with(
            source,
            grid,
            &self::resolutions(resolutions, grid)?,
            crs.as_deref(),
        )
        .map_err(value_error)?
        .with_output_policy(output);
        if let Some(nodata) = nodata {
            provider = provider.with_nodata(nodata);
        }
        if let Some(band) = band {
            provider = provider.with_band(band);
        }
        if let Some(bbox) = bbox {
            provider = provider.with_bbox(bbox);
        }
        Ok(Self {
            inner: Arc::new(sedona_table(raster_h3_sedona::RasterH3Table::continuous(
                provider,
            ))),
            description: match grid {
                GridKind::H3 => format!("RasterH3Table.continuous({source:?})"),
                grid => format!(
                    "RasterH3Table.continuous({source:?}, grid={:?})",
                    grid.as_str()
                ),
            },
        })
    }

    /// Categorical statistics (`majority_class`, `majority_fraction`, `histogram`, ...) per
    /// cell, in wide format. Arguments as for `continuous`.
    #[staticmethod]
    #[pyo3(signature = (source, resolutions=None, *, crs=None, nodata=None, bbox=None, grid=None, nodata_policy=None, emit_nodata_value=None, decimals=None))]
    #[allow(clippy::too_many_arguments)]
    fn categorical(
        source: &str,
        resolutions: Option<Resolutions>,
        crs: Option<String>,
        nodata: Option<f64>,
        bbox: Option<[f64; 4]>,
        grid: Option<&str>,
        nodata_policy: Option<&str>,
        emit_nodata_value: Option<f64>,
        decimals: Option<u8>,
    ) -> PyResult<Self> {
        let grid = grid_of(grid)?;
        let output = output_policy(nodata_policy, emit_nodata_value, decimals)?;
        let mut provider = CategoricalRasterTableProvider::try_new_with(
            source,
            grid,
            &self::resolutions(resolutions, grid)?,
            crs.as_deref(),
        )
        .map_err(value_error)?
        .with_output_policy(output);
        if let Some(nodata) = nodata {
            provider = provider.with_nodata(nodata);
        }
        if let Some(bbox) = bbox {
            provider = provider.with_bbox(bbox);
        }
        Ok(Self {
            inner: Arc::new(sedona_table(raster_h3_sedona::RasterH3Table::categorical(
                provider,
            ))),
            description: match grid {
                GridKind::H3 => format!("RasterH3Table.categorical({source:?})"),
                grid => format!(
                    "RasterH3Table.categorical({source:?}, grid={:?})",
                    grid.as_str()
                ),
            },
        })
    }

    /// Export the table as a `sedonadb_table_provider` capsule. SedonaDB takes ownership
    /// of the provider inside; each call exports a fresh one over the same table.
    fn __sedonadb_table_provider__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        // The scan ignores the session; it only has to satisfy the FFI wrapper.
        let session = Arc::new(SessionContext::new().state());
        let exported = ExportedTableProvider::new(
            Arc::clone(&self.inner) as Arc<dyn datafusion::catalog::TableProvider>,
            session,
            runtime()?,
        );
        let provider: SedonaCTableProvider = exported.into();
        PyCapsule::new_with_value(py, provider, c"sedonadb_table_provider")
    }

    fn __repr__(&self) -> String {
        self.description.clone()
    }
}

/// Largest accepted `memory_budget_mb` (1 TiB), as for `COPY ... OPTIONS`.
const MAX_MEMORY_BUDGET_MB: usize = 1 << 20;

fn write_error(err: DataFusionError) -> PyErr {
    match err {
        DataFusionError::IoError(e) => PyOSError::new_err(e.to_string()),
        DataFusionError::External(e) if e.is::<std::io::Error>() => {
            PyOSError::new_err(e.to_string())
        }
        other => PyValueError::new_err(other.to_string()),
    }
}

/// Write rows as cell features (H3, A5, S2 or HEALPix) to a PMTiles v3 archive at `path`;
/// returns the number of features written.
///
/// `data` is anything exposing `__arrow_c_stream__`: a SedonaDB DataFrame, a PyArrow table
/// or a Polars DataFrame. The cell column is `cell_column` (or its earlier name,
/// `h3_column`), else the first of `{h3,a5,s2,healpix}_{index,hex}`, a bare grid name,
/// `cell`, `hex`, else the first column; the grid is `grid` if given, else the one the
/// column is named for, else H3. The column it may be unsigned or signed 64-bit
/// or a hex string. Rows where it is null are skipped and any other invalid value raises.
/// Every other column becomes a feature property (binary columns such as `wkb` are left
/// out). `min_zoom` / `max_zoom` keep only cells whose PMTiles zoom is in range, and
/// `memory_budget_mb` (default 256) bounds the rows held in memory before sorting spills to
/// temporary files. The archive is written atomically.
#[pyfunction]
#[pyo3(signature = (data, path, *, h3_column=None, min_zoom=None, max_zoom=None, memory_budget_mb=None, cell_column=None, grid=None))]
#[allow(clippy::too_many_arguments)]
fn write_pmtiles(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    path: PathBuf,
    h3_column: Option<String>,
    min_zoom: Option<u8>,
    max_zoom: Option<u8>,
    memory_budget_mb: Option<usize>,
    cell_column: Option<String>,
    grid: Option<&str>,
) -> PyResult<usize> {
    // `cell_column` is the grid-neutral name of `h3_column`.
    let h3_column = match (cell_column, h3_column) {
        (Some(a), Some(b)) if a != b => {
            return Err(PyValueError::new_err(
                "pass cell_column or h3_column, not both",
            ))
        }
        (a, b) => a.or(b),
    };
    let mut options = raster_h3_sedona::PmtilesWriteOptions {
        h3_column,
        grid: grid.map(|g| grid_of(Some(g))).transpose()?,
        min_zoom,
        max_zoom,
        ..Default::default()
    };
    if let Some(mb) = memory_budget_mb {
        if !(1..=MAX_MEMORY_BUDGET_MB).contains(&mb) {
            return Err(PyValueError::new_err(format!(
                "memory_budget_mb must be between 1 and {MAX_MEMORY_BUDGET_MB}, got {mb}"
            )));
        }
        options.memory_budget_bytes = mb << 20;
    }
    if !data.hasattr("__arrow_c_stream__")? {
        return Err(PyTypeError::new_err(
            "data must expose __arrow_c_stream__ (a SedonaDB DataFrame, PyArrow table, \
             Polars DataFrame, ...)",
        ));
    }
    let capsule = data.call_method0("__arrow_c_stream__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let stream_ptr = capsule
        .pointer_checked(Some(c"arrow_array_stream"))?
        .as_ptr()
        .cast::<FFI_ArrowArrayStream>();
    // SAFETY: an `arrow_array_stream` capsule holds a valid `ArrowArrayStream`; `from_raw`
    // moves it out and marks the capsule's copy released, so it is released exactly once.
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(stream_ptr) };
    let reader = ArrowArrayStreamReader::try_new(stream).map_err(value_error)?;
    let schema = reader.schema();
    let summary = py
        .detach(move || {
            raster_h3_sedona::write_record_batches(
                &schema,
                reader.map(|b| b.map_err(DataFusionError::from)),
                &path,
                options,
            )
        })
        .map_err(write_error)?;
    Ok(summary.valid_features)
}

#[pymodule(name = "raster_h3_sedona")]
fn raster_h3_sedona_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRasterH3Table>()?;
    m.add_class::<functions::PyScalarFunction>()?;
    m.add_class::<functions::PyScalarFunctions>()?;
    m.add_function(wrap_pyfunction!(functions::scalar_functions, m)?)?;
    m.add_function(wrap_pyfunction!(write_pmtiles, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
