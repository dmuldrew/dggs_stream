//! # `dggs_stream`
//!
//! Unified workspace facade re-exporting:
//! - [`dggs_stream_core`] (`core`): Pure-Rust GeoTIFF/COG reader, CRS reprojection, H3 scanline aggregator,
//!   Arrow C Data/Stream interface (`ArrowArrayStream`), Parquet/GeoParquet, and PMTiles v3 export.
//! - [`dggs_stream_cli`] (`cli`): Command-line streaming (`ipc`, `jsonl`, `parquet`, `pmtiles`).
//! - [`dggs_stream_duckdb`] (`duckdb`): DuckDB C-API loadable extension (`LOAD 'dggs_stream'`).
//!
//! Behind optional features (off by default, both enabled by `full`):
//! - `dggs_stream_python` (`python`, feature `python`): Python / Polars IO plugin and Arrow PyCapsule
//!   (`__arrow_c_stream__`) bindings.
//! - `dggs_stream_pg` (`pg`, feature `pg`): PostgreSQL V1 extension (`Set-Returning Functions` and H3
//!   scalar functions).
//!
//! These are opt-in because this crate's `cdylib` is packaged as the DuckDB extension: linking the
//! Python or PostgreSQL adapters would make it export their C symbols as well.

pub use dggs_stream_cli as cli;
pub use dggs_stream_core as core;
pub use dggs_stream_duckdb as duckdb;
#[cfg(feature = "pg")]
pub use dggs_stream_pg as pg;
#[cfg(feature = "python")]
pub use dggs_stream_python as python;

pub use dggs_stream_duckdb::*;

/// Ensure the linker retains DuckDB C-API extension entry points in `libdggs_stream.{so,dylib,dll}`.
#[doc(hidden)]
pub fn _keep_duckdb_c_api_symbols() -> [usize; 6] {
    [
        dggs_stream_init as *const () as usize,
        dggs_stream_init_c_api as *const () as usize,
        dggs_stream_version as *const () as usize,
        // Former-name aliases (see `dggs_stream_duckdb::raster_h3_init_c_api`).
        raster_h3_init as *const () as usize,
        raster_h3_init_c_api as *const () as usize,
        raster_h3_version as *const () as usize,
    ]
}
