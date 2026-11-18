//! Cross-engine and packaging tests that need the root `dggs_stream` cdylib or bins.

#[path = "../../crates/dggs-stream-core/tests/it/helpers.rs"]
mod helpers;

// Linux only (uses `nm`); with the `python` or `pg` feature the cdylib exports their symbols by design.
#[cfg(all(target_os = "linux", not(any(feature = "python", feature = "pg"))))]
mod cdylib_exports;
mod duckdb_sql_smoke;
mod duckdb_trusted_repository;
mod multi_engine_e2e;
mod package_extension;
mod pg_extension_e2e;
