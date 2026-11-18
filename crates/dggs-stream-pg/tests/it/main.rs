//! dggs-stream-pg integration tests, linked into a single test binary.
//!
//! The `fmgr` suite needs `--features pg-extension` (and `DGGS_STREAM_PG_MAJOR` or
//! `pg_config`); it links against the in-process server stand-ins in `fmgr/server.rs`.

#[path = "../../../dggs-stream-core/tests/it/helpers.rs"]
mod helpers;

#[cfg(feature = "pg-extension")]
mod fmgr;
mod grids;
mod srf;
