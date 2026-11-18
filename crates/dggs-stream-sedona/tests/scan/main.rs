//! Integration tests for the `scan` module (formerly the `dggs-stream-datafusion` crate),
//! linked into a single test binary.

#[path = "../../../dggs-stream-core/tests/it/helpers.rs"]
mod helpers;

mod bbox_pushdown;
mod categorical_options;
mod compaction_pushdown;
mod continuous_options;
mod partitions;
mod rows;
mod scalar_udfs;
mod statistics;
mod transform_failures;
