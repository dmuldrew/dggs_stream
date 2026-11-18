//! dggs-stream-python integration tests, linked into a single test binary.

#[path = "../../../dggs-stream-core/tests/it/helpers.rs"]
mod helpers;

mod batch_reader;
mod c_abi;
mod capsules;
mod exports;
mod grids;
mod partitions;
mod scalars;
mod transform_failures;
