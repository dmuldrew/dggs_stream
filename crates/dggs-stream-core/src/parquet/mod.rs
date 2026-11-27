//! Native Parquet and OGC GeoParquet 1.1 streaming export module.

pub mod column_order;
pub mod geoparquet_metadata;
mod multiband;
mod parent;
pub use multiband::{
    write_multiband_parquet, write_multiband_parquet_with_config, MULTIBAND_ROW_GROUP_BUDGET_BYTES,
};
pub use parent::{
    write_parent_multiband, write_parent_multiband_with_config, write_parent_records,
};
pub mod pipeline;
pub mod writer;

pub use geoparquet_metadata::build_geoparquet_metadata;
pub use pipeline::{
    run_parquet_streaming_pipeline, run_parquet_streaming_pipeline_with_progress,
    ParquetRowGroupBuffer, ParquetStreamer, RecordStreamer,
};
pub use writer::{
    CategoricalRowGroupBuffer, ContinuousRowGroupBuffer, ParquetExportConfig, ParquetWriter,
};
