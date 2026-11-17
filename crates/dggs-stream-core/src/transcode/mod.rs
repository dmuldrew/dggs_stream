//! Cross-format dataset transcoding pipelines.
//!
//! Provides direct format-to-format conversion (such as H3-indexed Parquet to PMTiles v3 archives)
//! with streaming horizon eviction to maintain a strictly bounded memory footprint.

pub mod parquet_tiler;

pub use parquet_tiler::{
    process_parquet_to_pmtiles, process_parquet_to_pmtiles_for, scan_row_group_h3_extent,
    RowGroupExtent,
};
