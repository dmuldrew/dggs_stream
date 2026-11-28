//! Multi-Resolution Scanline Horizon Streamer for Categorical Data
//!
//! Provides single-pass streaming aggregation across multiple H3 resolution levels simultaneously
//! for discrete integer / land cover categories and classification maps.

use crate::aggregator::cell_hash::CellIdBuildHasher;
use std::collections::HashMap;
use std::sync::Arc;
use tiff::decoder::DecodingResult;

use crate::aggregator::categorical::{CategoricalAccumulator, OVERFLOW_BYTES_PER_CLASS};
use crate::aggregator::remap::CategoryRemapper;

use super::categorical::{process_categorical_chunk_payload_into, MultiCategoricalRecord};
use super::config::MultiResolutionConfig;
use super::controller::{HorizonStreamKernel, MultiHorizonStreamer};
use super::walker::WalkContext;

pub use super::sharded_map::{get_shard as pub_get_shard, NUM_SHARDS as PUB_NUM_SHARDS};

/// Kernel implementation for categorical/classified raster aggregation
#[derive(Clone)]
pub struct CategoricalKernel {
    pub band: usize,
    pub min_count: Option<f64>,
    pub min_majority_fraction: Option<f64>,
    pub remapper: Option<Arc<CategoryRemapper>>,
    pub histogram: Option<Arc<crate::histogram::HistogramSpec>>,
}

impl CategoricalKernel {
    /// The kernel a stream built from `config` uses (filters, band and options).
    pub fn from_config(config: &MultiResolutionConfig) -> Self {
        CategoricalKernel {
            band: config.band,
            min_count: config.min_count,
            min_majority_fraction: config.min_majority_fraction,
            remapper: config.remapper.clone(),
            histogram: config.histogram.clone().map(Arc::new),
        }
    }
}

impl HorizonStreamKernel for CategoricalKernel {
    type Accumulator = CategoricalAccumulator;
    type Record = MultiCategoricalRecord;

    #[inline(always)]
    fn new_parent_accumulator(&self) -> Self::Accumulator {
        CategoricalAccumulator::default()
    }

    /// A sample adds at most one map entry (the inline slot cost) and at most
    /// one class to one cell, whose heap storage stays within
    /// [`OVERFLOW_BYTES_PER_CLASS`] per class: the classes of a worker cell
    /// never outnumber its samples.
    fn worker_bytes_per_sample(&self) -> usize {
        super::controller::inline_worker_bytes_per_sample::<CategoricalAccumulator>()
            + OVERFLOW_BYTES_PER_CLASS
    }

    #[inline(always)]
    fn passes_filter(&self, acc: &Self::Accumulator) -> bool {
        if let Some(min_c) = self.min_count {
            if acc.total_count < min_c {
                return false;
            }
        }
        if let Some(min_frac) = self.min_majority_fraction {
            let (_, _, frac) = acc.majority();
            if frac < min_frac {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    fn make_record(&self, resolution: u8, cell_u64: u64, acc: Self::Accumulator) -> Self::Record {
        MultiCategoricalRecord {
            resolution,
            cell_id: cell_u64,
            accumulator: acc,
        }
    }

    #[inline(always)]
    fn sample_count(acc: &Self::Accumulator) -> f64 {
        acc.total_count
    }

    #[inline(always)]
    fn process_chunk(
        &self,
        window: &WalkContext<'_>,
        decoding_result: &mut DecodingResult,
        nodata: Option<f64>,
        local_maps: &mut [HashMap<u64, Self::Accumulator, CellIdBuildHasher>],
    ) -> bool {
        process_categorical_chunk_payload_into(
            window,
            decoding_result,
            nodata,
            self.band,
            self.remapper.as_deref(),
            local_maps,
        )
    }

    #[inline(always)]
    fn process_window(
        &self,
        window: &WalkContext<'_>,
        samples: super::borrowed::BorrowedSamples<'_>,
        nodata: Option<f64>,
        local_maps: &mut [HashMap<u64, Self::Accumulator, CellIdBuildHasher>],
    ) -> bool {
        super::categorical::process_categorical_borrowed_into(
            window,
            samples,
            nodata,
            self.band,
            self.remapper.as_deref(),
            local_maps,
        )
    }
}

/// Single-pass streaming aggregator across multiple H3 resolutions (Categorical Data)
pub struct MultiCategoricalHorizonStreamer {
    inner: MultiHorizonStreamer<CategoricalKernel>,
    /// The `interpolate` / `overlay` stream, when `config.transfer` is not the default; the
    /// horizon kernel in `inner` is then idle.
    transfer: Option<Box<crate::transfer::TransferStream<CategoricalAccumulator>>>,
    transfer_mode: crate::transfer::TransferMode,
}

impl MultiCategoricalHorizonStreamer {
    /// The histogram this scan was asked for (see [`crate::histogram`]): with bins, the
    /// classes of its records are bin indices.
    pub fn histogram(&self) -> Option<&crate::histogram::HistogramSpec> {
        self.inner.kernel().histogram.as_deref()
    }

    /// Category remapping applied before aggregation, if configured
    pub fn remapper(&self) -> Option<&CategoryRemapper> {
        self.inner.kernel().remapper.as_deref()
    }
}

super::streamer_facade::impl_horizon_streamer_facade!(
    MultiCategoricalHorizonStreamer,
    CategoricalKernel,
    CategoricalAccumulator,
    MultiCategoricalRecord,
    true
);
