//! Multi-Resolution Scanline Horizon Streamer for Continuous Data
//!
//! Provides single-pass streaming aggregation across multiple H3 resolution levels simultaneously
//! for continuous/floating-point raster bands and spectral indices.

use crate::aggregator::cell_hash::CellIdBuildHasher;
use std::collections::HashMap;
use tiff::decoder::DecodingResult;

use crate::aggregator::accumulator::ContinuousAccumulator;

use super::config::{MultiResolutionConfig, SpectralFormula};
use super::continuous::{
    process_continuous_chunk_payload_into, ContinuousOptions, MultiContinuousRecord,
};
use super::controller::{HorizonStreamKernel, MultiHorizonStreamer};
use super::walker::WalkContext;

/// Kernel implementation for continuous (floating point / spectral) raster aggregation
#[derive(Clone)]
pub struct ContinuousKernel {
    pub band: usize,
    pub spectral_formula: Option<SpectralFormula>,
    pub min_count: Option<f64>,
    pub min_mean: Option<f64>,
    pub max_mean: Option<f64>,
    pub track_quantiles: bool,
}

impl ContinuousKernel {
    fn options(&self) -> ContinuousOptions {
        ContinuousOptions {
            band: self.band,
            spectral_formula: self.spectral_formula,
            track_quantiles: self.track_quantiles,
        }
    }
}

impl ContinuousKernel {
    /// The kernel a stream built from `config` uses (filters, band and options).
    pub fn from_config(config: &MultiResolutionConfig) -> Self {
        ContinuousKernel {
            band: config.band,
            spectral_formula: config.spectral_formula,
            min_count: config.min_count,
            min_mean: config.min_mean,
            max_mean: config.max_mean,
            track_quantiles: config.track_quantiles(),
        }
    }
}

impl HorizonStreamKernel for ContinuousKernel {
    type Accumulator = ContinuousAccumulator;
    type Record = MultiContinuousRecord;

    #[inline(always)]
    fn new_parent_accumulator(&self) -> Self::Accumulator {
        if self.track_quantiles {
            ContinuousAccumulator::with_quantiles()
        } else {
            ContinuousAccumulator::default()
        }
    }

    fn worker_bytes_per_sample(&self) -> usize {
        // Without quantile sketches an `ContinuousAccumulator` owns no heap state.
        if self.track_quantiles {
            1024usize.max(size_of::<ContinuousAccumulator>().saturating_mul(8))
        } else {
            super::controller::inline_worker_bytes_per_sample::<ContinuousAccumulator>()
        }
    }

    #[inline(always)]
    fn passes_filter(&self, acc: &Self::Accumulator) -> bool {
        if let Some(min_c) = self.min_count {
            if acc.count < min_c {
                return false;
            }
        }
        if let Some(min_m) = self.min_mean {
            if acc.mean() < min_m {
                return false;
            }
        }
        if let Some(max_m) = self.max_mean {
            if acc.mean() > max_m {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    fn make_record(&self, resolution: u8, cell_u64: u64, acc: Self::Accumulator) -> Self::Record {
        MultiContinuousRecord {
            resolution,
            cell_id: cell_u64,
            accumulator: acc,
        }
    }

    #[inline(always)]
    fn sample_count(acc: &Self::Accumulator) -> f64 {
        acc.count
    }

    #[inline(always)]
    fn process_chunk(
        &self,
        window: &WalkContext<'_>,
        decoding_result: &mut DecodingResult,
        nodata: Option<f64>,
        local_maps: &mut [HashMap<u64, Self::Accumulator, CellIdBuildHasher>],
    ) -> bool {
        process_continuous_chunk_payload_into(
            window,
            decoding_result,
            nodata,
            self.options(),
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
        super::continuous::process_continuous_borrowed_into(
            window,
            samples,
            nodata,
            self.options(),
            local_maps,
        )
    }
}

/// Single-pass streaming aggregator across multiple H3 resolutions (Continuous Data)
pub struct MultiScanHorizonStreamer {
    inner: MultiHorizonStreamer<ContinuousKernel>,
    /// The `interpolate` / `overlay` stream, when `config.transfer` is not the default; the
    /// horizon kernel in `inner` is then idle.
    transfer: Option<Box<crate::transfer::TransferStream<ContinuousAccumulator>>>,
    transfer_mode: crate::transfer::TransferMode,
}

impl MultiScanHorizonStreamer {
    /// Whether records carry streaming quantile sketches
    pub fn track_quantiles(&self) -> bool {
        self.inner.kernel().track_quantiles
    }
}

super::streamer_facade::impl_horizon_streamer_facade!(
    MultiScanHorizonStreamer,
    ContinuousKernel,
    ContinuousAccumulator,
    MultiContinuousRecord,
    false
);
