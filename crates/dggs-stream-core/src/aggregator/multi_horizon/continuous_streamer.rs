//! Multi-Resolution Scanline Horizon Streamer for Continuous Data
//!
//! Provides single-pass streaming aggregation across multiple H3 resolution levels simultaneously
//! for continuous/floating-point raster bands and spectral indices.

use crate::aggregator::cell_hash::H3BuildHasher;
use h3o::Resolution;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tiff::decoder::DecodingResult;

use crate::aggregator::accumulator::H3Accumulator;
use crate::error::Result;
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::raster::mosaic::MosaicReader;

use super::config::{MultiResolutionConfig, SpectralFormula};
use super::continuous::{
    process_continuous_chunk_payload_into, ContinuousOptions, MultiContinuousRecord,
};
use super::controller::{HorizonStreamKernel, MultiHorizonStreamer, RecordStreamer};
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
    type Accumulator = H3Accumulator;
    type Record = MultiContinuousRecord;

    #[inline(always)]
    fn new_parent_accumulator(&self) -> Self::Accumulator {
        if self.track_quantiles {
            H3Accumulator::with_quantiles()
        } else {
            H3Accumulator::default()
        }
    }

    fn worker_bytes_per_sample(&self) -> usize {
        // Without quantile sketches an `H3Accumulator` owns no heap state.
        if self.track_quantiles {
            1024usize.max(size_of::<H3Accumulator>().saturating_mul(8))
        } else {
            super::controller::inline_worker_bytes_per_sample::<H3Accumulator>()
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
            h3_index: cell_u64,
            accumulator: acc,
        }
    }

    #[inline(always)]
    fn process_chunk(
        &self,
        window: &WalkContext<'_>,
        decoding_result: &mut DecodingResult,
        nodata: Option<f64>,
        local_maps: &mut [HashMap<u64, Self::Accumulator, H3BuildHasher>],
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
        local_maps: &mut [HashMap<u64, Self::Accumulator, H3BuildHasher>],
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
    track_quantiles: bool,
    /// The `interpolate` / `overlay` stream, when `config.transfer` is not the default; the
    /// horizon kernel in `inner` is then idle.
    transfer: Option<Box<crate::transfer::TransferStream<H3Accumulator>>>,
    transfer_mode: crate::transfer::TransferMode,
}

impl MultiScanHorizonStreamer {
    /// Whether records carry streaming quantile sketches
    pub fn track_quantiles(&self) -> bool {
        self.track_quantiles
    }

    /// Initialize a new MultiScanHorizonStreamer from a single GeoTIFF reader
    pub fn new(reader: GeoTiffStreamReader, config: &MultiResolutionConfig) -> Result<Self> {
        let kernel = ContinuousKernel::from_config(config);
        let track_quantiles = kernel.track_quantiles;
        let mut inner = MultiHorizonStreamer::new(reader, config, kernel)?;
        let transfer = transfer_stream(&mut inner, config)?;
        Ok(Self {
            inner,
            track_quantiles,
            transfer,
            transfer_mode: config.transfer,
        })
    }

    /// Initialize a new MultiScanHorizonStreamer from a multi-file MosaicReader
    pub fn new_mosaic(mosaic: Arc<MosaicReader>, config: &MultiResolutionConfig) -> Result<Self> {
        let kernel = ContinuousKernel::from_config(config);
        let track_quantiles = kernel.track_quantiles;
        let mut inner = MultiHorizonStreamer::new_mosaic(mosaic, config, kernel)?;
        let transfer = transfer_stream(&mut inner, config)?;
        Ok(Self {
            inner,
            track_quantiles,
            transfer,
            transfer_mode: config.transfer,
        })
    }

    /// Pull up to `max_rows` completed multi-resolution records using multi-core chunk-row parallelism
    #[inline(always)]
    pub fn fetch_next_batch(&mut self, max_rows: usize) -> Result<Vec<MultiContinuousRecord>> {
        if self.transfer.is_some() {
            let mut records = Vec::new();
            self.drain_completed_into(max_rows, |_, record| records.push(record))?;
            return Ok(records);
        }
        self.inner.fetch_next_batch(max_rows)
    }

    /// Drain a partitioned scan (`config.partition`): completed records into `on_record`
    /// and partial cells into `on_partial`, up to `max_rows` in total. 0 means end of
    /// stream. See [`super::partition`].
    #[inline(always)]
    pub fn drain_partitioned_into<F, G>(
        &mut self,
        max_rows: usize,
        on_record: F,
        on_partial: G,
    ) -> Result<usize>
    where
        F: FnMut(usize, MultiContinuousRecord),
        G: FnMut(super::partition::PartialCell<H3Accumulator>),
    {
        self.inner
            .drain_partitioned_into(max_rows, on_record, on_partial)
    }

    /// For a partitioned scan, `(interior, boundary)` cells classified so far.
    pub fn partition_cell_counts(&self) -> Option<(u64, u64)> {
        self.inner.partition_cell_counts()
    }

    /// A merger turning all partitions' partial cells into final records with the
    /// filters of `config`.
    pub fn partial_merger(
        config: &MultiResolutionConfig,
    ) -> super::partial::PartialMerger<ContinuousKernel> {
        super::partial::PartialMerger::new(ContinuousKernel::from_config(config))
    }

    /// Drain up to `max_rows` completed records directly into a closure with zero heap allocation
    #[inline(always)]
    pub fn drain_completed_into<F>(&mut self, max_rows: usize, consumer: F) -> Result<usize>
    where
        F: FnMut(usize, MultiContinuousRecord),
    {
        let mut consumer = consumer;
        // With a runtime profile, count the cells handed out and the samples behind
        // those of the first resolution. One check per drain call otherwise.
        let profile = self.inner.run_profile().cloned();
        let first_resolution = self.inner.resolution_u8s().first().copied();
        let (mut cells, mut samples) = (0u64, 0.0f64);
        let mut consumer = |i: usize, record: MultiContinuousRecord| {
            if profile.is_some() {
                cells += 1;
                if Some(record.resolution) == first_resolution {
                    samples += record.accumulator.count;
                }
            }
            consumer(i, record)
        };
        let drained = match &mut self.transfer {
            Some(transfer) => {
                transfer.drain_into(max_rows, |i, resolution, h3_index, accumulator| {
                    consumer(
                        i,
                        MultiContinuousRecord {
                            resolution,
                            h3_index,
                            accumulator,
                        },
                    )
                })
            }
            None => self.inner.drain_completed_into(max_rows, &mut consumer),
        };
        if let Some(profile) = &profile {
            use crate::run_profile::Counter;
            profile.count(Counter::Cells, cells);
            profile.count(Counter::SamplesIndexed, samples.round() as u64);
        }
        drained
    }

    /// Advance scanline horizon until at least `min_rows` completed records are available or finished
    #[inline(always)]
    pub fn advance_until_completed(&mut self, min_rows: usize) -> Result<()> {
        match &mut self.transfer {
            Some(transfer) => transfer.advance_until_completed(min_rows),
            None => self.inner.advance_until_completed(min_rows),
        }
    }

    /// Return current southernmost latitude reached by scanline horizon
    #[inline(always)]
    pub fn current_lat_horizon(&self) -> f64 {
        match &self.transfer {
            Some(transfer) => transfer.current_lat_horizon(),
            None => self.inner.current_lat_horizon(),
        }
    }

    /// Target H3 resolutions
    #[inline(always)]
    pub fn resolutions(&self) -> &[Resolution] {
        self.inner.resolutions()
    }

    /// Target H3 resolution integer levels
    #[inline(always)]
    pub fn resolution_u8s(&self) -> &[u8] {
        self.inner.resolution_u8s()
    }

    /// Stage timings and counters, including observed kernel concurrency.
    pub fn metrics(&self) -> &super::profile::StreamProfile {
        self.inner.metrics()
    }

    /// Number of partial aggregation runs persisted under memory pressure.
    pub fn spill_run_count(&self) -> u64 {
        self.inner.spill_run_count()
    }

    /// Spill runs for each configured resolution, in order.
    pub fn spill_run_counts(&self) -> Vec<u64> {
        self.inner.spill_run_counts()
    }

    /// Valid sample points dropped by failed or out-of-domain coordinate
    /// transforms. Non-zero means some valid pixels are missing from the output.
    pub fn transform_failures(&self) -> u64 {
        match &self.transfer {
            Some(transfer) => transfer.transform_failures(),
            None => self.inner.transform_failures(),
        }
    }

    /// How serializers turn this stream's cells into output values: the NoData policy
    /// and value, and the decimal rounding.
    pub fn output_policy(&self) -> crate::aggregator::output_policy::OutputPolicy {
        self.inner.output_policy()
    }

    /// Peak active-map allocation estimate (not whole-process memory).
    pub fn peak_active_bytes(&self) -> usize {
        self.inner.peak_active_bytes()
    }

    /// Return total active in-flight cells across all resolutions
    #[inline(always)]
    pub fn active_cell_count(&self) -> usize {
        self.inner.active_cell_count()
    }

    /// Check if stream is fully drained and finished
    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        match &self.transfer {
            Some(transfer) => transfer.is_finished(),
            None => self.inner.is_finished(),
        }
    }

    /// The transfer mode this stream runs (`AssignCenters` for the default scan).
    pub fn transfer_mode(&self) -> crate::transfer::TransferMode {
        if self.transfer.is_some() {
            self.transfer_mode
        } else {
            crate::transfer::TransferMode::AssignCenters
        }
    }
}

impl Deref for MultiScanHorizonStreamer {
    type Target = MultiHorizonStreamer<ContinuousKernel>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MultiScanHorizonStreamer {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl RecordStreamer for MultiScanHorizonStreamer {
    type Record = MultiContinuousRecord;

    #[inline(always)]
    fn drain_completed_into<F>(&mut self, max_rows: usize, consumer: F) -> Result<usize>
    where
        F: FnMut(usize, Self::Record),
    {
        MultiScanHorizonStreamer::drain_completed_into(self, max_rows, consumer)
    }

    #[inline(always)]
    fn current_lat_horizon(&self) -> f64 {
        MultiScanHorizonStreamer::current_lat_horizon(self)
    }

    #[inline(always)]
    fn is_finished(&self) -> bool {
        MultiScanHorizonStreamer::is_finished(self)
    }

    #[inline(always)]
    fn bounds_wgs84(&self) -> Option<[f64; 4]> {
        Some(self.inner.mosaic().mosaic_bounds_wgs84)
    }

    #[inline(always)]
    fn compacts_children(&self) -> bool {
        self.inner.compacts_children()
    }

    #[inline(always)]
    fn resolution_u8s(&self) -> &[u8] {
        self.inner.resolution_u8s()
    }

    #[inline(always)]
    fn grid(&self) -> crate::grid::GridKind {
        self.inner.grid()
    }

    #[inline(always)]
    fn output_policy(&self) -> crate::aggregator::output_policy::OutputPolicy {
        self.inner.output_policy()
    }

    fn run_profile(&self) -> Option<Arc<crate::run_profile::RunProfile>> {
        self.inner.run_profile().cloned()
    }
}

impl Drop for MultiScanHorizonStreamer {
    fn drop(&mut self) {
        // A scan abandoned early (a LIMIT) still reports what it did.
        self.inner.publish_profile();
        if let Some(profile) = self.inner.run_profile() {
            profile.stream_finished();
        }
    }
}

/// The transfer stream of a non-default `config.transfer`, with the horizon kernel's
/// prefetch stopped: the transfer modes read the raster themselves.
fn transfer_stream<K: HorizonStreamKernel>(
    inner: &mut MultiHorizonStreamer<K>,
    config: &MultiResolutionConfig,
) -> Result<Option<Box<crate::transfer::TransferStream<H3Accumulator>>>> {
    if config.transfer.is_assign_centers() {
        return Ok(None);
    }
    inner.stop_prefetch();
    let stream = crate::transfer::TransferStream::new(inner.mosaic_arc(), config)?;
    Ok(Some(Box::new(stream)))
}
