//! Multi-Resolution Scanline Horizon Streamer for Categorical Data
//!
//! Provides single-pass streaming aggregation across multiple H3 resolution levels simultaneously
//! for discrete integer / land cover categories and classification maps.

use crate::aggregator::cell_hash::CellIdBuildHasher;
use h3o::Resolution;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tiff::decoder::DecodingResult;

use crate::aggregator::categorical::CategoricalAccumulator;
use crate::aggregator::remap::CategoryRemapper;
use crate::error::Result;
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::raster::mosaic::MosaicReader;

use super::categorical::{process_categorical_chunk_payload_into, MultiCategoricalRecord};
use super::config::MultiResolutionConfig;
use super::controller::{HorizonStreamKernel, MultiHorizonStreamer, RecordStreamer};
use super::walker::WalkContext;

pub use super::sharded_map::{get_shard as pub_get_shard, NUM_SHARDS as PUB_NUM_SHARDS};

/// Kernel implementation for categorical/classified raster aggregation
#[derive(Clone)]
pub struct CategoricalKernel {
    pub band: usize,
    pub min_count: Option<f64>,
    pub min_majority_fraction: Option<f64>,
    pub remapper: Option<Arc<CategoryRemapper>>,
}

impl CategoricalKernel {
    /// The kernel a stream built from `config` uses (filters, band and options).
    pub fn from_config(config: &MultiResolutionConfig) -> Self {
        CategoricalKernel {
            band: config.band,
            min_count: config.min_count,
            min_majority_fraction: config.min_majority_fraction,
            remapper: config.remapper.clone(),
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
    remapper: Option<Arc<CategoryRemapper>>,
    /// The `sample` / `overlay` stream, when `config.transfer` is not the default; the
    /// horizon kernel in `inner` is then idle.
    transfer: Option<Box<crate::transfer::TransferStream<CategoricalAccumulator>>>,
    transfer_mode: crate::transfer::TransferMode,
    histogram: Option<Arc<crate::histogram::HistogramSpec>>,
}

impl MultiCategoricalHorizonStreamer {
    /// The histogram this scan was asked for (see [`crate::histogram`]): with bins, the
    /// classes of its records are bin indices.
    pub fn histogram(&self) -> Option<&crate::histogram::HistogramSpec> {
        self.histogram.as_deref()
    }

    /// Category remapping applied before aggregation, if configured
    pub fn remapper(&self) -> Option<&CategoryRemapper> {
        self.remapper.as_deref()
    }

    /// Initialize a new MultiCategoricalHorizonStreamer from a single GeoTIFF reader
    pub fn new(reader: GeoTiffStreamReader, config: &MultiResolutionConfig) -> Result<Self> {
        let kernel = CategoricalKernel::from_config(config);
        let remapper = kernel.remapper.clone();
        let mut inner = MultiHorizonStreamer::new(reader, config, kernel)?;
        let transfer = transfer_stream(&mut inner, config)?;
        Ok(Self {
            inner,
            remapper,
            transfer,
            transfer_mode: config.transfer,
            histogram: config.histogram.clone().map(Arc::new),
        })
    }

    /// Initialize a new MultiCategoricalHorizonStreamer from a multi-file MosaicReader
    pub fn new_mosaic(mosaic: Arc<MosaicReader>, config: &MultiResolutionConfig) -> Result<Self> {
        let kernel = CategoricalKernel::from_config(config);
        let remapper = kernel.remapper.clone();
        let mut inner = MultiHorizonStreamer::new_mosaic(mosaic, config, kernel)?;
        let transfer = transfer_stream(&mut inner, config)?;
        Ok(Self {
            inner,
            remapper,
            transfer,
            transfer_mode: config.transfer,
            histogram: config.histogram.clone().map(Arc::new),
        })
    }

    /// Pull up to `max_rows` completed multi-resolution records using multi-core chunk-row parallelism
    #[inline(always)]
    pub fn fetch_next_batch(&mut self, max_rows: usize) -> Result<Vec<MultiCategoricalRecord>> {
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
        F: FnMut(usize, MultiCategoricalRecord),
        G: FnMut(super::partition::PartialCell<CategoricalAccumulator>),
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
    ) -> super::partial::PartialMerger<CategoricalKernel> {
        super::partial::PartialMerger::new(CategoricalKernel::from_config(config))
    }

    /// Drain up to `max_rows` completed records directly into a closure with zero heap allocation
    #[inline(always)]
    pub fn drain_completed_into<F>(&mut self, max_rows: usize, consumer: F) -> Result<usize>
    where
        F: FnMut(usize, MultiCategoricalRecord),
    {
        let mut consumer = consumer;
        // With a runtime profile, count the cells handed out and the samples behind
        // those of the first resolution. One check per drain call otherwise.
        let profile = self.inner.run_profile().cloned();
        let first_resolution = self.inner.resolution_u8s().first().copied();
        let (mut cells, mut samples) = (0u64, 0.0f64);
        let mut consumer = |i: usize, record: MultiCategoricalRecord| {
            if profile.is_some() {
                cells += 1;
                if Some(record.resolution) == first_resolution {
                    samples += record.accumulator.total_count;
                }
            }
            consumer(i, record)
        };
        let drained = match &mut self.transfer {
            Some(transfer) => {
                transfer.drain_into(max_rows, |i, resolution, cell_id, accumulator| {
                    consumer(
                        i,
                        MultiCategoricalRecord {
                            resolution,
                            cell_id,
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

    /// Stage timings and counters, including observed kernel concurrency.
    pub fn metrics(&self) -> &super::profile::StreamProfile {
        self.inner.metrics()
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

impl Deref for MultiCategoricalHorizonStreamer {
    type Target = MultiHorizonStreamer<CategoricalKernel>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MultiCategoricalHorizonStreamer {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl RecordStreamer for MultiCategoricalHorizonStreamer {
    type Record = MultiCategoricalRecord;

    #[inline(always)]
    fn drain_completed_into<F>(&mut self, max_rows: usize, consumer: F) -> Result<usize>
    where
        F: FnMut(usize, Self::Record),
    {
        MultiCategoricalHorizonStreamer::drain_completed_into(self, max_rows, consumer)
    }

    #[inline(always)]
    fn current_lat_horizon(&self) -> f64 {
        MultiCategoricalHorizonStreamer::current_lat_horizon(self)
    }

    #[inline(always)]
    fn is_finished(&self) -> bool {
        MultiCategoricalHorizonStreamer::is_finished(self)
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

impl Drop for MultiCategoricalHorizonStreamer {
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
) -> Result<Option<Box<crate::transfer::TransferStream<CategoricalAccumulator>>>> {
    if config.transfer.is_assign_centers() {
        return Ok(None);
    }
    inner.stop_prefetch();
    let stream = crate::transfer::TransferStream::new(inner.mosaic_arc(), config)?;
    Ok(Some(Box::new(stream)))
}
