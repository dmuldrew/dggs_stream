//! Shared façade implementation for typed multi-resolution horizon streamers.

use crate::error::Result;
use crate::raster::mosaic::MosaicReader;
use std::sync::Arc;

use super::config::MultiResolutionConfig;

/// Build a `TransferStream` using `resolved` when `config.transfer` differs (e.g. `Auto`).
pub(super) fn open_transfer_stream<A: crate::transfer::TransferAccumulator>(
    mosaic: Arc<MosaicReader>,
    config: &MultiResolutionConfig,
    resolved: crate::transfer::TransferMode,
) -> Result<crate::transfer::TransferStream<A>> {
    let mut resolved_cfg;
    let cfg = if resolved == config.transfer {
        config
    } else {
        resolved_cfg = config.clone();
        resolved_cfg.transfer = resolved;
        &resolved_cfg
    };
    crate::transfer::TransferStream::new(mosaic, cfg)
}

/// Implements the construction, transfer dispatch, draining, `RecordStreamer`, profiling,
/// and shutdown boilerplate shared by continuous and categorical horizon streamers.
///
/// Kernel-specific code stays in the kernel and in small extension blocks next to it.
macro_rules! impl_horizon_streamer_facade {
    ($streamer:ident, $kernel:ty, $accumulator:ty, $record:ty, $categorical:expr) => {
        impl $streamer {
            /// Initialize from a single GeoTIFF reader.
            pub fn new(
                reader: crate::raster::geotiff::GeoTiffStreamReader,
                config: &super::config::MultiResolutionConfig,
            ) -> crate::error::Result<Self> {
                let mosaic =
                    std::sync::Arc::new(crate::raster::mosaic::MosaicReader::from_single_reader(
                        reader,
                        config.bbox,
                        config.custom_crs.as_deref(),
                    )?);
                Self::new_mosaic(mosaic, config)
            }

            /// Initialize from a multi-file mosaic reader.
            pub fn new_mosaic(
                mosaic: std::sync::Arc<crate::raster::mosaic::MosaicReader>,
                config: &super::config::MultiResolutionConfig,
            ) -> crate::error::Result<Self> {
                let kernel = <$kernel>::from_config(config);
                let resolved =
                    config
                        .transfer
                        .resolve_for_mosaic(mosaic.as_ref(), config, $categorical);
                let (inner, transfer) = if resolved.is_assign_centers() {
                    (
                        super::controller::MultiHorizonStreamer::new_mosaic(
                            mosaic, config, kernel,
                        )?,
                        None,
                    )
                } else {
                    let inner =
                        super::controller::MultiHorizonStreamer::new_mosaic_without_prefetch(
                            mosaic, config, kernel,
                        )?;
                    let stream = super::streamer_facade::open_transfer_stream(
                        inner.mosaic_arc(),
                        config,
                        resolved,
                    )?;
                    (inner, Some(Box::new(stream)))
                };
                Ok(Self {
                    inner,
                    transfer,
                    transfer_mode: resolved,
                })
            }

            /// Pull up to `max_rows` completed multi-resolution records.
            #[inline(always)]
            pub fn fetch_next_batch(
                &mut self,
                max_rows: usize,
            ) -> crate::error::Result<Vec<$record>> {
                if self.transfer.is_some() {
                    let mut records = Vec::new();
                    self.drain_completed_into(max_rows, |_, record| records.push(record))?;
                    return Ok(records);
                }
                self.inner.fetch_next_batch(max_rows)
            }

            /// Drain a partitioned scan into completed-record and partial-cell consumers.
            #[inline(always)]
            pub fn drain_partitioned_into<F, G>(
                &mut self,
                max_rows: usize,
                on_record: F,
                on_partial: G,
            ) -> crate::error::Result<usize>
            where
                F: FnMut(usize, $record),
                G: FnMut(super::partition::PartialCell<$accumulator>),
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
                config: &super::config::MultiResolutionConfig,
            ) -> super::partial::PartialMerger<$kernel> {
                super::partial::PartialMerger::new(<$kernel>::from_config(config))
            }

            /// Drain up to `max_rows` completed records directly into a closure.
            #[inline(always)]
            pub fn drain_completed_into<F>(
                &mut self,
                max_rows: usize,
                consumer: F,
            ) -> crate::error::Result<usize>
            where
                F: FnMut(usize, $record),
            {
                let mut consumer = consumer;
                // With a runtime profile, count the cells handed out and the samples behind
                // those of the first resolution. One check per drain call otherwise.
                let profile = self.inner.run_profile().cloned();
                let first_resolution = self.inner.resolution_u8s().first().copied();
                let (mut cells, mut samples) = (0u64, 0.0f64);
                let mut consumer = |i: usize, record: $record| {
                    if profile.is_some() {
                        cells += 1;
                        if Some(record.resolution) == first_resolution {
                            samples +=
                                <$kernel as super::controller::HorizonStreamKernel>::sample_count(
                                    &record.accumulator,
                                );
                        }
                    }
                    consumer(i, record)
                };
                let drained = match &mut self.transfer {
                    Some(transfer) => {
                        transfer.drain_into(max_rows, |i, resolution, cell_id, accumulator| {
                            consumer(
                                i,
                                self.inner
                                    .kernel()
                                    .make_record(resolution, cell_id, accumulator),
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

            /// Advance the scanline horizon until at least `min_rows` records are available or finished.
            #[inline(always)]
            pub fn advance_until_completed(&mut self, min_rows: usize) -> crate::error::Result<()> {
                match &mut self.transfer {
                    Some(transfer) => transfer.advance_until_completed(min_rows),
                    None => self.inner.advance_until_completed(min_rows),
                }
            }

            /// Current southernmost latitude reached by the scanline horizon.
            #[inline(always)]
            pub fn current_lat_horizon(&self) -> f64 {
                match &self.transfer {
                    Some(transfer) => transfer.current_lat_horizon(),
                    None => self.inner.current_lat_horizon(),
                }
            }

            /// Target grid resolutions.
            #[inline(always)]
            pub fn resolutions(&self) -> &[h3o::Resolution] {
                self.inner.resolutions()
            }

            /// Target grid resolution integer levels.
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

            /// Valid samples dropped by failed or out-of-domain coordinate transforms.
            pub fn transform_failures(&self) -> u64 {
                match &self.transfer {
                    Some(transfer) => transfer.transform_failures(),
                    None => self.inner.transform_failures(),
                }
            }

            /// NoData policy/value and decimal rounding for serializers of this stream.
            pub fn output_policy(&self) -> crate::aggregator::output_policy::OutputPolicy {
                self.inner.output_policy()
            }

            /// Peak active-map allocation estimate (not whole-process memory).
            pub fn peak_active_bytes(&self) -> usize {
                self.inner.peak_active_bytes()
            }

            /// Total active in-flight cells across all resolutions.
            #[inline(always)]
            pub fn active_cell_count(&self) -> usize {
                self.inner.active_cell_count()
            }

            /// Whether the stream is fully drained and finished.
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

        impl std::ops::Deref for $streamer {
            type Target = super::controller::MultiHorizonStreamer<$kernel>;

            #[inline(always)]
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $streamer {
            #[inline(always)]
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }

        impl super::controller::RecordStreamer for $streamer {
            type Record = $record;

            #[inline(always)]
            fn drain_completed_into<F>(
                &mut self,
                max_rows: usize,
                consumer: F,
            ) -> crate::error::Result<usize>
            where
                F: FnMut(usize, Self::Record),
            {
                Self::drain_completed_into(self, max_rows, consumer)
            }

            #[inline(always)]
            fn current_lat_horizon(&self) -> f64 {
                Self::current_lat_horizon(self)
            }

            #[inline(always)]
            fn is_finished(&self) -> bool {
                Self::is_finished(self)
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

            fn run_profile(&self) -> Option<std::sync::Arc<crate::run_profile::RunProfile>> {
                self.inner.run_profile().cloned()
            }
        }

        impl Drop for $streamer {
            fn drop(&mut self) {
                // A scan abandoned early (a LIMIT) still reports what it did.
                self.inner.publish_profile();
                if let Some(profile) = self.inner.run_profile() {
                    profile.stream_finished();
                }
            }
        }
    };
}

pub(crate) use impl_horizon_streamer_facade;
