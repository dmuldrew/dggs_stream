//! Single-pass continuous statistics for selected TIFF bands.
use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::sync::Arc;

use tiff::decoder::DecodingResult;

use super::borrowed::BorrowedSamples;
use super::spill::SpillAccumulator;
use super::walker::{is_transparent, walk_interleaved, ScanlineEngine, WalkContext};
use super::{
    inline_worker_bytes_per_sample, AccumulatorMerge, HorizonStreamKernel, MultiHorizonStreamer,
    MultiResolutionConfig,
};
use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::cell_hash::CellIdBuildHasher;
use crate::aggregator::simd::SimdSpanAccumulate;
use crate::error::{DggsStreamError, Result};
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::transfer::{TransferMode, TransferStream};

/// Band numbers are one-based. Records retain the requested order.
pub enum Bands<'a> {
    /// Include every TIFF sample band in file order.
    All,
    /// Include the listed one-based bands in order. Each number must be distinct.
    Selected(&'a [usize]),
}

/// One occupied cell. A band with no valid samples has count zero.
pub struct MultiBandRecord {
    /// Resolution of the output cell on the configured grid.
    pub resolution: u8,
    /// Grid-native identifier of the output cell.
    pub cell_id: u64,
    /// One accumulator per selected band, in selection order.
    pub statistics: Vec<ContinuousAccumulator>,
}

/// A center-assignment or overlay scan that shares TIFF decoding, projection and grid
/// indexing across the selected bands. Each band's NoData and calibration are
/// evaluated independently. Alpha transparency applies to the whole pixel.
///
/// ```no_run
/// use dggs_stream_core::aggregator::multi_horizon::multiband::{Bands, MultiBandStreamer};
/// use dggs_stream_core::aggregator::multi_horizon::MultiResolutionConfig;
/// use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
///
/// # fn main() -> dggs_stream_core::error::Result<()> {
/// let reader = GeoTiffStreamReader::open("imagery.tif")?;
/// let config = MultiResolutionConfig::single(8);
/// let mut scan = MultiBandStreamer::new(reader, &config, Bands::Selected(&[4, 1]))?;
/// loop {
///     let rows = scan.fetch_next_batch(1024)?;
///     if rows.is_empty() { break; }
///     for row in rows {
///         let nir = &row.statistics[0];
///         if nir.count > 0.0 { println!("{} {}", row.cell_id, nir.mean()); }
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct MultiBandStreamer {
    inner: Scan,
    profile: Option<Arc<crate::run_profile::RunProfile>>,
    /// Indexed samples are counted at this resolution only, as in single-band scans.
    first_resolution: Option<u8>,
}

enum Scan {
    Centers(Box<MultiHorizonStreamer<Kernel>>),
    Overlay(Box<TransferStream<Vec<ContinuousAccumulator>>>),
}

impl MultiBandStreamer {
    /// Open a continuous center-assignment or overlay scan over one interleaved or band-separated raster.
    /// Returns an error for an empty, duplicate or out-of-range band selection.
    pub fn new(
        mut reader: GeoTiffStreamReader,
        config: &MultiResolutionConfig,
        bands: Bands<'_>,
    ) -> Result<Self> {
        let mut preview_config;
        let config = if config.auto_overview {
            let native = Arc::new(crate::raster::mosaic::MosaicReader::from_single_reader(
                reader,
                config.bbox,
                config.custom_crs.as_deref(),
            )?);
            preview_config = config.clone();
            preview_config.transfer = config.transfer.resolve_for_multiband(&native, config);
            let mosaic = native.for_overview_scan(config, preview_config.transfer)?;
            reader = mosaic.tiles[0].reader.clone();
            &preview_config
        } else {
            config
        };
        let spp = usize::from(reader.metadata.samples_per_pixel);
        let bands: Vec<usize> = match bands {
            Bands::All => (1..=spp).collect(),
            Bands::Selected(bands) => bands.to_vec(),
        };
        let mut unique = HashSet::new();
        if bands.is_empty()
            || bands
                .iter()
                .any(|&b| b == 0 || b > spp || !unique.insert(b))
        {
            return Err(DggsStreamError::InvalidParameter(format!(
                "expected distinct band numbers in 1..={spp}"
            )));
        }
        if matches!(config.transfer, TransferMode::Interpolate(_))
            || config.band != 1
            || config.spectral_formula.is_some()
            || config.partition.is_some()
            || config.histogram.is_some()
            || config.remapper.is_some()
        {
            return Err(DggsStreamError::InvalidParameter("multi-band scans require continuous center assignment or overlay without a single-band selector, formula, histogram, remapping or partition".into()));
        }
        if config.min_mean.is_some()
            || config.max_mean.is_some()
            || config.min_count.is_some()
            || config.min_majority_fraction.is_some()
        {
            return Err(DggsStreamError::InvalidParameter(
                "single-band row filters are undefined for multi-band scans".into(),
            ));
        }
        let bands = reader.project_bands(bands);
        let (overlay_mosaic, resolved) = if config.transfer.is_auto() {
            let mosaic = Arc::new(crate::raster::mosaic::MosaicReader::from_single_reader(
                reader,
                config.bbox,
                config.custom_crs.as_deref(),
            )?);
            let resolved = config.transfer.resolve_for_multiband(&mosaic, config);
            if matches!(resolved, TransferMode::Overlay { .. }) {
                (Some(mosaic), resolved)
            } else {
                let mut resolved_cfg = config.clone();
                resolved_cfg.transfer = resolved;
                let kernel = Kernel {
                    bands,
                    quantiles: resolved_cfg.track_quantiles(),
                };
                let streamer = MultiHorizonStreamer::new_mosaic(mosaic, &resolved_cfg, kernel)?;
                return Ok(Self {
                    profile: streamer.run_profile().cloned(),
                    first_resolution: streamer.resolution_u8s().first().copied(),
                    inner: Scan::Centers(Box::new(streamer)),
                });
            }
        } else if matches!(config.transfer, TransferMode::Overlay { .. }) {
            let mosaic = Arc::new(crate::raster::mosaic::MosaicReader::from_single_reader(
                reader,
                config.bbox,
                config.custom_crs.as_deref(),
            )?);
            (Some(mosaic), config.transfer)
        } else {
            let kernel = Kernel {
                bands,
                quantiles: config.track_quantiles(),
            };
            let streamer = MultiHorizonStreamer::new(reader, config, kernel)?;
            return Ok(Self {
                profile: streamer.run_profile().cloned(),
                first_resolution: streamer.resolution_u8s().first().copied(),
                inner: Scan::Centers(Box::new(streamer)),
            });
        };
        if let Some(mosaic) = overlay_mosaic {
            config.validate()?;
            let mut resolved_cfg;
            let cfg = if resolved == config.transfer {
                config
            } else {
                resolved_cfg = config.clone();
                resolved_cfg.transfer = resolved;
                &resolved_cfg
            };
            let stream = TransferStream::new_bands(mosaic, cfg, bands)?;
            return Ok(Self {
                inner: Scan::Overlay(Box::new(stream)),
                profile: cfg.profile.clone(),
                first_resolution: cfg.resolutions.first().copied(),
            });
        }
        unreachable!()
    }

    pub fn transform_failures(&self) -> u64 {
        match &self.inner {
            Scan::Centers(s) => s.transform_failures(),
            Scan::Overlay(stream) => stream.transform_failures(),
        }
    }

    /// Read at most `max_rows` completed cells.
    pub fn fetch_next_batch(&mut self, max_rows: usize) -> Result<Vec<MultiBandRecord>> {
        let rows = match &mut self.inner {
            Scan::Centers(s) => s.fetch_next_batch(max_rows)?,
            Scan::Overlay(stream) => {
                let mut rows = Vec::new();
                stream.drain_into(max_rows, |_, resolution, cell_id, statistics| {
                    rows.push(MultiBandRecord {
                        resolution,
                        cell_id,
                        statistics,
                    })
                })?;
                rows
            }
        };
        if let Some(profile) = &self.profile {
            use crate::run_profile::Counter;
            // Bands share pixels: count each cell's samples once, by its fullest band.
            let samples: f64 = rows
                .iter()
                .filter(|r| Some(r.resolution) == self.first_resolution)
                .map(|r| r.statistics.iter().map(|s| s.count).fold(0.0, f64::max))
                .sum();
            profile.count(Counter::Cells, rows.len() as u64);
            profile.count(Counter::SamplesIndexed, samples.round() as u64);
        }
        Ok(rows)
    }
}

impl Drop for MultiBandStreamer {
    fn drop(&mut self) {
        if let Scan::Centers(s) = &mut self.inner {
            s.publish_profile();
        }
        if let Some(profile) = &self.profile {
            profile.stream_finished();
        }
    }
}

#[derive(Clone, Default)]
struct BandStats(Vec<ContinuousAccumulator>);

impl AccumulatorMerge for BandStats {
    fn merge(&mut self, other: &Self) {
        if self.0.is_empty() {
            self.0.clone_from(&other.0);
        } else {
            for (dst, src) in self.0.iter_mut().zip(&other.0) {
                dst.merge(src);
            }
        }
    }

    fn heap_bytes(&self) -> usize {
        self.0.capacity() * size_of::<ContinuousAccumulator>()
            + self
                .0
                .iter()
                .map(AccumulatorMerge::heap_bytes)
                .sum::<usize>()
    }
}

impl SpillAccumulator for BandStats {
    fn write_state(&self, out: &mut impl Write) -> io::Result<()> {
        out.write_all(&(self.0.len() as u16).to_le_bytes())?;
        for band in &self.0 {
            band.write_state(out)?;
        }
        Ok(())
    }

    fn read_state(input: &mut impl Read, limit: usize) -> io::Result<Self> {
        let mut bytes = [0; 2];
        input.read_exact(&mut bytes)?;
        let n = usize::from(u16::from_le_bytes(bytes));
        let mut remaining = limit
            .checked_sub(size_of::<Self>() + n * size_of::<ContinuousAccumulator>())
            .filter(|_| n != 0)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid multi-band spill state size",
                )
            })?;
        let mut bands = Vec::with_capacity(n);
        for _ in 0..n {
            let band = ContinuousAccumulator::read_state(
                input,
                remaining + size_of::<ContinuousAccumulator>(),
            )?;
            remaining -= band.heap_bytes();
            bands.push(band);
        }
        Ok(Self(bands))
    }
}

struct Kernel {
    bands: Vec<usize>,
    quantiles: bool,
}

type Map = HashMap<u64, BandStats, CellIdBuildHasher>;

impl HorizonStreamKernel for Kernel {
    type Accumulator = BandStats;
    type Record = MultiBandRecord;
    fn new_parent_accumulator(&self) -> BandStats {
        BandStats(
            (0..self.bands.len())
                .map(|_| {
                    if self.quantiles {
                        ContinuousAccumulator::with_quantiles()
                    } else {
                        ContinuousAccumulator::default()
                    }
                })
                .collect(),
        )
    }
    fn worker_bytes_per_sample(&self) -> usize {
        let per_band = if self.quantiles {
            1024
        } else {
            size_of::<ContinuousAccumulator>()
        };
        inline_worker_bytes_per_sample::<BandStats>() + self.bands.len() * per_band
    }
    fn passes_filter(&self, _: &BandStats) -> bool {
        true
    }
    fn make_record(&self, resolution: u8, cell_id: u64, acc: BandStats) -> MultiBandRecord {
        MultiBandRecord {
            resolution,
            cell_id,
            statistics: acc.0,
        }
    }
    fn process_chunk(
        &self,
        ctx: &WalkContext<'_>,
        data: &mut DecodingResult,
        nodata: Option<f64>,
        maps: &mut [Map],
    ) -> bool {
        self.process_window(ctx, BorrowedSamples::from(&*data), nodata, maps)
    }
    fn process_window(
        &self,
        ctx: &WalkContext<'_>,
        samples: BorrowedSamples<'_>,
        nodata: Option<f64>,
        maps: &mut [Map],
    ) -> bool {
        crate::dispatch_samples!(samples, nodata, |slice, nd| {
            let engine = BandEngine {
                ctx,
                kernel: self,
                nodata: nd,
            };
            walk_interleaved(
                ctx,
                slice,
                |pixel| {
                    if is_transparent(ctx, pixel) {
                        return engine.masked_sample();
                    }
                    (ctx.emit_nodata
                        || self
                            .bands
                            .iter()
                            .any(|&band| engine.value(pixel, band).is_some()))
                    .then_some(pixel)
                },
                &engine,
                maps,
            );
        });
        true
    }
}

struct BandEngine<'a, T> {
    ctx: &'a WalkContext<'a>,
    kernel: &'a Kernel,
    nodata: Option<T>,
}

impl<T: SimdSpanAccumulate> BandEngine<'_, T> {
    fn value(&self, pixel: &[T], band: usize) -> Option<f64> {
        if !self.ctx.band_present(band - 1) {
            return None;
        }
        let sample = pixel[band - 1];
        sample
            .is_valid(self.nodata)
            .then(|| self.ctx.scaling(band).apply(sample.to_f64_val()))
            .filter(|x| x.is_finite())
    }
}

impl<'a, T: SimdSpanAccumulate + 'a> ScanlineEngine<T, BandStats> for BandEngine<'a, T> {
    type Sample = &'a [T];
    fn masked_sample(&self) -> Option<Self::Sample> {
        self.ctx.emit_nodata.then_some(&[])
    }
    fn combine_sample_weights(&self) -> bool {
        true
    }
    fn new_acc(&self) -> BandStats {
        self.kernel.new_parent_accumulator()
    }
    fn clear_acc(&self, acc: &mut BandStats) {
        for band in &mut acc.0 {
            band.clear();
        }
    }
    fn has_samples(&self, acc: &BandStats) -> bool {
        self.ctx.emit_nodata || acc.0.iter().any(|b| b.count > 0.0)
    }
    fn merge_acc(&self, dst: &mut BandStats, src: &BandStats) {
        dst.merge(src);
    }
    fn update_sample(&self, acc: &mut BandStats, pixel: &[T], weight: f64) {
        if pixel.is_empty() {
            return;
        }
        for (&band, stats) in self.kernel.bands.iter().zip(&mut acc.0) {
            if let Some(value) = self.value(pixel, band) {
                if weight == 1.0 {
                    stats.update(value)
                } else {
                    stats.update_weighted(value, weight)
                }
            }
        }
    }
    fn get_sample(&self, _: T) -> Option<Self::Sample> {
        unreachable!("interleaved walker supplies the pixel")
    }
    fn accumulate_span(&self, _: &mut BandStats, _: &[T]) {
        unreachable!("interleaved walker uses individual pixels")
    }
    fn accumulate_span_multi(&self, _: &mut [BandStats], _: &[T]) {
        unreachable!("interleaved walker uses individual pixels")
    }
}
