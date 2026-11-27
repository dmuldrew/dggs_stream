//! A band of raster rows held natively (zero-copy tiles) or widened to `f64`,
//! for interpolation and overlay.

use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use tiff::decoder::DecodingResult;

use crate::aggregator::multi_horizon::borrowed::BorrowedSamples;
use crate::aggregator::simd::SimdSpanAccumulate;
use crate::error::{DggsStreamError, Result};
use crate::raster::geotiff::{GeoTiffStreamReader, MaskChunkCache};
use crate::raster::metadata::BandScaling;
use crate::raster::mosaic::TileDescriptor;
use crate::raster::prefetch::{decoded_buffer_bytes, PrefetchedChunkReader};
use crate::raster::RasterChunk;

use super::TransferAccumulator;

enum CachedChunk {
    AllNoData {
        nodata_bits: Option<u64>,
    },
    NativeSingleBand {
        bounds: RasterChunk,
        row_stride: usize,
        data: Arc<DecodingResult>,
    },
    DenseF64 {
        bounds: RasterChunk,
        planes: Arc<Vec<f64>>,
    },
}

impl CachedChunk {
    fn byte_size(&self) -> usize {
        match self {
            Self::AllNoData { .. } => 0,
            Self::NativeSingleBand { data, .. } => decoded_buffer_bytes(data),
            Self::DenseF64 { planes, .. } => planes.capacity() * size_of::<f64>(),
        }
    }
}

/// Decoded chunks kept between consecutive windows of one tile, so a chunk that spans
/// two windows (or supplies an interpolation margin) is decoded once.
#[derive(Default)]
pub struct ChunkCache {
    byte_budget: Option<usize>,
    mask: MaskChunkCache,
    chunks: HashMap<u32, CachedChunk>,
    /// Most decoded chunk bytes held at once (for `PeakDecodedBytes`).
    peak_bytes: usize,
}

impl ChunkCache {
    pub(in crate::transfer) fn with_budget(bytes: usize) -> Self {
        Self {
            byte_budget: Some(bytes),
            ..Self::default()
        }
    }

    /// Most decoded chunk bytes this cache has held at once.
    pub(in crate::transfer) fn peak_bytes(&self) -> usize {
        self.peak_bytes
    }

    /// Forget every chunk (when moving to another tile).
    pub fn clear(&mut self) {
        self.clear_and_recycle(None);
    }

    pub(in crate::transfer) fn clear_and_recycle(
        &mut self,
        prefetcher: Option<&PrefetchedChunkReader>,
    ) {
        if let Some(pf) = prefetcher {
            for (_, chunk) in self.chunks.drain() {
                if let CachedChunk::NativeSingleBand { data, .. } = chunk {
                    if let Ok(buf) = Arc::try_unwrap(data) {
                        pf.recycle(buf);
                    }
                }
            }
        } else {
            self.chunks.clear();
        }
        self.mask = Default::default();
    }

    fn retain_chunk_window(
        &mut self,
        chunk_rows: &RangeInclusive<u32>,
        chunk_cols: &Range<u32>,
        chunks_across: u32,
        prefetcher: Option<&PrefetchedChunkReader>,
    ) {
        let keep = |index: u32| {
            let chunk_row = index / chunks_across;
            let chunk_col = index % chunks_across;
            chunk_rows.contains(&chunk_row) && chunk_cols.contains(&chunk_col)
        };
        if let Some(pf) = prefetcher {
            for (_, chunk) in self.chunks.extract_if(|&index, _| !keep(index)) {
                if let CachedChunk::NativeSingleBand { data, .. } = chunk {
                    if let Ok(buf) = Arc::try_unwrap(data) {
                        pf.recycle(buf);
                    }
                }
            }
        } else {
            self.chunks.retain(|&index, _| keep(index));
        }
    }
}

/// Whether chunk `index` carries no readable data for `band_indices` or `alpha_index`.
#[inline]
pub(super) fn chunk_is_absent(
    reader: &GeoTiffStreamReader,
    index: u32,
    band_indices: &[usize],
    alpha_index: Option<usize>,
) -> bool {
    alpha_index.is_some_and(|a| !reader.band_chunk_present(index, a))
        || !band_indices
            .iter()
            .any(|&b| reader.band_chunk_present(index, b))
}

/// Whether a tile and band selection qualify for zero-copy single-band native windows.
#[inline]
pub(super) fn supports_native_tiled_window(tile: &TileDescriptor, band_count: usize) -> bool {
    let meta = &tile.reader.metadata;
    band_count == 1
        && meta.samples_per_pixel.max(1) == 1
        && meta.alpha_sample.is_none()
        && !meta.has_internal_mask()
}

struct NativeChunk {
    bounds: RasterChunk,
    row_stride: usize,
    data: Arc<DecodingResult>,
}

struct TiledNativeWindow {
    chunk_width: u32,
    chunk_height: u32,
    first_chunk_row: u32,
    first_chunk_col: u32,
    chunk_rows: u32,
    chunk_cols: u32,
    chunks: Vec<Option<NativeChunk>>,
    nodata: Option<f64>,
    scaling: BandScaling,
}

impl TiledNativeWindow {
    #[inline]
    fn chunk_at(&self, col: u32, row: u32) -> Option<&NativeChunk> {
        let chunk_row = row / self.chunk_height;
        let chunk_col = col / self.chunk_width;
        let rel_row = chunk_row.checked_sub(self.first_chunk_row)?;
        let rel_col = chunk_col.checked_sub(self.first_chunk_col)?;
        if rel_col >= self.chunk_cols {
            return None;
        }
        let slot = rel_row as usize * self.chunk_cols as usize + rel_col as usize;
        self.chunks.get(slot)?.as_ref()
    }

    #[inline]
    fn pixel_value(&self, col: u32, row: u32) -> Option<f64> {
        let chunk = self.chunk_at(col, row)?;
        let local_row = (row - chunk.bounds.row_offset) as usize;
        let local_col = (col - chunk.bounds.col_offset) as usize;
        let idx = local_row * chunk.row_stride + local_col;
        let samples = BorrowedSamples::from(chunk.data.as_ref());
        crate::dispatch_samples!(samples, self.nodata, |slice, nd| {
            slice.get(idx).copied().and_then(|v| {
                SimdSpanAccumulate::is_valid(v, nd)
                    .then(|| self.scaling.apply(SimdSpanAccumulate::to_f64_val(v)))
                    .filter(|scaled| scaled.is_finite())
            })
        })
    }

    fn accumulate_unit_span<A: TransferAccumulator>(
        &self,
        row: u32,
        col_start: u32,
        col_end: u32,
        acc: &mut A,
    ) -> usize {
        let chunk_row = row / self.chunk_height;
        let Some(rel_row) = chunk_row.checked_sub(self.first_chunk_row) else {
            return 0;
        };
        let c_start = (col_start / self.chunk_width).max(self.first_chunk_col);
        let c_end = col_end
            .div_ceil(self.chunk_width)
            .min(self.first_chunk_col + self.chunk_cols);
        let mut valid = 0;
        for chunk_col in c_start..c_end {
            let rel_col = chunk_col - self.first_chunk_col;
            let slot = rel_row as usize * self.chunk_cols as usize + rel_col as usize;
            let Some(Some(chunk)) = self.chunks.get(slot) else {
                continue;
            };
            let tile_c0 = chunk.bounds.col_offset;
            let tile_c1 = tile_c0 + chunk.bounds.width;
            let seg_c0 = col_start.max(tile_c0);
            let seg_c1 = col_end.min(tile_c1);
            if seg_c0 >= seg_c1 {
                continue;
            }
            let local_row = (row - chunk.bounds.row_offset) as usize;
            let local_c0 = (seg_c0 - tile_c0) as usize;
            let len = (seg_c1 - seg_c0) as usize;
            let samples = BorrowedSamples::from(chunk.data.as_ref());
            let sub = samples
                .window(local_row * chunk.row_stride + local_c0, len)
                .expect("validated native chunk bounds");
            valid += acc.add_band_native_unit_slice(0, sub, self.nodata, self.scaling);
        }
        valid
    }
}

enum WindowStorage {
    DenseF64(Vec<f64>),
    TiledNative(TiledNativeWindow),
}

/// Selected bands’ values over a contiguous range of rows and the raster's full width.
/// Exposed pixels behave as `f64` where NoData and non-finite samples yield `None`.
pub struct RowWindow {
    width: u32,
    raster_height: u32,
    first_row: u32,
    rows: u32,
    storage: WindowStorage,
}

/// What to read from a tile: the band and how stored values become values.
#[derive(Debug, Clone, Copy)]
pub struct BandSelection {
    /// 1-based band.
    pub band: usize,
    /// NoData override; `None` uses the raster's own.
    pub nodata: Option<f64>,
    /// Apply the band's GDAL scale and offset.
    pub apply_scale_offset: bool,
}

impl RowWindow {
    /// A window over the given values (row-major, `width` per row), starting at
    /// `first_row` of a raster `raster_height` rows tall.
    pub fn from_values(width: u32, raster_height: u32, first_row: u32, values: Vec<f64>) -> Self {
        let rows = if width == 0 {
            0
        } else {
            (values.len() / width as usize) as u32
        };
        Self {
            width,
            raster_height,
            first_row,
            rows,
            storage: WindowStorage::DenseF64(values),
        }
    }

    /// Raster width in pixels.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Height of the whole raster in rows (not of the window).
    pub fn raster_height(&self) -> u32 {
        self.raster_height
    }

    /// The rows the window holds.
    pub fn rows(&self) -> Range<u32> {
        self.first_row..self.first_row + self.rows
    }

    /// Returns `true` if any chunk intersecting the pixel bounding box may contain valid data
    /// (always `true` for dense `f64` windows).
    #[inline]
    pub(super) fn bbox_has_any_data(&self, min_x: f64, max_x: f64, min_y: f64, max_y: f64) -> bool {
        if !(min_x < max_x && min_y < max_y) {
            return false;
        }
        match &self.storage {
            WindowStorage::DenseF64(_) => true,
            WindowStorage::TiledNative(native) => {
                if native.chunk_rows == 0
                    || native.chunk_cols == 0
                    || self.rows == 0
                    || self.width == 0
                {
                    return false;
                }
                let r_lo = min_y.floor().max(f64::from(self.first_row));
                let r_hi = max_y.ceil().min(f64::from(self.first_row + self.rows));
                let c_lo = min_x.floor().max(0.0);
                let c_hi = max_x.ceil().min(f64::from(self.width));
                if !(r_lo < r_hi && c_lo < c_hi) {
                    return false;
                }
                let r0 = r_lo as u32;
                let r1 = (r_hi as u32).saturating_sub(1);
                let c0 = c_lo as u32;
                let c1 = (c_hi as u32).saturating_sub(1);
                let cr0 = (r0 / native.chunk_height).max(native.first_chunk_row);
                let cr1 =
                    (r1 / native.chunk_height).min(native.first_chunk_row + native.chunk_rows - 1);
                let cc0 = (c0 / native.chunk_width).max(native.first_chunk_col);
                let cc1 =
                    (c1 / native.chunk_width).min(native.first_chunk_col + native.chunk_cols - 1);
                if cr0 > cr1 || cc0 > cc1 {
                    return false;
                }
                for cr in cr0..=cr1 {
                    let rel_row = (cr - native.first_chunk_row) as usize;
                    for cc in cc0..=cc1 {
                        let rel_col = (cc - native.first_chunk_col) as usize;
                        let slot = rel_row * native.chunk_cols as usize + rel_col;
                        if native.chunks.get(slot).is_some_and(Option::is_some) {
                            return true;
                        }
                    }
                }
                false
            }
        }
    }

    /// The value of pixel `(col, row)`; `None` if it is NoData or outside the window.
    #[inline]
    pub fn get(&self, col: i64, row: i64) -> Option<f64> {
        let r = row - i64::from(self.first_row);
        if col < 0 || col >= i64::from(self.width) || r < 0 || r >= i64::from(self.rows) {
            return None;
        }
        match &self.storage {
            WindowStorage::DenseF64(values) => {
                let value = values[r as usize * self.width as usize + col as usize];
                (!value.is_nan()).then_some(value)
            }
            WindowStorage::TiledNative(native) => native.pixel_value(col as u32, row as u32),
        }
    }

    /// Read `rows` of `tile` (clamped to the raster). Chunks are decoded through
    /// `cache`, which afterwards holds only the chunks this window used.
    pub fn load(
        tile: &TileDescriptor,
        selection: BandSelection,
        rows: Range<u32>,
        cache: &mut ChunkCache,
    ) -> Result<Self> {
        Self::load_bands(
            tile,
            &[selection.band],
            selection,
            rows,
            0..tile.reader.metadata.width,
            cache,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn load_bands(
        tile: &TileDescriptor,
        bands: &[usize],
        selection: BandSelection,
        rows: Range<u32>,
        cols: Range<u32>,
        cache: &mut ChunkCache,
        prefetcher: Option<&PrefetchedChunkReader>,
        profile: Option<&crate::run_profile::RunProfile>,
    ) -> Result<Self> {
        debug_assert!(!bands.is_empty());
        let meta = &tile.reader.metadata;
        let (width, height) = (meta.width, meta.height);
        let rows = rows.start.min(height)..rows.end.min(height);
        let cols = cols.start.min(width)..cols.end.min(width);
        let layout = tile.reader.chunk_layout;
        let plane_len = rows.len() * width as usize;
        if rows.is_empty() || cols.is_empty() || width == 0 {
            let values = vec![f64::NAN; plane_len * bands.len()];
            return Ok(Self {
                width,
                raster_height: height,
                first_row: rows.start,
                rows: rows.len() as u32,
                storage: WindowStorage::DenseF64(values),
            });
        }
        let chunk_rows = rows.start / layout.chunk_height..=(rows.end - 1) / layout.chunk_height;
        let chunk_cols = cols.start / layout.chunk_width..cols.end.div_ceil(layout.chunk_width);
        cache.retain_chunk_window(&chunk_rows, &chunk_cols, layout.chunks_across, prefetcher);
        let mut cached_bytes = cache
            .chunks
            .values()
            .map(CachedChunk::byte_size)
            .sum::<usize>();
        let nodata = selection.nodata.or(meta.nodata);
        let spp = usize::from(meta.samples_per_pixel.max(1));

        // A pixel whose alpha sample is zero is transparent: missing, as in the centre
        // kernel (`walker::is_transparent`).
        let alpha_index = meta.alpha_sample.map(usize::from).filter(|&a| a < spp);
        let layouts: Vec<_> = bands
            .iter()
            .map(|&band| {
                let scaling = if selection.apply_scale_offset {
                    meta.band_scaling(band)
                } else {
                    BandScaling::IDENTITY
                };
                (
                    Layout {
                        spp,
                        band_index: band.saturating_sub(1).min(spp - 1),
                        alpha_index,
                    },
                    scaling,
                )
            })
            .collect();
        let band_indices: Vec<_> = layouts.iter().map(|(l, _)| l.band_index).collect();

        let chunk_rows_count = (*chunk_rows.end() - *chunk_rows.start() + 1) as usize;
        let chunk_cols_count = chunk_cols.len();
        let est_window_bytes = chunk_rows_count
            .saturating_mul(chunk_cols_count)
            .saturating_mul(tile.reader.decoded_chunk_bytes());
        let has_dense_cached = cache
            .chunks
            .values()
            .any(|c| matches!(c, CachedChunk::DenseF64 { .. }));
        let use_native = spp == 1
            && alpha_index.is_none()
            && !meta.has_internal_mask()
            && bands.len() == 1
            && !has_dense_cached
            && cache
                .byte_budget
                .is_none_or(|limit| est_window_bytes <= limit);

        let mut native_chunks = use_native.then(|| {
            (0..chunk_rows_count * chunk_cols_count)
                .map(|_| None)
                .collect::<Vec<Option<NativeChunk>>>()
        });
        let mut values = if use_native {
            Vec::new()
        } else {
            vec![f64::NAN; plane_len * bands.len()]
        };

        // Open lazily so cached and sparse chunks need no TIFF decoder.
        let mut decoder = None;
        let first_chunk_row = *chunk_rows.start();
        let first_chunk_col = chunk_cols.start;
        for chunk_row in chunk_rows {
            for chunk_col in chunk_cols.clone() {
                let index = chunk_row * layout.chunks_across + chunk_col;
                let bounds = layout.get_chunk_bounds(index, width, height);
                if bounds.width == 0 || bounds.height == 0 {
                    continue;
                }
                if chunk_is_absent(&tile.reader, index, &band_indices, alpha_index) {
                    continue;
                }

                if let Some(cached) = cache.chunks.get(&index) {
                    let mut cache_hit = true;
                    match cached {
                        CachedChunk::AllNoData { nodata_bits }
                            if *nodata_bits == nodata.map(f64::to_bits) => {}
                        CachedChunk::AllNoData { .. } => {
                            cache_hit = false;
                        }
                        CachedChunk::NativeSingleBand {
                            bounds: cbounds,
                            row_stride,
                            data,
                        } => {
                            if let Some(ref mut grid) = native_chunks {
                                let slot = (chunk_row - first_chunk_row) as usize
                                    * chunk_cols_count
                                    + (chunk_col - first_chunk_col) as usize;
                                grid[slot] = Some(NativeChunk {
                                    bounds: *cbounds,
                                    row_stride: *row_stride,
                                    data: Arc::clone(data),
                                });
                            } else {
                                let planes = convert_chunk_to_dense(
                                    tile,
                                    index,
                                    *cbounds,
                                    data.as_ref(),
                                    nodata,
                                    &layouts,
                                    &mut cache.mask,
                                )?;
                                copy_dense_to_window(
                                    &planes,
                                    *cbounds,
                                    &rows,
                                    width,
                                    plane_len,
                                    &mut values,
                                );
                            }
                        }
                        CachedChunk::DenseF64 {
                            bounds: cbounds,
                            planes,
                        } => {
                            copy_dense_to_window(
                                planes,
                                *cbounds,
                                &rows,
                                width,
                                plane_len,
                                &mut values,
                            );
                        }
                    }
                    if cache_hit {
                        continue;
                    }
                }

                let (bounds, data) = if let Some(pf) = prefetcher {
                    let stall = profile.map(|_| crate::run_profile::PhaseTimer::start());
                    let next = pf.next_chunk();
                    if let (Some(profile), Some(timer)) = (profile, stall) {
                        timer.stop(profile, crate::run_profile::Phase::Stall);
                    }
                    let (got_idx, bounds, data) = next.ok_or_else(|| {
                        DggsStreamError::StreamFailed(
                            "prefetch queue ended before window chunk".into(),
                        )
                    })??;
                    if got_idx != index {
                        return Err(DggsStreamError::StreamFailed(format!(
                            "prefetch chunk mismatch: expected {index}, got {got_idx}"
                        )));
                    }
                    (bounds, data)
                } else {
                    let decoder = match &mut decoder {
                        Some(decoder) => decoder,
                        slot @ None => slot.insert(tile.reader.open_decoder()?),
                    };
                    let (bounds, data) = decoder.read_chunk(index)?;
                    if let Some(profile) = profile {
                        profile.count(
                            crate::run_profile::Counter::Chunks,
                            tile.reader.physical_chunks_present(index),
                        );
                        profile.count(
                            crate::run_profile::Counter::DecodedBytes,
                            decoded_buffer_bytes(&data) as u64,
                        );
                    }
                    (bounds, data)
                };

                if let Some(ref mut grid) = native_chunks {
                    let row_stride = validated_native_stride(&bounds, &data)?;
                    if BorrowedSamples::from(&data).all_nodata(nodata) {
                        if let Some(pf) = prefetcher {
                            pf.recycle(data);
                        }
                        cache.chunks.insert(
                            index,
                            CachedChunk::AllNoData {
                                nodata_bits: nodata.map(f64::to_bits),
                            },
                        );
                        continue;
                    }
                    let bytes = decoded_buffer_bytes(&data);
                    if cache
                        .byte_budget
                        .is_some_and(|limit| bytes > limit.saturating_sub(cached_bytes))
                    {
                        cache.clear_and_recycle(prefetcher);
                        cached_bytes = 0;
                    }
                    let data = Arc::new(data);
                    let slot = (chunk_row - first_chunk_row) as usize * chunk_cols_count
                        + (chunk_col - first_chunk_col) as usize;
                    grid[slot] = Some(NativeChunk {
                        bounds,
                        row_stride,
                        data: Arc::clone(&data),
                    });
                    cached_bytes += bytes;
                    cache.peak_bytes = cache.peak_bytes.max(cached_bytes);
                    cache.chunks.insert(
                        index,
                        CachedChunk::NativeSingleBand {
                            bounds,
                            row_stride,
                            data,
                        },
                    );
                } else {
                    let chunk_len = bounds.width as usize * bounds.height as usize;
                    let bytes = chunk_len * bands.len() * size_of::<f64>();
                    if cache
                        .byte_budget
                        .is_some_and(|limit| bytes > limit.saturating_sub(cached_bytes))
                    {
                        // Previous chunks have already been copied into their windows.
                        // An oversized chunk may be decoded only in isolation.
                        cache.clear_and_recycle(prefetcher);
                        cached_bytes = 0;
                    }
                    let out = convert_chunk_to_dense(
                        tile,
                        index,
                        bounds,
                        &data,
                        nodata,
                        &layouts,
                        &mut cache.mask,
                    )?;
                    if let Some(pf) = prefetcher {
                        pf.recycle(data);
                    }
                    copy_dense_to_window(&out, bounds, &rows, width, plane_len, &mut values);
                    cached_bytes += out.capacity() * size_of::<f64>();
                    cache.peak_bytes = cache.peak_bytes.max(cached_bytes);
                    cache.chunks.insert(
                        index,
                        CachedChunk::DenseF64 {
                            bounds,
                            planes: Arc::new(out),
                        },
                    );
                }
            }
        }

        let storage = match native_chunks {
            Some(chunks) => WindowStorage::TiledNative(TiledNativeWindow {
                chunk_width: layout.chunk_width,
                chunk_height: layout.chunk_height,
                first_chunk_row,
                first_chunk_col,
                chunk_rows: chunk_rows_count as u32,
                chunk_cols: chunk_cols_count as u32,
                chunks,
                nodata,
                scaling: layouts[0].1,
            }),
            None => WindowStorage::DenseF64(values),
        };
        Ok(Self {
            width,
            raster_height: height,
            first_row: rows.start,
            rows: rows.len() as u32,
            storage,
        })
    }

    /// Value of a known in-window pixel at `(row_off, col)` in `slot`.
    #[inline]
    pub(super) fn band_value_at(&self, slot: usize, row_off: usize, col: usize) -> Option<f64> {
        match &self.storage {
            WindowStorage::DenseF64(values) => {
                let value = values[slot * self.rows as usize * self.width as usize
                    + row_off * self.width as usize
                    + col];
                value.is_finite().then_some(value)
            }
            WindowStorage::TiledNative(native) => {
                debug_assert_eq!(slot, 0);
                native.pixel_value(col as u32, self.first_row + row_off as u32)
            }
        }
    }

    /// Accumulate a contiguous horizontal unit-weight pixel span (`col_start..col_end`)
    /// at window row `row_off` into `acc`, returning the number of valid observations.
    #[inline]
    pub(super) fn accumulate_unit_span<A: TransferAccumulator>(
        &self,
        slot: usize,
        row_off: usize,
        col_start: usize,
        col_end: usize,
        acc: &mut A,
    ) -> usize {
        if col_start >= col_end {
            return 0;
        }
        match &self.storage {
            WindowStorage::DenseF64(values) => {
                let base = slot * self.rows as usize * self.width as usize
                    + row_off * self.width as usize
                    + col_start;
                acc.add_band_unit_slice(slot, &values[base..base + (col_end - col_start)])
            }
            WindowStorage::TiledNative(native) => {
                debug_assert_eq!(slot, 0);
                native.accumulate_unit_span(
                    self.first_row + row_off as u32,
                    col_start as u32,
                    col_end as u32,
                    acc,
                )
            }
        }
    }
}

fn validated_native_stride(bounds: &RasterChunk, data: &DecodingResult) -> Result<usize> {
    let samples_len = match data {
        DecodingResult::U8(v) => v.len(),
        DecodingResult::U16(v) => v.len(),
        DecodingResult::U32(v) => v.len(),
        DecodingResult::U64(v) => v.len(),
        DecodingResult::I8(v) => v.len(),
        DecodingResult::I16(v) => v.len(),
        DecodingResult::I32(v) => v.len(),
        DecodingResult::I64(v) => v.len(),
        DecodingResult::F32(v) => v.len(),
        DecodingResult::F64(v) => v.len(),
        DecodingResult::F16(v) => v.len(),
    };
    let width = bounds.width as usize;
    let height = bounds.height as usize;
    let min_len = width
        .checked_mul(height)
        .ok_or_else(|| DggsStreamError::InvalidMetadata("sample window overflow".into()))?;
    let row_stride = samples_len.checked_div(height).unwrap_or(width).max(width);
    if samples_len < min_len || (height > 0 && (height - 1) * row_stride + width > samples_len) {
        return Err(DggsStreamError::InvalidMetadata(
            "decoded chunk shorter than its declared dimensions".into(),
        ));
    }
    Ok(row_stride)
}

fn convert_chunk_to_dense(
    tile: &TileDescriptor,
    index: u32,
    bounds: RasterChunk,
    data: &DecodingResult,
    nodata: Option<f64>,
    layouts: &[(Layout, BandScaling)],
    mask_cache: &mut MaskChunkCache,
) -> Result<Vec<f64>> {
    let chunk_len = bounds.width as usize * bounds.height as usize;
    let mut out = Vec::with_capacity(chunk_len * layouts.len());
    let samples = BorrowedSamples::from(data);
    crate::dispatch_samples!(samples, nodata, |slice, nd| {
        for &(layout, scaling) in layouts {
            if tile.reader.band_chunk_present(index, layout.band_index) {
                convert(slice, nd, &bounds, layout, scaling, &mut out);
            } else {
                out.resize(out.len() + chunk_len, f64::NAN);
            }
        }
    });
    if let Some(mask) = tile.reader.read_mask(bounds, mask_cache)? {
        for i in 0..chunk_len {
            let x = bounds.col_offset as usize + i % bounds.width as usize;
            let y = bounds.row_offset as usize + i / bounds.width as usize;
            if !mask.is_valid(x, y) {
                for plane in out.chunks_exact_mut(chunk_len) {
                    plane[i] = f64::NAN;
                }
            }
        }
    }
    Ok(out)
}

fn copy_dense_to_window(
    planes: &[f64],
    bounds: RasterChunk,
    rows: &Range<u32>,
    width: u32,
    plane_len: usize,
    values: &mut [f64],
) {
    let top = bounds.row_offset.max(rows.start);
    let bottom = (bounds.row_offset + bounds.height).min(rows.end);
    if top >= bottom {
        return;
    }
    let copy_w = bounds.width as usize;
    let chunk_len = copy_w * bounds.height as usize;
    for (src_plane, dst_plane) in planes
        .chunks_exact(chunk_len)
        .zip(values.chunks_exact_mut(plane_len))
    {
        for row in top..bottom {
            let src = (row - bounds.row_offset) as usize * copy_w;
            let dst = (row - rows.start) as usize * width as usize + bounds.col_offset as usize;
            dst_plane[dst..dst + copy_w].copy_from_slice(&src_plane[src..src + copy_w]);
        }
    }
}

/// Where a pixel's samples sit in an interleaved chunk.
#[derive(Clone, Copy)]
struct Layout {
    /// Samples per pixel.
    spp: usize,
    /// Zero-based sample of the selected band.
    band_index: usize,
    /// Zero-based alpha sample, if the raster has one.
    alpha_index: Option<usize>,
}

/// Append one band of a decoded chunk to `out` as `f64`, row-major over the chunk's own
/// `bounds` (decoded tiles may be padded to the full tile width). NoData and
/// transparent (zero-alpha) pixels become NaN.
fn convert<T: SimdSpanAccumulate>(
    slice: &[T],
    nodata: Option<T>,
    bounds: &RasterChunk,
    layout: Layout,
    scaling: BandScaling,
    out: &mut Vec<f64>,
) {
    let Layout {
        spp,
        band_index,
        alpha_index,
    } = layout;
    let (width, height) = (bounds.width as usize, bounds.height as usize);
    let stride = (slice.len() / (spp * height)).max(width);
    for row in 0..height {
        for col in 0..width {
            let pixel = (row * stride + col) * spp;
            let transparent = alpha_index
                .and_then(|a| slice.get(pixel + a))
                .is_some_and(|alpha| alpha.to_f64_val() == 0.0);
            let value = slice
                .get(pixel + band_index)
                .filter(|_| !transparent)
                .copied()
                .filter(|v| v.is_valid(nodata))
                .map(|v| scaling.apply(v.to_f64_val()))
                .filter(|v| v.is_finite());
            out.push(value.unwrap_or(f64::NAN));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raster::geotiff::{GeoTiffStreamReader, DECODER_OPENS};
    use tiff::encoder::{colortype, TiffEncoder};
    use tiff::tags::Tag;

    #[test]
    fn sparse_and_cached_strips_skip_decoder_setup_and_misses_share_it() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = TiffEncoder::new(file.as_file()).unwrap();
        let mut image = encoder.new_image::<colortype::Gray8>(2, 4).unwrap();
        image.rows_per_strip(1).unwrap();
        image
            .encoder()
            .write_tag(Tag::ModelPixelScaleTag, &[0.01f64, 0.01, 0.0][..])
            .unwrap();
        image
            .encoder()
            .write_tag(
                Tag::ModelTiepointTag,
                &[0.0f64, 0.0, 0.0, -122.0, 38.0, 0.0][..],
            )
            .unwrap();
        image.write_data(&[0, 0, 11, 12, 21, 22, 0, 0]).unwrap();
        let mut reader = GeoTiffStreamReader::open(file.path()).unwrap();
        let info = Arc::make_mut(reader.chunk_info.as_mut().unwrap());
        for i in [0, 3] {
            Arc::make_mut(&mut info.chunk_offsets)[i] = 0;
            Arc::make_mut(&mut info.chunk_bytes)[i] = 0;
        }
        let tile = TileDescriptor::new(0, reader, Some("EPSG:4326")).unwrap();
        let band = BandSelection {
            band: 1,
            nodata: None,
            apply_scale_offset: false,
        };
        let mut cache = ChunkCache::default();
        let before = DECODER_OPENS.get();
        let first = RowWindow::load(&tile, band, 0..1, &mut cache).unwrap();
        assert_eq!(first.get(0, 0), None);
        assert_eq!(DECODER_OPENS.get(), before);
        let mixed = RowWindow::load(&tile, band, 0..3, &mut cache).unwrap();
        assert_eq!(
            (mixed.get(0, 0), mixed.get(0, 1), mixed.get(1, 2)),
            (None, Some(11.0), Some(22.0))
        );
        assert_eq!(DECODER_OPENS.get(), before + 1);
        let reused = RowWindow::load(&tile, band, 1..4, &mut cache).unwrap();
        assert_eq!(
            (reused.get(1, 1), reused.get(0, 2), reused.get(0, 3)),
            (Some(12.0), Some(21.0), None)
        );
        assert_eq!(DECODER_OPENS.get(), before + 1);
    }
}

#[cfg(test)]
mod cache_budget_tests {
    use super::*;
    use crate::raster::geotiff::GeoTiffStreamReader;

    #[test]
    fn decoded_tiles_within_a_window_respect_budget() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/internal-mask.tif");
        let tile = TileDescriptor::new(0, GeoTiffStreamReader::open(path).unwrap(), None).unwrap();
        let selection = BandSelection {
            band: 1,
            nodata: None,
            apply_scale_offset: true,
        };
        let mut unbounded = ChunkCache::default();
        let expected = RowWindow::load(&tile, selection, 0..8, &mut unbounded).unwrap();
        assert!(unbounded.chunks.len() > 1);
        let width = tile.reader.metadata.width as i64;
        for budget in [1, 2048] {
            let mut cache = ChunkCache::with_budget(budget);
            let actual = RowWindow::load(&tile, selection, 0..8, &mut cache).unwrap();
            assert_eq!(cache.chunks.len(), 1);
            for r in 0..8i64 {
                for c in 0..width {
                    assert_eq!(actual.get(c, r), expected.get(c, r), "({c}, {r})");
                }
            }
        }
    }

    #[test]
    fn native_single_band_budget_evicts_and_matches_unbounded() {
        use tiff::encoder::TiffEncoder;
        use tiff::tags::Tag;

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = TiffEncoder::new(file.as_file()).unwrap();
        let mut image = encoder
            .new_image::<tiff::encoder::colortype::Gray32Float>(8, 8)
            .unwrap();
        image.rows_per_strip(2).unwrap();
        image
            .encoder()
            .write_tag(Tag::ModelPixelScaleTag, &[0.01f64, 0.01, 0.0][..])
            .unwrap();
        image
            .encoder()
            .write_tag(
                Tag::ModelTiepointTag,
                &[0.0f64, 0.0, 0.0, -122.0, 38.0, 0.0][..],
            )
            .unwrap();
        let data: Vec<f32> = (0..64).map(|i| (i + 1) as f32).collect();
        image.write_data(&data).unwrap();

        let tile = TileDescriptor::new(
            0,
            GeoTiffStreamReader::open(file.path()).unwrap(),
            Some("EPSG:4326"),
        )
        .unwrap();
        let selection = BandSelection {
            band: 1,
            nodata: None,
            apply_scale_offset: false,
        };
        let mut unbounded = ChunkCache::default();
        let expected = RowWindow::load(&tile, selection, 0..8, &mut unbounded).unwrap();
        assert!(matches!(expected.storage, WindowStorage::TiledNative(_)));
        assert_eq!(unbounded.chunks.len(), 4);

        let mut bounded = ChunkCache::with_budget(1);
        let actual = RowWindow::load(&tile, selection, 0..8, &mut bounded).unwrap();
        assert!(matches!(actual.storage, WindowStorage::DenseF64(_)));
        assert_eq!(bounded.chunks.len(), 1);
        for r in 0..8i64 {
            for c in 0..8i64 {
                assert_eq!(actual.get(c, r), expected.get(c, r), "({c}, {r})");
            }
        }
    }

    fn write_raw_tiled_tiff(
        (width, height): (u32, u32),
        (tile_w, tile_h): (u32, u32),
        (sample_format, bits): (u16, u16),
        nodata: Option<&str>,
        gdal_meta: Option<&str>,
        tiles: &[Vec<u8>],
    ) -> tempfile::NamedTempFile {
        use std::io::Write;
        use tiff::encoder::TiffEncoder;
        use tiff::tags::Tag;

        let mut file = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = TiffEncoder::new(file.as_file_mut()).unwrap();
        let mut ifd = encoder.image_directory().unwrap();
        let mut offsets = Vec::with_capacity(tiles.len());
        let mut counts = Vec::with_capacity(tiles.len());
        for payload in tiles {
            if payload.is_empty() {
                offsets.push(0u32);
                counts.push(0u32);
            } else {
                offsets.push(u32::try_from(ifd.write_data(payload.as_slice()).unwrap()).unwrap());
                counts.push(u32::try_from(payload.len()).unwrap());
            }
        }
        ifd.write_tag(Tag::ImageWidth, width).unwrap();
        ifd.write_tag(Tag::ImageLength, height).unwrap();
        ifd.write_tag(Tag::BitsPerSample, bits).unwrap();
        ifd.write_tag(Tag::SampleFormat, sample_format).unwrap();
        ifd.write_tag(Tag::Compression, 1u16).unwrap();
        ifd.write_tag(Tag::PhotometricInterpretation, 1u16).unwrap();
        ifd.write_tag(Tag::SamplesPerPixel, 1u16).unwrap();
        ifd.write_tag(Tag::TileWidth, tile_w).unwrap();
        ifd.write_tag(Tag::TileLength, tile_h).unwrap();
        ifd.write_tag(Tag::TileOffsets, offsets.as_slice()).unwrap();
        ifd.write_tag(Tag::TileByteCounts, counts.as_slice())
            .unwrap();
        ifd.write_tag(Tag::ModelPixelScaleTag, &[0.001f64, 0.001, 0.0][..])
            .unwrap();
        ifd.write_tag(
            Tag::ModelTiepointTag,
            &[0.0f64, 0.0, 0.0, -122.0, 38.0, 0.0][..],
        )
        .unwrap();
        if let Some(nd) = nodata {
            ifd.write_tag(Tag::GdalNodata, nd).unwrap();
        }
        if let Some(meta) = gdal_meta {
            ifd.write_tag(Tag::Unknown(42112), meta).unwrap();
        }
        ifd.finish().unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn native_window_matches_dense_across_types_nodata_and_scaling() {
        use crate::aggregator::accumulator::ContinuousAccumulator;
        use crate::aggregator::categorical::CategoricalAccumulator;
        use crate::transfer::OverlayPartial;

        // 24x6 image tiled as 16x6 across 2 tiles (right tile is edge-cropped to 8 cols,
        // left tile has sparse/nodata/extreme values).
        let (w, h, tw, th) = (24u32, 6u32, 16u32, 6u32);
        let row = 1usize;

        // Case 1: u8 with scaling + nodata + uniform span + mixed span.
        let mut tile0_u8 = vec![0u8; (tw * th) as usize];
        for r in 0..th as usize {
            for c in 0..tw as usize {
                tile0_u8[r * tw as usize + c] = if c == 3 { 255 } else { ((r + c) % 5 + 1) as u8 };
            }
        }
        let tile1_u8 = vec![7u8; (tw * th) as usize];
        let gdal_neg_scale = r#"<GDALMetadata><Item name="SCALE" sample="0">-2.5</Item><Item name="OFFSET" sample="0">10.0</Item></GDALMetadata>"#;
        let f_u8 = write_raw_tiled_tiff(
            (w, h),
            (tw, th),
            (1, 8),
            Some("255"),
            Some(gdal_neg_scale),
            &[tile0_u8, tile1_u8],
        );
        let t_u8 = TileDescriptor::new(
            0,
            GeoTiffStreamReader::open(f_u8.path()).unwrap(),
            Some("EPSG:4326"),
        )
        .unwrap();
        let sel = BandSelection {
            band: 1,
            nodata: None,
            apply_scale_offset: true,
        };
        let mut native_cache = ChunkCache::default();
        let native_win = RowWindow::load(&t_u8, sel, 0..h, &mut native_cache).unwrap();
        let mut dense_cache = ChunkCache::with_budget(1);
        let dense_win = RowWindow::load(&t_u8, sel, 0..h, &mut dense_cache).unwrap();
        assert!(matches!(native_win.storage, WindowStorage::TiledNative(_)));
        assert!(matches!(dense_win.storage, WindowStorage::DenseF64(_)));

        let mut nat_cont = ContinuousAccumulator::default();
        let mut den_cont = ContinuousAccumulator::default();
        let mut nat_quant = ContinuousAccumulator::with_quantiles();
        let mut den_quant = ContinuousAccumulator::with_quantiles();
        let mut nat_part = OverlayPartial::default();
        let mut den_part = OverlayPartial::default();
        assert_eq!(
            native_win.accumulate_unit_span(0, row, 2, 22, &mut nat_cont),
            dense_win.accumulate_unit_span(0, row, 2, 22, &mut den_cont)
        );
        assert_eq!(
            native_win.accumulate_unit_span(0, row, 2, 22, &mut nat_quant),
            dense_win.accumulate_unit_span(0, row, 2, 22, &mut den_quant)
        );
        assert_eq!(
            native_win.accumulate_unit_span(0, row, 2, 22, &mut nat_part),
            dense_win.accumulate_unit_span(0, row, 2, 22, &mut den_part)
        );
        assert_eq!(nat_cont.count, den_cont.count);
        assert!((nat_cont.sum - den_cont.sum).abs() < 1e-12);
        assert!((nat_cont.min - den_cont.min).abs() < 1e-12);
        assert!((nat_cont.max - den_cont.max).abs() < 1e-12);
        assert!((nat_cont.m2 - den_cont.m2).abs() < 1e-12);
        assert!((nat_quant.quantile(0.5) - den_quant.quantile(0.5)).abs() < 1e-12);
        assert!((nat_part.stats.sum - den_part.stats.sum).abs() < 1e-12);

        // Categorical (identity scaling, u8): mixed across tile0 + uniform across tile1.
        let sel_cat = BandSelection {
            band: 1,
            nodata: None,
            apply_scale_offset: false,
        };
        let nat_win_cat =
            RowWindow::load(&t_u8, sel_cat, 0..h, &mut ChunkCache::default()).unwrap();
        let den_win_cat =
            RowWindow::load(&t_u8, sel_cat, 0..h, &mut ChunkCache::with_budget(1)).unwrap();
        let mut nat_cat = CategoricalAccumulator::default();
        let mut den_cat = CategoricalAccumulator::default();
        assert_eq!(
            nat_win_cat.accumulate_unit_span(0, row, 2, 22, &mut nat_cat),
            den_win_cat.accumulate_unit_span(0, row, 2, 22, &mut den_cat)
        );
        assert_eq!(nat_cat, den_cat);

        // Case 2: i16 with sparse second tile, and f32 with NaN/Inf/NoData.
        let mut tile0_i16 = Vec::with_capacity((tw * th) as usize * 2);
        for r in 0..th as usize {
            for c in 0..tw as usize {
                let v: i16 = if c == 4 {
                    -999
                } else {
                    (c as i16) - 10 + r as i16
                };
                tile0_i16.extend_from_slice(&v.to_le_bytes());
            }
        }
        let f_i16 = write_raw_tiled_tiff(
            (w, h),
            (tw, th),
            (2, 16),
            Some("-999"),
            None,
            &[tile0_i16, Vec::new()],
        );
        let t_i16 = TileDescriptor::new(
            0,
            GeoTiffStreamReader::open(f_i16.path()).unwrap(),
            Some("EPSG:4326"),
        )
        .unwrap();
        let nat_i16 = RowWindow::load(&t_i16, sel_cat, 0..h, &mut ChunkCache::default()).unwrap();
        let den_i16 =
            RowWindow::load(&t_i16, sel_cat, 0..h, &mut ChunkCache::with_budget(1)).unwrap();
        let (mut a1, mut a2) = (
            ContinuousAccumulator::default(),
            ContinuousAccumulator::default(),
        );
        assert_eq!(
            nat_i16.accumulate_unit_span(0, row, 0, w as usize, &mut a1),
            den_i16.accumulate_unit_span(0, row, 0, w as usize, &mut a2)
        );
        assert_eq!(a1.count, a2.count);
        assert!((a1.sum - a2.sum).abs() < 1e-12);
        assert!((a1.m2 - a2.m2).abs() < 1e-12);

        let mut tile0_f32 = Vec::with_capacity((tw * th) as usize * 4);
        let mut tile1_f32 = Vec::with_capacity((tw * th) as usize * 4);
        for _ in 0..th {
            for c in 0..tw {
                let v: f32 = match c {
                    1 => f32::NAN,
                    2 => -9999.0,
                    3 => f32::INFINITY,
                    _ => (c + 1) as f32 * 0.5,
                };
                tile0_f32.extend_from_slice(&v.to_le_bytes());
            }
            for c in 0..tw {
                tile1_f32.extend_from_slice(&((c as f32) + 2.25).to_le_bytes());
            }
        }
        let f_f32 = write_raw_tiled_tiff(
            (w, h),
            (tw, th),
            (3, 32),
            Some("-9999"),
            None,
            &[tile0_f32, tile1_f32],
        );
        let t_f32 = TileDescriptor::new(
            0,
            GeoTiffStreamReader::open(f_f32.path()).unwrap(),
            Some("EPSG:4326"),
        )
        .unwrap();
        let nat_f32 = RowWindow::load(&t_f32, sel_cat, 0..h, &mut ChunkCache::default()).unwrap();
        let den_f32 =
            RowWindow::load(&t_f32, sel_cat, 0..h, &mut ChunkCache::with_budget(1)).unwrap();
        let (mut f1, mut f2) = (
            ContinuousAccumulator::default(),
            ContinuousAccumulator::default(),
        );
        assert_eq!(
            nat_f32.accumulate_unit_span(0, row, 0, w as usize, &mut f1),
            den_f32.accumulate_unit_span(0, row, 0, w as usize, &mut f2)
        );
        assert_eq!(f1.count, f2.count);
        assert!((f1.sum - f2.sum).abs() < 1e-12);
        assert!((f1.m2 - f2.m2).abs() < 1e-12);

        // Case 3: f64 with scaling overflowing on `1e308` -> filtered per pixel identically to DenseF64.
        let mut tile0_f64 = Vec::with_capacity((tw * th) as usize * 8);
        let mut tile1_f64 = Vec::with_capacity((tw * th) as usize * 8);
        for _ in 0..th {
            for c in 0..tw {
                let v: f64 = match c {
                    1 => f64::NAN,
                    2 => -9999.0,
                    5 => 1e308,
                    _ => (c + 1) as f64,
                };
                tile0_f64.extend_from_slice(&v.to_le_bytes());
            }
            for c in 0..tw {
                tile1_f64.extend_from_slice(&((c + 10) as f64).to_le_bytes());
            }
        }
        let gdal_big_scale = r#"<GDALMetadata><Item name="SCALE" sample="0">10.0</Item><Item name="OFFSET" sample="0">1.0</Item></GDALMetadata>"#;
        let f_f64 = write_raw_tiled_tiff(
            (w, h),
            (tw, th),
            (3, 64),
            Some("-9999"),
            Some(gdal_big_scale),
            &[tile0_f64, tile1_f64],
        );
        let t_f64 = TileDescriptor::new(
            0,
            GeoTiffStreamReader::open(f_f64.path()).unwrap(),
            Some("EPSG:4326"),
        )
        .unwrap();
        let nat_f64 = RowWindow::load(&t_f64, sel, 0..h, &mut ChunkCache::default()).unwrap();
        let den_f64 = RowWindow::load(&t_f64, sel, 0..h, &mut ChunkCache::with_budget(1)).unwrap();
        let (mut s1, mut s2) = (
            ContinuousAccumulator::default(),
            ContinuousAccumulator::default(),
        );
        assert_eq!(
            nat_f64.accumulate_unit_span(0, row, 0, w as usize, &mut s1),
            den_f64.accumulate_unit_span(0, row, 0, w as usize, &mut s2)
        );
        assert_eq!(s1.count, s2.count);
        assert!((s1.sum - s2.sum).abs() < 1e-12);
        assert!((s1.m2 - s2.m2).abs() < 1e-12);
    }

    #[test]
    fn all_nodata_native_chunks_are_skipped_recycled_and_match_dense() {
        use crate::aggregator::accumulator::ContinuousAccumulator;

        // 32x8 tiled as 16x8: left tile all NoData, right tile mixed valid/NoData.
        let (w, h, tw, th) = (32u32, 8u32, 16u32, 8u32);
        let tile0_f32: Vec<u8> = (0..tw * th)
            .flat_map(|i| {
                let v = if i % 2 == 0 { -9999.0f32 } else { f32::NAN };
                v.to_le_bytes()
            })
            .collect();
        let tile1_f32: Vec<u8> = (0..tw * th)
            .flat_map(|i| {
                let v = if i % 5 == 0 {
                    -9999.0f32
                } else {
                    (i % 11 + 1) as f32
                };
                v.to_le_bytes()
            })
            .collect();
        let f_f32 = write_raw_tiled_tiff(
            (w, h),
            (tw, th),
            (3, 32),
            Some("-9999"),
            None,
            &[tile0_f32, tile1_f32.clone()],
        );
        let t_f32 = TileDescriptor::new(
            0,
            GeoTiffStreamReader::open(f_f32.path()).unwrap(),
            Some("EPSG:4326"),
        )
        .unwrap();
        let sel = BandSelection {
            band: 1,
            nodata: None,
            apply_scale_offset: false,
        };
        let pf = PrefetchedChunkReader::spawn_with_workers(t_f32.reader.clone(), vec![0, 1], 16, 2);
        let mut cache = ChunkCache::default();
        let native_win =
            RowWindow::load_bands(&t_f32, &[1], sel, 0..h, 0..w, &mut cache, Some(&pf), None)
                .unwrap();
        assert!(matches!(native_win.storage, WindowStorage::TiledNative(_)));
        assert!(matches!(
            cache.chunks.get(&0),
            Some(CachedChunk::AllNoData { .. })
        ));
        assert_eq!(cache.chunks[&0].byte_size(), 0);
        assert!(matches!(
            cache.chunks.get(&1),
            Some(CachedChunk::NativeSingleBand { .. })
        ));

        // Bbox entirely inside tile 0 reports false; crossing into tile 1 reports true.
        assert!(!native_win.bbox_has_any_data(0.5, 15.5, 1.0, 7.0));
        assert!(native_win.bbox_has_any_data(15.5, 16.5, 1.0, 7.0));
        assert!(native_win.bbox_has_any_data(16.0, 32.0, 0.0, 8.0));
        assert!(!native_win.bbox_has_any_data(32.0, 40.0, 0.0, 8.0));
        assert!(!native_win.bbox_has_any_data(f64::NAN, 10.0, 0.0, 4.0));

        let mut dense_cache = ChunkCache::with_budget(1);
        let dense_win = RowWindow::load(&t_f32, sel, 0..h, &mut dense_cache).unwrap();
        assert!(matches!(dense_win.storage, WindowStorage::DenseF64(_)));
        for r in 0..h as usize {
            for c in 0..w as i64 {
                assert_eq!(native_win.get(c, r as i64), dense_win.get(c, r as i64));
            }
            let (mut a1, mut a2) = (
                ContinuousAccumulator::default(),
                ContinuousAccumulator::default(),
            );
            assert_eq!(
                native_win.accumulate_unit_span(0, r, 0, w as usize, &mut a1),
                dense_win.accumulate_unit_span(0, r, 0, w as usize, &mut a2)
            );
            assert_eq!(a1.count, a2.count);
            assert!((a1.sum - a2.sum).abs() < 1e-12);
            assert!((a1.m2 - a2.m2).abs() < 1e-12);
        }

        // Also check unsigned integer nodata (u8) all-nodata chunk detection.
        let tile0_u8 = vec![255u8; (tw * th) as usize];
        let tile1_u8 = vec![9u8; (tw * th) as usize];
        let f_u8 = write_raw_tiled_tiff(
            (w, h),
            (tw, th),
            (1, 8),
            Some("255"),
            None,
            &[tile0_u8, tile1_u8],
        );
        let t_u8 = TileDescriptor::new(
            0,
            GeoTiffStreamReader::open(f_u8.path()).unwrap(),
            Some("EPSG:4326"),
        )
        .unwrap();
        let mut cache_u8 = ChunkCache::default();
        let nat_u8 = RowWindow::load(&t_u8, sel, 0..h, &mut cache_u8).unwrap();
        assert!(matches!(
            cache_u8.chunks.get(&0),
            Some(CachedChunk::AllNoData { .. })
        ));
        assert!(!nat_u8.bbox_has_any_data(0.0, 16.0, 0.0, 8.0));
        assert!(nat_u8.bbox_has_any_data(16.0, 32.0, 0.0, 8.0));
        assert_eq!(nat_u8.get(4, 2), None);
        assert_eq!(nat_u8.get(20, 2), Some(9.0));
    }
}
