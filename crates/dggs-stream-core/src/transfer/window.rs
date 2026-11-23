//! A band of raster rows held as `f64`, for interpolation and overlay.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use crate::aggregator::multi_horizon::borrowed::BorrowedSamples;
use crate::aggregator::simd::SimdSpanAccumulate;
use crate::error::Result;
use crate::raster::mosaic::TileDescriptor;
use crate::raster::RasterChunk;

/// Selected bands’ values in separate planes over a contiguous range of rows and the raster's full width.
/// NoData (and any non-finite value) is NaN.
pub struct RowWindow {
    width: u32,
    raster_height: u32,
    first_row: u32,
    rows: u32,
    values: Vec<f64>,
}

/// Decoded chunks kept between consecutive windows of one tile, so a chunk that spans
/// two windows (or supplies an interpolation margin) is decoded once.
#[derive(Default)]
pub struct ChunkCache {
    byte_budget: Option<usize>,
    mask: crate::raster::geotiff::MaskChunkCache,
    chunks: HashMap<u32, Arc<Vec<f64>>>,
}

impl ChunkCache {
    pub(in crate::transfer) fn with_budget(bytes: usize) -> Self {
        Self {
            byte_budget: Some(bytes),
            ..Self::default()
        }
    }

    /// Forget every chunk (when moving to another tile).
    pub fn clear(&mut self) {
        self.chunks.clear();
        self.mask = Default::default();
    }
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
            values,
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

    /// The value of pixel `(col, row)`; `None` if it is NoData or outside the window.
    #[inline]
    pub fn get(&self, col: i64, row: i64) -> Option<f64> {
        let r = row - i64::from(self.first_row);
        if col < 0 || col >= i64::from(self.width) || r < 0 || r >= i64::from(self.rows) {
            return None;
        }
        let value = self.values[r as usize * self.width as usize + col as usize];
        (!value.is_nan()).then_some(value)
    }

    /// Read `rows` of `tile` (clamped to the raster). Chunks are decoded through
    /// `cache`, which afterwards holds only the chunks this window used.
    pub fn load(
        tile: &TileDescriptor,
        selection: BandSelection,
        rows: Range<u32>,
        cache: &mut ChunkCache,
    ) -> Result<Self> {
        Self::load_bands(tile, &[selection.band], selection, rows, cache, None)
    }

    pub(super) fn load_bands(
        tile: &TileDescriptor,
        bands: &[usize],
        selection: BandSelection,
        rows: Range<u32>,
        cache: &mut ChunkCache,
        profile: Option<&crate::run_profile::RunProfile>,
    ) -> Result<Self> {
        debug_assert!(!bands.is_empty());
        let meta = &tile.reader.metadata;
        let (width, height) = (meta.width, meta.height);
        let rows = rows.start.min(height)..rows.end.min(height);
        let layout = tile.reader.chunk_layout;
        let plane_len = rows.len() * width as usize;
        let mut values = vec![f64::NAN; plane_len * bands.len()];
        if rows.is_empty() || width == 0 {
            return Ok(Self::from_values(width, height, rows.start, values));
        }
        let chunk_rows = rows.start / layout.chunk_height..=(rows.end - 1) / layout.chunk_height;
        cache
            .chunks
            .retain(|index, _| chunk_rows.contains(&(index / layout.chunks_across)));
        let mut cached_bytes = cache
            .chunks
            .values()
            .map(|v| v.capacity() * size_of::<f64>())
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
                    crate::raster::metadata::BandScaling::IDENTITY
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
        // Open lazily so cached and sparse chunks need no TIFF decoder.
        let mut decoder = None;
        for chunk_row in chunk_rows {
            for chunk_col in 0..layout.chunks_across {
                let index = chunk_row * layout.chunks_across + chunk_col;
                let bounds = layout.get_chunk_bounds(index, width, height);
                if bounds.width == 0 || bounds.height == 0 {
                    continue;
                }
                let decoded = match cache.chunks.get(&index) {
                    Some(decoded) => Arc::clone(decoded),
                    None => {
                        let chunk_len = bounds.width as usize * bounds.height as usize;
                        let bytes = chunk_len * bands.len() * size_of::<f64>();
                        if cache
                            .byte_budget
                            .is_some_and(|limit| bytes > limit.saturating_sub(cached_bytes))
                        {
                            // Previous chunks have already been copied into their windows.
                            // An oversized chunk may be decoded only in isolation.
                            cache.chunks.clear();
                            cached_bytes = 0;
                        }
                        let mut out = Vec::with_capacity(chunk_len * bands.len());
                        let present = |slot| tile.reader.band_chunk_present(index, slot);
                        if alpha_index.is_some_and(|a| !present(a))
                            || !layouts.iter().any(|(layout, _)| present(layout.band_index))
                        {
                            out.resize(chunk_len * bands.len(), f64::NAN);
                        } else {
                            let decoder = match &mut decoder {
                                Some(decoder) => decoder,
                                slot @ None => slot.insert(tile.reader.open_decoder()?),
                            };
                            let (_, data) = decoder.read_chunk(index)?;
                            if let Some(profile) = profile {
                                profile.count(
                                    crate::run_profile::Counter::Chunks,
                                    tile.reader.physical_chunks_present(index),
                                );
                            }
                            let samples = BorrowedSamples::from(&data);
                            crate::dispatch_samples!(samples, nodata, |slice, nd| {
                                for &(layout, scaling) in &layouts {
                                    if present(layout.band_index) {
                                        convert(slice, nd, &bounds, layout, scaling, &mut out);
                                    } else {
                                        out.resize(out.len() + chunk_len, f64::NAN);
                                    }
                                }
                            });
                            if let Some(mask) = tile.reader.read_mask(bounds, &mut cache.mask)? {
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
                        }
                        cached_bytes += out.capacity() * size_of::<f64>();
                        let decoded = Arc::new(out);
                        cache.chunks.insert(index, Arc::clone(&decoded));
                        decoded
                    }
                };
                let top = bounds.row_offset.max(rows.start);
                let bottom = (bounds.row_offset + bounds.height).min(rows.end);
                let chunk_len = bounds.width as usize * bounds.height as usize;
                for (src_plane, dst_plane) in decoded
                    .chunks_exact(chunk_len)
                    .zip(values.chunks_exact_mut(plane_len))
                {
                    for row in top..bottom {
                        let src = (row - bounds.row_offset) as usize * bounds.width as usize;
                        let dst = (row - rows.start) as usize * width as usize
                            + bounds.col_offset as usize;
                        dst_plane[dst..dst + bounds.width as usize]
                            .copy_from_slice(&src_plane[src..src + bounds.width as usize]);
                    }
                }
            }
        }
        Ok(Self {
            width,
            raster_height: height,
            first_row: rows.start,
            rows: rows.len() as u32,
            values,
        })
    }

    /// Value of a known in-window pixel in the selected band slot.
    pub(super) fn band_value(&self, slot: usize, pixel: usize) -> Option<f64> {
        let value = self.values[slot * self.rows as usize * self.width as usize + pixel];
        value.is_finite().then_some(value)
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
    scaling: crate::raster::metadata::BandScaling,
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
        for budget in [1, 2048] {
            let mut cache = ChunkCache::with_budget(budget);
            let actual = RowWindow::load(&tile, selection, 0..8, &mut cache).unwrap();
            assert_eq!(cache.chunks.len(), 1);
            assert!(actual
                .values
                .iter()
                .zip(&expected.values)
                .all(|(a, b)| a.to_bits() == b.to_bits()));
        }
    }
}
