//! A band of raster rows held as `f64`, for interpolation and overlay.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use crate::aggregator::multi_horizon::borrowed::BorrowedSamples;
use crate::aggregator::simd::SimdSpanAccumulate;
use crate::error::Result;
use crate::raster::mosaic::TileDescriptor;
use crate::raster::RasterChunk;

/// One band's values over a contiguous range of rows and the raster's full width.
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
    chunks: HashMap<u32, Arc<Vec<f64>>>,
}

impl ChunkCache {
    /// Forget every chunk (when moving to another tile).
    pub fn clear(&mut self) {
        self.chunks.clear();
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
        let meta = &tile.reader.metadata;
        let (width, height) = (meta.width, meta.height);
        let rows = rows.start.min(height)..rows.end.min(height);
        let layout = tile.reader.chunk_layout;
        let mut values = vec![f64::NAN; rows.len() * width as usize];
        if rows.is_empty() || width == 0 {
            return Ok(Self::from_values(width, height, rows.start, values));
        }
        let chunk_rows = rows.start / layout.chunk_height..=(rows.end - 1) / layout.chunk_height;
        cache
            .chunks
            .retain(|index, _| chunk_rows.contains(&(index / layout.chunks_across)));
        let nodata = selection.nodata.or(meta.nodata);
        let spp = usize::from(meta.samples_per_pixel.max(1));
        let band_index = selection.band.saturating_sub(1).min(spp - 1);
        // A pixel whose alpha sample is zero is transparent: missing, as in the centre
        // kernel (`walker::is_transparent`).
        let alpha_index = meta.alpha_sample.map(usize::from).filter(|&a| a < spp);
        let scaling = if selection.apply_scale_offset {
            meta.band_scaling(selection.band)
        } else {
            crate::raster::metadata::BandScaling::IDENTITY
        };
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
                        let mut out =
                            Vec::with_capacity(bounds.width as usize * bounds.height as usize);
                        // A chunk the file does not store holds no samples.
                        if tile.reader.is_sparse_chunk(index) {
                            out.resize(bounds.width as usize * bounds.height as usize, f64::NAN);
                        } else {
                            let decoder = match &mut decoder {
                                Some(decoder) => decoder,
                                slot @ None => slot.insert(tile.reader.open_decoder()?),
                            };
                            let (_, data) = decoder.read_chunk(index)?;
                            let samples = BorrowedSamples::from(&data);
                            crate::dispatch_samples!(samples, nodata, |slice, nd| {
                                convert(
                                    slice,
                                    nd,
                                    &bounds,
                                    Layout {
                                        spp,
                                        band_index,
                                        alpha_index,
                                    },
                                    scaling,
                                    &mut out,
                                )
                            });
                        }
                        let decoded = Arc::new(out);
                        cache.chunks.insert(index, Arc::clone(&decoded));
                        decoded
                    }
                };
                let top = bounds.row_offset.max(rows.start);
                let bottom = (bounds.row_offset + bounds.height).min(rows.end);
                for row in top..bottom {
                    let src = (row - bounds.row_offset) as usize * bounds.width as usize;
                    let dst =
                        (row - rows.start) as usize * width as usize + bounds.col_offset as usize;
                    values[dst..dst + bounds.width as usize]
                        .copy_from_slice(&decoded[src..src + bounds.width as usize]);
                }
            }
        }
        Ok(Self::from_values(width, height, rows.start, values))
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
