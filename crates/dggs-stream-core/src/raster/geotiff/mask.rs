//! TIFF transparency masks are separate images, with their own chunk grid.
use std::borrow::Cow;
use std::collections::HashSet;
use std::io::{Read, Seek};
use std::sync::Arc;
use tiff::decoder::{Decoder, IfdDecoder, Limits};
use tiff::tags::{CompressionMethod, Tag};

use super::{ChunkDecoder, ChunkLayout, GeoTiffStreamReader};
use crate::error::{DggsStreamError, Result};
use crate::raster::RasterChunk;

fn invalid(reason: &str) -> DggsStreamError {
    DggsStreamError::InvalidMetadata(format!("internal TIFF mask: {reason}"))
}

#[derive(Debug)]
pub(super) struct InternalMask {
    layout: ChunkLayout,
    tiled: bool,
    compression: CompressionMethod,
    lsb_first: bool,
    offsets: Vec<u64>,
    lengths: Vec<u64>,
}

impl InternalMask {
    pub(super) fn find<R: Read + Seek>(
        decoder: &mut Decoder<R>,
        w: u32,
        h: u32,
    ) -> Result<Option<Arc<Self>>> {
        let primary = decoder.ifd_pointer();
        let mut base = decoder.image_ifd();
        let mut next = base.directory().next();
        let children = base
            .find_tag(Tag::SubIfd)?
            .map(|v| v.into_ifd_vec())
            .transpose()?
            .unwrap_or_default();
        if children.is_empty() && next.is_none() {
            return Ok(None);
        }
        let mut chain = HashSet::new();
        let mut children = children.into_iter();
        loop {
            let child = children.next();
            let Some(ptr) = child.or(next) else {
                return Ok(None);
            };
            if Some(ptr) == primary || (child.is_none() && !chain.insert(ptr)) {
                return Err(invalid("repeated image directory"));
            }
            let dir = decoder.read_directory(ptr)?;
            let mut tags = decoder.read_directory_tags(&dir);
            let flags = tags
                .find_tag_unsigned::<u32>(Tag::NewSubfileType)?
                .unwrap_or(0);
            if child.is_none() {
                // A new primary page starts a different dataset.
                if flags & (1 | 4) == 0 {
                    return Ok(None);
                }
                next = dir.next();
            }
            if flags & (1 | 4) != 4 {
                continue;
            }
            if tags.get_tag_u32(Tag::ImageWidth)? != w || tags.get_tag_u32(Tag::ImageLength)? != h {
                return Err(invalid(
                    "full-resolution mask dimensions differ from the image",
                ));
            }
            return Ok(Some(Arc::new(Self::parse(&mut tags, w, h)?)));
        }
    }

    pub(super) fn parse(tags: &mut IfdDecoder<'_>, w: u32, h: u32) -> Result<Self> {
        if tags.get_tag_u32(Tag::PhotometricInterpretation)? != 4
            || tags
                .find_tag_unsigned::<u16>(Tag::BitsPerSample)?
                .unwrap_or(1)
                != 1
            || tags
                .find_tag_unsigned::<u16>(Tag::SamplesPerPixel)?
                .unwrap_or(1)
                != 1
            || tags.find_tag_unsigned::<u16>(Tag::Predictor)?.unwrap_or(1) != 1
        {
            return Err(invalid(
                "expected a single 1-bit transparency sample without a predictor",
            ));
        }
        let compression = CompressionMethod::from_u16_exhaustive(
            tags.find_tag_unsigned(Tag::Compression)?.unwrap_or(1),
        );
        if !matches!(
            compression,
            CompressionMethod::None
                | CompressionMethod::Deflate
                | CompressionMethod::OldDeflate
                | CompressionMethod::LZW
                | CompressionMethod::ZSTD
        ) {
            return Err(invalid("unsupported compression"));
        }
        let lsb_first = match tags.find_tag_unsigned::<u16>(Tag::FillOrder)?.unwrap_or(1) {
            1 => false,
            2 => true,
            _ => return Err(invalid("unsupported FillOrder")),
        };
        let tile_width = tags.find_tag_unsigned::<u32>(Tag::TileWidth)?;
        let (cw, ch, offsets_tag, lengths_tag) = if let Some(cw) = tile_width {
            (
                cw,
                tags.get_tag_u32(Tag::TileLength)?,
                Tag::TileOffsets,
                Tag::TileByteCounts,
            )
        } else {
            (
                w,
                tags.find_tag_unsigned::<u32>(Tag::RowsPerStrip)?
                    .unwrap_or(h),
                Tag::StripOffsets,
                Tag::StripByteCounts,
            )
        };
        if cw == 0 || ch == 0 || w.div_ceil(cw).checked_mul(h.div_ceil(ch)).is_none() {
            return Err(invalid("invalid chunk grid"));
        }
        let layout = ChunkLayout::new_tiled(w, h, cw, ch);
        let offsets = tags.get_tag_u64_vec(offsets_tag)?;
        let lengths = tags.get_tag_u64_vec(lengths_tag)?;
        if offsets.len() != layout.total_chunks as usize || lengths.len() != offsets.len() {
            return Err(invalid("chunk table length differs from the mask grid"));
        }
        Ok(Self {
            layout,
            tiled: tile_width.is_some(),
            compression,
            lsb_first,
            offsets,
            lengths,
        })
    }

    fn decode(
        &self,
        source: &super::RasterSource,
        idx: u32,
        height: u32,
    ) -> Result<Option<Vec<u8>>> {
        let limits = Limits::default();
        let offset = self.offsets[idx as usize];
        let length = self.lengths[idx as usize];
        if offset == 0 && length == 0 {
            return Ok(None); // Sparse mask blocks are all invalid.
        }
        let len = usize::try_from(length)
            .ok()
            .filter(|n| *n <= limits.intermediate_buffer_size)
            .ok_or_else(|| invalid("compressed chunk too large"))?;
        if offset == 0 || len == 0 {
            return Err(invalid("incomplete sparse chunk"));
        }
        let payload = source.byte_range(offset, len)?;
        let src_stride = self.layout.chunk_width.div_ceil(8) as usize;
        let stored_rows = if self.tiled {
            self.layout.chunk_height
        } else {
            height
        };
        let size = src_stride
            .checked_mul(stored_rows as usize)
            .filter(|n| *n <= limits.decoding_buffer_size)
            .ok_or_else(|| invalid("decoded chunk too large"))?;
        let mut packed = vec![0; size];
        let mut deflate = matches!(
            self.compression,
            CompressionMethod::Deflate | CompressionMethod::OldDeflate
        )
        .then(libdeflater::Decompressor::new);
        let mut lzw = (self.compression == CompressionMethod::LZW)
            .then(|| weezl::decode::Decoder::with_tiff_size_switch(weezl::BitOrder::Msb, 8));
        let mut zstd = (self.compression == CompressionMethod::ZSTD)
            .then(zstd::bulk::Decompressor::new)
            .transpose()?;
        let mut decode = |out: &mut [u8]| {
            ChunkDecoder::decode_chunk_bytes_into(
                deflate.as_mut(),
                lzw.as_mut(),
                zstd.as_mut(),
                self.compression,
                payload.as_ref(),
                out,
            )
        };
        match decode(&mut packed) {
            Ok(()) => {}
            Err(super::FastDecodeError::Unsupported(_))
                if !self.tiled && height < self.layout.chunk_height =>
            {
                // Some writers pad the last Deflate strip to RowsPerStrip. Retry only
                // that nominal layout, still enforcing the decoder's allocation limit.
                let padded_size = src_stride
                    .checked_mul(self.layout.chunk_height as usize)
                    .filter(|n| *n <= limits.decoding_buffer_size)
                    .ok_or_else(|| invalid("padded strip too large"))?;
                packed.resize(padded_size, 0);
                decode(&mut packed).map_err(|e| e.into_inner())?;
                packed.truncate(size);
            }
            Err(e) => return Err(e.into_inner()),
        }
        Ok(Some(packed))
    }
}

/// Retain at most one shared mask block per scan, across adjacent data tiles.
/// Matching grids stream directly without retaining an extra decoded block.
#[derive(Default)]
pub(crate) struct MaskChunkCache {
    block: Option<(Arc<InternalMask>, u32, Vec<u8>)>,
}

impl MaskChunkCache {
    fn read<'a>(
        &'a mut self,
        reader: &GeoTiffStreamReader,
        mask: &Arc<InternalMask>,
        idx: u32,
        chunk: RasterChunk,
        data: RasterChunk,
    ) -> Result<Option<Cow<'a, [u8]>>> {
        const MAX_RETAINED_BYTES: usize = 2 << 20;
        let shared = chunk.width > data.width || chunk.height > data.height;
        if !shared {
            return Ok(mask
                .decode(&reader.source, idx, chunk.height)?
                .map(Cow::Owned));
        }
        if !self
            .block
            .as_ref()
            .is_some_and(|(owner, key, _)| Arc::ptr_eq(owner, mask) && *key == idx)
        {
            self.block = None;
            let Some(bytes) = mask.decode(&reader.source, idx, chunk.height)? else {
                return Ok(None);
            };
            if bytes.capacity() > MAX_RETAINED_BYTES {
                return Ok(Some(Cow::Owned(bytes)));
            }
            self.block = Some((Arc::clone(mask), idx, bytes));
        }
        Ok(self
            .block
            .as_ref()
            .map(|(_, _, bytes)| Cow::Borrowed(bytes.as_slice())))
    }
}

/// Packed validity bits for one data chunk, indexed by absolute raster coordinates.
#[derive(Debug)]
pub struct MaskWindow {
    bounds: RasterChunk,
    stride: usize,
    bits: Vec<u8>,
}

impl MaskWindow {
    /// Test a pixel inside the data chunk covered by this window.
    #[inline]
    pub fn is_valid(&self, col: usize, row: usize) -> bool {
        let x = col - self.bounds.col_offset as usize;
        let y = row - self.bounds.row_offset as usize;
        self.bits[y * self.stride + x / 8] & (1 << (x % 8)) != 0
    }
}

impl GeoTiffStreamReader {
    /// Whether this raster has an internal, full-resolution TIFF validity mask.
    pub fn has_internal_mask(&self) -> bool {
        self.metadata.internal_mask.is_some()
    }

    /// Read the validity mask over a data chunk, regardless of the mask's tile/strip size.
    pub(crate) fn read_mask(
        &self,
        bounds: RasterChunk,
        cache: &mut MaskChunkCache,
    ) -> Result<Option<MaskWindow>> {
        let Some(mask) = &self.metadata.internal_mask else {
            return Ok(None);
        };
        let limits = Limits::default();
        let stride = bounds.width.div_ceil(8) as usize;
        let size = stride
            .checked_mul(bounds.height as usize)
            .filter(|n| *n <= limits.decoding_buffer_size)
            .ok_or_else(|| invalid("window too large"))?;
        let mut window = MaskWindow {
            bounds,
            stride,
            bits: vec![0; size],
        };
        let mut valid_pixels = 0u64;
        let right = bounds.col_offset + bounds.width;
        let bottom = bounds.row_offset + bounds.height;
        let layout = mask.layout;
        for r in bounds.row_offset / layout.chunk_height..bottom.div_ceil(layout.chunk_height) {
            for c in bounds.col_offset / layout.chunk_width..right.div_ceil(layout.chunk_width) {
                let idx = r * layout.chunks_across + c;
                let chunk = layout.get_chunk_bounds(idx, self.metadata.width, self.metadata.height);
                let Some(packed) = cache.read(self, mask, idx, chunk, bounds)? else {
                    continue;
                };
                let src_stride = layout.chunk_width.div_ceil(8) as usize;
                for y in bounds.row_offset.max(chunk.row_offset)
                    ..bottom.min(chunk.row_offset + chunk.height)
                {
                    let src_row = (y - chunk.row_offset) as usize * src_stride;
                    let dst_row = (y - bounds.row_offset) as usize * stride;
                    for x in bounds.col_offset.max(chunk.col_offset)
                        ..right.min(chunk.col_offset + chunk.width)
                    {
                        let sx = (x - chunk.col_offset) as usize;
                        let bit = if mask.lsb_first { sx % 8 } else { 7 - sx % 8 };
                        if packed[src_row + sx / 8] & (1 << bit) != 0 {
                            valid_pixels += 1;
                            let dx = (x - bounds.col_offset) as usize;
                            window.bits[dst_row + dx / 8] |= 1 << (dx % 8);
                        }
                    }
                }
            }
        }
        // An all-valid chunk can still use the single-band SIMD walk.
        Ok((valid_pixels != u64::from(bounds.width) * u64::from(bounds.height)).then_some(window))
    }
}

#[cfg(test)]
mod tests {
    use super::super::RasterSource;
    use super::*;
    use flate2::{write::ZlibEncoder, Compression};
    use std::io::Write;

    #[test]
    fn final_deflate_strip_accepts_exact_or_nominal_rows() {
        fn decode(last_strip: &[u8], nominal_rows: u32) -> Result<Vec<u8>> {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
            encoder.write_all(last_strip)?;
            let compressed = encoder.finish()?;
            let mut mmap = memmap2::MmapMut::map_anon(1 + compressed.len())?;
            mmap[1..].copy_from_slice(&compressed);
            let source = RasterSource::Local(Arc::new(mmap.make_read_only()?));
            let layout = ChunkLayout::new_tiled(9, 3, 9, nominal_rows);
            let idx = layout.total_chunks - 1;
            let mask = InternalMask {
                layout,
                tiled: false,
                compression: CompressionMethod::Deflate,
                lsb_first: false,
                offsets: vec![1; layout.total_chunks as usize],
                lengths: vec![compressed.len() as u64; layout.total_chunks as usize],
            };
            Ok(mask.decode(&source, idx, 1)?.unwrap())
        }
        let valid = [0x42, 0x80];
        assert_eq!(decode(&valid, 2).unwrap(), valid);
        assert_eq!(decode(&[0x42, 0x80, 0xff, 0xff], 2).unwrap(), valid);
        assert!(decode(&[0x42], 2).is_err());
        assert!(decode(&[0; 3], 2).is_err());
        assert!(decode(&[0; 5], 2).is_err());
        assert!(decode(&[0; 4], u32::MAX).is_err());
    }
}
