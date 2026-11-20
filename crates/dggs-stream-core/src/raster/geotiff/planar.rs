//! Map spatial tiles/strips to band-major TIFF chunk indices.
use super::*;

impl GeoTiffStreamReader {
    pub(crate) fn is_planar(&self) -> bool {
        self.metadata.planes.is_some()
    }

    pub(crate) fn physical_chunks(&self, index: u32) -> impl Iterator<Item = u32> + '_ {
        self.metadata.planes.iter().flat_map(move |planes| {
            planes
                .iter()
                .map(move |&p| u32::from(p) * self.chunk_layout.total_chunks + index)
        })
    }

    /// Compact the physical planes for a validated band selection. The kernel
    /// sees selected bands first, with an extra transparency sample if needed.
    pub(crate) fn project_bands(&mut self, bands: Vec<usize>) -> Vec<usize> {
        if !self.is_planar() {
            return bands;
        }
        let mut planes: Vec<_> = bands.iter().map(|b| (b - 1) as u16).collect();
        if let Some(alpha) = self.metadata.alpha_sample {
            let position = planes.iter().position(|&p| p == alpha).unwrap_or_else(|| {
                planes.push(alpha);
                planes.len() - 1
            });
            self.metadata.alpha_sample = Some(position as u16);
        }
        self.metadata.band_scaling = planes
            .iter()
            .map(|&p| self.metadata.band_scaling(usize::from(p) + 1))
            .collect();
        self.metadata.samples_per_pixel = planes.len() as u16;
        self.metadata.planes = Some(planes.into());
        (1..=bands.len()).collect()
    }

    pub(crate) fn band_chunk_present(&self, index: u32, slot: usize) -> bool {
        let physical = self
            .metadata
            .planes
            .as_ref()
            .map_or(index as usize, |planes| {
                usize::from(planes[slot]) * self.chunk_layout.total_chunks as usize + index as usize
            });
        self.chunk_info.as_ref().is_none_or(|info| {
            info.chunk_offsets.get(physical) != Some(&0)
                && info.chunk_bytes.get(physical) != Some(&0)
        })
    }

    pub(crate) fn physical_chunks_present(&self, index: u32) -> u64 {
        let slots = self
            .metadata
            .planes
            .as_ref()
            .map_or(1, |planes| planes.len());
        (0..slots)
            .filter(|&slot| self.band_chunk_present(index, slot))
            .count() as u64
    }
}

impl ChunkDecoder<'_> {
    pub(super) fn spatial_index(&self, physical: u32) -> u32 {
        if self.planes.is_some() {
            physical % self.chunk_layout.total_chunks
        } else {
            physical
        }
    }

    pub(super) fn read_planar_chunk(
        &mut self,
        index: u32,
        planes: &[u16],
        mut output: Option<DecodingResult>,
        queue: Option<(
            &crate::raster::remote_prefetch::RemoteChunkPrefetchQueue,
            usize,
        )>,
    ) -> Result<(RasterChunk, DecodingResult)> {
        if index >= self.chunk_layout.total_chunks || planes.is_empty() {
            return Err(DggsStreamError::InvalidMetadata(
                "invalid planar chunk index or empty plane set".into(),
            ));
        }
        let bounds = self
            .chunk_layout
            .get_chunk_bounds(index, self.width, self.height);
        for (slot, &plane) in planes.iter().enumerate() {
            let physical = u32::from(plane) * self.chunk_layout.total_chunks + index;
            // Release each response slice before requesting the next plane so
            // a scan can progress with room for a single response.
            let (_, data) = {
                let payload = match queue {
                    Some((queue, tile)) => queue.get_chunk_payload(tile, physical)?,
                    None => None,
                };
                self.read_physical_chunk(physical, payload.as_deref(), None)?
            };
            if planes.len() == 1 {
                return Ok((bounds, data));
            }
            let pixels = bounds.width as usize * bounds.height as usize;
            let length = pixels.checked_mul(planes.len()).ok_or_else(|| {
                DggsStreamError::InvalidMetadata("planar chunk size overflow".into())
            })?;
            // Match the TIFF sample type once per plane, outside the pixel loop.
            macro_rules! scatter {
                ($($kind:ident),+) => {
                    match data {
                        $(DecodingResult::$kind(values) => {
                            if values.len() != pixels {
                                return Err(DggsStreamError::InvalidMetadata("unexpected decoded plane length".into()));
                            }
                            if slot == 0 && !matches!(output, Some(DecodingResult::$kind(_))) {
                                output = Some(DecodingResult::$kind(Vec::new()));
                            }
                            let Some(DecodingResult::$kind(dest)) = &mut output else {
                                return Err(DggsStreamError::InvalidMetadata("inconsistent plane types".into()));
                            };
                            if slot == 0 {
                                dest.resize(length, Default::default());
                            }
                            for (pixel, sample) in dest.chunks_exact_mut(planes.len()).zip(values) {
                                pixel[slot] = sample;
                            }
                        }),+
                    }
                };
            }
            scatter!(U8, U16, U32, U64, I8, I16, I32, I64, F16, F32, F64);
        }
        Ok((bounds, output.expect("nonempty plane set")))
    }
}
