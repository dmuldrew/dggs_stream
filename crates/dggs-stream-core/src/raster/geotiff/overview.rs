//! Select a reduced image in a COG's primary IFD chain. Image indices include masks.
use super::*;
use std::collections::{HashMap, HashSet};
use std::io::Read;

impl GeoTiffStreamReader {
    /// Coarsest compatible image whose pixel area stays within the supplied
    /// multiple of the native pixel area. Zero identifies the native image.
    pub(crate) fn select_overview(&mut self, max_area_ratio: f64) -> Result<usize> {
        if max_area_ratio <= 1.0 || self.image_index != 0 {
            return Ok(self.image_index);
        }
        let selected = match &self.source {
            RasterSource::Local(map) => {
                self.overview_image(Decoder::new(Cursor::new(&map[..]))?, max_area_ratio)?
            }
            RasterSource::Remote(remote) => self.overview_image(
                Decoder::new(HttpRangeReader::new(remote.clone()))?,
                max_area_ratio,
            )?,
        };
        if let Some((index, metadata, layout, info)) = selected {
            self.image_index = index;
            self.metadata = metadata;
            self.chunk_layout = layout;
            self.chunk_info = info;
        }
        Ok(self.image_index)
    }

    fn overview_image<R: Read + Seek>(
        &self,
        mut decoder: Decoder<R>,
        max_ratio: f64,
    ) -> Result<Option<OverviewImage>> {
        let base = &self.metadata;
        let mut next = decoder.image_ifd().directory().next();
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();
        let mut masks = HashMap::new();
        let mut index = 0;
        while let Some(ptr) = next {
            if !seen.insert(ptr) {
                return Err(DggsStreamError::InvalidMetadata(
                    "repeated overview IFD".into(),
                ));
            }
            index += 1;
            let dir = decoder.read_directory(ptr)?;
            next = dir.next();
            let mut tags = decoder.read_directory_tags(&dir);
            let flags = tags
                .find_tag_unsigned::<u32>(Tag::NewSubfileType)?
                .unwrap_or(0);
            // A separate primary image starts another dataset.
            if flags & 5 == 0 {
                break;
            }
            let w = tags.get_tag_u32(Tag::ImageWidth)?;
            let h = tags.get_tag_u32(Tag::ImageLength)?;
            if w == 0 || h == 0 || w > base.width || h > base.height {
                continue;
            }
            if flags & 5 == 5 {
                masks.insert((w, h), ptr);
            } else if flags & 5 == 1 {
                let ratio = (f64::from(base.width) / f64::from(w))
                    * (f64::from(base.height) / f64::from(h));
                if ratio > 1.0 && ratio <= max_ratio {
                    candidates.push((ratio, index, w, h));
                }
            }
        }
        candidates.sort_by(|a, b| b.0.total_cmp(&a.0));
        for (_, index, w, h) in candidates {
            decoder.seek_to_image(index)?;
            let children = decoder
                .find_tag(Tag::SubIfd)?
                .map(|v| v.into_ifd_vec())
                .transpose()?
                .unwrap_or_default();
            let mut mask_ptr = masks.get(&(w, h)).copied();
            for child in children {
                let dir = decoder.read_directory(child)?;
                let mut tags = decoder.read_directory_tags(&dir);
                if tags
                    .find_tag_unsigned::<u32>(Tag::NewSubfileType)?
                    .unwrap_or(0)
                    & 4
                    != 0
                    && tags.get_tag_u32(Tag::ImageWidth)? == w
                    && tags.get_tag_u32(Tag::ImageLength)? == h
                {
                    mask_ptr = Some(child);
                    break;
                }
            }
            let mask = match mask_ptr {
                Some(ptr) => {
                    let dir = decoder.read_directory(ptr)?;
                    Some(Arc::new(InternalMask::parse(
                        &mut decoder.read_directory_tags(&dir),
                        w,
                        h,
                    )?))
                }
                None if base.has_internal_mask() => continue,
                None => None,
            };
            let (mut meta, layout, info) =
                Self::parse_metadata_and_layout(&mut decoder, Some(base))?;
            if meta.samples_per_pixel != base.samples_per_pixel
                || meta.alpha_sample != base.alpha_sample
            {
                continue;
            }
            meta.internal_mask = mask;
            return Ok(Some((index, meta, layout, info)));
        }
        Ok(None)
    }
}
type OverviewImage = (
    usize,
    GeoTiffMetadata,
    ChunkLayout,
    Option<Arc<TiffChunkInfo>>,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masked_overview_requires_a_matching_mask() {
        // End the test COG's primary chain before one or both reduced masks.
        // The full-resolution mask and all image IFDs stay present.
        let original = include_bytes!("../../../tests/fixtures/overview-mask.tif");
        for (last_image, expected_ifd, expected_size) in [(4, 2, (64, 32)), (3, 0, (129, 65))] {
            let mut bytes = original.to_vec();
            let mut decoder = Decoder::new(Cursor::new(&bytes)).unwrap();
            decoder.seek_to_image(last_image).unwrap();
            let pos = decoder.ifd_pointer().unwrap().0 as usize;
            let entries = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
            let next = pos + 2 + 12 * entries;
            bytes[next..next + 4].fill(0);
            let tmp = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(tmp.path(), bytes).unwrap();
            let mut image = GeoTiffStreamReader::open(tmp.path()).unwrap();
            assert_eq!(image.select_overview(100.0).unwrap(), expected_ifd);
            assert_eq!((image.metadata.width, image.metadata.height), expected_size);
            assert!(image.metadata.has_internal_mask());
        }
    }
}
