//! `auto_overview` scans of a masked COG whose reduced masks are missing.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::aggregator::ContinuousAccumulator;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use tiff::decoder::Decoder;

fn scan(path: &Path, auto_overview: bool) -> BTreeMap<u64, ContinuousAccumulator> {
    let mut config = MultiResolutionConfig::single(7);
    config.auto_overview = auto_overview;
    let reader = GeoTiffStreamReader::open(path).unwrap();
    let mut streamer = MultiScanHorizonStreamer::new(reader, &config).unwrap();
    let mut cells = BTreeMap::new();
    while streamer
        .drain_completed_into(777, |_, rec| {
            cells.insert(rec.cell_id, rec.accumulator);
        })
        .unwrap()
        > 0
    {}
    cells
}

/// Copy the masked fixture with its primary IFD chain ended after `last_image`.
fn truncated_chain(last_image: usize) -> tempfile::NamedTempFile {
    let original = include_bytes!("../fixtures/overview-mask.tif");
    let mut bytes = original.to_vec();
    let mut decoder = Decoder::new(Cursor::new(&bytes)).unwrap();
    decoder.seek_to_image(last_image).unwrap();
    let pos = decoder.ifd_pointer().unwrap().0 as usize;
    let entries = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
    let next = pos + 2 + 12 * entries;
    bytes[next..next + 4].fill(0);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), bytes).unwrap();
    tmp
}

#[test]
fn masked_overview_without_a_matching_mask_falls_back_to_native_pixels() {
    // Chain ends at image 3: the 64x32 image is present without its mask, so no
    // reduced image qualifies and the scan reads native pixels.
    let no_mask = truncated_chain(3);
    assert_eq!(scan(no_mask.path(), true), scan(no_mask.path(), false));
    // Chain ends at image 4: the 64x32 image keeps its mask and is selected.
    let with_mask = truncated_chain(4);
    assert_ne!(scan(with_mask.path(), true), scan(with_mask.path(), false));
}
