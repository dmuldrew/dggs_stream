//! The 96×64 RGB chart uses 32×32 JPEG tiles, quality 90, 2×2 chroma subsampling
//! and shared quantization tables. Regenerate its lossless GDAL 3.13.3 reference with
//! `gdal_translate -co COMPRESS=DEFLATE -co PHOTOMETRIC=RGB jpeg-ycbcr.tif jpeg-gdal-rgb.tif`.

use super::MockHttpServer;
use crate::codecs::ifd_entry;
use dggs_stream_core::aggregator::multi_horizon::{
    multiband::{Bands, MultiBandStreamer},
    MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tiff::decoder::{Decoder, DecodingResult};
use tiff::tags::Tag;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

type Stats = (f64, f64, f64, f64);

fn single(path: &Path, band: usize, bbox: Option<[f64; 4]>) -> BTreeMap<u64, Stats> {
    let config = MultiResolutionConfig {
        band,
        bbox,
        ..MultiResolutionConfig::single(10)
    };
    let mut scan =
        MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(path).unwrap(), &config).unwrap();
    let mut cells = BTreeMap::new();
    loop {
        let rows = scan.fetch_next_batch(32).unwrap();
        if rows.is_empty() {
            return cells;
        }
        for row in rows {
            let a = row.accumulator;
            assert!(cells
                .insert(row.cell_id, (a.count, a.sum, a.min, a.max))
                .is_none());
        }
    }
}

fn check_wide(path: &Path, bbox: Option<[f64; 4]>) {
    let reference = fixture("jpeg-gdal-rgb.tif");
    let mut expected = [3, 1].map(|band| single(&reference, band, bbox));
    assert!(!expected[0].is_empty());
    let config = MultiResolutionConfig {
        bbox,
        ..MultiResolutionConfig::single(10)
    };
    let mut scan = MultiBandStreamer::new(
        GeoTiffStreamReader::open(path).unwrap(),
        &config,
        Bands::Selected(&[3, 1]),
    )
    .unwrap();
    loop {
        let rows = scan.fetch_next_batch(32).unwrap();
        if rows.is_empty() {
            break;
        }
        for row in rows {
            for (a, band) in row.statistics.iter().zip(&mut expected) {
                assert_eq!(
                    band.remove(&row.cell_id),
                    Some((a.count, a.sum, a.min, a.max))
                );
            }
        }
    }
    assert!(expected.iter().all(BTreeMap::is_empty));
}

#[test]
fn jpeg_ycbcr_matches_gdal_rgb() {
    let path = fixture("jpeg-ycbcr.tif");
    let reference = fixture("jpeg-gdal-rgb.tif");
    let mut tags = Decoder::new(std::fs::File::open(&path).unwrap()).unwrap();
    assert!(tags.get_tag_u8_vec(Tag::JPEGTables).unwrap().len() > 4);
    let mut gdal = Decoder::new(std::fs::File::open(&reference).unwrap()).unwrap();
    let DecodingResult::U8(rgb) = gdal.read_image().unwrap() else {
        panic!("expected GDAL RGB bytes")
    };
    let reader = GeoTiffStreamReader::open(&path).unwrap();
    let mut decoder = reader.open_decoder().unwrap();
    for i in 0..reader.chunk_layout.total_chunks {
        let (tile, data) = decoder.read_chunk(i).unwrap();
        let DecodingResult::U8(actual) = data else {
            panic!("expected RGB bytes")
        };
        assert_eq!(actual.len(), tile.width as usize * tile.height as usize * 3);
        for (r, row) in actual.chunks_exact(tile.width as usize * 3).enumerate() {
            let start = ((tile.row_offset as usize + r) * reader.metadata.width as usize
                + tile.col_offset as usize)
                * 3;
            assert_eq!(row, &rgb[start..start + row.len()]);
        }
    }
    for band in 1..=3 {
        assert_eq!(single(&path, band, None), single(&reference, band, None));
    }
    check_wide(&path, None);
}

#[test]
fn jpeg_ycbcr_http_bbox_fetches_only_selected_tile() {
    if !MockHttpServer::is_networking_supported() {
        return;
    }
    // Move the six contiguous payloads past the initial metadata probe, without
    // changing the shared JPEG tables or any compressed pixel data.
    let path = fixture("jpeg-ycbcr.tif");
    let original = std::fs::read(&path).unwrap();
    let info = GeoTiffStreamReader::open(&path)
        .unwrap()
        .chunk_info
        .unwrap();
    let probe = dggs_stream_core::raster::http_range::DEFAULT_BLOCK_SIZE;
    let first = info.chunk_offsets[0] as usize;
    let mut bytes = original[..first].to_vec();
    let entry = ifd_entry(&mut bytes, 324);
    let table = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as usize;
    bytes.resize(probe, 0);
    for (i, &offset) in info.chunk_offsets.iter().enumerate() {
        let relocated = (probe + offset as usize - first) as u32;
        bytes[table + 4 * i..table + 4 * i + 4].copy_from_slice(&relocated.to_le_bytes());
    }
    bytes.extend_from_slice(&original[first..]);
    let server = MockHttpServer::start(bytes);
    let url = format!("{}/jpeg.tif", server.url_base);
    // Strictly inside the middle tile of the first row.
    check_wide(Path::new(&url), Some([-121.96, 37.98, -121.95, 37.99]));
    let ranges: Vec<_> = server
        .recorded_headers()
        .iter()
        .filter_map(|h| {
            let h = h.trim().to_ascii_lowercase();
            h.strip_prefix("range:").map(|v| v.trim().to_string())
        })
        .collect();
    let start = probe as u64 + info.chunk_offsets[1] - first as u64;
    let end = start + info.chunk_bytes[1] - 1;
    assert_eq!(
        ranges,
        [
            format!("bytes=0-{}", probe - 1),
            format!("bytes={start}-{end}")
        ]
    );
}

#[test]
fn jpeg_ycbcr_rejects_nonstandard_reference_levels() {
    let mut bytes = std::fs::read(fixture("jpeg-ycbcr.tif")).unwrap();
    let entry = ifd_entry(&mut bytes, 532);
    let values = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as usize;
    bytes[values..values + 4].copy_from_slice(&16u32.to_le_bytes());
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), bytes).unwrap();
    let err = GeoTiffStreamReader::open(temp.path())
        .err()
        .expect("nonstandard luma range");
    assert!(
        err.to_string()
            .contains("unsupported JPEG/YCbCr color parameters"),
        "{err}"
    );
}
