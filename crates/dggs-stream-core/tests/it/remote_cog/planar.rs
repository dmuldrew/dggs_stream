use super::*;
use dggs_stream_core::aggregator::multi_horizon::multiband::{Bands, MultiBandStreamer};
use dggs_stream_core::aggregator::multi_horizon::MultiResolutionConfig;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::run_profile::RunProfile;

#[test]
fn twelve_band_range_scan_decodes_only_selected_planes_and_alpha() {
    if !MockHttpServer::is_networking_supported() {
        return;
    }
    for (layout, chunks, corrupt_unused) in [
        ("interleaved", 4, false),
        ("planar", 12, false),
        ("planar", 12, true),
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/fixtures/multiband-12-{layout}.tif"));
        let mut bytes = std::fs::read(&path).unwrap();
        if corrupt_unused {
            // Any accidental decode of an unselected plane must fail even if its
            // bytes are already in the HTTP header cache.
            let reader = GeoTiffStreamReader::open(&path).unwrap();
            let info = reader.chunk_info.unwrap();
            for (index, (&offset, &len)) in info
                .chunk_offsets
                .iter()
                .zip(info.chunk_bytes.iter())
                .enumerate()
            {
                let band = index / 4 + 1;
                if ![3, 1, 12].contains(&band) {
                    bytes[offset as usize..(offset + len) as usize].fill(0xff);
                }
            }
        }
        let server = MockHttpServer::start(bytes);
        let start = std::time::Instant::now();
        let reader = GeoTiffStreamReader::open(format!("{}/fixture.tif", server.url_base)).unwrap();
        let profile = RunProfile::new();
        let cfg = MultiResolutionConfig {
            profile: Some(profile.clone()),
            ..MultiResolutionConfig::single(12)
        };
        let mut scan = MultiBandStreamer::new(reader, &cfg, Bands::Selected(&[3, 1])).unwrap();
        let mut counts = [0.0; 2];
        loop {
            let rows = scan.fetch_next_batch(64).unwrap();
            if rows.is_empty() {
                break;
            }
            for row in rows {
                for (count, stat) in counts.iter_mut().zip(row.statistics) {
                    *count += stat.count;
                }
            }
        }
        drop(scan);
        let elapsed = start.elapsed();
        assert_eq!(counts, [8652.0, 8755.0]);
        assert_eq!(profile.report().chunks, chunks);
        if corrupt_unused {
            continue;
        }
        eprintln!(
            "{layout}: {chunks} decoded chunks, {} transferred bytes, {} requests, {elapsed:.2?}",
            server.bytes_served.load(Ordering::SeqCst),
            server.request_count.load(Ordering::SeqCst)
        );
    }
}
