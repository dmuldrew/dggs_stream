#![no_main]

use std::io::Write;

use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(data).unwrap();
    // Exercise the core metadata path without decoding unbounded image dimensions.
    let _ = GeoTiffStreamReader::open(file.path());
});
