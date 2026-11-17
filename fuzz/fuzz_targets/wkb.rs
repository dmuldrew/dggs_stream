#![no_main]

use dggs_stream_core::encoding::wkb_reader::wkb_to_multipolygon;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = wkb_to_multipolygon(data);
});
