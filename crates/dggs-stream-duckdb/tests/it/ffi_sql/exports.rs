// =============================================================================
// 4. Direct Export Table Functions (Parquet & PMTiles) with CRS Override
// =============================================================================

use std::collections::HashMap;
use std::ptr;

use dggs_stream_duckdb::ffi::duckdb_c::*;
use dggs_stream_duckdb::functions::parquet_table_function::{
    parquet_bind, parquet_init, parquet_scan,
};
use dggs_stream_duckdb::functions::pmtiles_table_function::{
    parquet_pmtiles_bind, parquet_pmtiles_init, parquet_pmtiles_scan, pmtiles_bind, pmtiles_init,
    pmtiles_scan,
};

use super::mock_api::install_mock_api;
use super::{MockBindContext, MockDataChunk, MockFuncContext, MockInitContext, MockVal};
use crate::helpers::TestGeoTiffBuilder;

#[test]
fn test_export_table_functions_with_crs_override() {
    install_mock_api();
    // Create an unreferenced GeoTIFF (no GeoKeys) so `crs := 'EPSG:4326'` is required
    let (_tmp, tif_path) = TestGeoTiffBuilder::new(24, 24)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .with_geokeys(false)
        .create_constant_tempfile(42.0f32);

    let out_dir = tempfile::tempdir().unwrap();
    let parquet_path = out_dir.path().join("exported.parquet");
    let pmtiles_path = out_dir.path().join("exported.pmtiles");
    let transcode_pmtiles_path = out_dir.path().join("transcoded.pmtiles");

    // 1. h3_raster_to_parquet with crs := 'EPSG:4326' and geoparquet := true
    let mut pq_named = HashMap::new();
    pq_named.insert("resolution".to_string(), MockVal::Int(8));
    pq_named.insert("crs".to_string(), MockVal::Str("EPSG:4326".to_string()));
    pq_named.insert("geoparquet".to_string(), MockVal::Bool(true));

    let mut pq_bind = MockBindContext::new(
        vec![
            MockVal::Str(tif_path.to_string_lossy().into_owned()),
            MockVal::Str(parquet_path.to_string_lossy().into_owned()),
        ],
        pq_named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        parquet_bind(&mut pq_bind as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(pq_bind.error.is_none());

    let mut pq_init = MockInitContext::new(pq_bind.bind_data, (0..7).collect());
    // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        parquet_init(&mut pq_init as *mut MockInitContext as duckdb_init_info);
    }
    let mut pq_func = MockFuncContext {
        bind_data: pq_bind.bind_data,
        init_data: pq_init.init_data,
        local_init_data: ptr::null_mut(),
        error: None,
    };
    let mut pq_chunk = MockDataChunk::new(7, 2048);
    // SAFETY: the function context carries bind/init/local data installed above (still
    // owned by their live contexts), and the chunk has one 2048-row mock vector per
    // projected column.
    unsafe {
        parquet_scan(
            &mut pq_func as *mut MockFuncContext as duckdb_function_info,
            &mut pq_chunk as *mut MockDataChunk as duckdb_data_chunk,
        );
    }
    assert_eq!(pq_chunk.size, 1);
    let pq_status = std::str::from_utf8(&pq_chunk.vectors[5].strings[0]).unwrap();
    assert_eq!(pq_status, "SUCCESS", "Parquet export failed: {pq_status}");
    // SAFETY: the callbacks above wrote values of these columns' declared types for rows
    // `0..size` into 8-aligned buffers with room for at least that many rows.
    let pq_hex_count = unsafe { *(pq_chunk.vectors[0].data.as_ptr() as *const i64) };
    assert!(pq_hex_count > 0);
    // SAFETY: as above; column 6 is the BIGINT `transform_failures`.
    let pq_failures = unsafe { *(pq_chunk.vectors[6].data.as_ptr() as *const i64) };
    assert_eq!(pq_failures, 0);
    assert!(parquet_path.exists());

    // 2. h3_raster_to_pmtiles with source_crs := 'EPSG:4326'
    let mut pm_named = HashMap::new();
    pm_named.insert("resolution".to_string(), MockVal::Int(8));
    pm_named.insert(
        "source_crs".to_string(),
        MockVal::Str("EPSG:4326".to_string()),
    );
    pm_named.insert(
        "properties".to_string(),
        MockVal::Str("mean,count".to_string()),
    );

    let mut pm_bind = MockBindContext::new(
        vec![
            MockVal::Str(tif_path.to_string_lossy().into_owned()),
            MockVal::Str(pmtiles_path.to_string_lossy().into_owned()),
        ],
        pm_named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        pmtiles_bind(&mut pm_bind as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(pm_bind.error.is_none());

    let mut pm_init = MockInitContext::new(pm_bind.bind_data, (0..8).collect());
    // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        pmtiles_init(&mut pm_init as *mut MockInitContext as duckdb_init_info);
    }
    let mut pm_func = MockFuncContext {
        bind_data: pm_bind.bind_data,
        init_data: pm_init.init_data,
        local_init_data: ptr::null_mut(),
        error: None,
    };
    let mut pm_chunk = MockDataChunk::new(8, 2048);
    // SAFETY: the function context carries bind/init/local data installed above (still
    // owned by their live contexts), and the chunk has one 2048-row mock vector per
    // projected column.
    unsafe {
        pmtiles_scan(
            &mut pm_func as *mut MockFuncContext as duckdb_function_info,
            &mut pm_chunk as *mut MockDataChunk as duckdb_data_chunk,
        );
    }
    assert_eq!(pm_chunk.size, 1);
    let pm_status = std::str::from_utf8(&pm_chunk.vectors[6].strings[0]).unwrap();
    assert_eq!(pm_status, "SUCCESS", "PMTiles export failed: {pm_status}");
    // SAFETY: column 7 is the BIGINT `transform_failures`, written for row 0 above.
    let pm_failures = unsafe { *(pm_chunk.vectors[7].data.as_ptr() as *const i64) };
    assert_eq!(pm_failures, 0);
    assert!(pmtiles_path.exists());

    // 3. h3_parquet_to_pmtiles on the generated Parquet file
    let mut pp_bind = MockBindContext::new(
        vec![
            MockVal::Str(parquet_path.to_string_lossy().into_owned()),
            MockVal::Str(transcode_pmtiles_path.to_string_lossy().into_owned()),
        ],
        HashMap::new(),
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        parquet_pmtiles_bind(&mut pp_bind as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(pp_bind.error.is_none());

    let mut pp_init = MockInitContext::new(pp_bind.bind_data, (0..7).collect());
    // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        parquet_pmtiles_init(&mut pp_init as *mut MockInitContext as duckdb_init_info);
    }
    let mut pp_func = MockFuncContext {
        bind_data: pp_bind.bind_data,
        init_data: pp_init.init_data,
        local_init_data: ptr::null_mut(),
        error: None,
    };
    let mut pp_chunk = MockDataChunk::new(7, 2048);
    // SAFETY: the function context carries bind/init/local data installed above (still
    // owned by their live contexts), and the chunk has one 2048-row mock vector per
    // projected column.
    unsafe {
        parquet_pmtiles_scan(
            &mut pp_func as *mut MockFuncContext as duckdb_function_info,
            &mut pp_chunk as *mut MockDataChunk as duckdb_data_chunk,
        );
    }
    assert_eq!(pp_chunk.size, 1);
    let pp_status = std::str::from_utf8(&pp_chunk.vectors[6].strings[0]).unwrap();
    assert_eq!(pp_status, "SUCCESS", "Parquet->PMTiles failed: {pp_status}");
    // SAFETY: the callbacks above wrote values of these columns' declared types for rows
    // `0..size` into 8-aligned buffers with room for at least that many rows.
    let transcoded_count = unsafe { *(pp_chunk.vectors[0].data.as_ptr() as *const i64) };
    assert_eq!(transcoded_count, pq_hex_count);
    assert!(transcode_pmtiles_path.exists());

    let invalid_transcode_pmtiles_path = out_dir.path().join("invalid-column.pmtiles");
    let mut invalid_pp_named = HashMap::new();
    invalid_pp_named.insert(
        "h3_column".to_string(),
        MockVal::Str("missing_h3_column".to_string()),
    );
    let mut invalid_pp_bind = MockBindContext::new(
        vec![
            MockVal::Str(parquet_path.to_string_lossy().into_owned()),
            MockVal::Str(
                invalid_transcode_pmtiles_path
                    .to_string_lossy()
                    .into_owned(),
            ),
        ],
        invalid_pp_named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        parquet_pmtiles_bind(&mut invalid_pp_bind as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(invalid_pp_bind.error.is_none());

    let mut invalid_pp_init = MockInitContext::new(invalid_pp_bind.bind_data, (0..7).collect());
    // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        parquet_pmtiles_init(&mut invalid_pp_init as *mut MockInitContext as duckdb_init_info);
    }
    let mut invalid_pp_func = MockFuncContext {
        bind_data: invalid_pp_bind.bind_data,
        init_data: invalid_pp_init.init_data,
        local_init_data: ptr::null_mut(),
        error: None,
    };
    let mut invalid_pp_chunk = MockDataChunk::new(7, 2048);
    // SAFETY: the function context carries bind/init/local data installed above (still
    // owned by their live contexts), and the chunk has one 2048-row mock vector per
    // projected column.
    unsafe {
        parquet_pmtiles_scan(
            &mut invalid_pp_func as *mut MockFuncContext as duckdb_function_info,
            &mut invalid_pp_chunk as *mut MockDataChunk as duckdb_data_chunk,
        );
    }
    assert_eq!(invalid_pp_chunk.size, 1);
    let invalid_pp_status = std::str::from_utf8(&invalid_pp_chunk.vectors[6].strings[0]).unwrap();
    assert!(
        invalid_pp_status.starts_with("ERROR:"),
        "{invalid_pp_status}"
    );
    assert!(
        invalid_pp_status.contains("missing_h3_column"),
        "{invalid_pp_status}"
    );
    // SAFETY: the callbacks above wrote values of these columns' declared types for rows
    // `0..size` into 8-aligned buffers with room for at least that many rows.
    let invalid_count = unsafe { *(invalid_pp_chunk.vectors[0].data.as_ptr() as *const i64) };
    assert_eq!(invalid_count, 0);
    assert!(!invalid_transcode_pmtiles_path.exists());
}

#[test]
fn test_raster_to_parquet_reports_transform_failures() {
    install_mock_api();
    // 10 x 20 one-degree pixels from 95N: the top 5 rows lie beyond the pole.
    let (_tmp, tif_path) = TestGeoTiffBuilder::new(10, 20)
        .origin(0.0, 95.0)
        .pixel_size(1.0)
        .create_constant_tempfile(3.0f32);
    let out_dir = tempfile::tempdir().unwrap();
    let parquet_path = out_dir.path().join("past_pole.parquet");

    let mut named = HashMap::new();
    named.insert("resolution".to_string(), MockVal::Int(3));
    let mut bind = MockBindContext::new(
        vec![
            MockVal::Str(tif_path.to_string_lossy().into_owned()),
            MockVal::Str(parquet_path.to_string_lossy().into_owned()),
        ],
        named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        parquet_bind(&mut bind as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(bind.error.is_none());
    let mut init = MockInitContext::new(bind.bind_data, (0..7).collect());
    // SAFETY: the context is a live `MockInitContext` carrying the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        parquet_init(&mut init as *mut MockInitContext as duckdb_init_info);
    }
    let mut func = MockFuncContext {
        bind_data: bind.bind_data,
        init_data: init.init_data,
        local_init_data: ptr::null_mut(),
        error: None,
    };
    let mut chunk = MockDataChunk::new(7, 2048);
    // SAFETY: the function context carries bind/init data installed above (still owned
    // by their live contexts), and the chunk has one 2048-row vector per column.
    unsafe {
        parquet_scan(
            &mut func as *mut MockFuncContext as duckdb_function_info,
            &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
        );
    }
    assert_eq!(chunk.size, 1);
    let status = std::str::from_utf8(&chunk.vectors[5].strings[0]).unwrap();
    assert_eq!(status, "SUCCESS", "{status}");
    // SAFETY: column 6 is the BIGINT `transform_failures`, written for row 0 above.
    let failures = unsafe { *(chunk.vectors[6].data.as_ptr() as *const i64) };
    assert_eq!(failures, 50);
}

/// Bind `h3_raster_to_parquet(src, out, named...)`, run `between` after bind (as
/// DuckDB may between preparing and executing a plan), then scan. Returns the
/// status and the summary row count.
fn export_parquet_with(
    src: &std::path::Path,
    out: &std::path::Path,
    named: HashMap<String, MockVal>,
    between: impl FnOnce(),
) -> (String, i64) {
    install_mock_api();
    let mut bind = MockBindContext::new(
        vec![
            MockVal::Str(src.to_string_lossy().into_owned()),
            MockVal::Str(out.to_string_lossy().into_owned()),
        ],
        named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        parquet_bind(&mut bind as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(bind.error.is_none(), "{:?}", bind.error);
    between();
    let mut init = MockInitContext::new(bind.bind_data, (0..7).collect());
    // SAFETY: the context is a live `MockInitContext` carrying the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        parquet_init(&mut init as *mut MockInitContext as duckdb_init_info);
    }
    let mut func = MockFuncContext {
        bind_data: bind.bind_data,
        init_data: init.init_data,
        local_init_data: ptr::null_mut(),
        error: None,
    };
    let mut chunk = MockDataChunk::new(7, 2048);
    // SAFETY: the function context carries bind/init data installed above (still owned
    // by their live contexts), and the chunk has one 2048-row vector per column.
    unsafe {
        parquet_scan(
            &mut func as *mut MockFuncContext as duckdb_function_info,
            &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
        );
    }
    assert_eq!(chunk.size, 1);
    let status = String::from_utf8(chunk.vectors[5].strings[0].clone()).unwrap();
    // SAFETY: column 0 is the BIGINT `total_hexagons`, written for row 0 above.
    let count = unsafe { *(chunk.vectors[0].data.as_ptr() as *const i64) };
    (status, count)
}

fn multiband_fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dggs-stream-core/tests/fixtures")
        .join(name)
}

fn wide_options(compression: &str) -> HashMap<String, MockVal> {
    HashMap::from([
        ("resolution".to_string(), MockVal::Int(12)),
        ("bands".to_string(), MockVal::Str("3,1".to_string())),
        ("geoparquet".to_string(), MockVal::Bool(true)),
        (
            "compression".to_string(),
            MockVal::Str(compression.to_string()),
        ),
    ])
}

/// Row count, first-column codec and the sum of `band_1_mean` of a Parquet file.
fn wide_summary(path: &std::path::Path) -> (i64, parquet::basic::Compression, f64) {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;
    let file = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
    let meta = file.metadata();
    let names: Vec<&str> = meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name())
        .collect();
    let mean = names.iter().position(|n| *n == "band_1_mean").unwrap();
    let mut sum = 0.0;
    for row in file.get_row_iter(None).unwrap() {
        sum += row.unwrap().get_double(mean).unwrap_or(0.0);
    }
    (
        meta.file_metadata().num_rows(),
        meta.row_group(0).column(0).compression(),
        sum,
    )
}

#[test]
fn wide_export_reads_the_raster_present_at_execution() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.tif");
    let replacement = dir.path().join("replacement.tif");
    let out = dir.path().join("wide.parquet");

    // Reference exports of each raster on its own.
    std::fs::copy(multiband_fixture("multiband-12-planar.tif"), &src).unwrap();
    let (status, original_rows) = export_parquet_with(&src, &out, wide_options("zstd"), || {});
    assert_eq!(status, "SUCCESS");
    let original = wide_summary(&out);
    std::fs::copy(multiband_fixture("multiband-rgb.tif"), &src).unwrap();
    let (status, _) = export_parquet_with(&src, &out, wide_options("zstd"), || {});
    assert_eq!(status, "SUCCESS");
    let fresh = wide_summary(&out);
    assert!(
        fresh.0 != original.0 || fresh.2 != original.2,
        "fixtures must differ"
    );

    // Bind against the 12-band raster, then atomically rename the RGB raster over
    // it before execution: the export must scan the RGB raster.
    std::fs::copy(multiband_fixture("multiband-12-planar.tif"), &src).unwrap();
    std::fs::copy(multiband_fixture("multiband-rgb.tif"), &replacement).unwrap();
    let (status, rows) = export_parquet_with(&src, &out, wide_options("zstd"), || {
        std::fs::rename(&replacement, &src).unwrap();
    });
    assert_eq!(status, "SUCCESS");
    assert!(original_rows > 0);
    let replaced = wide_summary(&out);
    assert_eq!(rows, replaced.0);
    assert_eq!((replaced.0, replaced.2), (fresh.0, fresh.2));
}

#[test]
fn wide_export_honours_every_documented_codec() {
    use parquet::basic::Compression;
    let dir = tempfile::tempdir().unwrap();
    let src = multiband_fixture("multiband-12-interleaved.tif");
    for (option, expected) in [
        ("snappy", Compression::SNAPPY),
        ("zstd", Compression::ZSTD(Default::default())),
        ("gzip", Compression::GZIP(Default::default())),
        // Raw LZ4 framing, which DuckDB reads back.
        ("lz4", Compression::LZ4_RAW),
        ("brotli", Compression::BROTLI(Default::default())),
        ("none", Compression::UNCOMPRESSED),
    ] {
        let out = dir.path().join(format!("{option}.parquet"));
        let (status, rows) = export_parquet_with(&src, &out, wide_options(option), || {});
        assert_eq!(status, "SUCCESS", "{option}");
        let (written, codec, _) = wide_summary(&out);
        assert_eq!(written, rows, "{option}");
        assert_eq!(
            std::mem::discriminant(&codec),
            std::mem::discriminant(&expected),
            "{option}: {codec:?}"
        );
    }
}
