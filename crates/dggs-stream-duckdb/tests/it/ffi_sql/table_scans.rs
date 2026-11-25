// =============================================================================
// 3. Continuous & Categorical (Wide + Long) Table Function Execution via FFI
// =============================================================================

use std::collections::HashMap;

use dggs_stream_duckdb::ffi::duckdb_c::*;
use dggs_stream_duckdb::functions::categorical_table_function::{
    raster_h3_categorical_bind, raster_h3_categorical_init, raster_h3_categorical_init_local,
    raster_h3_categorical_scan,
};
use dggs_stream_duckdb::functions::table_function::{
    raster_h3_bind, raster_h3_init, raster_h3_init_local, raster_h3_scan,
    ContinuousRasterGlobalData,
};

use super::mock_api::install_mock_api;
use super::{MockBindContext, MockDataChunk, MockFuncContext, MockInitContext, MockVal};
use crate::helpers::TestGeoTiffBuilder;

#[test]
fn test_continuous_table_function_full_scan_and_projection_pushdown() {
    install_mock_api();
    let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| (c + r + 10) as f32);

    let mut named = HashMap::new();
    named.insert("resolution".to_string(), MockVal::Int(8));
    named.insert("geom".to_string(), MockVal::Bool(true));
    named.insert("quantiles".to_string(), MockVal::Str("p50,iqr".to_string()));
    named.insert("min_count".to_string(), MockVal::Double(0.5));
    named.insert("workers".to_string(), MockVal::UInt(2));

    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        raster_h3_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(
        bind_ctx.error.is_none(),
        "Bind failed: {:?}",
        bind_ctx.error
    );
    // 10 base columns + 1 geom + 2 quantiles = 13 columns
    assert_eq!(bind_ctx.result_columns.len(), 13);
    assert_eq!(bind_ctx.result_columns[10].0, "geom");
    assert_eq!(bind_ctx.result_columns[11].0, "p50");
    assert_eq!(bind_ctx.result_columns[12].0, "iqr");

    let all_cols: Vec<usize> = (0..13).collect();
    let mut init_ctx = MockInitContext::new(bind_ctx.bind_data, all_cols);
    let mut local_ctx = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        raster_h3_init(&mut init_ctx as *mut MockInitContext as duckdb_init_info);
        raster_h3_init_local(&mut local_ctx as *mut MockInitContext as duckdb_init_info);
    }
    assert!(init_ctx.error.is_none());

    let mut func_ctx = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_ctx.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };

    let mut total_rows = 0usize;
    let mut total_pixel_count = 0.0f64;
    loop {
        let mut chunk = MockDataChunk::new(13, 2048);
        // SAFETY: the function context carries bind/init/local data installed above (still
        // owned by their live contexts), and the chunk has one 2048-row mock vector per
        // projected column.
        unsafe {
            raster_h3_scan(
                &mut func_ctx as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        assert!(func_ctx.error.is_none());
        if chunk.size == 0 {
            break;
        }
        total_rows += chunk.size;
        // SAFETY: the callbacks above wrote values of these columns' declared types for
        // rows `0..size` into 8-aligned buffers with room for at least that many rows.
        unsafe {
            let counts = chunk.vectors[4].data.as_ptr() as *const f64;
            let p50s = chunk.vectors[11].data.as_ptr() as *const f64;
            let iqrs = chunk.vectors[12].data.as_ptr() as *const f64;
            for i in 0..chunk.size {
                total_pixel_count += *counts.add(i);
                assert!((*p50s.add(i)).is_finite());
                assert!((*iqrs.add(i)) >= 0.0);
                assert_eq!(chunk.vectors[9].strings[i].len(), 125);
                assert_eq!(chunk.vectors[10].strings[i].len(), 125);
            }
        }
    }
    assert!(total_rows > 0);
    assert!((total_pixel_count - 1024.0).abs() < 1e-6);

    // Zero-column projection pushdown (SELECT count(*))
    let mut init_zero = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        raster_h3_init(&mut init_zero as *mut MockInitContext as duckdb_init_info);
    }
    let mut func_zero = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_zero.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };
    let mut zero_col_rows = 0usize;
    loop {
        let mut chunk = MockDataChunk::new(0, 2048);
        // SAFETY: the function context carries bind/init/local data installed above (still
        // owned by their live contexts), and the chunk has one 2048-row mock vector per
        // projected column.
        unsafe {
            raster_h3_scan(
                &mut func_zero as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        if chunk.size == 0 {
            break;
        }
        zero_col_rows += chunk.size;
    }
    assert_eq!(zero_col_rows, total_rows);
}

#[test]
fn test_continuous_quantile_pushdown_and_batched_columns() {
    install_mock_api();
    // Negative, zero and positive values so sketches use every bin family.
    let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| ((c * 7 + r * 3) % 50) as f32 - 10.0);

    let mut named = HashMap::new();
    named.insert("resolution".to_string(), MockVal::Int(8));
    named.insert("geom".to_string(), MockVal::Bool(true));
    named.insert(
        "quantiles".to_string(),
        MockVal::Str("p50,p90,iqr".to_string()),
    );
    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        raster_h3_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(
        bind_ctx.error.is_none(),
        "Bind failed: {:?}",
        bind_ctx.error
    );
    // 10 base columns + geom (10) + p50 (11), p90 (12), iqr (13).
    assert_eq!(bind_ctx.result_columns.len(), 14);
    let bind_data = bind_ctx.bind_data;

    let tracks_quantiles = |projection: Vec<usize>| -> bool {
        let mut init = MockInitContext::new(bind_data, projection);
        // SAFETY: the context is a live `MockInitContext` carrying the bind data installed
        // by the bind callback, which is still owned by its bind context.
        unsafe {
            raster_h3_init(&mut init as *mut MockInitContext as duckdb_init_info);
        }
        assert!(init.error.is_none(), "Init failed: {:?}", init.error);
        // SAFETY: init installed a `ContinuousRasterGlobalData` as `init_data`; it stays alive
        // until `init` is dropped below, after the guard is released.
        let global = unsafe { &*(init.init_data as *const ContinuousRasterGlobalData) };
        let ContinuousRasterGlobalData::Single(global) = global else {
            panic!("expected single band")
        };
        let tracked = global.streamer.lock().unwrap().track_quantiles();
        drop(init);
        tracked
    };
    assert!(!tracks_quantiles(vec![0, 2, 4]), "no quantile columns");
    assert!(
        !tracks_quantiles(vec![0, 9, 10]),
        "wkb and geom are not quantiles"
    );
    assert!(!tracks_quantiles(Vec::new()), "count(*)");
    assert!(tracks_quantiles(vec![0, 13]), "iqr alone needs sketches");
    assert!(tracks_quantiles((0..14).collect()));

    // Scan `projection` (h3_index first, then f64 columns) into h3_index -> values.
    let scan = |projection: Vec<usize>| -> HashMap<u64, Vec<f64>> {
        let ncols = projection.len();
        let mut init = MockInitContext::new(bind_data, projection);
        let mut local = MockInitContext::new(bind_data, Vec::new());
        // SAFETY: the contexts are live `MockInitContext`s that carry the bind data
        // installed by the bind callback, which is still owned by its bind context.
        unsafe {
            raster_h3_init(&mut init as *mut MockInitContext as duckdb_init_info);
            raster_h3_init_local(&mut local as *mut MockInitContext as duckdb_init_info);
        }
        assert!(init.error.is_none(), "Init failed: {:?}", init.error);
        let mut func = MockFuncContext {
            bind_data,
            init_data: init.init_data,
            local_init_data: local.init_data,
            error: None,
        };
        let mut out = HashMap::new();
        loop {
            let mut chunk = MockDataChunk::new(ncols, 2048);
            // SAFETY: the function context carries bind/init/local data installed above
            // (still owned by their live contexts), and the chunk has one 2048-row mock
            // vector per projected column.
            unsafe {
                raster_h3_scan(
                    &mut func as *mut MockFuncContext as duckdb_function_info,
                    &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
                );
            }
            assert!(func.error.is_none(), "Scan failed: {:?}", func.error);
            if chunk.size == 0 {
                break;
            }
            // SAFETY: column 0 holds UBIGINT h3 indices and the rest DOUBLEs, written for
            // rows `0..size` into 8-aligned buffers with room for that many rows.
            unsafe {
                let cells = chunk.vectors[0].data.as_ptr();
                for i in 0..chunk.size {
                    let row = (1..ncols)
                        .map(|c| *(chunk.vectors[c].data.as_ptr() as *const f64).add(i))
                        .collect();
                    assert!(out.insert(*cells.add(i), row).is_none(), "duplicate cell");
                }
            }
        }
        out
    };

    // Reference: every DOUBLE column in schema order. Row positions:
    // mean 0, stddev 1, count 2, min 3, max 4, sum 5, p50 6, p90 7, iqr 8.
    let full = scan(vec![0, 2, 3, 4, 5, 6, 7, 11, 12, 13]);
    // Quantiles reordered and repeated, mixed with a base column: iqr, p50, p90, p50, mean.
    let got = scan(vec![0, 13, 11, 12, 11, 2]);
    assert!(!got.is_empty());
    assert_eq!(got.len(), full.len());

    let close = |a: f64, b: f64| (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0);
    for (cell, row) in &got {
        let f = &full[cell];
        let want = [f[8], f[6], f[7], f[6], f[0]];
        for (k, (&g, &w)) in row.iter().zip(&want).enumerate() {
            assert!(close(g, w), "cell {cell} column {k}: {g} vs {w}");
        }
        let (min, max, p50, p90, iqr) = (f[3], f[4], f[6], f[7], f[8]);
        assert!(
            (min - 1e-9..=p90 + 1e-9).contains(&p50),
            "p50 {p50} outside [{min}, {p90}]"
        );
        assert!(p90 <= max + 1e-9, "p90 {p90} above max {max}");
        assert!(iqr >= 0.0);
    }
}

#[test]
fn test_categorical_table_function_wide_and_long_formats() {
    install_mock_api();
    for grid in ["h3", "a5"] {
        let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
            .origin(-122.45, 37.80)
            .pixel_size(0.001)
            .create_tempfile::<u8, _>(|c, r| ((c + r) % 3 + 1) as u8);

        // 0. Invalid remap syntax sets bind error cleanly
        let mut bad_remap = HashMap::new();
        bad_remap.insert(
            "remap".to_string(),
            MockVal::Str("invalid_without_colon".to_string()),
        );
        let mut bind_bad = MockBindContext::new(
            vec![MockVal::Str(path.to_string_lossy().into_owned())],
            bad_remap,
        );
        // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
        // treats as the bind info.
        unsafe {
            raster_h3_categorical_bind(&mut bind_bad as *mut MockBindContext as duckdb_bind_info);
        }
        assert!(bind_bad.error.is_some());

        // 1. Wide Format with remap
        let mut named_wide = HashMap::new();
        named_wide.insert("grid".into(), MockVal::Str(grid.into()));
        named_wide.insert("resolution".to_string(), MockVal::Int(8));
        named_wide.insert("format".to_string(), MockVal::Str("wide".to_string()));
        named_wide.insert(
            "remap".to_string(),
            MockVal::Str("1: 10, 2: 20, 3: 30".to_string()),
        );
        named_wide.insert("geom".to_string(), MockVal::Bool(true));

        let mut bind_wide = MockBindContext::new(
            vec![MockVal::Str(path.to_string_lossy().into_owned())],
            named_wide,
        );
        // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
        // treats as the bind info.
        unsafe {
            raster_h3_categorical_bind(&mut bind_wide as *mut MockBindContext as duckdb_bind_info);
        }
        assert!(bind_wide.error.is_none());
        assert_eq!(bind_wide.result_columns.len(), 14);

        let wide_cols: Vec<usize> = (0..14).collect();
        let mut init_wide = MockInitContext::new(bind_wide.bind_data, wide_cols);
        let mut local_wide = MockInitContext::new(bind_wide.bind_data, Vec::new());
        // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
        // by the bind callback, which is still owned by its bind context.
        unsafe {
            raster_h3_categorical_init(&mut init_wide as *mut MockInitContext as duckdb_init_info);
            raster_h3_categorical_init_local(
                &mut local_wide as *mut MockInitContext as duckdb_init_info,
            );
        }
        let mut func_wide = MockFuncContext {
            bind_data: bind_wide.bind_data,
            init_data: init_wide.init_data,
            local_init_data: local_wide.init_data,
            error: None,
        };

        let mut wide_cells = 0usize;
        let mut wide_total_pixels = 0.0f64;
        loop {
            let mut chunk = MockDataChunk::new(14, 2048);
            // SAFETY: the function context carries bind/init/local data installed above (still
            // owned by their live contexts), and the chunk has one 2048-row mock vector per
            // projected column.
            unsafe {
                raster_h3_categorical_scan(
                    &mut func_wide as *mut MockFuncContext as duckdb_function_info,
                    &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
                );
            }
            if chunk.size == 0 {
                break;
            }
            wide_cells += chunk.size;
            // SAFETY: the callbacks above wrote values of these columns' declared types for
            // rows `0..size` into 8-aligned buffers with room for at least that many rows.
            unsafe {
                let maj_classes = chunk.vectors[2].data.as_ptr() as *const i64;
                let totals = chunk.vectors[6].data.as_ptr() as *const f64;
                for (i, hist) in chunk.vectors[7].strings[..chunk.size].iter().enumerate() {
                    let mc = *maj_classes.add(i);
                    assert!(
                        mc == 10 || mc == 20 || mc == 30,
                        "Remapped majority class must be in {{10, 20, 30}}, got {mc}"
                    );
                    wide_total_pixels += *totals.add(i);
                    let hist = std::str::from_utf8(hist).unwrap();
                    assert!(hist.starts_with('{') && hist.ends_with('}'));
                }
            }
        }
        assert!(wide_cells > 0);
        assert!((wide_total_pixels - 1024.0).abs() < 1e-6);

        // 2. Long Format
        let mut named_long = HashMap::new();
        named_long.insert("grid".into(), MockVal::Str(grid.into()));
        named_long.insert("resolution".to_string(), MockVal::Int(8));
        named_long.insert("format".to_string(), MockVal::Str("long".to_string()));
        named_long.insert(
            "remap".to_string(),
            MockVal::Str("1: 10, 2: 20, 3: 30".to_string()),
        );
        named_long.insert("geom".to_string(), MockVal::Bool(true));

        let mut bind_long = MockBindContext::new(
            vec![MockVal::Str(path.to_string_lossy().into_owned())],
            named_long,
        );
        // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
        // treats as the bind info.
        unsafe {
            raster_h3_categorical_bind(&mut bind_long as *mut MockBindContext as duckdb_bind_info);
        }
        assert!(bind_long.error.is_none());
        assert_eq!(bind_long.result_columns.len(), 13);

        let long_cols: Vec<usize> = (0..13).collect();
        let mut init_long = MockInitContext::new(bind_long.bind_data, long_cols);
        let mut local_long = MockInitContext::new(bind_long.bind_data, Vec::new());
        // SAFETY: the contexts are live `MockInitContext`s that carry the bind data installed
        // by the bind callback, which is still owned by its bind context.
        unsafe {
            raster_h3_categorical_init(&mut init_long as *mut MockInitContext as duckdb_init_info);
            raster_h3_categorical_init_local(
                &mut local_long as *mut MockInitContext as duckdb_init_info,
            );
        }
        let mut func_long = MockFuncContext {
            bind_data: bind_long.bind_data,
            init_data: init_long.init_data,
            local_init_data: local_long.init_data,
            error: None,
        };

        let mut long_rows = 0usize;
        let mut long_total_pixels = 0.0f64;
        loop {
            let mut chunk = MockDataChunk::new(13, 2048);
            // SAFETY: the function context carries bind/init/local data installed above (still
            // owned by their live contexts), and the chunk has one 2048-row mock vector per
            // projected column.
            unsafe {
                raster_h3_categorical_scan(
                    &mut func_long as *mut MockFuncContext as duckdb_function_info,
                    &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
                );
            }
            if chunk.size == 0 {
                break;
            }
            long_rows += chunk.size;
            // SAFETY: the callbacks above wrote values of these columns' declared types for
            // rows `0..size` into 8-aligned buffers with room for at least that many rows.
            unsafe {
                let cats = chunk.vectors[2].data.as_ptr() as *const i64;
                let counts = chunk.vectors[3].data.as_ptr() as *const f64;
                let fracs = chunk.vectors[4].data.as_ptr() as *const f64;
                for i in 0..chunk.size {
                    let c = *cats.add(i);
                    assert!(c == 10 || c == 20 || c == 30);
                    assert!(*fracs.add(i) > 0.0 && *fracs.add(i) <= 1.0);
                    long_total_pixels += *counts.add(i);
                    assert_eq!(
                        chunk.vectors[11].strings[i].len(),
                        if grid == "a5" { 109 } else { 125 }
                    );
                    assert_eq!(
                        chunk.vectors[12].strings[i].len(),
                        if grid == "a5" { 109 } else { 125 }
                    );
                }
            }
        }
        assert!(
            long_rows > wide_cells,
            "Long format must expand multi-class cells into multiple rows"
        );
        assert!((long_total_pixels - 1024.0).abs() < 1e-6);
    }
}

#[test]
fn test_a5_continuous_table_scan() {
    install_mock_api();
    let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| (c + r + 10) as f32);

    let mut named = HashMap::new();
    named.insert("grid".to_string(), MockVal::Str("a5".to_string()));
    named.insert("resolution".to_string(), MockVal::Int(14));
    named.insert("geom".to_string(), MockVal::Bool(true));

    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: live `MockBindContext` for the installed mock C API.
    unsafe {
        raster_h3_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(
        bind_ctx.error.is_none(),
        "A5 Bind failed: {:?}",
        bind_ctx.error
    );
    assert_eq!(bind_ctx.result_columns[0].0, "a5_index");
    assert_eq!(bind_ctx.result_columns[1].0, "a5_hex");
    assert_eq!(bind_ctx.result_columns[10].0, "geom");

    let all_cols: Vec<usize> = (0..11).collect();
    let mut init_ctx = MockInitContext::new(bind_ctx.bind_data, all_cols);
    let mut local_ctx = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: live `MockInitContext`s carrying bind_data from `bind_ctx`.
    unsafe {
        raster_h3_init(&mut init_ctx as *mut MockInitContext as duckdb_init_info);
        raster_h3_init_local(&mut local_ctx as *mut MockInitContext as duckdb_init_info);
    }
    assert!(init_ctx.error.is_none());

    let mut func_ctx = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_ctx.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };

    let mut total_rows = 0usize;
    let mut total_pixel_count = 0.0f64;
    loop {
        let mut chunk = MockDataChunk::new(11, 2048);
        // SAFETY: live `MockFuncContext` and `MockDataChunk`.
        unsafe {
            raster_h3_scan(
                &mut func_ctx as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        assert!(func_ctx.error.is_none());
        if chunk.size == 0 {
            break;
        }
        total_rows += chunk.size;
        // SAFETY: columns populated by `raster_h3_scan`.
        unsafe {
            let cells = chunk.vectors[0].data.as_ptr();
            let counts = chunk.vectors[4].data.as_ptr() as *const f64;
            for i in 0..chunk.size {
                let cell = *cells.add(i);
                assert!(dggs_stream_duckdb::grid::a5_is_valid_cell(cell));
                total_pixel_count += *counts.add(i);
                // A5 pentagon WKB is 109 bytes (5 vertices + closing vertex = 6 points * 16 + 13)
                assert_eq!(chunk.vectors[9].strings[i].len(), 109);
                assert_eq!(chunk.vectors[10].strings[i].len(), 109);
            }
        }
    }
    assert!(total_rows > 0);
    assert!((total_pixel_count - 1024.0).abs() < 1e-6);
}

/// Scan `path` on `grid` with the continuous table function, returning `(cell, count)`
/// per row and checking the columns every grid shares.
fn scan_nested_grid(
    path: &std::path::Path,
    grid: dggs_stream_duckdb::grid::GridKind,
    extra: Option<(String, MockVal)>,
) -> Vec<(u64, f64)> {
    let mut named = HashMap::new();
    named.insert("grid".to_string(), MockVal::Str(grid.to_string()));
    named.insert("resolution".to_string(), MockVal::Int(14));
    named.insert("geom".to_string(), MockVal::Bool(true));
    named.extend(extra);

    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: live `MockBindContext` for the installed mock C API.
    unsafe {
        raster_h3_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(
        bind_ctx.error.is_none(),
        "{grid} bind: {:?}",
        bind_ctx.error
    );
    assert_eq!(bind_ctx.result_columns[0].0, grid.index_column_name());
    assert_eq!(bind_ctx.result_columns[1].0, grid.hex_column_name());
    assert_eq!(bind_ctx.result_columns[10].0, "geom");

    let mut init_ctx = MockInitContext::new(bind_ctx.bind_data, (0..11).collect());
    let mut local_ctx = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: live `MockInitContext`s carrying bind_data from `bind_ctx`.
    unsafe {
        raster_h3_init(&mut init_ctx as *mut MockInitContext as duckdb_init_info);
        raster_h3_init_local(&mut local_ctx as *mut MockInitContext as duckdb_init_info);
    }
    assert!(init_ctx.error.is_none());

    let mut func_ctx = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_ctx.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };
    let mut rows = Vec::new();
    loop {
        let mut chunk = MockDataChunk::new(11, 2048);
        // SAFETY: live `MockFuncContext` and `MockDataChunk`.
        unsafe {
            raster_h3_scan(
                &mut func_ctx as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        assert!(func_ctx.error.is_none());
        if chunk.size == 0 {
            break;
        }
        // SAFETY: columns populated by `raster_h3_scan`.
        unsafe {
            let cells = chunk.vectors[0].data.as_ptr();
            let counts = chunk.vectors[4].data.as_ptr() as *const f64;
            for i in 0..chunk.size {
                let cell = *cells.add(i);
                assert_eq!(grid.resolution(cell), Some(14), "{grid}");
                assert_eq!(
                    std::str::from_utf8(&chunk.vectors[1].strings[i]).unwrap(),
                    format!("{cell:x}")
                );
                // Four corners plus the closing point: 13 + 5 * 16 bytes.
                assert_eq!(chunk.vectors[9].strings[i].len(), 93);
                assert_eq!(chunk.vectors[10].strings[i], chunk.vectors[9].strings[i]);
                rows.push((cell, *counts.add(i)));
            }
        }
    }
    rows
}

#[test]
fn test_s2_and_healpix_continuous_table_scans_and_cell_filters() {
    use dggs_stream_duckdb::grid::GridKind;
    install_mock_api();
    let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| (c + r + 10) as f32);

    for grid in [GridKind::S2, GridKind::Healpix] {
        let rows = scan_nested_grid(&path, grid, None);
        assert!(rows.len() > 1, "{grid}");
        let pixels: f64 = rows.iter().map(|r| r.1).sum();
        assert!((pixels - 1024.0).abs() < 1e-6, "{grid}");
        // Every pixel centre's cell is present.
        for (col, row) in [(0.5, 0.5), (15.5, 20.5), (31.5, 31.5)] {
            let cell = grid
                .cell_at(37.80 - row * 0.001, -122.45 + col * 0.001, 14)
                .unwrap();
            assert!(rows.iter().any(|r| r.0 == cell), "{grid}");
        }

        // `<grid>_cell` and `<grid>_hex` restrict the scan to one coarser cell's box.
        let parent = grid.parent(rows[rows.len() / 2].0, 13).unwrap();
        let mut children = Vec::new();
        grid.children_into(parent, 14, &mut children);
        for filter in [
            (format!("{grid}_cell"), MockVal::UInt(parent)),
            (format!("{grid}_hex"), MockVal::Str(format!("{parent:x}"))),
        ] {
            let filtered = scan_nested_grid(&path, grid, Some(filter));
            assert!(filtered.len() < rows.len(), "{grid}");
            for (cell, count) in &rows {
                if children.contains(cell) {
                    // Cells inside the filter cell keep all their pixels.
                    assert!(filtered.contains(&(*cell, *count)), "{grid}");
                }
            }
        }
    }
}

/// Binds and scans `raster_h3_bind` with `grid :=` and `resolutions :=`, returning the
/// distinct resolutions of the cells it emits, or the bind error.
fn scan_resolutions(
    path: &std::path::Path,
    grid: dggs_stream_duckdb::grid::GridKind,
    resolutions: &str,
) -> Result<Vec<u8>, String> {
    let mut named = HashMap::new();
    named.insert("grid".to_string(), MockVal::Str(grid.to_string()));
    named.insert(
        "resolutions".to_string(),
        MockVal::Str(resolutions.to_string()),
    );
    named.insert("geom".to_string(), MockVal::Bool(true));
    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: live `MockBindContext` for the installed mock C API.
    unsafe {
        raster_h3_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    if let Some(error) = bind_ctx.error.take() {
        return Err(error);
    }
    let columns = bind_ctx.result_columns.len();
    let mut init_ctx = MockInitContext::new(bind_ctx.bind_data, (0..columns).collect());
    let mut local_ctx = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: live `MockInitContext`s carrying bind_data from `bind_ctx`.
    unsafe {
        raster_h3_init(&mut init_ctx as *mut MockInitContext as duckdb_init_info);
        raster_h3_init_local(&mut local_ctx as *mut MockInitContext as duckdb_init_info);
    }
    assert!(init_ctx.error.is_none());
    let mut func_ctx = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_ctx.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };
    let mut seen = Vec::new();
    loop {
        let mut chunk = MockDataChunk::new(columns, 2048);
        // SAFETY: live `MockFuncContext` and `MockDataChunk`.
        unsafe {
            raster_h3_scan(
                &mut func_ctx as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        assert!(func_ctx.error.is_none());
        if chunk.size == 0 {
            break;
        }
        // SAFETY: column 0 holds `chunk.size` 64-bit cell ids written by `raster_h3_scan`.
        let cells =
            unsafe { std::slice::from_raw_parts(chunk.vectors[0].data.as_ptr(), chunk.size) };
        for &cell in cells {
            let res = grid.resolution(cell).expect("a valid cell");
            if !seen.contains(&res) {
                seen.push(res);
            }
        }
    }
    Ok(seen)
}

#[test]
fn test_auto_resolution_on_every_grid() {
    use dggs_stream_core::raster::mosaic::{MosaicReader, OverlapRule};
    use dggs_stream_core::resolution::{auto_resolution, offset_resolution};
    use dggs_stream_duckdb::grid::GridKind;
    install_mock_api();
    let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| (c + r + 10) as f32);
    let mosaic = MosaicReader::open(
        std::slice::from_ref(&path),
        None,
        None,
        OverlapRule::default(),
    )
    .unwrap();
    for grid in GridKind::ALL {
        let matched = auto_resolution(grid, &mosaic, 0).unwrap();
        for (text, offset) in [("auto", 0), ("AUTO+1", 1), ("auto-2", -2)] {
            assert_eq!(
                scan_resolutions(&path, grid, text).unwrap(),
                vec![offset_resolution(grid, matched, offset)],
                "{grid} {text}"
            );
        }
        // Explicit lists still work, and a malformed `auto` is a bind error.
        assert_eq!(scan_resolutions(&path, grid, "5").unwrap(), vec![5]);
        let err = scan_resolutions(&path, grid, "auto+x").unwrap_err();
        assert!(err.contains("auto"), "{err}");
    }
    let missing = std::path::Path::new("/nonexistent/raster.tif");
    assert!(scan_resolutions(missing, GridKind::H3, "auto").is_err());
}

/// One row of a continuous scan: `(cell, mean or NULL, count, min or NULL)`.
type StatRow = (u64, Option<f64>, f64, Option<f64>);

/// Binds and scans `raster_h3_bind` with the given named parameters.
fn scan_stats(path: &std::path::Path, extra: Vec<(&str, MockVal)>) -> Result<Vec<StatRow>, String> {
    let mut named = HashMap::new();
    named.insert("geom".to_string(), MockVal::Bool(true));
    for (name, value) in extra {
        named.insert(name.to_string(), value);
    }
    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: live `MockBindContext` for the installed mock C API.
    unsafe {
        raster_h3_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    if let Some(error) = bind_ctx.error.take() {
        return Err(error);
    }
    let columns = bind_ctx.result_columns.len();
    let mut init_ctx = MockInitContext::new(bind_ctx.bind_data, (0..columns).collect());
    let mut local_ctx = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: live `MockInitContext`s carrying bind_data from `bind_ctx`.
    unsafe {
        raster_h3_init(&mut init_ctx as *mut MockInitContext as duckdb_init_info);
        raster_h3_init_local(&mut local_ctx as *mut MockInitContext as duckdb_init_info);
    }
    if let Some(error) = init_ctx.error.take() {
        return Err(error);
    }
    let mut func_ctx = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_ctx.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };
    let mut rows = Vec::new();
    loop {
        let mut chunk = MockDataChunk::new(columns, 2048);
        // SAFETY: live `MockFuncContext` and `MockDataChunk`.
        unsafe {
            raster_h3_scan(
                &mut func_ctx as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        assert!(func_ctx.error.is_none());
        if chunk.size == 0 {
            break;
        }
        for i in 0..chunk.size {
            // SAFETY: columns 0 (cell), 2 (mean), 4 (count) and 5 (min) hold `chunk.size`
            // 8-byte values written by `raster_h3_scan`.
            let f =
                |col: usize| unsafe { *(chunk.vectors[col].data.as_ptr() as *const f64).add(i) };
            let cell = chunk.vectors[0].data[i];
            let mean = chunk.vectors[2].is_valid(i).then(|| f(2));
            let min = chunk.vectors[5].is_valid(i).then(|| f(5));
            rows.push((cell, mean, f(4), min));
        }
    }
    rows.sort_by_key(|r| r.0);
    Ok(rows)
}

#[test]
fn test_nodata_policy_and_decimals() {
    install_mock_api();
    // NoData in the left third; values with fractional means elsewhere.
    let (_tmp, path) = TestGeoTiffBuilder::new(48, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| {
            if c < 16 {
                -9999.0
            } else {
                ((c * 7 + r * 3) % 11) as f32 + 0.25
            }
        });
    let base = || {
        vec![
            ("resolution", MockVal::Int(9)),
            ("nodata", MockVal::Double(-9999.0)),
        ]
    };
    let with = |extra: Vec<(&'static str, MockVal)>| {
        let mut params = base();
        params.extend(extra);
        scan_stats(&path, params)
    };

    let omit = with(vec![]).unwrap();
    assert!(omit.iter().all(|r| r.1.is_some() && r.2 > 0.0));
    assert_eq!(
        with(vec![("nodata_policy", MockVal::Str("omit".into()))]).unwrap(),
        omit
    );

    // emit: the all-NoData cells appear with count 0 and NULL statistics.
    let emit = with(vec![("nodata_policy", MockVal::Str("EMIT".into()))]).unwrap();
    let empty: Vec<_> = emit.iter().filter(|r| r.2 == 0.0).collect();
    assert!(!empty.is_empty() && emit.len() == omit.len() + empty.len());
    assert!(empty.iter().all(|r| r.1.is_none() && r.3.is_none()));
    let kept: Vec<_> = emit.iter().filter(|r| r.2 > 0.0).copied().collect();
    assert_eq!(kept, omit);

    // emit with a value: it replaces NULL.
    let filled = with(vec![
        ("nodata_policy", MockVal::Str("emit".into())),
        ("emit_nodata_value", MockVal::Double(-1.5)),
    ])
    .unwrap();
    assert_eq!(filled.len(), emit.len());
    for row in filled.iter().filter(|r| r.2 == 0.0) {
        assert_eq!((row.1, row.3), (Some(-1.5), Some(-1.5)));
    }

    // decimals: statistics are rounded, counts are not.
    let rounded = with(vec![("decimals", MockVal::Int(1))]).unwrap();
    assert_eq!(rounded.len(), omit.len());
    assert!(omit.iter().any(|r| (r.1.unwrap() * 10.0).fract() != 0.0));
    for (plain, round) in omit.iter().zip(&rounded) {
        assert_eq!(round.0, plain.0);
        assert_eq!(round.1, Some((plain.1.unwrap() * 10.0).round() / 10.0));
        assert_eq!(round.2, plain.2);
    }

    for (bad, needle) in [
        (
            vec![("nodata_policy", MockVal::Str("drop".into()))],
            "nodata_policy",
        ),
        (vec![("decimals", MockVal::Int(16))], "decimals"),
        (vec![("decimals", MockVal::Int(-1))], "decimals"),
        (
            vec![("emit_nodata_value", MockVal::Double(0.0))],
            "emit_nodata_value",
        ),
        (
            vec![
                ("nodata_policy", MockVal::Str("emit".into())),
                ("min_count", MockVal::Double(2.0)),
            ],
            "min_count",
        ),
    ] {
        let err = with(bad).unwrap_err();
        assert!(err.contains(needle), "{err}");
    }
}

#[test]
fn test_transfer_modes() {
    use dggs_stream_core::resolution::area_matched_resolution;
    use dggs_stream_duckdb::grid::GridKind;
    install_mock_api();
    let value = |c: u32, r: u32| ((c * 7 + r * 13) % 23 + 1) as f32;
    let (_tmp, path) = TestGeoTiffBuilder::new(40, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(value);
    let total: f64 = (0..32)
        .flat_map(|r| (0..40).map(move |c| f64::from(value(c, r))))
        .sum();
    let pixel_m2 = 111_320.0f64.powi(2) * 1e-6 * 37.8f64.to_radians().cos();
    for grid in GridKind::ALL {
        let matched = i64::from(area_matched_resolution(grid, pixel_m2));
        let scan = |res: i64, extra: Vec<(&'static str, MockVal)>| {
            let mut params = vec![
                ("grid", MockVal::Str(grid.to_string())),
                ("resolution", MockVal::Int(res)),
            ];
            params.extend(extra);
            scan_stats(&path, params)
        };
        // `(count, mean)` totals of a scan: overlapped area and mass.
        let totals = |rows: &[StatRow]| {
            rows.iter().fold((0.0, 0.0), |(area, mass), r| {
                (area + r.2, mass + r.2 * r.1.unwrap())
            })
        };
        let centres = scan(matched - 1, vec![]).unwrap();
        let overlay = scan(
            matched - 1,
            vec![("transfer", MockVal::Str("overlay".into()))],
        )
        .unwrap();
        assert!(overlay.len() >= centres.len(), "{grid}");
        let (area, mass) = totals(&overlay);
        assert!((area / 1280.0 - 1.0).abs() < 1e-6, "{grid}: {area}");
        assert!((mass / total - 1.0).abs() < 1e-6, "{grid}: {mass}");
        let preserved = scan(
            matched - 1,
            vec![
                ("transfer", MockVal::Str("overlay".into())),
                ("mass_preserve", MockVal::Bool(true)),
            ],
        )
        .unwrap();
        let (area, mass) = totals(&preserved);
        assert!(
            (area - 1280.0).abs() < 1e-6 && (mass - total).abs() < 1e-6 * total,
            "{grid}"
        );

        // Sampling two steps finer than the pixels fills every cell on the raster.
        let fine_centres = scan(matched + 2, vec![]).unwrap();
        let sampled = scan(
            matched + 2,
            vec![
                ("transfer", MockVal::Str("interpolate".into())),
                ("interpolation", MockVal::Str("nearest".into())),
            ],
        )
        .unwrap();
        assert!(sampled.len() > 3 * fine_centres.len(), "{grid}");
        for row in &sampled {
            let (lat, lng) = grid.center_lat_lng(row.0).unwrap();
            let c = ((lng + 122.45) / 0.001).floor() as u32;
            let r = ((37.80 - lat) / 0.001).floor() as u32;
            assert_eq!(row.1, Some(f64::from(value(c, r))), "{grid}");
            assert_eq!(row.2, 1.0);
        }

        for (bad, needle) in [
            (vec![("transfer", MockVal::Str("smear".into()))], "transfer"),
            (
                vec![("interpolation", MockVal::Str("bilinear".into()))],
                "interpolation",
            ),
            (
                vec![("mass_preserve", MockVal::Bool(true))],
                "mass_preserve",
            ),
        ] {
            let err = scan(matched, bad).unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
    }
}

/// `profile := true` leaves the scan's rows alone and keeps its telemetry as the core's
/// last report.
#[test]
fn test_profile_named_parameter() {
    install_mock_api();
    let (_tmp, path) = TestGeoTiffBuilder::new(40, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| (c + r) as f32);
    let plain = scan_stats(&path, vec![("resolution", MockVal::Int(9))]).unwrap();
    assert!(dggs_stream_core::run_profile::last_report().is_none());
    let profiled = scan_stats(
        &path,
        vec![
            ("resolution", MockVal::Int(9)),
            ("profile", MockVal::Bool(true)),
        ],
    )
    .unwrap();
    assert_eq!(plain, profiled);
    let report = dggs_stream_core::run_profile::last_report().expect("a report");
    assert_eq!(report.pixels_read, 40 * 32);
    assert_eq!(report.cells as usize, plain.len());
    assert!(report.phase("kernel").unwrap().wall_ms > 0.0);
    assert!(report.phase("grid_index").is_some());
}

/// Wide categorical rows as `(cell, total_count, histogram text)`.
fn scan_histograms(
    path: &std::path::Path,
    extra: Vec<(&str, MockVal)>,
) -> Result<Vec<(u64, f64, String)>, String> {
    let mut named = HashMap::new();
    for (name, value) in extra {
        named.insert(name.to_string(), value);
    }
    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: live `MockBindContext` for the installed mock C API.
    unsafe {
        raster_h3_categorical_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    if let Some(error) = bind_ctx.error.take() {
        return Err(error);
    }
    let columns = bind_ctx.result_columns.len();
    let mut init_ctx = MockInitContext::new(bind_ctx.bind_data, (0..columns).collect());
    let mut local_ctx = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: live `MockInitContext`s carrying bind_data from `bind_ctx`.
    unsafe {
        raster_h3_categorical_init(&mut init_ctx as *mut MockInitContext as duckdb_init_info);
        raster_h3_categorical_init_local(
            &mut local_ctx as *mut MockInitContext as duckdb_init_info,
        );
    }
    if let Some(error) = init_ctx.error.take() {
        return Err(error);
    }
    let mut func_ctx = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_ctx.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };
    let mut rows = Vec::new();
    loop {
        let mut chunk = MockDataChunk::new(columns, 2048);
        // SAFETY: live `MockFuncContext` and `MockDataChunk`.
        unsafe {
            raster_h3_categorical_scan(
                &mut func_ctx as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        assert!(func_ctx.error.is_none());
        if chunk.size == 0 {
            break;
        }
        for i in 0..chunk.size {
            // SAFETY: column 6 (total_count) holds `chunk.size` doubles written by the scan.
            let total = unsafe { *(chunk.vectors[6].data.as_ptr() as *const f64).add(i) };
            let text = String::from_utf8(chunk.vectors[7].strings[i].clone()).unwrap();
            rows.push((chunk.vectors[0].data[i], total, text));
        }
    }
    Ok(rows)
}

/// `histogram :=` writes the `histogram` column as the spec asks, on every grid.
#[test]
fn test_histogram_parameters() {
    use dggs_stream_duckdb::grid::GridKind;
    install_mock_api();
    let value = |c: u32, r: u32| ((c * 7 + r * 13) % 40) as f32 * 0.25 + 0.1;
    let (_tmp, path) = TestGeoTiffBuilder::new(40, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(value);
    // Pixels per bin of [0, 2, 4, 6, 8]; values above 8 are not counted.
    let mut per_bin = [0.0f64; 4];
    for r in 0..32 {
        for c in 0..40 {
            let v = value(c, r);
            if v <= 8.0 {
                per_bin[((v / 2.0) as usize).min(3)] += 1.0;
            }
        }
    }
    for grid in GridKind::ALL {
        let scan = |extra: Vec<(&'static str, MockVal)>| {
            let mut params = vec![
                ("grid", MockVal::Str(grid.to_string())),
                ("resolutions", MockVal::Str("auto-1".into())),
            ];
            params.extend(extra);
            scan_histograms(&path, params)
        };
        // `[[lower, upper, weight], ...]` per cell, summed per bin over the cells.
        let bins = |rows: &[(u64, f64, String)]| -> [f64; 4] {
            let mut sums = [0.0; 4];
            for (_, total, text) in rows {
                let entries: Vec<[f64; 3]> = serde_json::from_str(text).unwrap();
                assert_eq!(entries.len(), 4);
                for (i, [lower, upper, weight]) in entries.iter().enumerate() {
                    assert_eq!((*lower, *upper), (2.0 * i as f64, 2.0 * i as f64 + 2.0));
                    sums[i] += weight;
                }
                let weights: f64 = entries.iter().map(|e| e[2]).sum();
                assert!((weights - total).abs() < 1e-9 || weights <= 1.0 + 1e-9);
            }
            sums
        };
        let counts = scan(vec![("histogram", MockVal::Str("4:0:8".into()))]).unwrap();
        assert_eq!(bins(&counts), per_bin, "{grid}");
        let edges = scan(vec![("histogram", MockVal::Str("0,2,4,6,8".into()))]).unwrap();
        assert_eq!(edges, counts, "{grid}");
        let area = scan(vec![
            ("histogram", MockVal::Str("4:0:8".into())),
            ("histogram_weight", MockVal::Str("area".into())),
        ])
        .unwrap();
        for (got, want) in bins(&area).iter().zip(per_bin) {
            assert!((got / want - 1.0).abs() < 1e-6, "{grid}: {got} vs {want}");
        }
        // One cell holding the whole raster (resolution 0) still takes every pixel.
        let whole = scan_histograms(
            &path,
            vec![
                ("grid", MockVal::Str(grid.to_string())),
                ("resolution", MockVal::Int(0)),
                ("histogram", MockVal::Str("4:0:8".into())),
                ("histogram_weight", MockVal::Str("area".into())),
            ],
        )
        .unwrap();
        for (got, want) in bins(&whole).iter().zip(per_bin) {
            assert!(
                (got / want - 1.0).abs() < 1e-6,
                "{grid} res 0: {got} vs {want}"
            );
        }
        let fractions = scan(vec![
            ("histogram", MockVal::Str("4:0:8".into())),
            ("histogram_normalize", MockVal::Str("fraction".into())),
        ])
        .unwrap();
        let total: f64 = bins(&fractions).iter().sum();
        assert!((total - fractions.len() as f64).abs() < 1e-9, "{grid}");

        // By value: an object of class weights (counts, not the default fractions).
        let values = scan(vec![("histogram", MockVal::Str("values".into()))]).unwrap();
        for (_, total, text) in &values {
            let classes: std::collections::BTreeMap<String, f64> =
                serde_json::from_str(text).unwrap();
            assert_eq!(classes.values().sum::<f64>(), *total, "{grid}");
        }

        for bad in [
            vec![("histogram", MockVal::Str("0:0:1".into()))],
            vec![("histogram_normalize", MockVal::Str("density".into()))],
            vec![("histogram_weight", MockVal::Str("mass".into()))],
            vec![
                ("histogram_weight", MockVal::Str("area".into())),
                ("transfer", MockVal::Str("interpolate".into())),
            ],
        ] {
            assert!(scan(bad.clone()).is_err(), "{grid}: {bad:?}");
        }
    }
}

/// The table function and the CLI return the same cells and statistics for the same
/// raster and options, on every grid and in every transfer mode. The CLI is compared
/// with the Python bindings and DataFusion SQL, on a raster with the same pixels, by
/// `cross_engine` in `dggs-stream-sedona`.
#[test]
fn test_cross_engine_consistency_with_the_cli() {
    use dggs_stream_cli::{parse_stream_args, stream_raster_to_writer, StreamOutputFormat};
    use dggs_stream_duckdb::grid::GridKind;
    install_mock_api();
    // Pixel values are the column index: a field linear in longitude.
    let (_tmp, path) = TestGeoTiffBuilder::new(48, 48)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, _| c as f32);
    let source = path.to_string_lossy().into_owned();
    type Options = Vec<(&'static str, MockVal)>;
    let text = |s: &str| MockVal::Str(s.into());
    let cases: Vec<(&str, &str, &[&str], Options)> = vec![
        ("centres", "auto", &[], vec![]),
        (
            "centres, emit, decimals",
            "auto-1",
            &["--nodata-policy", "emit", "--decimals", "2"],
            vec![
                ("nodata_policy", text("emit")),
                ("decimals", MockVal::Int(2)),
            ],
        ),
        (
            "overlay, partly missing cells",
            "auto-1",
            &[
                "--transfer",
                "overlay",
                "--nodata",
                "20",
                "--valid-coverage-threshold",
                "0.95",
            ],
            vec![
                ("transfer", text("overlay")),
                ("nodata", MockVal::Double(20.0)),
                ("valid_coverage_threshold", MockVal::Double(0.95)),
            ],
        ),
        (
            "overlay, mass preserving",
            "auto-1",
            &["--transfer", "overlay", "--mass-preserve"],
            vec![
                ("transfer", text("overlay")),
                ("mass_preserve", MockVal::Bool(true)),
            ],
        ),
        (
            "sample, bilinear",
            "auto+1",
            &["--transfer", "interpolate", "--interpolation", "bilinear"],
            vec![
                ("transfer", text("interpolate")),
                ("interpolation", text("bilinear")),
            ],
        ),
        (
            "sample, bicubic",
            "auto+1",
            &["--transfer", "interpolate", "--interpolation", "bicubic"],
            vec![
                ("transfer", text("interpolate")),
                ("interpolation", text("bicubic")),
            ],
        ),
    ];
    let close = |x: f64, y: f64| (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0);
    for grid in GridKind::ALL {
        for (name, resolution, cli_options, sql_options) in &cases {
            let what = format!("{grid}, {name}");
            let mut args: Vec<String> = vec![source.clone(), "--grid".into(), grid.to_string()];
            args.extend(["-r".to_string(), resolution.to_string()]);
            args.extend(cli_options.iter().map(|s| s.to_string()));
            let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl).unwrap();
            let mut out = Vec::new();
            stream_raster_to_writer(&opts, &mut out).unwrap();
            let column = grid.index_column_name();
            // `cell -> (count, mean, min)`.
            let from_cli: std::collections::BTreeMap<u64, (f64, Option<f64>, Option<f64>)> =
                String::from_utf8(out)
                    .unwrap()
                    .lines()
                    .map(|line| {
                        let row: serde_json::Value = serde_json::from_str(line).unwrap();
                        (
                            row[column].as_u64().unwrap(),
                            (
                                row["count"].as_f64().unwrap(),
                                row["mean"].as_f64(),
                                row["min"].as_f64(),
                            ),
                        )
                    })
                    .collect();
            assert!(!from_cli.is_empty(), "{what}");

            let mut params = vec![
                ("grid", MockVal::Str(grid.to_string())),
                ("resolutions", MockVal::Str(resolution.to_string())),
            ];
            params.extend(sql_options.iter().cloned());
            let rows = scan_stats(&path, params).unwrap();
            assert_eq!(rows.len(), from_cli.len(), "{what}: cells");
            for (cell, mean, count, min) in rows {
                let (cli_count, cli_mean, cli_min) = *from_cli
                    .get(&cell)
                    .unwrap_or_else(|| panic!("{what}: {cell:x}"));
                assert!(close(count, cli_count), "{what}: count of {cell:x}");
                for (ours, theirs) in [(mean, cli_mean), (min, cli_min)] {
                    match (ours.filter(|v| v.is_finite()), theirs) {
                        (Some(x), Some(y)) => assert!(close(x, y), "{what}: {cell:x}"),
                        (None, None) => {}
                        other => panic!("{what}: {cell:x}: {other:?}"),
                    }
                }
            }
        }
    }
}

/// `(fraction, entropy)` of every row of a long-format categorical scan.
fn long_fractions(path: &std::path::Path, decimals: Option<i64>) -> Vec<(f64, f64)> {
    let mut named = HashMap::new();
    named.insert("resolution".to_string(), MockVal::Int(7));
    named.insert("format".to_string(), MockVal::Str("long".to_string()));
    if let Some(d) = decimals {
        named.insert("decimals".to_string(), MockVal::Int(d));
    }
    let mut bind = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: a live `MockBindContext`, which the mock C API treats as the bind info.
    unsafe {
        raster_h3_categorical_bind(&mut bind as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(bind.error.is_none(), "{:?}", bind.error);
    let columns = bind.result_columns.len();
    let mut init = MockInitContext::new(bind.bind_data, (0..columns).collect());
    let mut local = MockInitContext::new(bind.bind_data, Vec::new());
    // SAFETY: live `MockInitContext`s carrying the bind data installed above.
    unsafe {
        raster_h3_categorical_init(&mut init as *mut MockInitContext as duckdb_init_info);
        raster_h3_categorical_init_local(&mut local as *mut MockInitContext as duckdb_init_info);
    }
    let mut func = MockFuncContext {
        bind_data: bind.bind_data,
        init_data: init.init_data,
        local_init_data: local.init_data,
        error: None,
    };
    let mut out = Vec::new();
    loop {
        let mut chunk = MockDataChunk::new(columns, 2048);
        // SAFETY: live function context and a chunk with one vector per column.
        unsafe {
            raster_h3_categorical_scan(
                &mut func as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        assert!(func.error.is_none());
        if chunk.size == 0 {
            break;
        }
        for i in 0..chunk.size {
            // SAFETY: columns 4 (fraction) and 8 (entropy) hold `chunk.size` doubles.
            let f =
                |col: usize| unsafe { *(chunk.vectors[col].data.as_ptr() as *const f64).add(i) };
            out.push((f(4), f(8)));
        }
    }
    out
}

#[test]
fn long_categorical_rows_are_rounded() {
    install_mock_api();
    let (_tmp, path) = TestGeoTiffBuilder::new(48, 48)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_tempfile(|c, r| ((c * 7 + r * 3) % 3 + 1) as u8);
    let plain = long_fractions(&path, None);
    let rounded = long_fractions(&path, Some(2));
    assert_eq!(plain.len(), rounded.len());
    let round = |v: f64| (v * 100.0).round() / 100.0;
    assert!(plain.iter().any(|&(f, e)| f != round(f) || e != round(e)));
    for (p, r) in plain.iter().zip(&rounded) {
        assert_eq!(*r, (round(p.0), round(p.1)));
    }
}

/// Rows a ground-area scan returns for `projection`, scanning to the end.
fn ground_area_row_count(path: &std::path::Path, projection: Vec<usize>) -> Result<usize, String> {
    let named = HashMap::from([
        ("resolution".to_string(), MockVal::Int(8)),
        ("ground_area".to_string(), MockVal::Bool(true)),
    ]);
    scan_row_count(path, named, projection)
}

/// Rows a continuous scan with `named` options returns for `projection`.
fn scan_row_count(
    path: &std::path::Path,
    named: HashMap<String, MockVal>,
    projection: Vec<usize>,
) -> Result<usize, String> {
    install_mock_api();
    let mut bind_ctx = MockBindContext::new(
        vec![MockVal::Str(path.to_string_lossy().into_owned())],
        named,
    );
    // SAFETY: the context is a live `MockBindContext`, which the installed mock C API
    // treats as the bind info.
    unsafe {
        raster_h3_bind(&mut bind_ctx as *mut MockBindContext as duckdb_bind_info);
    }
    assert!(bind_ctx.error.is_none(), "{:?}", bind_ctx.error);
    let width = projection.len().max(1);
    let mut init_ctx = MockInitContext::new(bind_ctx.bind_data, projection);
    let mut local_ctx = MockInitContext::new(bind_ctx.bind_data, Vec::new());
    // SAFETY: the contexts are live `MockInitContext`s carrying the bind data installed
    // by the bind callback, which is still owned by its bind context.
    unsafe {
        raster_h3_init(&mut init_ctx as *mut MockInitContext as duckdb_init_info);
        raster_h3_init_local(&mut local_ctx as *mut MockInitContext as duckdb_init_info);
    }
    if let Some(error) = init_ctx.error.take() {
        return Err(error);
    }
    let mut func_ctx = MockFuncContext {
        bind_data: bind_ctx.bind_data,
        init_data: init_ctx.init_data,
        local_init_data: local_ctx.init_data,
        error: None,
    };
    let mut rows = 0;
    loop {
        let mut chunk = MockDataChunk::new(width, 2048);
        // SAFETY: the function context carries bind/init/local data installed above
        // (still owned by their live contexts), and the chunk has one 2048-row mock
        // vector per projected column.
        unsafe {
            raster_h3_scan(
                &mut func_ctx as *mut MockFuncContext as duckdb_function_info,
                &mut chunk as *mut MockDataChunk as duckdb_data_chunk,
            );
        }
        if let Some(error) = func_ctx.error.take() {
            return Err(error);
        }
        if chunk.size == 0 {
            return Ok(rows);
        }
        rows += chunk.size;
    }
}

#[test]
fn ground_area_count_star_accepts_the_row_id_projection() {
    let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .epsg(4326)
        .create_f32_tempfile(|c, r| (c + r + 10) as f32);
    let expected = ground_area_row_count(&path, vec![0]).unwrap();
    assert!(expected > 0);
    // DuckDB 1.2.0 projects only its synthetic row-ID column for COUNT(*).
    let row_id = u64::MAX as usize;
    assert_eq!(ground_area_row_count(&path, vec![row_id]), Ok(expected));
    assert_eq!(ground_area_row_count(&path, Vec::new()), Ok(expected));
}

#[test]
fn band_selection_count_star_accepts_the_row_id_projection() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dggs-stream-core/tests/fixtures/multiband-12-planar.tif");
    let named = || {
        HashMap::from([
            ("resolution".to_string(), MockVal::Int(12)),
            ("bands".to_string(), MockVal::Str("3,1".to_string())),
        ])
    };
    let expected = scan_row_count(&path, named(), vec![0]).unwrap();
    assert!(expected > 0);
    let row_id = u64::MAX as usize;
    assert_eq!(scan_row_count(&path, named(), vec![row_id]), Ok(expected));
}
