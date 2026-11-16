//! Every grid (`h3`, `a5`, `s2`, `healpix`) through SQL: table functions, pushdown,
//! scalar functions, polygon fill and `COPY ... TO '*.pmtiles'`. Expected cells come from
//! `GridKind::cell_at` on each pixel centre, a path separate from the scan's.

mod common;

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Field, Float64Type, Int64Type, Schema, UInt64Type, UInt8Type,
};
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::prelude::{col, lit, SessionContext};
use raster_h3_core::grid::GridKind;
use raster_h3_core::pmtiles::pyramid::grid_res_to_zoom;
use raster_h3_sedona::scan::{CategoricalRasterTableProvider, ContinuousRasterTableProvider};
use raster_h3_sedona::{
    register_all, write_pmtiles, write_record_batches, PmtilesWriteOptions, RasterH3Table,
    GEOARROW_WKB_EXTENSION,
};

use common::{collect, gradient_tif, rows, source};

const SIZE: u32 = 48;

/// A resolution of each grid whose cells hold a handful of 0.001° pixels.
fn res_of(grid: GridKind) -> u8 {
    match grid {
        GridKind::H3 => 8,
        GridKind::A5 | GridKind::Healpix => 13,
        GridKind::S2 => 14,
    }
}

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    register_all(&ctx);
    ctx
}

/// `cell -> (count, sum)` of the `gradient_tif(SIZE, SIZE)` pixels, by pixel centre.
fn expected(grid: GridKind, res: u8) -> BTreeMap<u64, (f64, f64)> {
    let mut cells = BTreeMap::new();
    for y in 0..SIZE {
        for x in 0..SIZE {
            let lon = -122.45 + 0.001 * (f64::from(x) + 0.5);
            let lat = 37.80 - 0.001 * (f64::from(y) + 0.5);
            let entry = cells
                .entry(grid.cell_at(lat, lon, res).unwrap())
                .or_insert((0.0, 0.0));
            entry.0 += 1.0;
            entry.1 += f64::from(x);
        }
    }
    cells
}

/// `cell -> (count, sum)` from the first three columns of `batches`.
fn cell_stats(batches: &[RecordBatch]) -> BTreeMap<u64, (f64, f64)> {
    let mut cells = BTreeMap::new();
    for b in batches {
        let ids = b.column(0).as_primitive::<UInt64Type>();
        let counts = b.column(1).as_primitive::<Float64Type>();
        let sums = b.column(2).as_primitive::<Float64Type>();
        for i in 0..b.num_rows() {
            assert!(cells
                .insert(ids.value(i), (counts.value(i), sums.value(i)))
                .is_none());
        }
    }
    cells
}

#[tokio::test(flavor = "multi_thread")]
async fn table_functions_aggregate_onto_every_grid() {
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    let ctx = context();
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let want = expected(grid, res);
        assert!(want.len() > 4, "{grid}");
        let (index, hex) = (grid.index_column_name(), grid.hex_column_name());
        // The per-grid function, and the H3-named one with a grid argument.
        for from in [
            format!("{grid}_raster_continuous_aggregate('{src}', {res})"),
            format!("h3_raster_continuous_aggregate('{src}', {res}, '{grid}')"),
            format!(
                "h3_raster_continuous_aggregate('{src}', '{}', {res})",
                grid.label()
            ),
        ] {
            let got = collect(&ctx, &format!("SELECT {index}, count, sum FROM {from}")).await;
            assert_eq!(cell_stats(&got), want, "{from}");
        }

        let from = format!("{grid}_raster_continuous_aggregate('{src}', {res})");
        let all = collect(&ctx, &format!("SELECT * FROM {from}")).await;
        let schema = all[0].schema();
        assert_eq!(schema.field(0).name(), index);
        assert_eq!(schema.field(1).name(), hex);
        let wkb = schema.field_with_name("wkb").unwrap();
        assert_eq!(
            wkb.metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_WKB_EXTENSION)
        );
        for b in &all {
            let ids = b.column(0).as_primitive::<UInt64Type>();
            let hexes = b.column(1).as_string::<i32>();
            let resolutions = b
                .column_by_name("resolution")
                .unwrap()
                .as_primitive::<UInt8Type>();
            for i in 0..b.num_rows() {
                assert_eq!(hexes.value(i), format!("{:x}", ids.value(i)));
                assert_eq!(resolutions.value(i), res);
            }
        }

        // Projection, COUNT(*), LIMIT and a pushed filter.
        let counted = collect(&ctx, &format!("SELECT COUNT(*) FROM {from}")).await;
        let counted = counted[0].column(0).as_primitive::<Int64Type>().value(0);
        assert_eq!(counted as usize, want.len());
        let limited = collect(&ctx, &format!("SELECT {hex} FROM {from} LIMIT 3")).await;
        assert_eq!(rows(&limited), 3);
        let median = {
            let mut counts: Vec<f64> = want.values().map(|v| v.0).collect();
            counts.sort_by(f64::total_cmp);
            counts[counts.len() / 2]
        };
        let filtered = collect(
            &ctx,
            &format!("SELECT {index}, count, sum FROM {from} WHERE count >= {median}"),
        )
        .await;
        let kept: BTreeMap<_, _> = want
            .iter()
            .filter(|(_, v)| v.0 >= median)
            .map(|(k, v)| (*k, *v))
            .collect();
        assert_eq!(cell_stats(&filtered), kept, "{grid}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn categorical_functions_aggregate_onto_every_grid() {
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    let ctx = context();
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let want = expected(grid, res);
        let got = collect(
            &ctx,
            &format!(
                "SELECT {}, total_count FROM {grid}_raster_categorical_aggregate('{src}', {res})",
                grid.index_column_name()
            ),
        )
        .await;
        let mut cells = BTreeMap::new();
        for b in &got {
            let ids = b.column(0).as_primitive::<UInt64Type>();
            let totals = b.column(1).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                cells.insert(ids.value(i), totals.value(i));
            }
        }
        let want: BTreeMap<u64, f64> = want.into_iter().map(|(k, v)| (k, v.0)).collect();
        assert_eq!(cells, want, "{grid}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resolutions_are_checked_against_the_grid() {
    let tif = gradient_tif(8, 8);
    let src = source(&tif);
    let ctx = context();
    for grid in GridKind::ALL {
        let finest = grid.max_resolution();
        let err = ctx
            .sql(&format!(
                "SELECT * FROM {grid}_raster_continuous_aggregate('{src}', {})",
                finest + 1
            ))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(&format!("resolution must be between 0 and {finest}")),
            "{err}"
        );
    }
    // 20 is fine for A5, whether the grid comes before or after it; not for H3.
    for args in ["20, 'a5'", "'a5', 20"] {
        let sql =
            format!("SELECT a5_index FROM h3_raster_continuous_aggregate('{src}', {args}) LIMIT 1");
        assert_eq!(rows(&collect(&ctx, &sql).await), 1);
    }
    assert!(ctx
        .sql(&format!(
            "SELECT * FROM h3_raster_continuous_aggregate('{src}', 20)"
        ))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn providers_take_a_grid_and_push_filters_down() {
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let provider = ContinuousRasterTableProvider::try_new(&src, vec![res - 1, res])
            .unwrap()
            .with_grid(grid);
        assert_eq!(provider.grid(), grid);
        let table = RasterH3Table::continuous(provider);
        assert_eq!(table.grid(), grid);
        let exact = [
            col("count").gt_eq(lit(2.0)),
            col("resolution").eq(lit(i64::from(res))),
        ];
        let pushdown = table
            .supports_filters_pushdown(&exact.iter().collect::<Vec<_>>())
            .unwrap();
        assert_eq!(pushdown, vec![TableProviderFilterPushDown::Exact; 2]);

        let ctx = context();
        ctx.register_table("cells", Arc::new(table)).unwrap();
        let fine = collect(
            &ctx,
            &format!(
                "SELECT {}, count, sum FROM cells WHERE resolution = {res}",
                grid.index_column_name()
            ),
        )
        .await;
        assert_eq!(cell_stats(&fine), expected(grid, res), "{grid}");

        // A spatial predicate on the cell geometry narrows the scan and stays correct. The
        // raster is wide enough that the pushed box, widened by a cell diameter, is not all
        // of it.
        let wide = gradient_tif(240, 240);
        let src = source(&wide);
        let all_cells = rows(
            &collect(
                &context(),
                &format!("SELECT 1 FROM {grid}_raster_continuous_aggregate('{src}', {res})"),
            )
            .await,
        );
        let (lon, lat) = (-122.43, 37.78);
        let cell = grid.cell_at(lat, lon, res).unwrap();
        for geometry in [
            "wkb".to_string(),
            format!("{grid}_to_wkb({})", grid.index_column_name()),
        ] {
            let sql = format!(
                "SELECT {} FROM cells WHERE resolution = {res} \
                 AND st_intersects({geometry}, {grid}_to_wkb({cell}))",
                grid.index_column_name()
            );
            // Plain DataFusion has no ST_Intersects; a stand-in keeps the plan valid and
            // lets every candidate row through, so the result is what pushdown kept.
            let ctx = context();
            ctx.register_udf(always_true("st_intersects"));
            let provider = ContinuousRasterTableProvider::try_new(&src, vec![res])
                .unwrap()
                .with_grid(grid);
            ctx.register_table("cells", Arc::new(RasterH3Table::continuous(provider)))
                .unwrap();
            let kept = collect(&ctx, &sql).await;
            let ids: Vec<u64> = kept
                .iter()
                .flat_map(|b| b.column(0).as_primitive::<UInt64Type>().values().to_vec())
                .collect();
            assert!(ids.contains(&cell), "{grid} {geometry}");
            assert!(
                ids.len() < all_cells,
                "{grid} {geometry}: {} of {all_cells} cells, not narrowed",
                ids.len()
            );
        }

        let categorical = CategoricalRasterTableProvider::try_new(&src, vec![res])
            .unwrap()
            .with_grid(grid);
        assert_eq!(RasterH3Table::categorical(categorical).grid(), grid);
    }
}

/// A two-geometry predicate that keeps every row.
fn always_true(name: &str) -> datafusion::logical_expr::ScalarUDF {
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    use datafusion::scalar::ScalarValue;
    create_udf(
        name,
        vec![DataType::Binary, DataType::Binary],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|_: &[ColumnarValue]| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn scalar_functions_exist_for_every_grid() {
    let ctx = context();
    // lon 0, lat -89.5 is on S2 face 5, whose ids set the top bit.
    for (lon, lat) in [(-122.4194, 37.7749), (0.0, -89.5)] {
        for grid in GridKind::ALL {
            let res = 9u8;
            let cell = grid.cell_at(lat, lon, res).unwrap();
            let parent = grid.parent(cell, 4).unwrap();
            let (c_lat, c_lng) = grid.center_lat_lng(cell).unwrap();
            let g = grid.as_str();
            let out = collect(
                &ctx,
                &format!(
                    "SELECT {g}_lonlat_to_cell({lon}, {lat}, {res}) AS cell, \
                     {g}_to_string({cell}) AS hex, \
                     string_to_{g}('{cell:x}') AS back, \
                     {g}_to_lat({cell}) AS lat, {g}_to_lng({cell}) AS lng, \
                     {g}_get_resolution({cell}) AS res, \
                     {g}_is_valid({cell}) AS ok, {g}_is_valid('{cell:x}') AS ok_hex, \
                     {g}_is_valid_cell(0) AS zero, \
                     {g}_cell_to_parent({cell}, 4) AS parent, \
                     {g}_cell_to_parent('{cell:x}', 4) AS parent_hex, \
                     {g}_cell_to_parent({cell}, {}) AS finer, \
                     {g}_to_wkb({cell}) AS wkb, {g}_cell_to_geometry('{cell:x}') AS geom, \
                     {g}_to_wkb(0) AS no_wkb, \
                     {g}_lonlat_to_cell(0.0, 95.0, 3) AS bad_lat, \
                     {g}_lonlat_to_cell(0.0, 0.0, {}) AS bad_res",
                    res + 1,
                    grid.max_resolution() + 1
                ),
            )
            .await;
            let b = &out[0];
            let u64_at = |name: &str| {
                let c = b.column_by_name(name).unwrap();
                (!c.is_null(0)).then(|| c.as_primitive::<UInt64Type>().value(0))
            };
            let f64_at = |name: &str| {
                b.column_by_name(name)
                    .unwrap()
                    .as_primitive::<Float64Type>()
                    .value(0)
            };
            let str_at = |name: &str| {
                b.column_by_name(name)
                    .unwrap()
                    .as_string::<i32>()
                    .value(0)
                    .to_string()
            };
            let bool_at = |name: &str| b.column_by_name(name).unwrap().as_boolean().value(0);
            assert_eq!(u64_at("cell"), Some(cell), "{grid}");
            assert_eq!(str_at("hex"), format!("{cell:x}"));
            assert_eq!(u64_at("back"), Some(cell));
            assert_eq!((f64_at("lat"), f64_at("lng")), (c_lat, c_lng));
            let got_res = b.column_by_name("res").unwrap();
            assert_eq!(got_res.as_primitive::<Int64Type>().value(0), i64::from(res));
            assert!(bool_at("ok") && bool_at("ok_hex") && !bool_at("zero"));
            assert_eq!(u64_at("parent"), Some(parent));
            assert_eq!(str_at("parent_hex"), format!("{parent:x}"));
            assert_eq!(u64_at("finer"), None);
            let wkb = raster_h3_core::scalar_ops::grid_to_wkb_u64(grid, cell).unwrap();
            for name in ["wkb", "geom"] {
                let col = b.column_by_name(name).unwrap();
                assert_eq!(col.as_binary::<i32>().value(0), &wkb[..], "{grid} {name}");
                let field = b.schema().field_with_name(name).unwrap().clone();
                assert_eq!(
                    field
                        .metadata()
                        .get("ARROW:extension:name")
                        .map(String::as_str),
                    Some(GEOARROW_WKB_EXTENSION)
                );
            }
            assert!(b.column_by_name("no_wkb").unwrap().is_null(0));
            assert_eq!(u64_at("bad_lat"), None);
            assert_eq!(u64_at("bad_res"), None);
        }
    }
    let s2_south = GridKind::S2.cell_at(-89.5, 0.0, 9).unwrap();
    assert!(s2_south >> 63 == 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn scalar_functions_work_on_columns() {
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    let ctx = context();
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let g = grid.as_str();
        let (index, hex) = (grid.index_column_name(), grid.hex_column_name());
        let sql = format!(
            "SELECT COUNT(*) FROM {g}_raster_continuous_aggregate('{src}', {res}) \
             WHERE {g}_to_string({index}) = {hex} \
               AND string_to_{g}({hex}) = {index} \
               AND {g}_get_resolution({index}) = resolution \
               AND {g}_is_valid({index}) AND {g}_is_valid({hex}) \
               AND {g}_to_wkb({index}) = wkb AND {g}_to_wkb({hex}) = wkb \
               AND {g}_get_resolution({g}_cell_to_parent({index}, {})) = {} \
               AND {g}_lonlat_to_cell({g}_to_lng({index}), {g}_to_lat({index}), {res}) = {index}",
            res - 3,
            res - 3
        );
        let out = collect(&ctx, &sql).await;
        let n = out[0].column(0).as_primitive::<Int64Type>().value(0);
        assert_eq!(n as usize, expected(grid, res).len(), "{grid}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn polygon_fill_matches_core_on_every_grid() {
    let ctx = context();
    let ring = [
        (-122.45f64, 37.75f64),
        (-122.40, 37.75),
        (-122.40, 37.80),
        (-122.45, 37.80),
        (-122.45, 37.75),
    ];
    let mut wkb = vec![1u8];
    wkb.extend(3u32.to_le_bytes());
    wkb.extend(1u32.to_le_bytes());
    wkb.extend((ring.len() as u32).to_le_bytes());
    for (x, y) in ring {
        wkb.extend(x.to_le_bytes());
        wkb.extend(y.to_le_bytes());
    }
    let hex: String = wkb.iter().map(|b| format!("{b:02x}")).collect();
    for grid in GridKind::ALL {
        let res = i64::from(res_of(grid)) - 2;
        let mut want = Vec::new();
        raster_h3_core::scalar_ops::grid_polygon_to_cells_wkb_into(
            grid,
            &wkb,
            res,
            raster_h3_core::scalar_ops::ContainmentMode::ContainsCentroid,
            &mut want,
        )
        .unwrap();
        assert!(!want.is_empty());
        let out = collect(
            &ctx,
            &format!(
                "SELECT {grid}_polygon_to_cells(X'{hex}', {res}) AS cells, \
                 {grid}_polygon_to_cells(X'{hex}', {}) AS too_fine",
                grid.max_resolution() + 1
            ),
        )
        .await;
        let cells = out[0].column(0).as_list::<i32>().value(0);
        assert_eq!(
            cells.as_primitive::<UInt64Type>().values().to_vec(),
            want,
            "{grid}"
        );
        assert!(out[0].column(1).is_null(0));
    }
}

/// The archive's gzipped JSON metadata.
fn metadata(archive: &[u8]) -> serde_json::Value {
    assert_eq!(&archive[..7], b"PMTiles");
    let offset = u64::from_le_bytes(archive[24..32].try_into().unwrap()) as usize;
    let len = u64::from_le_bytes(archive[32..40].try_into().unwrap()) as usize;
    let mut json = String::new();
    flate2::read::GzDecoder::new(&archive[offset..offset + len])
        .read_to_string(&mut json)
        .unwrap();
    serde_json::from_str(&json).unwrap()
}

async fn copy_count(ctx: &SessionContext, sql: &str) -> Result<u64, String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let out = df.collect().await.map_err(|e| e.to_string())?;
    Ok(out[0].column(0).as_primitive::<UInt64Type>().value(0))
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_to_pmtiles_detects_every_grid() {
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    let ctx = context();
    let dir = tempfile::tempdir().unwrap();
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let cells = expected(grid, res).len() as u64;
        let (index, hex) = (grid.index_column_name(), grid.hex_column_name());
        let from = format!("{grid}_raster_continuous_aggregate('{src}', {res})");
        let zoom = u64::from(grid_res_to_zoom(grid, res));

        // The grid is read off the cell column's name, integer or hex, signed or not.
        for (tag, select) in [
            ("index", format!("SELECT {index}, mean, count FROM {from}")),
            ("hex", format!("SELECT mean, {hex} FROM {from}")),
            (
                "bare",
                format!("SELECT {index} AS {grid}, mean FROM {from}"),
            ),
        ] {
            let out = dir.path().join(format!("{grid}-{tag}.pmtiles"));
            let sql = format!("COPY ({select}) TO '{}'", out.display());
            assert_eq!(copy_count(&ctx, &sql).await.unwrap(), cells, "{grid} {tag}");
            let archive = std::fs::read(&out).unwrap();
            let meta = metadata(&archive);
            let layer = &meta["vector_layers"][0];
            assert_eq!(layer["minzoom"], zoom, "{grid} {tag}: {meta}");
            assert!(layer["fields"].get(index).is_some(), "{grid} {tag}: {meta}");
            assert!(layer["fields"].get("mean").is_some());
        }

        // A column named otherwise needs the grid option (H3 is assumed without it).
        let renamed = format!("SELECT {index} AS id, mean FROM {from}");
        let out = dir.path().join(format!("{grid}-renamed.pmtiles"));
        let with_grid = format!(
            "COPY ({renamed}) TO '{}' OPTIONS (cell_column 'id', grid '{grid}')",
            out.display()
        );
        assert_eq!(copy_count(&ctx, &with_grid).await.unwrap(), cells, "{grid}");
        if grid != GridKind::H3 {
            let without = format!(
                "COPY ({renamed}) TO '{}' OPTIONS (cell_column 'id')",
                out.display()
            );
            let err = copy_count(&ctx, &without).await.unwrap_err();
            assert!(err.contains("is not a valid H3 cell"), "{err}");
        }

        // Signed 64-bit ids (PostgreSQL, Spark) are the same bits, top bit included.
        let unsigned = collect(&ctx, &format!("SELECT {index} FROM {from}")).await;
        let signed_schema = Arc::new(Schema::new(vec![Field::new(index, DataType::Int64, true)]));
        let signed: Vec<_> = unsigned
            .iter()
            .map(|b| {
                let ids = b.column(0).as_primitive::<UInt64Type>();
                let ids: Int64Array = ids.values().iter().map(|&v| Some(v as i64)).collect();
                RecordBatch::try_new(Arc::clone(&signed_schema), vec![Arc::new(ids)])
                    .map_err(Into::into)
            })
            .collect();
        let summary =
            write_record_batches(&signed_schema, signed, &out, PmtilesWriteOptions::default())
                .unwrap();
        assert_eq!(summary.valid_features as u64, cells, "{grid} signed");

        // The DataFrame API takes the same options.
        let df = ctx.sql(&renamed).await.unwrap();
        let options = PmtilesWriteOptions {
            h3_column: Some("id".into()),
            grid: Some(grid),
            ..Default::default()
        };
        let written = write_pmtiles(df, out.to_str().unwrap(), options)
            .await
            .unwrap();
        assert_eq!(written, cells);
    }
    let bad = format!(
        "COPY (SELECT h3_index FROM h3_raster_continuous_aggregate('{src}', 8)) \
         TO '{}' OPTIONS (grid 'hexagons')",
        dir.path().join("bad.pmtiles").display()
    );
    let err = copy_count(&ctx, &bad).await.unwrap_err();
    assert!(err.contains("PMTiles option 'grid'"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_grid_columns_prefer_the_named_grid() {
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    let ctx = context();
    let dir = tempfile::tempdir().unwrap();
    let res = res_of(GridKind::S2);
    let cells = expected(GridKind::S2, res).len() as u64;
    // An H3 column beside the S2 one: H3 wins by default, `grid` picks S2's.
    let select = format!(
        "SELECT h3_lonlat_to_cell(s2_to_lng(s2_index), s2_to_lat(s2_index), 9) AS h3_index, \
         s2_index FROM s2_raster_continuous_aggregate('{src}', {res})"
    );
    let out = dir.path().join("mixed.pmtiles");
    let default = format!("COPY ({select}) TO '{}'", out.display());
    assert_eq!(copy_count(&ctx, &default).await.unwrap(), cells);
    let meta = metadata(&std::fs::read(&out).unwrap());
    let h3_zoom = u64::from(grid_res_to_zoom(GridKind::H3, 9));
    assert_eq!(meta["vector_layers"][0]["minzoom"], h3_zoom);

    let s2 = format!("COPY ({select}) TO '{}' OPTIONS (grid 's2')", out.display());
    assert_eq!(copy_count(&ctx, &s2).await.unwrap(), cells);
    let meta = metadata(&std::fs::read(&out).unwrap());
    let s2_zoom = u64::from(grid_res_to_zoom(GridKind::S2, res));
    assert_eq!(meta["vector_layers"][0]["minzoom"], s2_zoom);
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_resolution_matches_cells_to_pixels_on_every_grid() {
    use raster_h3_core::raster::mosaic::{MosaicReader, OverlapRule};
    use raster_h3_core::resolution::{auto_resolution, offset_resolution};
    use raster_h3_sedona::ResolutionSpec;
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    let ctx = context();
    let mosaic = MosaicReader::open(
        &[tif.path().to_path_buf()],
        None,
        None,
        OverlapRule::default(),
    )
    .unwrap();
    let resolutions = |batches: &[RecordBatch]| -> Vec<u8> {
        let mut seen: Vec<u8> = batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<UInt8Type>().values().to_vec())
            .collect();
        seen.sort_unstable();
        seen.dedup();
        seen
    };
    for grid in GridKind::ALL {
        let matched = auto_resolution(grid, &mosaic, 0).unwrap();
        for (text, offset) in [("auto", 0), ("AUTO+1", 1), ("auto-2", -2)] {
            let want = offset_resolution(grid, matched, offset);
            for from in [
                format!("{grid}_raster_continuous_aggregate('{src}', '{text}')"),
                format!("h3_raster_continuous_aggregate('{src}', '{grid}', '{text}')"),
                format!("{grid}_raster_categorical_aggregate('{src}', '{text}')"),
            ] {
                let got = collect(&ctx, &format!("SELECT resolution FROM {from}")).await;
                assert_eq!(resolutions(&got), vec![want], "{from}");
            }
            let spec = ResolutionSpec::parse(grid, text).unwrap();
            let provider =
                ContinuousRasterTableProvider::try_new_with(&src, grid, &spec, None).unwrap();
            assert_eq!(provider.config.resolutions, vec![want]);
            assert_eq!(provider.grid(), grid);
        }
        // The resolution is fixed at planning, so a filter on it pushes down exactly.
        let from = format!("{grid}_raster_continuous_aggregate('{src}', 'auto')");
        let all = rows(&collect(&ctx, &format!("SELECT 1 FROM {from}")).await);
        let same = collect(
            &ctx,
            &format!("SELECT 1 FROM {from} WHERE resolution = {matched}"),
        )
        .await;
        assert_eq!(rows(&same), all);
        let other = collect(
            &ctx,
            &format!("SELECT 1 FROM {from} WHERE resolution = {}", matched + 1),
        )
        .await;
        assert_eq!(rows(&other), 0);

        for bad in [
            format!("{grid}_raster_continuous_aggregate('{src}', 'auto', 9)"),
            format!("{grid}_raster_continuous_aggregate('{src}', 'auto+x')"),
            format!("{grid}_raster_continuous_aggregate('/nonexistent.tif', 'auto')"),
        ] {
            assert!(
                ctx.sql(&format!("SELECT * FROM {bad}")).await.is_err(),
                "{bad}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nodata_policy_and_decimals_on_every_grid() {
    use raster_h3_sedona::{NodataPolicy, OutputPolicy};
    const NODATA: f32 = -9999.0;
    // NoData in the left third; fractional values elsewhere. No GeoKeys: the CRS is given.
    let pixel = |c: u32, r: u32| {
        if c < SIZE / 3 {
            NODATA
        } else {
            ((c * 7 + r * 3) % 11) as f32 + 0.375
        }
    };
    let tif = common::write_tif_grid(SIZE, SIZE, (-122.45, 37.80), 0.001, pixel);
    let src = source(&tif);
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let index = grid.index_column_name();
        // `cell -> (valid count, valid sum)`, including cells with no valid sample.
        let mut want: BTreeMap<u64, (f64, f64)> = BTreeMap::new();
        for r in 0..SIZE {
            for c in 0..SIZE {
                let lon = -122.45 + 0.001 * (f64::from(c) + 0.5);
                let lat = 37.80 - 0.001 * (f64::from(r) + 0.5);
                let e = want
                    .entry(grid.cell_at(lat, lon, res).unwrap())
                    .or_default();
                if pixel(c, r) != NODATA {
                    e.0 += 1.0;
                    e.1 += f64::from(pixel(c, r));
                }
            }
        }
        let empty = want.values().filter(|v| v.0 == 0.0).count();
        assert!(empty > 1 && empty < want.len(), "{grid}");

        let table = |policy: OutputPolicy| {
            let provider = ContinuousRasterTableProvider::try_new(&src, vec![res])
                .unwrap()
                .with_grid(grid)
                .with_crs("EPSG:4326")
                .with_nodata(f64::from(NODATA))
                .with_output_policy(policy);
            RasterH3Table::continuous(provider)
        };
        let query = |policy: OutputPolicy, sql: String| {
            let ctx = context();
            ctx.register_table("cells", Arc::new(table(policy)))
                .unwrap();
            async move { collect(&ctx, &sql).await }
        };
        let stats = format!("SELECT {index}, count, sum, mean, min FROM cells");

        // omit: only cells with a valid sample; the columns are not nullable.
        let omit = query(OutputPolicy::default(), stats.clone()).await;
        assert_eq!(rows(&omit), want.len() - empty, "{grid}");
        assert!(!omit[0]
            .schema()
            .field_with_name("mean")
            .unwrap()
            .is_nullable());

        // emit: every cell; NULL mean and min where there is no valid sample.
        let emit_policy = OutputPolicy {
            nodata: NodataPolicy::Emit,
            ..Default::default()
        };
        let emit = query(emit_policy, stats.clone()).await;
        assert!(emit[0]
            .schema()
            .field_with_name("mean")
            .unwrap()
            .is_nullable());
        let mut seen = 0;
        for b in &emit {
            let ids = b.column(0).as_primitive::<UInt64Type>();
            let counts = b.column(1).as_primitive::<Float64Type>();
            let sums = b.column(2).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                let (count, sum) = want[&ids.value(i)];
                assert_eq!((counts.value(i), sums.value(i)), (count, sum), "{grid}");
                assert_eq!(b.column(3).is_null(i), count == 0.0);
                assert_eq!(b.column(4).is_null(i), count == 0.0);
                seen += 1;
            }
        }
        assert_eq!(seen, want.len(), "{grid}");

        // SQL sees the nulls, and a count filter is applied to the rows, not pushed.
        let nulls = query(
            emit_policy,
            "SELECT COUNT(*) FROM cells WHERE mean IS NULL".into(),
        )
        .await;
        assert_eq!(
            nulls[0].column(0).as_primitive::<Int64Type>().value(0) as usize,
            empty
        );
        let counted = query(emit_policy, "SELECT 1 FROM cells WHERE count >= 1".into()).await;
        assert_eq!(rows(&counted), want.len() - empty, "{grid}");
        let pushdown = table(emit_policy)
            .supports_filters_pushdown(&[&col("count").gt_eq(lit(1.0))])
            .unwrap();
        assert_eq!(pushdown, vec![TableProviderFilterPushDown::Unsupported]);

        // A NoData value and decimals.
        let filled = query(
            OutputPolicy {
                nodata: NodataPolicy::Emit,
                nodata_value: Some(-1.25),
                decimals: Some(1),
            },
            stats.clone(),
        )
        .await;
        assert!(!filled[0]
            .schema()
            .field_with_name("mean")
            .unwrap()
            .is_nullable());
        for b in &filled {
            let ids = b.column(0).as_primitive::<UInt64Type>();
            let means = b.column(3).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                let (count, sum) = want[&ids.value(i)];
                let expected = if count == 0.0 {
                    -1.25
                } else {
                    (sum / count * 10.0).round() / 10.0
                };
                assert_eq!(means.value(i), expected, "{grid}");
            }
        }

        // The table functions take the options as `key=value` strings.
        let ctx = context();
        let from = format!(
            "{grid}_raster_continuous_aggregate('{src}', {res}, 'EPSG:4326', \
             'nodata_policy=emit', 'decimals=0')"
        );
        // The function has no NoData override, so every cell has samples here.
        let all = collect(&ctx, &format!("SELECT mean, count FROM {from}")).await;
        assert_eq!(rows(&all), want.len(), "{grid}");
        for b in &all {
            let means = b.column(0).as_primitive::<Float64Type>();
            assert!(means.values().iter().all(|m| m.fract() == 0.0));
        }
        for bad in [
            "'nodata_policy=drop'",
            "'decimals=16'",
            "'emit_nodata_value=1'",
            "'nodata_policy=emit', 'emit_nodata_value=nan'",
        ] {
            let sql = format!(
                "SELECT * FROM {grid}_raster_continuous_aggregate('{src}', {res}, 'EPSG:4326', {bad})"
            );
            assert!(ctx.sql(&sql).await.is_err(), "{bad}");
        }

        // Categorical: empty cells carry NULL, or the NoData class.
        for (value, nullable) in [(None, true), (Some(255.0), false)] {
            let provider = CategoricalRasterTableProvider::try_new(&src, vec![res])
                .unwrap()
                .with_grid(grid)
                .with_crs("EPSG:4326")
                .with_nodata(f64::from(NODATA))
                .with_output_policy(OutputPolicy {
                    nodata: NodataPolicy::Emit,
                    nodata_value: value,
                    decimals: Some(2),
                });
            let ctx = context();
            ctx.register_table("classes", Arc::new(RasterH3Table::categorical(provider)))
                .unwrap();
            let out = collect(&ctx, "SELECT majority_class, total_count FROM classes").await;
            let field = out[0]
                .schema()
                .field_with_name("majority_class")
                .unwrap()
                .clone();
            assert_eq!(field.is_nullable(), nullable);
            let mut empties = 0;
            for b in &out {
                let totals = b.column(1).as_primitive::<Float64Type>();
                for i in 0..b.num_rows() {
                    if totals.value(i) == 0.0 {
                        empties += 1;
                        assert_eq!(b.column(0).is_null(i), nullable);
                        if !nullable {
                            assert_eq!(b.column(0).as_primitive::<Int64Type>().value(i), 255);
                        }
                    }
                }
            }
            assert_eq!(empties, empty, "{grid}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transfer_modes_on_every_grid() {
    use raster_h3_core::resolution::area_matched_resolution;
    use raster_h3_sedona::{SampleMethod, TransferMode};
    let value = |c: u32, r: u32| ((c * 7 + r * 13) % 23 + 1) as f32;
    let tif = common::write_tif_grid(SIZE, SIZE, (-122.45, 37.80), 0.001, value);
    let src = source(&tif);
    let pixels = f64::from(SIZE * SIZE);
    let total: f64 = (0..SIZE)
        .flat_map(|r| (0..SIZE).map(move |c| f64::from(value(c, r))))
        .sum();
    let pixel_m2 = 111_320.0f64.powi(2) * 1e-6 * 37.8f64.to_radians().cos();
    let ctx = context();
    // `(rows, sum of count, sum of sum)` of a query.
    let totals = |batches: Vec<RecordBatch>| {
        let mut out = (0usize, 0.0, 0.0);
        for b in &batches {
            out.0 += b.num_rows();
            out.1 += b
                .column(0)
                .as_primitive::<Float64Type>()
                .values()
                .iter()
                .sum::<f64>();
            out.2 += b
                .column(1)
                .as_primitive::<Float64Type>()
                .values()
                .iter()
                .sum::<f64>();
        }
        out
    };
    for grid in GridKind::ALL {
        let matched = area_matched_resolution(grid, pixel_m2);
        let coarse = matched - 1;
        let from = |res: u8, options: &str| {
            format!("{grid}_raster_continuous_aggregate('{src}', {res}, 'EPSG:4326'{options})")
        };
        let query = |from: String| {
            let ctx = &ctx;
            async move { totals(collect(ctx, &format!("SELECT count, sum FROM {from}")).await) }
        };

        let centres = query(from(coarse, "")).await;
        assert_eq!((centres.1, centres.2), (pixels, total), "{grid}");
        let overlay = query(from(coarse, ", 'transfer=overlay'")).await;
        assert!(overlay.0 >= centres.0);
        assert!(
            (overlay.1 / pixels - 1.0).abs() < 1e-6,
            "{grid}: {}",
            overlay.1
        );
        assert!(
            (overlay.2 / total - 1.0).abs() < 1e-6,
            "{grid}: {}",
            overlay.2
        );
        let preserved = query(from(coarse, ", 'transfer=overlay', 'mass_preserve=true'")).await;
        assert!((preserved.2 - total).abs() < 1e-7 * total, "{grid}");
        let touched = query(from(
            coarse,
            ", 'transfer=overlay', 'overlay_weighted=false'",
        ))
        .await;
        assert!(touched.1 > pixels && touched.0 == overlay.0, "{grid}");

        // Sampling finer than the pixels; pushdown still applies to the sampled table.
        let fine = matched + 2;
        let fine_centres = query(from(fine, "")).await;
        for method in ["nearest", "bilinear", "bicubic", "lanczos"] {
            let options = format!(", 'transfer=sample', 'sample_method={method}'");
            let sampled = query(from(fine, &options)).await;
            assert!(sampled.0 > 3 * fine_centres.0, "{grid} {method}");
            assert_eq!(sampled.1, sampled.0 as f64, "one sample per cell");
        }
        let limited = collect(
            &ctx,
            &format!(
                "SELECT {} FROM {} WHERE mean >= 12 LIMIT 5",
                grid.hex_column_name(),
                from(fine, ", 'transfer=sample'")
            ),
        )
        .await;
        assert_eq!(rows(&limited), 5);

        // The provider API takes the same modes.
        let provider = ContinuousRasterTableProvider::try_new(&src, vec![fine])
            .unwrap()
            .with_grid(grid)
            .with_crs("EPSG:4326")
            .with_transfer(TransferMode::Sample(SampleMethod::Nearest));
        let table_ctx = context();
        table_ctx
            .register_table("sampled", Arc::new(RasterH3Table::continuous(provider)))
            .unwrap();
        let nearest = collect(
            &table_ctx,
            &format!("SELECT {}, mean FROM sampled", grid.index_column_name()),
        )
        .await;
        for b in &nearest {
            let ids = b.column(0).as_primitive::<UInt64Type>();
            let means = b.column(1).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                let (lat, lng) = grid.center_lat_lng(ids.value(i)).unwrap();
                let c = ((lng + 122.45) / 0.001).floor() as u32;
                let r = ((37.80 - lat) / 0.001).floor() as u32;
                assert_eq!(means.value(i), f64::from(value(c, r)), "{grid}");
            }
        }

        // Categorical: nearest sampling and overlay.
        let classes = collect(
            &ctx,
            &format!(
                "SELECT total_count FROM {grid}_raster_categorical_aggregate('{src}', {coarse}, \
                 'EPSG:4326', 'transfer=overlay')"
            ),
        )
        .await;
        let area: f64 = classes
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Float64Type>().values().to_vec())
            .sum();
        assert!((area / pixels - 1.0).abs() < 1e-6, "{grid}");

        for bad in [
            ", 'transfer=smear'",
            ", 'sample_method=bilinear'",
            ", 'mass_preserve=true'",
            ", 'transfer=overlay', 'mass_preserve=perhaps'",
        ] {
            let sql = format!("SELECT * FROM {}", from(coarse, bad));
            let failed = match ctx.sql(&sql).await {
                Err(_) => true,
                Ok(df) => df.collect().await.is_err(),
            };
            assert!(failed, "{bad}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn providers_report_a_run_profile() {
    let tif = gradient_tif(SIZE, SIZE);
    let src = source(&tif);
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let profile = raster_h3_sedona::RunProfile::new();
        let provider = ContinuousRasterTableProvider::try_new(&src, vec![res])
            .unwrap()
            .with_grid(grid)
            .with_profile(Arc::clone(&profile));
        let ctx = context();
        ctx.register_table("cells", Arc::new(RasterH3Table::continuous(provider)))
            .unwrap();
        let batches = collect(
            &ctx,
            &format!("SELECT {}, count, sum FROM cells", grid.index_column_name()),
        )
        .await;
        assert_eq!(cell_stats(&batches), expected(grid, res), "{grid}");
        let report = profile.report();
        assert_eq!(report.pixels_read, u64::from(SIZE * SIZE), "{grid}");
        assert_eq!(report.cells as usize, expected(grid, res).len(), "{grid}");
        assert!(report.phase("kernel").unwrap().wall_ms > 0.0, "{grid}");
        assert!(report.to_json().contains("\"grid_index\""), "{grid}");
    }
}
