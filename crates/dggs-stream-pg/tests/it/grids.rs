//! Every grid through the Rust SRFs and the V1 `bigint` scalars. Cell ids are `bigint`s
//! holding the id's 64 bits, so S2 ids on faces 4 and 5 (top bit set) are negative.

use std::collections::BTreeMap;

use dggs_stream_core::grid::GridKind;
use dggs_stream_core::scalar_ops::grid_to_wkb_u64;
use dggs_stream_pg::{
    datum_to_float8, datum_to_int64, float8_to_datum, int64_to_datum, pg_a5_cell_to_parent,
    pg_a5_get_resolution, pg_a5_is_valid, pg_a5_lonlat_to_cell, pg_a5_to_lat, pg_a5_to_lng,
    pg_func_call_info, pg_h3_cell_to_parent, pg_h3_get_resolution, pg_h3_is_valid,
    pg_h3_lonlat_to_cell, pg_h3_to_lat, pg_h3_to_lng, pg_healpix_cell_to_parent,
    pg_healpix_get_resolution, pg_healpix_is_valid, pg_healpix_lonlat_to_cell, pg_healpix_to_lat,
    pg_healpix_to_lng, pg_s2_cell_to_parent, pg_s2_get_resolution, pg_s2_is_valid,
    pg_s2_lonlat_to_cell, pg_s2_to_lat, pg_s2_to_lng, Datum, FunctionCallInfoBaseData,
    NullableDatum, PgCategoricalSrf, PgContinuousSrf,
};

use crate::helpers::TestGeoTiffBuilder;

type V1 = unsafe extern "C" fn(pg_func_call_info) -> Datum;

/// The V1 scalars of one grid: `[to_lat, to_lng, get_resolution, is_valid,
/// cell_to_parent, lonlat_to_cell]`.
pub fn scalars(grid: GridKind) -> [V1; 6] {
    match grid {
        GridKind::H3 => [
            pg_h3_to_lat,
            pg_h3_to_lng,
            pg_h3_get_resolution,
            pg_h3_is_valid,
            pg_h3_cell_to_parent,
            pg_h3_lonlat_to_cell,
        ],
        GridKind::A5 => [
            pg_a5_to_lat,
            pg_a5_to_lng,
            pg_a5_get_resolution,
            pg_a5_is_valid,
            pg_a5_cell_to_parent,
            pg_a5_lonlat_to_cell,
        ],
        GridKind::S2 => [
            pg_s2_to_lat,
            pg_s2_to_lng,
            pg_s2_get_resolution,
            pg_s2_is_valid,
            pg_s2_cell_to_parent,
            pg_s2_lonlat_to_cell,
        ],
        GridKind::Healpix => [
            pg_healpix_to_lat,
            pg_healpix_to_lng,
            pg_healpix_get_resolution,
            pg_healpix_is_valid,
            pg_healpix_cell_to_parent,
            pg_healpix_lonlat_to_cell,
        ],
    }
}

fn call(f: V1, args: &[Datum]) -> Option<Datum> {
    let args: Vec<NullableDatum> = args
        .iter()
        .map(|&value| NullableDatum {
            value,
            isnull: false,
        })
        .collect();
    let mut fc = FunctionCallInfoBaseData::with_args(&args);
    // SAFETY: `fc` is a fully initialized local call info whose arguments match the SQL
    // signature of `f`.
    let d = unsafe { f(&mut fc) };
    (!fc.isnull).then_some(d)
}

/// A resolution of each grid whose cells hold a handful of 0.001° pixels.
pub fn res_of(grid: GridKind) -> u8 {
    match grid {
        GridKind::H3 => 8,
        GridKind::A5 | GridKind::Healpix => 13,
        GridKind::S2 => 14,
    }
}

/// Origins of a northern raster and a far-southern one near longitude 0, which lies on
/// S2 face 5.
pub const ORIGINS: [(f64, f64); 2] = [(-122.45, 37.80), (0.5, -60.0)];
const SIZE: u32 = 40;

fn value(c: u32, r: u32) -> f32 {
    ((c + 2 * r) % 9 + 1) as f32
}

/// `cell -> (count, sum)` by pixel centre.
fn expected(grid: GridKind, origin: (f64, f64)) -> BTreeMap<u64, (f64, f64)> {
    let mut cells = BTreeMap::new();
    for r in 0..SIZE {
        for c in 0..SIZE {
            let lon = origin.0 + 0.001 * (f64::from(c) + 0.5);
            let lat = origin.1 - 0.001 * (f64::from(r) + 0.5);
            let e = cells
                .entry(grid.cell_at(lat, lon, res_of(grid)).unwrap())
                .or_insert((0.0, 0.0));
            e.0 += 1.0;
            e.1 += f64::from(value(c, r));
        }
    }
    cells
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads a memory-mapped raster, which Miri cannot follow"
)]
fn srfs_aggregate_onto_every_grid() {
    for origin in ORIGINS {
        let (_tmp, path) = TestGeoTiffBuilder::new(SIZE, SIZE)
            .origin(origin.0, origin.1)
            .pixel_size(0.001)
            .create_f32_tempfile(value);
        let path = path.to_str().unwrap();
        for grid in GridKind::ALL {
            let res = res_of(grid);
            let want = expected(grid, origin);
            assert!(want.len() > 4);
            let mut got = BTreeMap::new();
            for row in PgContinuousSrf::open_for(grid, path, vec![res], None, None, None).unwrap() {
                let row = row.unwrap();
                // The bigint is the id's bit pattern; the hex text is the unsigned id.
                let cell = row.cell_id as u64;
                assert_eq!(row.cell_hex, format!("{cell:x}"));
                assert_eq!(
                    u64::from_str_radix(&row.cell_hex, 16).unwrap() as i64,
                    row.cell_id
                );
                assert_eq!(row.resolution, i16::from(res));
                assert_eq!(row.wkb, grid_to_wkb_u64(grid, cell).unwrap());
                assert!(got.insert(cell, (row.count, row.sum)).is_none());
            }
            assert_eq!(got, want, "{grid} at {origin:?}");

            let totals: BTreeMap<u64, f64> =
                PgCategoricalSrf::open_for(grid, path, vec![res], None, None, None)
                    .unwrap()
                    .map(|row| {
                        let row = row.unwrap();
                        assert_eq!(row.cell_hex, format!("{:x}", row.cell_id as u64));
                        (row.cell_id as u64, row.total_count)
                    })
                    .collect();
            let want_totals: BTreeMap<u64, f64> = want.iter().map(|(k, v)| (*k, v.0)).collect();
            assert_eq!(totals, want_totals, "{grid} at {origin:?}");
        }
    }
    // The southern raster's S2 cells are on face 5: negative as bigints, and still exact.
    let south = expected(GridKind::S2, ORIGINS[1]);
    assert!(south.keys().all(|&cell| (cell as i64) < 0));
    // `open` is the H3 scan.
    let (_tmp, path) = TestGeoTiffBuilder::new(8, 8)
        .pixel_size(0.001)
        .create_f32_tempfile(value);
    let path = path.to_str().unwrap();
    let h3: Vec<_> = PgContinuousSrf::open(path, vec![8], None, None, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let explicit: Vec<_> = PgContinuousSrf::open_for(GridKind::H3, path, vec![8], None, None, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(h3.len(), explicit.len());
    assert!(PgContinuousSrf::open_for(GridKind::H3, path, vec![16], None, None, None).is_err());
    assert!(PgContinuousSrf::open_for(GridKind::A5, path, vec![16], None, None, None).is_ok());
    assert!(
        PgContinuousSrf::open_for(GridKind::Healpix, path, vec![30], None, None, None).is_err()
    );
}

#[test]
fn bigint_scalars_match_core_on_every_grid() {
    for grid in GridKind::ALL {
        let [to_lat, to_lng, get_resolution, is_valid, cell_to_parent, lonlat_to_cell] =
            scalars(grid);
        for (lon, lat) in [(-122.4194, 37.7749), (0.5, -60.0), (179.99, -0.01)] {
            for res in [0u8, 7, grid.max_resolution() - 1] {
                let cell = grid.cell_at(lat, lon, res).unwrap();
                let id = int64_to_datum(cell as i64);
                let indexed = call(
                    lonlat_to_cell,
                    &[
                        float8_to_datum(lon),
                        float8_to_datum(lat),
                        int64_to_datum(i64::from(res)),
                    ],
                );
                assert_eq!(
                    indexed.map(|d| datum_to_int64(d) as u64),
                    Some(cell),
                    "{grid}"
                );
                let (c_lat, c_lng) = grid.center_lat_lng(cell).unwrap();
                assert_eq!(call(to_lat, &[id]).map(datum_to_float8), Some(c_lat));
                assert_eq!(call(to_lng, &[id]).map(datum_to_float8), Some(c_lng));
                assert_eq!(
                    call(get_resolution, &[id]).map(datum_to_int64),
                    Some(i64::from(res))
                );
                assert_eq!(call(is_valid, &[id]), Some(1));
                let parent = call(cell_to_parent, &[id, int64_to_datum(0)]);
                assert_eq!(
                    parent.map(|d| datum_to_int64(d) as u64),
                    grid.parent(cell, 0)
                );
                assert_eq!(
                    call(cell_to_parent, &[id, int64_to_datum(i64::from(res) + 1)]),
                    None
                );
            }
        }
        // Invalid cells and out-of-range arguments are NULL (or false).
        let zero = int64_to_datum(0);
        assert_eq!(call(to_lat, &[zero]), None);
        assert_eq!(call(to_lng, &[zero]), None);
        assert_eq!(call(get_resolution, &[zero]), None);
        assert_eq!(call(is_valid, &[zero]), Some(0));
        assert_eq!(call(cell_to_parent, &[zero, zero]), None);
        let finest = i64::from(grid.max_resolution());
        let at = |lon: f64, lat: f64, res: i64| {
            call(
                lonlat_to_cell,
                &[
                    float8_to_datum(lon),
                    float8_to_datum(lat),
                    int64_to_datum(res),
                ],
            )
        };
        assert_eq!(at(0.0, 0.0, finest + 1), None);
        assert_eq!(at(0.0, 0.0, -1), None);
        assert_eq!(at(0.0, 91.0, 3), None);
        assert_eq!(at(f64::NAN, 0.0, 3), None);
        // Longitudes wrap.
        assert_eq!(at(190.0, 10.0, 5), at(-170.0, 10.0, 5));
    }
    let south = GridKind::S2.cell_at(-60.0, 0.5, 12).unwrap();
    assert!((south as i64) < 0, "S2 face 5 ids are negative bigints");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads a memory-mapped raster, which Miri cannot follow"
)]
fn auto_resolution_matches_core_on_every_grid() {
    use dggs_stream_core::raster::mosaic::{MosaicReader, OverlapRule};
    use dggs_stream_core::resolution::{auto_resolution, offset_resolution};
    use dggs_stream_pg::pg_auto_resolution;
    let (_tmp, path) = TestGeoTiffBuilder::new(SIZE, SIZE)
        .pixel_size(0.001)
        .create_f32_tempfile(value);
    let mosaic = MosaicReader::open(
        std::slice::from_ref(&path),
        None,
        None,
        OverlapRule::default(),
    )
    .unwrap();
    let path = path.to_str().unwrap();
    for grid in GridKind::ALL {
        let matched = auto_resolution(grid, &mosaic, 0).unwrap();
        for offset in [0, 1, -2, 100, -100] {
            assert_eq!(
                pg_auto_resolution(grid, path, offset, None).unwrap(),
                offset_resolution(grid, matched, offset),
                "{grid} {offset}"
            );
        }
        // A scan at that resolution has about one pixel per cell.
        let cells = PgContinuousSrf::open_for(grid, path, vec![matched], None, None, None)
            .unwrap()
            .count();
        let per_cell = f64::from(SIZE * SIZE) / cells as f64;
        assert!((0.3..4.0).contains(&per_cell), "{grid}: {per_cell}");
        assert!(pg_auto_resolution(grid, "/nonexistent.tif", 0, None).is_err());
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads a memory-mapped raster, which Miri cannot follow"
)]
fn nodata_policy_and_decimals_on_every_grid() {
    use dggs_stream_core::aggregator::{NodataPolicy, OutputPolicy};
    const NODATA: f32 = -9999.0;
    // NoData in the left third; values with fractional means elsewhere.
    let pixel = |c: u32, r: u32| {
        if c < SIZE / 3 {
            NODATA
        } else {
            ((c * 7 + r * 3) % 11) as f32 + 0.25
        }
    };
    let (_tmp, path) = TestGeoTiffBuilder::new(SIZE, SIZE)
        .pixel_size(0.001)
        .create_f32_tempfile(pixel);
    let path = path.to_str().unwrap();
    for grid in GridKind::ALL {
        let res = res_of(grid);
        let open = |policy: OutputPolicy| -> Vec<dggs_stream_pg::PgContinuousRow> {
            let mut rows: Vec<_> = PgContinuousSrf::open_with(
                grid,
                path,
                vec![res],
                None,
                Some(f64::from(NODATA)),
                None,
                policy,
            )
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
            rows.sort_by_key(|r| r.cell_id);
            rows
        };
        let omit = open(OutputPolicy::default());
        assert!(omit.iter().all(|r| r.count > 0.0 && !r.mean.is_nan()));

        let emit = open(OutputPolicy {
            nodata: NodataPolicy::Emit,
            ..Default::default()
        });
        let empty: Vec<_> = emit.iter().filter(|r| r.count == 0.0).collect();
        assert!(!empty.is_empty(), "{grid}");
        assert_eq!(emit.len(), omit.len() + empty.len(), "{grid}");
        for row in &empty {
            assert!(row.mean.is_nan() && row.min.is_nan() && row.max.is_nan());
            assert_eq!((row.sum, row.stddev), (0.0, None));
            assert!(!row.wkb.is_empty());
        }
        // The other cells are unchanged (to rounding: the two scans sum in another order).
        let kept: Vec<_> = emit.iter().filter(|r| r.count > 0.0).collect();
        assert_eq!(kept.len(), omit.len(), "{grid}");
        for (a, b) in kept.iter().zip(&omit) {
            assert_eq!(
                (a.cell_id, a.count, a.min, a.max),
                (b.cell_id, b.count, b.min, b.max)
            );
            assert!((a.mean - b.mean).abs() <= 1e-12 * b.mean.abs(), "{grid}");
            assert_eq!(a.stddev.is_some(), b.stddev.is_some());
        }

        let filled = open(OutputPolicy {
            nodata: NodataPolicy::Emit,
            nodata_value: Some(-1.0),
            decimals: Some(1),
        });
        assert_eq!(filled.len(), emit.len());
        for (plain, row) in emit.iter().zip(&filled) {
            if plain.count == 0.0 {
                assert_eq!((row.mean, row.min, row.max), (-1.0, -1.0, -1.0));
            } else {
                assert_eq!(row.mean, (plain.mean * 10.0).round() / 10.0, "{grid}");
                assert_eq!(row.count, plain.count);
            }
        }

        // Categorical: a class for empty cells only with an integral NoData value.
        let (_c, classes) = TestGeoTiffBuilder::new(SIZE, SIZE)
            .pixel_size(0.001)
            .gdal_nodata("0")
            .create_tempfile(|c, r| {
                if c < SIZE / 3 {
                    0u8
                } else {
                    ((c + r) % 3 + 1) as u8
                }
            });
        for value in [None, Some(255.0)] {
            let rows: Vec<_> = PgCategoricalSrf::open_with(
                grid,
                classes.to_str().unwrap(),
                vec![res],
                None,
                None,
                None,
                OutputPolicy {
                    nodata: NodataPolicy::Emit,
                    nodata_value: value,
                    decimals: Some(2),
                },
            )
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
            let empties: Vec<_> = rows.iter().filter(|r| r.total_count == 0.0).collect();
            assert_eq!(empties.len(), empty.len(), "{grid}");
            for row in &empties {
                assert_eq!(row.majority_is_null, value.is_none());
                assert_eq!(row.majority_class, if value.is_some() { 255 } else { 0 });
            }
            assert!(rows
                .iter()
                .filter(|r| r.total_count > 0.0)
                .all(|r| !r.majority_is_null && (1..=3).contains(&r.majority_class)));
        }
    }
}
