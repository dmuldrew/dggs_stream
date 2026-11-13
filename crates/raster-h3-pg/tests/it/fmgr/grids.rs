//! The text/bytea scalars and aggregate SRFs of A5, S2 and HEALPix through the V1
//! calling convention, against core and the Rust SRFs.

use raster_h3_core::grid::GridKind;
use raster_h3_core::scalar_ops::grid_to_wkb_u64;
use raster_h3_pg::fmgr::{
    pg_a5_is_valid_text, pg_a5_raster_categorical_aggregate, pg_a5_raster_continuous_aggregate,
    pg_a5_to_string, pg_a5_to_wkb, pg_finfo_pg_a5_to_string,
    pg_finfo_pg_healpix_raster_continuous_aggregate, pg_finfo_pg_s2_to_wkb,
    pg_healpix_is_valid_text, pg_healpix_raster_categorical_aggregate,
    pg_healpix_raster_continuous_aggregate, pg_healpix_to_string, pg_healpix_to_wkb,
    pg_s2_is_valid_text, pg_s2_raster_categorical_aggregate, pg_s2_raster_continuous_aggregate,
    pg_s2_to_string, pg_s2_to_wkb, pg_string_to_a5, pg_string_to_healpix, pg_string_to_s2,
};
use raster_h3_pg::{PgCategoricalSrf, PgContinuousSrf};

use super::{call, f, int64_to_datum, read_varlena, run_srf, short_varlena, srf_args, varlena};
use crate::grids::{res_of, ORIGINS};
use crate::helpers::TestGeoTiffBuilder;

type V1 = unsafe extern "C" fn(raster_h3_pg::pg_func_call_info) -> raster_h3_pg::Datum;

const GRIDS: [GridKind; 3] = [GridKind::A5, GridKind::S2, GridKind::Healpix];

/// `[to_string, string_to, is_valid(text), to_wkb, continuous SRF, categorical SRF]`.
fn entry_points(grid: GridKind) -> [V1; 6] {
    match grid {
        GridKind::A5 => [
            pg_a5_to_string,
            pg_string_to_a5,
            pg_a5_is_valid_text,
            pg_a5_to_wkb,
            pg_a5_raster_continuous_aggregate,
            pg_a5_raster_categorical_aggregate,
        ],
        GridKind::S2 => [
            pg_s2_to_string,
            pg_string_to_s2,
            pg_s2_is_valid_text,
            pg_s2_to_wkb,
            pg_s2_raster_continuous_aggregate,
            pg_s2_raster_categorical_aggregate,
        ],
        GridKind::Healpix => [
            pg_healpix_to_string,
            pg_string_to_healpix,
            pg_healpix_is_valid_text,
            pg_healpix_to_wkb,
            pg_healpix_raster_continuous_aggregate,
            pg_healpix_raster_categorical_aggregate,
        ],
        GridKind::H3 => unreachable!("H3 has its own entry points"),
    }
}

#[test]
fn finfo_records() {
    for finfo in [
        pg_finfo_pg_a5_to_string,
        pg_finfo_pg_s2_to_wkb,
        pg_finfo_pg_healpix_raster_continuous_aggregate,
    ] {
        // SAFETY: each `pg_finfo_*` returns a pointer to the static `PG_FINFO_V1`.
        assert_eq!(unsafe { (*finfo()).api_version }, 1);
    }
}

#[test]
fn text_and_bytea_scalars_round_trip_signed_ids() {
    for grid in GRIDS {
        let [to_string, string_to, is_valid_text, to_wkb, ..] = entry_points(grid);
        // (0.5, -60) is on S2 face 5, whose ids are negative as bigints.
        for (lon, lat) in [(-122.4194, 37.7749), (0.5, -60.0)] {
            for res in [0u8, 9, grid.max_resolution() - 1] {
                let cell = grid.cell_at(lat, lon, res).unwrap();
                let id = super::val(int64_to_datum(cell as i64));
                let hex = format!("{cell:x}");
                let text = call(to_string, &[id]).unwrap();
                // SAFETY: `text` is a varlena Datum from the extension.
                assert_eq!(unsafe { read_varlena(text) }, hex.as_bytes(), "{grid}");
                for arg in [varlena(hex.as_bytes()), short_varlena(hex.as_bytes())] {
                    let back = call(string_to, &[arg]).unwrap();
                    assert_eq!(back as i64, cell as i64, "{grid}: the same 64 bits");
                    assert_eq!(call(is_valid_text, &[arg]), Some(1));
                }
                let wkb = call(to_wkb, &[id]).unwrap();
                // SAFETY: `wkb` is a varlena Datum from the extension.
                let wkb = unsafe { read_varlena(wkb) };
                assert_eq!(wkb, grid_to_wkb_u64(grid, cell).unwrap());
            }
        }
        let zero = super::val(int64_to_datum(0));
        assert_eq!(call(to_string, &[zero]), None);
        assert_eq!(call(to_wkb, &[zero]), None);
        for junk in ["", "zz", "0"] {
            assert_eq!(call(string_to, &[varlena(junk.as_bytes())]), None, "{junk}");
            assert_eq!(call(is_valid_text, &[varlena(junk.as_bytes())]), Some(0));
        }
        assert_eq!(call(to_string, &[super::NULL]), None);
        assert_eq!(call(string_to, &[super::NULL]), None);
    }
    let south = GridKind::S2.cell_at(-60.0, 0.5, 9).unwrap();
    assert!((south as i64) < 0);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads a memory-mapped raster, which Miri cannot follow"
)]
fn aggregate_srfs_match_the_rust_srfs() {
    for origin in ORIGINS {
        let (_tmp, path) = TestGeoTiffBuilder::new(32, 32)
            .origin(origin.0, origin.1)
            .pixel_size(0.001)
            .create_f32_tempfile(|c, r| ((c + r) % 10 + 1) as f32);
        for grid in GRIDS {
            let [.., continuous, categorical] = entry_points(grid);
            let res = res_of(grid);
            let args = srf_args(&path, Some(i32::from(res)), None, None, None);

            let mut got = run_srf(continuous, 10, &args, None);
            got.sort();
            let mut want: Vec<Vec<Option<String>>> = PgContinuousSrf::open_for(
                grid,
                path.to_str().unwrap(),
                vec![res],
                None,
                None,
                None,
            )
            .unwrap()
            .map(|r| {
                let r = r.unwrap();
                vec![
                    Some(r.h3_index.to_string()),
                    Some(r.h3_hex),
                    Some(r.mean.to_string()),
                    r.stddev.map(|s| s.to_string()),
                    Some(r.count.to_string()),
                    Some(r.min.to_string()),
                    Some(r.max.to_string()),
                    Some(r.sum.to_string()),
                    Some(r.resolution.to_string()),
                    Some(format!(
                        "\\x{}",
                        r.wkb.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    )),
                ]
            })
            .collect();
            want.sort();
            assert!(want.len() > 2, "{grid}");
            let round = |rows: &mut Vec<Vec<Option<String>>>| {
                for r in rows.iter_mut() {
                    for i in [2, 3, 4, 5, 6, 7] {
                        r[i] = r[i].as_ref().map(|v| f(v.parse().unwrap()));
                    }
                }
            };
            round(&mut got);
            round(&mut want);
            assert_eq!(got, want, "{grid} at {origin:?}");
            if grid == GridKind::S2 && origin.1 < 0.0 {
                // Signed text of a top-bit id, as PostgreSQL's int8 input reads it.
                assert!(got.iter().all(|r| r[0].as_ref().unwrap().starts_with('-')));
            }

            let rows = run_srf(categorical, 11, &args, None);
            let rust = PgCategoricalSrf::open_for(
                grid,
                path.to_str().unwrap(),
                vec![res],
                None,
                None,
                None,
            )
            .unwrap()
            .count();
            assert_eq!(rows.len(), rust, "{grid}");
            for row in &rows {
                let id: i64 = row[0].as_ref().unwrap().parse().unwrap();
                assert_eq!(row[1].as_ref().unwrap(), &format!("{:x}", id as u64));
            }
        }
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads a memory-mapped raster, which Miri cannot follow"
)]
fn srf_resolutions_follow_the_grid() {
    let (_tmp, path) = TestGeoTiffBuilder::new(8, 8)
        .pixel_size(0.001)
        .create_f32_tempfile(|_, _| 1.0);
    // 20 is past H3's range but fine for A5; one row per call proves the scan ran.
    let args = srf_args(&path, Some(20), None, None, None);
    let rows = run_srf(pg_a5_raster_continuous_aggregate, 10, &args, Some(1));
    assert_eq!(rows.len(), 1);
}
