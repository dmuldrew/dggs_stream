use dggs_stream_core::scalar_ops::h3_polygon_to_cells_wkb;
use geo_types::{Coord, LineString, MultiPolygon, Polygon};
use h3o::geom::{ContainmentMode, TilerBuilder};
use h3o::Resolution;

enum TestGeometry {
    Polygon(Polygon<f64>),
    MultiPolygon(MultiPolygon<f64>),
}

impl TestGeometry {
    fn wkb(&self) -> Vec<u8> {
        match self {
            Self::Polygon(polygon) => polygon_wkb(polygon),
            Self::MultiPolygon(multipolygon) => {
                let mut wkb = vec![1];
                wkb.extend(6u32.to_le_bytes());
                wkb.extend((multipolygon.0.len() as u32).to_le_bytes());
                for polygon in &multipolygon.0 {
                    wkb.extend(polygon_wkb(polygon));
                }
                wkb
            }
        }
    }

    fn reference_cells(&self, resolution: Resolution, containment: ContainmentMode) -> Vec<u64> {
        let mut tiler = TilerBuilder::new(resolution)
            .containment_mode(containment)
            .build();
        match self {
            Self::Polygon(polygon) => tiler.add(polygon.clone()).unwrap(),
            Self::MultiPolygon(multipolygon) => tiler.add_batch(multipolygon.clone()).unwrap(),
        }
        let cells = tiler.into_coverage().map(u64::from).collect();
        sorted_unique(cells)
    }
}

fn sorted_unique(mut cells: Vec<u64>) -> Vec<u64> {
    cells.sort_unstable();
    cells.dedup();
    cells
}

fn line_string(points: &[(f64, f64)]) -> LineString<f64> {
    LineString::new(points.iter().map(|&(x, y)| Coord { x, y }).collect())
}

fn rectangle(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Polygon<f64> {
    Polygon::new(
        line_string(&[
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        ]),
        Vec::new(),
    )
}

fn polygon_wkb(polygon: &Polygon<f64>) -> Vec<u8> {
    let mut wkb = vec![1];
    wkb.extend(3u32.to_le_bytes());
    wkb.extend((1_u32 + polygon.interiors().len() as u32).to_le_bytes());
    for ring in std::iter::once(polygon.exterior()).chain(polygon.interiors()) {
        wkb.extend((ring.0.len() as u32).to_le_bytes());
        for point in &ring.0 {
            wkb.extend(point.x.to_le_bytes());
            wkb.extend(point.y.to_le_bytes());
        }
    }
    wkb
}

#[test]
fn polygon_to_cells_matches_h3o_for_containment_modes_and_overlaps() {
    let exterior_with_hole = Polygon::new(
        line_string(&[
            (-122.49, 37.71),
            (-122.35, 37.71),
            (-122.35, 37.83),
            (-122.49, 37.83),
            (-122.49, 37.71),
        ]),
        vec![line_string(&[
            (-122.45, 37.75),
            (-122.40, 37.75),
            (-122.40, 37.79),
            (-122.45, 37.79),
            (-122.45, 37.75),
        ])],
    );
    let geometries = [
        (
            "simple polygon",
            TestGeometry::Polygon(rectangle(-122.47, 37.73, -122.37, 37.81)),
        ),
        (
            "polygon with a hole",
            TestGeometry::Polygon(exterior_with_hole),
        ),
        (
            "overlapping multipolygon",
            TestGeometry::MultiPolygon(MultiPolygon::new(vec![
                rectangle(-122.48, 37.73, -122.40, 37.81),
                rectangle(-122.43, 37.73, -122.35, 37.81),
            ])),
        ),
    ];
    let modes = [
        ContainmentMode::ContainsCentroid,
        ContainmentMode::ContainsBoundary,
        ContainmentMode::IntersectsBoundary,
        ContainmentMode::Covers,
    ];

    for (name, geometry) in &geometries {
        let wkb = geometry.wkb();
        for resolution in [6_u8, 7] {
            for mode in modes {
                let expected =
                    geometry.reference_cells(Resolution::try_from(resolution).unwrap(), mode);
                let actual = h3_polygon_to_cells_wkb(&wkb, i64::from(resolution), mode).unwrap();
                assert_eq!(
                    actual, expected,
                    "{name}, resolution {resolution}, containment {mode:?}"
                );
                assert!(
                    actual.windows(2).all(|pair| pair[0] < pair[1]),
                    "{name}, resolution {resolution}, containment {mode:?}: \
                     output must be sorted and deduplicated"
                );
            }
        }
    }
}

#[test]
fn s2_polar_faces_cover_every_longitude_at_level_zero() {
    use dggs_stream_core::scalar_ops::grid_polygon_to_cells_wkb_into;
    use dggs_stream_core::GridKind;

    // A polar face's boundary circles the pole; its ring must close the full turn, or a
    // wedge next to the closing meridian is missed.
    for lat in [84.0, -84.2] {
        let face = GridKind::S2.cell_at(lat, 0.0, 0).unwrap();
        let mut lon = -180.0;
        while lon < 180.0 {
            let wkb = polygon_wkb(&rectangle(lon, lat, lon + 0.2, lat + 0.2));
            for mode in [ContainmentMode::Covers, ContainmentMode::IntersectsBoundary] {
                let mut out = Vec::new();
                grid_polygon_to_cells_wkb_into(GridKind::S2, &wkb, 0, mode, &mut out).unwrap();
                assert_eq!(out, vec![face], "lon {lon} lat {lat} {mode:?}");
            }
            lon += 0.25;
        }
    }
}

#[test]
fn a5_compacted_polygon_expands_to_requested_resolution() {
    use dggs_stream_core::grid::GridKind;
    use dggs_stream_core::scalar_ops::a5_polygon_to_cells_wkb;

    let grid = GridKind::A5;
    let cell = grid.cell_at(37.8, -122.4, 5).unwrap();
    let mut vertices = [(0.0, 0.0); 10];
    let count = grid.boundary_lng_lat(cell, &mut vertices);
    let mut ring: Vec<_> = vertices[..count]
        .iter()
        .map(|&(x, y)| Coord { x, y })
        .collect();
    ring.push(ring[0]);
    let wkb = polygon_wkb(&Polygon::new(LineString::new(ring), vec![]));
    let cells = a5_polygon_to_cells_wkb(&wkb, 7, ContainmentMode::ContainsCentroid).unwrap();
    assert!(!cells.is_empty());
    assert!(cells.iter().all(|&id| grid.resolution(id) == Some(7)));
    assert!(cells.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(cells.contains(&grid.cell_at(37.8, -122.4, 7).unwrap()));
}
