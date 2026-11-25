//! Configurable Discrete Global Grid System (DGGS) abstraction supporting
//! Uber H3 hexagons ([`h3o`]), A5 equal-area dodecahedral pentagons ([`a5`]),
//! Google S2 cube-face quadrilaterals ([`s2`]) and HEALPix equal-area
//! quadrilaterals ([`healpix`], nested scheme, NUNIQ cell ids).

use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

use h3o::{CellIndex, LatLng, LatLngIndexer, Resolution};
use serde::{Deserialize, Serialize};

use crate::scalar_ops::H3_AREA_M2;

/// Maximum resolution index (`30`) across all supported grid systems, plus one (`0..=30`).
pub const MAX_GRID_RESOLUTIONS: usize = 31;

/// Supported Discrete Global Grid Systems (DGGS).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GridKind {
    /// Uber H3 hexagonal grid (`0..=15`, aperture 7).
    #[default]
    H3,
    /// A5 equal-area dodecahedral pentagonal grid (`0..=30`, aperture 4 for `r >= 2`).
    A5,
    /// Google S2 cube-face quadrilateral grid (levels `0..=30`, aperture 4).
    S2,
    /// HEALPix equal-area quadrilateral grid (depths `0..=29`, aperture 4), nested
    /// scheme. Cell ids are NUNIQ: `4 * 4^depth + nested_pixel`.
    Healpix,
}

/// How a cell's edges run between its vertices, which decides how far a cell can reach
/// south of them (see [`GridSpec::edge_dips_south`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeModel {
    /// Great-circle arcs. An arc bulges toward the nearer pole, so only an edge with an
    /// endpoint on or south of the equator can pass south of both endpoints.
    Geodesic,
    /// Curves that are not great-circle arcs and may pass south of both endpoints in
    /// either hemisphere.
    Curved,
    /// Sides of a diamond in a projection whose northing depends on latitude alone: the
    /// south vertex is the cell's southernmost point.
    IsoLatitudeDiamond,
}

/// Where the poles sit relative to a grid's cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoleModel {
    /// One cell at every resolution holds each pole in its interior; no cell has a
    /// vertex on a pole.
    InteriorAtEveryResolution,
    /// A resolution-0 cell holds each pole in its interior; finer cells meet at the
    /// pole as a vertex.
    InteriorAtBaseOnly,
    /// Cells meet at each pole as a vertex at every resolution.
    VertexOnly,
}

/// How a grid's resolutions map to Web Mercator zoom levels in PMTiles pyramids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoomModel {
    /// Aperture 7: the hand-tuned H3 table, where a resolution spans one or two zooms.
    Aperture7Table,
    /// Aperture 4, like the tile pyramid: zoom equals resolution, capped at `max_zoom`.
    OnePerLevel {
        /// Highest zoom written.
        max_zoom: u8,
    },
}

/// The algorithm that fills a polygon with a grid's cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolygonFill {
    /// `h3o`'s polygon tiler (geodesic containment).
    H3Tiler,
    /// The `a5` crate's `polygon_to_cells`.
    A5Native,
    /// The polygon fill's descent through the grid's 4-ary hierarchy, testing
    /// cells against the polygon in lon/lat.
    NestedDescent,
}

/// A bound, in degrees of latitude, on how far north or south of its centre a cell
/// reaches: `head[res]` for the coarsest resolutions, then `tail` halved for each
/// further one. Stretched cells (HEALPix near 45 degrees) can be wider east to west.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RadiusModel {
    /// Bounds of resolutions `0..head.len()`.
    pub head: &'static [f64],
    /// Bound at resolution `head.len()`; each finer resolution halves it.
    pub tail: f64,
}

impl RadiusModel {
    /// The bound at `res`.
    #[inline]
    pub fn at(&self, res: u8) -> f64 {
        match self.head.get(usize::from(res)) {
            Some(&radius) => radius,
            None => {
                let steps = i32::from(res) - self.head.len() as i32;
                // Past the table an aperture-7 grid shrinks faster than halving, so the
                // last entry halved stays a bound.
                self.tail * 0.5_f64.powi(steps)
            }
        }
    }
}

/// The fixed properties and invariants of one grid: the single place pipeline stages
/// and engine adapters read them from, instead of matching on [`GridKind`].
///
/// Per-cell operations (indexing, hierarchy, geometry) are methods of [`GridKind`];
/// the per-sample indexing kernels are the [`GridBackend`] types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GridSpec {
    /// The grid these properties describe.
    pub kind: GridKind,
    /// Canonical lowercase identifier (`"h3"`), also the prefix of SQL function names.
    pub name: &'static str,
    /// Display name (`"HEALPix"`).
    pub label: &'static str,
    /// Name of the 64-bit integer cell column (`"h3_index"`).
    pub index_column: &'static str,
    /// Name of the hexadecimal string cell column (`"h3_hex"`).
    pub hex_column: &'static str,
    /// Finest resolution; resolutions are `0..=max_resolution`.
    pub max_resolution: u8,
    /// Number of resolution-0 cells.
    pub base_cell_count: u16,
    /// Children of an ordinary cell at the next resolution (see
    /// [`GridKind::expected_immediate_children`] for the exceptions).
    pub aperture: u8,
    /// Whether every cell of a resolution has the same area.
    pub equal_area: bool,
    /// Whether ids use bit 63, so they do not fit a signed 64-bit integer as a
    /// non-negative value. Adapters without an unsigned type keep the bit pattern.
    pub ids_use_sign_bit: bool,
    /// Whether the Parquet exporters annotate the index column `UINT_64`. H3's stays a
    /// plain `INT64`, as in every earlier release (its ids never use the sign bit).
    pub parquet_index_unsigned: bool,
    /// How edges run between vertices.
    pub edges: EdgeModel,
    /// Where the poles sit.
    pub poles: PoleModel,
    /// Whether streams may evict cells by raster row (`eviction_space::RowSpace`) rather
    /// than latitude. The row caps are certified for great-circle edges of H3's size
    /// bounds only.
    pub row_space_eviction: bool,
    /// PMTiles zoom mapping.
    pub zoom: ZoomModel,
    /// Bound on a cell's north-south extent around its centre, per resolution.
    pub radius: RadiusModel,
    /// Polygon-fill algorithm.
    pub polygon_fill: PolygonFill,
}

/// Empirical bounds (with at least 8% headroom) for `h3o` cells.
const H3_RADIUS_DEG: [f64; 16] = [
    13.5, 5.2, 1.95, 0.74, 0.28, 0.106, 0.04, 0.015, 0.006, 0.0025, 0.001, 0.0004, 0.00015,
    0.00006, 0.000025, 0.00001,
];

static GRID_SPECS: [GridSpec; 4] = [
    GridSpec {
        kind: GridKind::H3,
        name: "h3",
        label: "H3",
        index_column: "h3_index",
        hex_column: "h3_hex",
        max_resolution: 15,
        base_cell_count: 122,
        aperture: 7,
        equal_area: false,
        ids_use_sign_bit: false,
        parquet_index_unsigned: false,
        edges: EdgeModel::Geodesic,
        poles: PoleModel::InteriorAtEveryResolution,
        row_space_eviction: true,
        zoom: ZoomModel::Aperture7Table,
        radius: RadiusModel {
            head: &H3_RADIUS_DEG,
            tail: 0.000005,
        },
        polygon_fill: PolygonFill::H3Tiler,
    },
    GridSpec {
        kind: GridKind::A5,
        name: "a5",
        label: "A5",
        index_column: "a5_index",
        hex_column: "a5_hex",
        max_resolution: 30,
        base_cell_count: 12,
        aperture: 4,
        equal_area: true,
        ids_use_sign_bit: true,
        parquet_index_unsigned: true,
        edges: EdgeModel::Curved,
        poles: PoleModel::InteriorAtBaseOnly,
        row_space_eviction: false,
        // One zoom per level: a tile then holds roughly 15 cells.
        zoom: ZoomModel::OnePerLevel { max_zoom: 24 },
        // Measured extents are 37.5, 21.5 and 11.5 degrees, then halve per level.
        radius: RadiusModel {
            head: &[42.0, 23.0],
            tail: 12.5,
        },
        polygon_fill: PolygonFill::A5Native,
    },
    GridSpec {
        kind: GridKind::S2,
        name: "s2",
        label: "S2",
        index_column: "s2_index",
        hex_column: "s2_hex",
        max_resolution: 30,
        base_cell_count: 6,
        aperture: 4,
        equal_area: false,
        ids_use_sign_bit: true,
        parquet_index_unsigned: true,
        edges: EdgeModel::Geodesic,
        poles: PoleModel::InteriorAtBaseOnly,
        row_space_eviction: false,
        // One zoom per level: a tile then holds roughly 6 cells.
        zoom: ZoomModel::OnePerLevel { max_zoom: 24 },
        // S2's maximum cell diagonal is 2.44 rad / 2^level; half of it is 70 degrees.
        radius: RadiusModel {
            head: &[],
            tail: 70.0,
        },
        polygon_fill: PolygonFill::NestedDescent,
    },
    GridSpec {
        kind: GridKind::Healpix,
        name: "healpix",
        label: "HEALPix",
        index_column: "healpix_index",
        hex_column: "healpix_hex",
        max_resolution: 29,
        base_cell_count: 12,
        aperture: 4,
        equal_area: true,
        ids_use_sign_bit: false,
        parquet_index_unsigned: true,
        edges: EdgeModel::IsoLatitudeDiamond,
        poles: PoleModel::VertexOnly,
        row_space_eviction: false,
        zoom: ZoomModel::OnePerLevel { max_zoom: 24 },
        // A polar base cell reaches 48 degrees from its centre; cells halve per depth
        // but stretch near the polar-cap boundary.
        radius: RadiusModel {
            head: &[],
            tail: 60.0,
        },
        polygon_fill: PolygonFill::NestedDescent,
    },
];

impl GridSpec {
    /// Every grid's spec, in [`GridKind::ALL`] order.
    pub fn all() -> &'static [GridSpec; 4] {
        &GRID_SPECS
    }

    /// Whether `res` is a resolution of this grid.
    #[inline]
    pub const fn has_resolution(&self, res: u8) -> bool {
        res <= self.max_resolution
    }

    /// Average cell area in square metres at `res` (clamped to the grid's range).
    #[inline]
    pub fn area_m2(&self, res: u8) -> f64 {
        self.kind.area_m2(res)
    }

    /// Whether a cell can have a vertex on a pole. Such a vertex has no longitude of its
    /// own, so bounds computed from vertex longitudes are not valid near it.
    #[inline]
    pub const fn has_pole_vertices(&self) -> bool {
        !matches!(self.poles, PoleModel::InteriorAtEveryResolution)
    }

    /// Whether an edge between vertices at latitudes `p_lat` and `q_lat` (degrees) can
    /// pass south of both, so a southern bound must be padded below the lower vertex.
    #[inline]
    pub fn edge_dips_south(&self, p_lat: f64, q_lat: f64) -> bool {
        match self.edges {
            EdgeModel::IsoLatitudeDiamond => false,
            EdgeModel::Curved => true,
            EdgeModel::Geodesic => p_lat <= 0.0 || q_lat <= 0.0,
        }
    }

    /// The northern mirror of [`Self::edge_dips_south`]: whether an edge can pass north
    /// of both of its endpoints.
    #[inline]
    pub fn edge_bulges_north(&self, p_lat: f64, q_lat: f64) -> bool {
        match self.edges {
            EdgeModel::IsoLatitudeDiamond => false,
            EdgeModel::Curved => true,
            EdgeModel::Geodesic => p_lat >= 0.0 || q_lat >= 0.0,
        }
    }

    /// A bound, in degrees of latitude, on how far north or south of its centre a cell
    /// at `res` reaches.
    #[inline]
    pub fn max_cell_radius_deg(&self, res: u8) -> f64 {
        self.radius.at(res)
    }

    /// A distance in kilometres larger than the diameter of any cell at `res`,
    /// including the points a cell owns along curved edges. Widening a box by it
    /// makes every cell that touches the box lie wholly inside the widened one.
    pub fn max_cell_diameter_km(&self, res: u8) -> f64 {
        /// Kilometres per degree of great-circle arc, rounded up.
        const KM_PER_DEG: f64 = 111.4;
        let res = res.min(self.max_resolution);
        match Resolution::try_from(res) {
            // The largest H3 cells have edges about 1.4x the average and a diameter
            // about 2x their edge, so roughly 2.8x the average edge; 4x leaves headroom.
            Ok(h3_res) if self.kind == GridKind::H3 => 4.0 * h3_res.edge_length_km(),
            // Four times the north-south reach: twice it is the diameter only for a
            // cell centred in its extent, and stretched cells (HEALPix near 45
            // degrees) are wider east to west.
            _ => 4.0 * self.max_cell_radius_deg(res) * KM_PER_DEG,
        }
    }

    /// Whether the 64-bit ids of this grid need an unsigned column type (or a documented
    /// bit-pattern conversion) to be stored without loss of the natural ordering.
    #[inline]
    pub const fn needs_unsigned_ids(&self) -> bool {
        self.ids_use_sign_bit
    }
}

impl GridKind {
    /// Every supported grid.
    pub const ALL: [Self; 4] = [Self::H3, Self::A5, Self::S2, Self::Healpix];

    /// The grid's fixed properties and invariants.
    #[inline]
    pub fn spec(self) -> &'static GridSpec {
        &GRID_SPECS[self as usize]
    }

    /// The grid whose discriminant (`GridKind as u8`) is `index`; H3 if there is none.
    /// Lets a grid be a const generic argument.
    pub const fn from_index(index: u8) -> Self {
        match index {
            1 => Self::A5,
            2 => Self::S2,
            3 => Self::Healpix,
            _ => Self::H3,
        }
    }

    /// The accepted identifiers, for error messages.
    pub const EXPECTED: &'static str = "'h3', 'a5', 's2' or 'healpix'";

    /// Parse a grid system identifier (case-insensitive: `"h3"`, `"a5"`, `"s2"` or `"healpix"`).
    #[inline]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "h3" => Some(Self::H3),
            "a5" => Some(Self::A5),
            "s2" => Some(Self::S2),
            "healpix" => Some(Self::Healpix),
            _ => None,
        }
    }

    /// Canonical lowercase identifier (`"h3"`, `"a5"`, `"s2"` or `"healpix"`).
    #[inline]
    pub fn as_str(self) -> &'static str {
        self.spec().name
    }

    /// Display name (`"H3"`, `"A5"`, `"S2"` or `"HEALPix"`).
    #[inline]
    pub fn label(self) -> &'static str {
        self.spec().label
    }

    /// Canonical 64-bit integer cell index column name (`"h3_index"`, `"a5_index"`, ...).
    #[inline]
    pub fn index_column_name(self) -> &'static str {
        self.spec().index_column
    }

    /// Canonical hexadecimal string cell index column name (`"h3_hex"`, `"a5_hex"`, ...).
    #[inline]
    pub fn hex_column_name(self) -> &'static str {
        self.spec().hex_column
    }

    /// The grid whose index or hex column is called `name` (case-insensitive), also
    /// accepting the bare identifier (`"s2"`).
    pub fn from_cell_column_name(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        Self::ALL.into_iter().find(|g| {
            name == g.index_column_name() || name == g.hex_column_name() || name == g.as_str()
        })
    }

    /// Maximum supported resolution level (`15` for H3, `30` for A5 and S2, `29` for HEALPix).
    #[inline]
    pub fn max_resolution(self) -> u8 {
        self.spec().max_resolution
    }

    /// Average cell area in square meters at `res`.
    #[inline]
    pub fn area_m2(self, res: u8) -> f64 {
        let res = res.min(self.max_resolution());
        match self {
            Self::H3 => H3_AREA_M2.get(res as usize).copied().unwrap_or(7.373e5),
            Self::A5 => a5::cell_area(i32::from(res)),
            Self::S2 => EARTH_AREA_M2 / (6.0 * (1u64 << (2 * res)) as f64),
            Self::Healpix => EARTH_AREA_M2 / (12.0 * (1u64 << (2 * res)) as f64),
        }
    }

    /// Return `true` if `cell` is a valid, canonical 64-bit cell ID for this grid.
    #[inline]
    pub fn is_valid(self, cell: u64) -> bool {
        match self {
            Self::H3 => CellIndex::try_from(cell).is_ok(),
            Self::A5 => a5_is_valid_cell(cell),
            Self::S2 => S2CellId(cell).is_valid(),
            Self::Healpix => healpix_decode(cell).is_some(),
        }
    }

    /// Extract the resolution (`0..=max_resolution`) of `cell`, or `None` if invalid.
    #[inline]
    pub fn resolution(self, cell: u64) -> Option<u8> {
        match self {
            Self::H3 => CellIndex::try_from(cell)
                .ok()
                .map(|idx| u8::from(idx.resolution())),
            Self::A5 => {
                if !a5_is_valid_cell(cell) {
                    return None;
                }
                u8::try_from(a5::get_resolution(cell)).ok()
            }
            Self::S2 => {
                let id = S2CellId(cell);
                id.is_valid().then(|| id.level() as u8)
            }
            Self::Healpix => healpix_decode(cell).map(|(depth, _)| depth),
        }
    }

    /// Compute the ancestor cell of `cell` at `parent_res`, or `None` if invalid or
    /// `parent_res` is finer than `cell`'s resolution.
    #[inline]
    pub fn parent(self, cell: u64, parent_res: u8) -> Option<u64> {
        match self {
            Self::H3 => {
                let idx = CellIndex::try_from(cell).ok()?;
                let target = Resolution::try_from(parent_res).ok()?;
                idx.parent(target).map(u64::from)
            }
            Self::A5 => {
                let cur_res = self.resolution(cell)?;
                if parent_res > cur_res {
                    return None;
                }
                a5::cell_to_parent(cell, Some(i32::from(parent_res))).ok()
            }
            Self::S2 => {
                let id = S2CellId(cell);
                (id.is_valid() && u64::from(parent_res) <= id.level())
                    .then(|| id.parent(u64::from(parent_res)).0)
            }
            Self::Healpix => {
                let (depth, pixel) = healpix_decode(cell)?;
                (parent_res <= depth)
                    .then(|| healpix_encode(parent_res, pixel >> (2 * (depth - parent_res))))
            }
        }
    }

    /// Immediate parent `(parent_res, parent_cell)` at resolution `res - 1`.
    #[inline]
    pub fn immediate_parent(self, cell: u64) -> Option<(u8, u64)> {
        let res = self.resolution(cell)?;
        if res == 0 {
            return None;
        }
        let parent_res = res - 1;
        let parent = self.parent(cell, parent_res)?;
        Some((parent_res, parent))
    }

    /// Append the immediate or descendant children of `cell` at `child_res` into `out`.
    #[inline]
    pub fn children_into(self, cell: u64, child_res: u8, out: &mut Vec<u64>) {
        out.clear();
        match self {
            Self::H3 => {
                if let (Ok(idx), Ok(res)) =
                    (CellIndex::try_from(cell), Resolution::try_from(child_res))
                {
                    out.extend(idx.children(res).map(u64::from));
                }
            }
            Self::A5 => {
                if self.is_valid(cell) {
                    if let Ok(children) = a5::cell_to_children(cell, Some(i32::from(child_res))) {
                        out.extend(children);
                        out.dedup();
                    }
                }
            }
            Self::S2 => {
                let id = S2CellId(cell);
                if id.is_valid()
                    && child_res <= self.max_resolution()
                    && u64::from(child_res) >= id.level()
                {
                    let end = id.child_end_at_level(u64::from(child_res));
                    let mut child = id.child_begin_at_level(u64::from(child_res));
                    while child != end {
                        out.push(child.0);
                        child = child.next();
                    }
                }
            }
            Self::Healpix => {
                if let Some((depth, pixel)) = healpix_decode(cell) {
                    if child_res >= depth && child_res <= self.max_resolution() {
                        let shift = 2 * (child_res - depth);
                        out.extend(
                            ((pixel << shift)..((pixel + 1) << shift))
                                .map(|child| healpix_encode(child_res, child)),
                        );
                    }
                }
            }
        }
    }

    /// Expected number of immediate children (`parent_res -> parent_res + 1`) for `parent_cell`.
    #[inline]
    pub fn expected_immediate_children(self, parent_cell: u64, parent_res: u8) -> usize {
        match self {
            Self::H3 => match CellIndex::try_from(parent_cell) {
                Ok(p) if p.is_pentagon() => 6,
                _ => 7,
            },
            Self::A5 => {
                if parent_res == 0 {
                    5
                } else {
                    4
                }
            }
            Self::S2 | Self::Healpix => 4,
        }
    }

    /// Cell center in WGS84 degrees `(lat, lng)` with `lng` in `[-180, 180]`.
    #[inline]
    pub fn center_lat_lng(self, cell: u64) -> Option<(f64, f64)> {
        match self {
            Self::H3 => CellIndex::try_from(cell).ok().map(|idx| {
                let ll = LatLng::from(idx);
                (ll.lat(), ll.lng())
            }),
            Self::A5 => {
                if !a5_is_valid_cell(cell) {
                    return None;
                }
                let ll = a5::cell_to_lonlat(cell).ok()?;
                Some((ll.latitude(), wrap_lon_180(ll.longitude())))
            }
            Self::S2 => {
                let id = S2CellId(cell);
                if !id.is_valid() {
                    return None;
                }
                let ll = s2::latlng::LatLng::from(id);
                Some((ll.lat.deg().clamp(-90.0, 90.0), wrap_lon_180(ll.lng.deg())))
            }
            Self::Healpix => {
                let (depth, pixel) = healpix_decode(cell)?;
                let c = healpix::get(depth).center(pixel);
                Some((c.lat.to_degrees(), wrap_lon_180(c.lon.to_degrees())))
            }
        }
    }

    /// Write the unclosed counter-clockwise boundary vertices `(lng, lat)` in degrees
    /// (`lng` in `[-180, 180]`) into `out` and return the vertex count (`<= 10`).
    #[inline]
    pub fn boundary_lng_lat(self, cell: u64, out: &mut [(f64, f64); 10]) -> usize {
        match self {
            Self::H3 => {
                let Ok(idx) = CellIndex::try_from(cell) else {
                    return 0;
                };
                let boundary = idx.boundary();
                let count = boundary.len().min(10);
                for (i, v) in boundary.iter().take(count).enumerate() {
                    out[i] = (v.lng(), v.lat());
                }
                count
            }
            Self::A5 => {
                if !a5_is_valid_cell(cell) {
                    return 0;
                }
                let Ok(verts) = a5::cell_to_boundary(
                    cell,
                    Some(a5::CellToBoundaryOptions {
                        closed_ring: false,
                        segments: Some(1),
                    }),
                ) else {
                    return 0;
                };
                let mut ring = [(0.0, 0.0); 9];
                let m = verts.len().min(9);
                for (slot, v) in ring.iter_mut().zip(&verts) {
                    *slot = (v.longitude(), v.latitude());
                }
                write_lng_lat_ring(&ring[..m], out)
            }
            Self::S2 => {
                let id = S2CellId(cell);
                if !id.is_valid() {
                    return 0;
                }
                // Counter-clockwise, as S2 defines them.
                let ring = s2::cell::Cell::from(id).vertices().map(|p| {
                    let ll = s2::latlng::LatLng::from(p);
                    (ll.lng.deg(), ll.lat.deg())
                });
                write_lng_lat_ring(&ring, out)
            }
            Self::Healpix => {
                let Some((depth, pixel)) = healpix_decode(cell) else {
                    return 0;
                };
                // South, east, north, west: counter-clockwise.
                let ring = healpix::get(depth)
                    .vertices(pixel)
                    .map(|v| (v.lon.to_degrees(), v.lat.to_degrees()));
                write_lng_lat_ring(&ring, out)
            }
        }
    }

    /// Whether cell edges are great-circle arcs, which lets latitude bounds assume an
    /// edge stays on its endpoints' side of the equator-ward bulge.
    #[inline]
    pub fn has_geodesic_edges(self) -> bool {
        self.spec().edges == EdgeModel::Geodesic
    }

    /// Conservative WGS84 bounding box `[min_lon, min_lat, max_lon, max_lat]` of a cell:
    /// every point of the cell is inside it. `min_lon > max_lon` for a box crossing the
    /// antimeridian; a cell holding or touching a pole spans every longitude. `None` for
    /// an invalid cell.
    pub fn wgs84_bbox(self, cell_u64: u64) -> Option<[f64; 4]> {
        let grid = self;
        let spec = grid.spec();
        let res = grid.resolution(cell_u64)?;
        let pole = grid.interior_pole(cell_u64);
        let (is_north_pole, is_south_pole) = (pole == Some(true), pole == Some(false));
        let mut verts = [(0.0, 0.0); 10];
        let n = grid.boundary_lng_lat(cell_u64, &mut verts);
        if n == 0 {
            return Some([-180.0, -90.0, 180.0, 90.0]);
        }

        let south_lat =
            crate::aggregator::horizon_streamer::compute_cell_south_lat_for(grid, cell_u64);
        // By symmetry, great-circle edges in the northern hemisphere can bulge northward by at most
        // (|delta_lat| + |delta_lon|) / 2 relative to their endpoints.
        let mut north_lat = if is_north_pole { 90.0 } else { -90.0_f64 };
        if !is_north_pole {
            for i in 0..n {
                let (p_lng, p_lat) = verts[i];
                let (q_lng, q_lat) = verts[(i + 1) % n];
                let mut edge_north = p_lat.max(q_lat).next_up();
                // Whether the edge can pass north of both endpoints depends on the grid's
                // edge model.
                if spec.edge_bulges_north(p_lat, q_lat) {
                    let direct = (p_lng - q_lng).abs();
                    let delta_lon = direct.min(360.0 - direct);
                    let delta_lat = (p_lat - q_lat).abs();
                    edge_north = (edge_north + 0.5 * (delta_lat + delta_lon)).next_up();
                }
                north_lat = north_lat.max(edge_north);
            }
            north_lat = north_lat.min(90.0);
        }

        if is_north_pole || is_south_pole || north_lat >= 89.5 || south_lat <= -89.5 {
            return Some([-180.0, south_lat, 180.0, north_lat]);
        }

        // Unwrap boundary vertex longitudes around vertex 0 so antimeridian-crossing
        // cells form a tight continuous interval instead of [-180, 180].
        let mut lngs = [0.0f64; 10];
        lngs[0] = verts[0].0;
        let mut min_u = lngs[0];
        let mut max_u = lngs[0];
        for i in 1..n {
            let mut lng = verts[i].0;
            while lng - lngs[i - 1] > 180.0 {
                lng -= 360.0;
            }
            while lng - lngs[i - 1] < -180.0 {
                lng += 360.0;
            }
            lngs[i] = lng;
            min_u = min_u.min(lng);
            max_u = max_u.max(lng);
        }

        // Pad longitude by a small geodesic safety margin scaled by sec(lat).
        let max_abs_lat = south_lat.abs().max(north_lat.abs()).min(89.0);
        let r = spec.max_cell_radius_deg(res);
        let pad_lon = (r * 0.25 / max_abs_lat.to_radians().cos()).max(1e-5);
        let lo = min_u - pad_lon;
        let hi = max_u + pad_lon;
        if hi - lo >= 360.0 {
            return Some([-180.0, south_lat, 180.0, north_lat]);
        }

        let wrap = crate::crs::transformer::wrap_lon;
        let (min_lon, max_lon) = if lo >= -180.0 && hi <= 180.0 {
            (lo, hi)
        } else {
            (wrap(lo), wrap(hi))
        };

        Some([min_lon, south_lat, max_lon, north_lat])
    }

    /// Append every resolution-0 cell of the grid to `out`.
    pub fn base_cells_into(self, out: &mut Vec<u64>) {
        match self {
            Self::H3 => out.extend(CellIndex::base_cells().map(u64::from)),
            Self::A5 => {
                if let Ok(cells) = a5::get_res0_cells() {
                    out.extend(cells);
                }
            }
            Self::S2 => out.extend((0..6).map(|face| S2CellId::from_face(face).0)),
            Self::Healpix => out.extend((0..12).map(|pixel| healpix_encode(0, pixel))),
        }
    }

    /// `Some(true)` if `cell` holds the north pole in its interior, `Some(false)` for
    /// the south pole, `None` otherwise (a pole on a vertex does not count).
    pub fn interior_pole(self, cell: u64) -> Option<bool> {
        let [north, south] = match self {
            Self::H3 => {
                let res = CellIndex::try_from(cell).ok()?.resolution();
                [90.0, -90.0]
                    .map(|lat| u64::from(LatLng::new(lat, 0.0).expect("pole").to_cell(res)))
            }
            Self::A5 => a5_pole_cells(),
            // The +z and -z cube faces; finer cells meet at the pole as a vertex.
            Self::S2 => [S2CellId::from_face(2).0, S2CellId::from_face(5).0],
            // Four base cells meet at each pole.
            Self::Healpix => return None,
        };
        if cell == north {
            Some(true)
        } else if cell == south {
            Some(false)
        } else {
            None
        }
    }

    /// Index a single WGS84 `(lat, lon)` coordinate in degrees at `res`.
    #[inline]
    pub fn cell_at(self, lat: f64, lon: f64, res: u8) -> Option<u64> {
        if !(-90.0..=90.0).contains(&lat) || !lon.is_finite() {
            return None;
        }
        match self {
            Self::H3 => {
                let r = Resolution::try_from(res).ok()?;
                let ll = LatLng::new(lat, lon).ok()?;
                Some(crate::aggregator::multi_horizon::profile::index(ll, r).into())
            }
            Self::A5 => {
                if res > 30 {
                    return None;
                }
                let pt = a5::core::coordinate_transforms::from_lon_lat(a5::LonLat::new(
                    wrap_lon_180(lon),
                    lat,
                ));
                if a5_near_pole(lat) {
                    crate::aggregator::multi_horizon::profile::a5_forget_last_cell(pt, res);
                }
                crate::aggregator::multi_horizon::profile::index_a5(pt, res)
            }
            Self::S2 | Self::Healpix => {
                if res > self.max_resolution() {
                    return None;
                }
                let mut cell = [0u64];
                match self {
                    Self::S2 => S2Backend::index_all(
                        &mut (),
                        S2Backend::make_point(lon, lat)?,
                        &[res],
                        &mut cell,
                    ),
                    _ => HealpixBackend::index_all(
                        &mut (),
                        HealpixBackend::make_point(lon, lat)?,
                        &[res],
                        &mut cell,
                    ),
                }
                Some(cell[0])
            }
        }
    }

    /// Return `true` if `cell` contains or touches the South Pole (`-90°`), requiring a
    /// conservative `-90.0` southern latitude bound.
    #[inline]
    pub fn is_south_pole_cell(self, cell: u64) -> bool {
        match self {
            Self::H3 => {
                static H3_SOUTH_POLE: OnceLock<[CellIndex; 16]> = OnceLock::new();
                let Ok(idx) = CellIndex::try_from(cell) else {
                    return false;
                };
                let cells = H3_SOUTH_POLE.get_or_init(|| {
                    core::array::from_fn(|r| {
                        LatLng::new(-90.0, 0.0)
                            .unwrap()
                            .to_cell(Resolution::try_from(r as u8).unwrap())
                    })
                });
                cells[u8::from(idx.resolution()) as usize] == idx
            }
            Self::A5 | Self::S2 | Self::Healpix => self.interior_pole(cell) == Some(false),
        }
    }
}

impl fmt::Display for GridKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GridKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
            .ok_or_else(|| format!("Invalid grid system '{s}': expected {}", Self::EXPECTED))
    }
}

/// The resolution-0 A5 cells containing the north and south poles, in that order
/// (finer cells meet at a pole as a vertex).
pub fn a5_pole_cells() -> [u64; 2] {
    static A5_POLES: OnceLock<[u64; 2]> = OnceLock::new();
    *A5_POLES.get_or_init(|| {
        [
            a5::lonlat_to_cell(a5::LonLat::new(0.0, 90.0), 0).expect("north pole a5"),
            a5::lonlat_to_cell(a5::LonLat::new(0.0, -90.0), 0).expect("south pole a5"),
        ]
    })
}

/// Latitudes within this many degrees of a pole (about 110 m) are indexed on A5 without
/// the `a5` crate's last-cell shortcut, which misplaces points within about 1e-5 degrees
/// of a pole (see `profile::a5_forget_last_cell`).
pub const A5_POLE_GUARD_DEG: f64 = 1e-3;

#[inline(always)]
fn a5_near_pole(lat: f64) -> bool {
    lat.abs() > 90.0 - A5_POLE_GUARD_DEG
}

/// Surface area of the authalic sphere in square meters.
const EARTH_AREA_M2: f64 = 5.100_656_217_240_886e14;

use s2::cellid::CellID as S2CellId;

/// Deepest HEALPix depth; a point is indexed once here and shifted to coarser depths.
const HEALPIX_MAX_DEPTH: u8 = healpix::MAX_DEPTH;

/// NUNIQ id of nested pixel `pixel` at `depth`: `4 * 4^depth + pixel`.
#[inline(always)]
pub const fn healpix_encode(depth: u8, pixel: u64) -> u64 {
    (4u64 << (2 * depth)) + pixel
}

/// `(depth, nested pixel)` of a NUNIQ id, or `None` if it is not one at depth `0..=29`.
#[inline]
pub const fn healpix_decode(uniq: u64) -> Option<(u8, u64)> {
    // Depth `d` ids fill `[4 * 4^d, 16 * 4^d)`.
    if uniq < 4 || uniq >= 16u64 << (2 * HEALPIX_MAX_DEPTH) {
        return None;
    }
    let depth = ((61 - uniq.leading_zeros()) >> 1) as u8;
    Some((depth, uniq - (4u64 << (2 * depth))))
}

/// Write an unclosed counter-clockwise `(lng, lat)` ring in degrees into `out`, with
/// longitudes wrapped to `[-180, 180]`, and return the vertex count.
///
/// A pole has no longitude (grids report an arbitrary one). A vertex on a pole is
/// replaced by two that run along it between the neighbouring vertices' meridians, so
/// the lon/lat ring covers the cell. `ring` holds at most 9 vertices.
fn write_lng_lat_ring(ring: &[(f64, f64)], out: &mut [(f64, f64); 10]) -> usize {
    let m = ring.len().min(9);
    let mut count = 0;
    let mut pole_expanded = false;
    for i in 0..m {
        let lat = ring[i].1.clamp(-90.0, 90.0);
        if !pole_expanded && lat.abs() >= 90.0 - 1e-9 {
            out[count] = (wrap_lon_180(ring[(i + m - 1) % m].0), lat);
            out[count + 1] = (wrap_lon_180(ring[(i + 1) % m].0), lat);
            count += 2;
            pole_expanded = true;
        } else {
            out[count] = (wrap_lon_180(ring[i].0), lat);
            count += 1;
        }
    }
    // A vertex on the antimeridian is at both -180 and 180. Put it on the side the
    // rest of the cell is on, so a cell that only touches the antimeridian is not
    // taken for one that crosses it.
    let side = |lng: f64| {
        if lng.abs() == 180.0 {
            0.0
        } else {
            lng.signum()
        }
    };
    let east = out[..count].iter().any(|v| side(v.0) > 0.0);
    let west = out[..count].iter().any(|v| side(v.0) < 0.0);
    if east != west {
        let edge = if east { 180.0 } else { -180.0 };
        for v in &mut out[..count] {
            if v.0.abs() == 180.0 {
                v.0 = edge;
            }
        }
    }
    count
}

/// Normalize a finite longitude (in degrees) into `[-180.0, 180.0]`.
#[inline(always)]
pub fn wrap_lon_180(lon: f64) -> f64 {
    if (-180.0..=180.0).contains(&lon) {
        return lon;
    }
    let wrapped = ((lon + 180.0) % 360.0 + 360.0) % 360.0 - 180.0;
    if wrapped == -180.0 && lon > 0.0 {
        180.0
    } else {
        wrapped
    }
}

/// Check whether `cell` is a valid, canonical A5 cell index at resolution `0..=30`.
#[inline]
pub fn a5_is_valid_cell(cell: u64) -> bool {
    thread_local! {
        /// The last cell this thread found valid (0 is never valid). Callers ask for a
        /// cell's centre, boundary and resolution in turn, each of which validates it.
        static LAST_VALID: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    if cell == 0 {
        return false;
    }
    if LAST_VALID.get() == cell {
        return true;
    }
    let valid = a5_decodes_canonically(cell);
    if valid {
        LAST_VALID.set(cell);
    }
    valid
}

fn a5_decodes_canonically(cell: u64) -> bool {
    let Ok(parsed) = a5::core::serialization::deserialize(cell) else {
        return false;
    };
    if !(0..=a5::MAX_RESOLUTION).contains(&parsed.resolution) {
        return false;
    }
    a5::core::serialization::serialize(&parsed) == Ok(cell)
}

/// Static dispatch backend for the inner pixel scanline walker (`walker.rs`).
pub trait GridBackend: Copy + Send + Sync + 'static {
    /// Projected point representation shared across all resolutions of a sample.
    type Point: Copy;
    /// Per-thread/per-window indexer state.
    type Indexer;

    /// Construct a fresh indexer for the given sorted `resolutions`.
    fn new_indexer(resolutions: &[u8]) -> Self::Indexer;

    /// Validate `(lon, lat)` in degrees and convert to `Self::Point`.
    fn make_point(lon: f64, lat: f64) -> Option<Self::Point>;

    /// Index `point` across all `resolutions`, writing the resulting `u64` cell IDs to `out`.
    fn index_all(
        indexer: &mut Self::Indexer,
        point: Self::Point,
        resolutions: &[u8],
        out: &mut [u64],
    );
}

/// The longitude a scalar walk computes for a column of a constant-latitude row:
/// `c0 + (col_offset + col + dx) * a` in the source CRS, then (for Web Mercator)
/// converted to degrees. Batched rows approximate it by stepping from the first
/// column; samples near a cell boundary are recomputed with this exact formula, so
/// they land in the same cell as the scalar walk.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExactLon {
    pub c0: f64,
    pub a: f64,
    pub col_offset: usize,
    pub dx: f64,
    pub mercator: bool,
}

impl ExactLon {
    /// A row whose column `col` lies at `lon_col0 + col * dlon` exactly.
    #[cfg(test)]
    pub(crate) fn linear(lon_col0: f64, dlon: f64) -> Self {
        Self {
            c0: lon_col0,
            a: dlon,
            col_offset: 0,
            dx: 0.0,
            mercator: false,
        }
    }

    #[inline]
    fn at(&self, col: usize) -> f64 {
        let x = self.c0 + ((self.col_offset + col) as f64 + self.dx) * self.a;
        if self.mercator {
            crate::crs::transformer::web_mercator_lon(x)
        } else {
            x
        }
    }
}

/// Optional row-hoisted & batched indexing for constant-latitude affine-longitude
/// scanlines (`RowBatchBackend::SUPPORTS_ROW_BATCH == true`).
///
/// Backends opt in when indexing at the finest requested resolution supports
/// per-row coordinate hoisting (such as S2 and HEALPix) and coarser requested
/// resolutions can be derived purely by bit masking/shifting (`parents_from_finest`).
pub(crate) trait RowBatchBackend: GridBackend {
    /// Per-row hoisted constants shared across every pixel in a constant-latitude row.
    type RowState: Copy;

    /// Whether this backend implements fast row-hoisted batch indexing.
    const SUPPORTS_ROW_BATCH: bool = false;

    /// Precompute row-invariant constants for a row at `lat_deg` with column `0` at
    /// `lon_col0_deg` and per-column longitude step `dlon_deg`, indexing at `max_res`.
    /// Returns `None` for singular rows (e.g. near poles or projection transitions).
    #[inline(always)]
    fn prepare_row(
        _lat_deg: f64,
        _lon_col0_deg: f64,
        _dlon_deg: f64,
        _exact: ExactLon,
        _max_res: u8,
    ) -> Option<Self::RowState> {
        None
    }

    /// Index consecutive columns `start_col .. start_col + out.len()` into `out`
    /// at the `max_res` given to [`Self::prepare_row`].
    #[inline(always)]
    fn index_row_batch(_state: &Self::RowState, _start_col: usize, _out: &mut [u64]) {}

    /// Derive cell IDs for all sorted/arbitrary `resolutions` from `finest_cell`
    /// (which is at `max_res`), writing into `out`.
    #[inline(always)]
    fn parents_from_finest(_finest_cell: u64, _max_res: u8, _resolutions: &[u8], _out: &mut [u64]) {
    }
}

/// Static [`GridBackend`] implementation for Uber H3 (`h3o`).
#[derive(Clone, Copy, Debug, Default)]
pub struct H3Backend;

impl GridBackend for H3Backend {
    type Point = LatLng;
    type Indexer = (LatLngIndexer, Vec<Resolution>);

    #[inline]
    fn new_indexer(resolutions: &[u8]) -> Self::Indexer {
        let h3_res = resolutions
            .iter()
            .map(|&r| Resolution::try_from(r).expect("validated H3 resolution"))
            .collect();
        (LatLngIndexer::default(), h3_res)
    }

    #[inline(always)]
    fn make_point(lon: f64, lat: f64) -> Option<Self::Point> {
        LatLng::new(lat, lon).ok()
    }

    #[inline(always)]
    fn index_all(
        indexer: &mut Self::Indexer,
        point: Self::Point,
        _resolutions: &[u8],
        out: &mut [u64],
    ) {
        crate::aggregator::multi_horizon::profile::index_all(
            &mut indexer.0,
            point,
            &indexer.1,
            out,
        );
    }
}

impl RowBatchBackend for H3Backend {
    type RowState = ();
}

/// Static [`GridBackend`] implementation for A5 pentagons (`a5`).
#[derive(Clone, Copy, Debug, Default)]
pub struct A5Backend;

impl GridBackend for A5Backend {
    /// The point on the sphere, and whether it is within [`A5_POLE_GUARD_DEG`] of a pole.
    type Point = (a5::coordinate_systems::Spherical, bool);
    type Indexer = ();

    #[inline]
    fn new_indexer(_resolutions: &[u8]) -> Self::Indexer {}

    #[inline(always)]
    fn make_point(lon: f64, lat: f64) -> Option<Self::Point> {
        if !lon.is_finite() || !(-90.0..=90.0).contains(&lat) {
            return None;
        }
        let norm_lon = wrap_lon_180(lon);
        Some((
            a5::core::coordinate_transforms::from_lon_lat(a5::LonLat::new(norm_lon, lat)),
            a5_near_pole(lat),
        ))
    }

    #[inline(always)]
    fn index_all(
        _indexer: &mut Self::Indexer,
        point: Self::Point,
        resolutions: &[u8],
        out: &mut [u64],
    ) {
        let (spherical, near_pole) = point;
        if near_pole {
            // Rare: one sample row at most in a raster that reaches a pole.
            for (cell, &res) in out.iter_mut().zip(resolutions) {
                crate::aggregator::multi_horizon::profile::a5_forget_last_cell(spherical, res);
                *cell = crate::aggregator::multi_horizon::profile::index_a5(spherical, res)
                    .unwrap_or(0);
            }
            return;
        }
        crate::aggregator::multi_horizon::profile::index_all_a5(spherical, resolutions, out);
    }
}

impl RowBatchBackend for A5Backend {
    type RowState = ();
}

/// 4-bit S2 Hilbert lookup table mapping `(i_nib << 6) | (j_nib << 2) | orientation`
/// to `(pos_bits << 2) | next_orientation`.
fn s2_lookup_pos() -> &'static [u16; 1024] {
    static LOOKUP_POS: OnceLock<[u16; 1024]> = OnceLock::new();
    LOOKUP_POS.get_or_init(|| {
        const SWAP_MASK: usize = 1;
        const INVERT_MASK: usize = 2;
        const POS_TO_IJ: [[usize; 4]; 4] = [[0, 1, 3, 2], [0, 2, 3, 1], [3, 2, 0, 1], [3, 1, 0, 2]];
        const POS_TO_ORIENTATION: [usize; 4] = [SWAP_MASK, 0, 0, INVERT_MASK | SWAP_MASK];

        fn init_lookup_cell(
            level: u8,
            i: usize,
            j: usize,
            orig_orientation: usize,
            pos: usize,
            orientation: usize,
            table: &mut [u16; 1024],
        ) {
            if level == 4 {
                let ij = (i << 4) + j;
                table[(ij << 2) + orig_orientation] = ((pos << 2) + orientation) as u16;
            } else {
                let next_level = level + 1;
                let i = i << 1;
                let j = j << 1;
                let pos = pos << 2;
                let r = &POS_TO_IJ[orientation];
                for (k, &ij_bits) in r.iter().enumerate() {
                    init_lookup_cell(
                        next_level,
                        i + (ij_bits >> 1),
                        j + (ij_bits & 1),
                        orig_orientation,
                        pos + k,
                        orientation ^ POS_TO_ORIENTATION[k],
                        table,
                    );
                }
            }
        }

        let mut table = [0u16; 1024];
        init_lookup_cell(0, 0, 0, 0, 0, 0, &mut table);
        init_lookup_cell(0, 0, 0, SWAP_MASK, 0, SWAP_MASK, &mut table);
        init_lookup_cell(0, 0, 0, INVERT_MASK, 0, INVERT_MASK, &mut table);
        init_lookup_cell(
            0,
            0,
            0,
            SWAP_MASK | INVERT_MASK,
            0,
            SWAP_MASK | INVERT_MASK,
            &mut table,
        );
        table
    })
}

/// Encode `(face, i, j)` at S2 `level` (`0..=30`) where `0 <= i, j < (1 << level)`.
#[inline(always)]
fn s2_face_ij_to_cell_at_level(lookup: &[u16; 1024], face: u64, i: u32, j: u32, level: u8) -> u64 {
    if level == 0 {
        return (face << 61) | (1u64 << 60);
    }
    let shift = 30 - level;
    let i30 = i << shift;
    let j30 = j << shift;
    let mut bits = (face & 1) as usize;
    let mut pos: u64 = 0;
    let min_k = (30 - level) / 4;
    for k in (min_k..=7).rev() {
        let r = k * 4;
        bits += (((i30 >> r) & 0xF) as usize) << 6;
        bits += (((j30 >> r) & 0xF) as usize) << 2;
        let v = lookup[bits];
        pos |= u64::from(v >> 2) << (2 * r);
        bits = usize::from(v & 3);
    }
    let raw = (face << 61) | (pos << 1);
    let lsb = 1u64 << (60 - 2 * level);
    let mask = !(lsb.wrapping_sub(1) | lsb);
    (raw & mask) | lsb
}

#[derive(Clone, Copy)]
pub(crate) struct S2RowState {
    lat_deg: f64,
    exact: ExactLon,
    theta_col0: f64,
    dtheta: f64,
    cos_phi: f64,
    sin_phi: f64,
    step_cos: [f64; 16],
    step_sin: [f64; 16],
    lookup: &'static [u16; 1024],
    max_res: u8,
    n_side: f64,
    eps: f64,
}

impl S2RowState {
    #[cold]
    #[inline(never)]
    fn fallback(&self, col: usize) -> u64 {
        let lon_deg = self.exact.at(col);
        S2CellId::from(s2::latlng::LatLng::from_degrees(
            self.lat_deg,
            wrap_lon_180(lon_deg),
        ))
        .parent(u64::from(self.max_res))
        .0
    }
}

/// Static [`GridBackend`] implementation for Google S2 (`s2`).
///
/// A sample is indexed once at the leaf level; every coarser level is a bit mask away.
#[derive(Clone, Copy, Debug, Default)]
pub struct S2Backend;

impl GridBackend for S2Backend {
    type Point = S2CellId;
    type Indexer = ();

    #[inline]
    fn new_indexer(_resolutions: &[u8]) -> Self::Indexer {}

    #[inline(always)]
    fn make_point(lon: f64, lat: f64) -> Option<Self::Point> {
        if !lon.is_finite() || !(-90.0..=90.0).contains(&lat) {
            return None;
        }
        Some(S2CellId::from(s2::latlng::LatLng::from_degrees(
            lat,
            wrap_lon_180(lon),
        )))
    }

    #[inline(always)]
    fn index_all(
        _indexer: &mut Self::Indexer,
        point: Self::Point,
        resolutions: &[u8],
        out: &mut [u64],
    ) {
        for (cell, &res) in out.iter_mut().zip(resolutions) {
            *cell = point.parent(u64::from(res)).0;
        }
    }
}

impl RowBatchBackend for S2Backend {
    type RowState = S2RowState;
    const SUPPORTS_ROW_BATCH: bool = true;

    #[inline]
    fn prepare_row(
        lat_deg: f64,
        lon_col0_deg: f64,
        dlon_deg: f64,
        exact: ExactLon,
        max_res: u8,
    ) -> Option<Self::RowState> {
        if !lat_deg.is_finite()
            || !lon_col0_deg.is_finite()
            || !dlon_deg.is_finite()
            || lat_deg.abs() > 89.9999
            || max_res > 30
        {
            return None;
        }
        let phi = lat_deg.to_radians();
        let (sin_phi, cos_phi) = phi.sin_cos();
        let theta_col0 = lon_col0_deg.to_radians();
        let dtheta = dlon_deg.to_radians();
        let mut step_cos = [0.0f64; 16];
        let mut step_sin = [0.0f64; 16];
        for k in 0..16 {
            let (s, c) = (k as f64 * dtheta).sin_cos();
            step_cos[k] = c;
            step_sin[k] = s;
        }
        let n_side = (1u64 << max_res) as f64;
        let eps = (1e-9 * n_side).max(1e-7);
        Some(S2RowState {
            exact,
            lat_deg,
            theta_col0,
            dtheta,
            cos_phi,
            sin_phi,
            step_cos,
            step_sin,
            lookup: s2_lookup_pos(),
            max_res,
            n_side,
            eps,
        })
    }

    #[inline(always)]
    fn index_row_batch(state: &Self::RowState, start_col: usize, out: &mut [u64]) {
        let z = state.sin_phi;
        let az = z.abs();
        let mut off = 0usize;
        while off < out.len() {
            let len = (out.len() - off).min(16);
            let theta0 = state.theta_col0 + (start_col + off) as f64 * state.dtheta;
            let (base_sin, base_cos) = theta0.sin_cos();
            let bx = state.cos_phi * base_cos;
            let by = state.cos_phi * base_sin;
            let mut xs = [0.0f64; 16];
            let mut ys = [0.0f64; 16];
            for k in 0..len {
                xs[k] = bx * state.step_cos[k] - by * state.step_sin[k];
                ys[k] = by * state.step_cos[k] + bx * state.step_sin[k];
            }
            for k in 0..len {
                let x = xs[k];
                let y = ys[k];
                let ax = x.abs();
                let ay = y.abs();
                let (face, u, v, edge_gap) = if ax >= ay && ax >= az {
                    let gap = ax - ay.max(az);
                    if x >= 0.0 {
                        (0u64, y / x, z / x, gap)
                    } else {
                        (3u64, z / x, y / x, gap)
                    }
                } else if ay >= az {
                    let gap = ay - ax.max(az);
                    if y >= 0.0 {
                        (1u64, -x / y, z / y, gap)
                    } else {
                        (4u64, z / y, -x / y, gap)
                    }
                } else {
                    let gap = az - ax.max(ay);
                    if z >= 0.0 {
                        (2u64, -x / z, -y / z, gap)
                    } else {
                        (5u64, -y / z, -x / z, gap)
                    }
                };
                if edge_gap < 1e-9 {
                    out[off + k] = state.fallback(start_col + off + k);
                    continue;
                }
                let s = if u >= 0.0 {
                    0.5 * (1.0 + 3.0 * u).sqrt()
                } else {
                    1.0 - 0.5 * (1.0 - 3.0 * u).sqrt()
                };
                let t = if v >= 0.0 {
                    0.5 * (1.0 + 3.0 * v).sqrt()
                } else {
                    1.0 - 0.5 * (1.0 - 3.0 * v).sqrt()
                };
                let si = s * state.n_side;
                let ti = t * state.n_side;
                let sf = si - si.floor();
                let tf = ti - ti.floor();
                if si <= state.eps
                    || si >= state.n_side - state.eps
                    || ti <= state.eps
                    || ti >= state.n_side - state.eps
                    || sf < state.eps
                    || sf > 1.0 - state.eps
                    || tf < state.eps
                    || tf > 1.0 - state.eps
                {
                    out[off + k] = state.fallback(start_col + off + k);
                    continue;
                }
                out[off + k] = s2_face_ij_to_cell_at_level(
                    state.lookup,
                    face,
                    si as u32,
                    ti as u32,
                    state.max_res,
                );
            }
            off += len;
        }
    }

    #[inline(always)]
    fn parents_from_finest(finest_cell: u64, _max_res: u8, resolutions: &[u8], out: &mut [u64]) {
        for (cell, &res) in out.iter_mut().zip(resolutions) {
            let lsb = 1u64 << (60 - 2 * res);
            *cell = (finest_cell & !(lsb.wrapping_sub(1) | lsb)) | lsb;
        }
    }
}

#[derive(Clone, Copy)]
enum HealpixRowRegion {
    Equatorial { y: f64 },
    NorthPolar { sigma: f64 },
    SouthPolar { sigma: f64 },
}

#[derive(Clone, Copy)]
pub(crate) struct HealpixRowState {
    lat_deg: f64,
    exact: ExactLon,
    x_col0: f64,
    dx_step: f64,
    region: HealpixRowRegion,
    max_res: u8,
    nside: f64,
    shift_to_base: u32,
    nuniq_offset: u64,
    eps: f64,
}

impl HealpixRowState {
    #[cold]
    #[inline(never)]
    fn fallback(&self, col: usize) -> u64 {
        let lon_deg = self.exact.at(col);
        let deep = healpix::get(HEALPIX_MAX_DEPTH).hash([
            lon_deg.rem_euclid(360.0).to_radians(),
            self.lat_deg.to_radians(),
        ]);
        healpix_encode(
            self.max_res,
            deep >> (2 * (HEALPIX_MAX_DEPTH - self.max_res)),
        )
    }
}

/// SWAR Morton bit interleave of `(i, j)` (`0 <= i, j < 2^29`), placing `i` in
/// even bits (`0, 2, 4, ...`) and `j` in odd bits (`1, 3, 5, ...`).
#[inline(always)]
fn morton_interleave_ij(i: u64, j: u64) -> u64 {
    #[inline(always)]
    fn part1by1(mut x: u64) -> u64 {
        x &= 0x0000_0000_FFFF_FFFF;
        x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
        x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
        x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        x = (x | (x << 2)) & 0x3333_3333_3333_3333;
        (x | (x << 1)) & 0x5555_5555_5555_5555
    }
    part1by1(i) | (part1by1(j) << 1)
}

/// Static [`GridBackend`] implementation for HEALPix (`healpix`, nested scheme).
///
/// A sample is hashed once at the deepest depth; every coarser depth is a shift away.
#[derive(Clone, Copy, Debug, Default)]
pub struct HealpixBackend;

impl GridBackend for HealpixBackend {
    /// Nested pixel at the maximum HEALPix depth.
    type Point = u64;
    type Indexer = ();

    #[inline]
    fn new_indexer(_resolutions: &[u8]) -> Self::Indexer {}

    #[inline(always)]
    fn make_point(lon: f64, lat: f64) -> Option<Self::Point> {
        if !lon.is_finite() || !(-90.0..=90.0).contains(&lat) {
            return None;
        }
        Some(
            healpix::get(HEALPIX_MAX_DEPTH)
                .hash([lon.rem_euclid(360.0).to_radians(), lat.to_radians()]),
        )
    }

    #[inline(always)]
    fn index_all(
        _indexer: &mut Self::Indexer,
        point: Self::Point,
        resolutions: &[u8],
        out: &mut [u64],
    ) {
        for (cell, &res) in out.iter_mut().zip(resolutions) {
            *cell = healpix_encode(res, point >> (2 * (HEALPIX_MAX_DEPTH - res)));
        }
    }
}

impl RowBatchBackend for HealpixBackend {
    type RowState = HealpixRowState;
    const SUPPORTS_ROW_BATCH: bool = true;

    #[inline]
    fn prepare_row(
        lat_deg: f64,
        lon_col0_deg: f64,
        dlon_deg: f64,
        exact: ExactLon,
        max_res: u8,
    ) -> Option<Self::RowState> {
        if !lat_deg.is_finite()
            || !lon_col0_deg.is_finite()
            || !dlon_deg.is_finite()
            || lat_deg.abs() > 89.9999
            || max_res > HEALPIX_MAX_DEPTH
        {
            return None;
        }
        let lat_rad = lat_deg.to_radians();
        let z = lat_rad.sin();
        let abs_z = z.abs();
        if (abs_z - 2.0 / 3.0).abs() < 1e-6 {
            return None;
        }
        let region = if abs_z <= 2.0 / 3.0 {
            HealpixRowRegion::Equatorial { y: 0.75 * z }
        } else {
            let cos_lat = lat_rad.cos();
            let one_minus_abs_z = (cos_lat * cos_lat) / (1.0 + abs_z);
            let sigma = (3.0 * one_minus_abs_z).sqrt();
            if z > 0.0 {
                HealpixRowRegion::NorthPolar { sigma }
            } else {
                HealpixRowRegion::SouthPolar { sigma }
            }
        };
        let nside = (1u64 << max_res) as f64;
        Some(HealpixRowState {
            exact,
            lat_deg,
            x_col0: lon_col0_deg / 90.0,
            dx_step: dlon_deg / 90.0,
            region,
            max_res,
            nside,
            shift_to_base: 2 * u32::from(max_res),
            nuniq_offset: 4u64 << (2 * max_res),
            eps: (1e-9 * nside).max(1e-7),
        })
    }

    #[inline(always)]
    fn index_row_batch(state: &Self::RowState, start_col: usize, out: &mut [u64]) {
        for (k, slot) in out.iter_mut().enumerate() {
            let col = start_col + k;
            let x_raw = state.x_col0 + col as f64 * state.dx_step;
            let x = x_raw - 4.0 * (x_raw * 0.25).floor();
            let q_f = x.floor();
            let dx = x - q_f;
            if !(1e-9..=1.0 - 1e-9).contains(&dx) {
                *slot = state.fallback(col);
                continue;
            }
            let q = (q_f as u64) & 3;
            let (d0h, u_ne, v_nw) = match state.region {
                HealpixRowRegion::Equatorial { y } => {
                    let p1 = dx + y;
                    let p2 = dx - y;
                    if (p1 - 0.5).abs() < 1e-9 || (p2 - 0.5).abs() < 1e-9 {
                        *slot = state.fallback(col);
                        continue;
                    }
                    if p1 >= 0.5 && p2 <= 0.5 {
                        (q, p1 - 0.5, 0.5 - p2)
                    } else if p1 < 0.5 && p2 > 0.5 {
                        (8 + q, p1 + 0.5, 1.5 - p2)
                    } else if p1 < 0.5 && p2 <= 0.5 {
                        (4 + q, p1 + 0.5, 0.5 - p2)
                    } else {
                        (4 + ((q + 1) & 3), p1 - 0.5, 1.5 - p2)
                    }
                }
                HealpixRowRegion::NorthPolar { sigma } => {
                    (q, 1.0 - (1.0 - dx) * sigma, 1.0 - dx * sigma)
                }
                HealpixRowRegion::SouthPolar { sigma } => (8 + q, dx * sigma, (1.0 - dx) * sigma),
            };
            let us = u_ne * state.nside;
            let vs = v_nw * state.nside;
            let uf = us - us.floor();
            let vf = vs - vs.floor();
            if us <= state.eps
                || us >= state.nside - state.eps
                || vs <= state.eps
                || vs >= state.nside - state.eps
                || uf < state.eps
                || uf > 1.0 - state.eps
                || vf < state.eps
                || vf > 1.0 - state.eps
            {
                *slot = state.fallback(col);
                continue;
            }
            let interleaved = morton_interleave_ij(us as u64, vs as u64);
            *slot = state.nuniq_offset + ((d0h << state.shift_to_base) | interleaved);
        }
    }

    #[inline(always)]
    fn parents_from_finest(finest_cell: u64, max_res: u8, resolutions: &[u8], out: &mut [u64]) {
        let pixel = finest_cell - (4u64 << (2 * max_res));
        for (cell, &res) in out.iter_mut().zip(resolutions) {
            *cell = healpix_encode(res, pixel >> (2 * (max_res - res)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grid_kind_basics() {
        assert_eq!(GridKind::parse("h3"), Some(GridKind::H3));
        assert_eq!(GridKind::parse("A5"), Some(GridKind::A5));
        assert_eq!(GridKind::parse("s9"), None);
        assert_eq!(GridKind::H3.max_resolution(), 15);
        assert_eq!(GridKind::A5.max_resolution(), 30);
        assert_eq!(GridKind::H3.index_column_name(), "h3_index");
        assert_eq!(GridKind::A5.index_column_name(), "a5_index");
    }

    #[test]
    fn test_a5_validity_and_hierarchy() {
        let cell = GridKind::A5.cell_at(37.7749, -122.4194, 10).unwrap();
        assert!(GridKind::A5.is_valid(cell));
        assert!(!GridKind::A5.is_valid(0));
        assert_eq!(GridKind::A5.resolution(cell), Some(10));

        let (parent_res, parent) = GridKind::A5.immediate_parent(cell).unwrap();
        assert_eq!(parent_res, 9);
        assert!(GridKind::A5.is_valid(parent));
        assert_eq!(GridKind::A5.resolution(parent), Some(9));

        let mut children = Vec::new();
        GridKind::A5.children_into(parent, 10, &mut children);
        assert_eq!(children.len(), 4);
        assert!(children.contains(&cell));

        let (lat, lng) = GridKind::A5.center_lat_lng(cell).unwrap();
        assert!((lat - 37.7749).abs() < 0.1);
        assert!((lng - (-122.4194)).abs() < 0.1);

        let mut boundary = [(0.0, 0.0); 10];
        let n = GridKind::A5.boundary_lng_lat(cell, &mut boundary);
        assert_eq!(n, 5);
    }

    #[test]
    fn a5_south_lat_bound_covers_curved_edges() {
        use crate::aggregator::horizon_streamer::compute_cell_south_lat_for;
        for res in 2u8..=5 {
            let mut lat = -88.0;
            while lat < 89.0 {
                let mut lon = -179.0;
                while lon < 180.0 {
                    let cell = GridKind::A5.cell_at(lat, lon, res).unwrap();
                    let dense = a5::cell_to_boundary(
                        cell,
                        Some(a5::CellToBoundaryOptions {
                            closed_ring: false,
                            segments: Some(32),
                        }),
                    )
                    .unwrap();
                    let true_south = dense
                        .iter()
                        .map(|p| p.latitude())
                        .fold(f64::INFINITY, f64::min);
                    let bound = compute_cell_south_lat_for(GridKind::A5, cell);
                    assert!(bound <= true_south, "res {res} cell {cell:x}");
                    lon += 9.7;
                }
                lat += 4.3;
            }
        }
    }

    #[test]
    fn a5_pole_vertex_is_spread_along_neighbour_meridians() {
        let mut buf = [0u8; crate::encoding::WKB_BUF_LEN];
        for lat in [89.999, -89.999] {
            for res in [1u8, 2, 5] {
                let cell = GridKind::A5.cell_at(lat, 100.0, res).unwrap();
                let mut verts = [(0.0, 0.0); 10];
                let n = GridKind::A5.boundary_lng_lat(cell, &mut verts);
                let verts = &verts[..n];
                assert_eq!(verts.iter().filter(|v| v.1.abs() == 90.0).count(), 2);
                let (w, e) = verts
                    .iter()
                    .fold((f64::MAX, f64::MIN), |(w, e), v| (w.min(v.0), e.max(v.0)));
                assert!(w <= 100.0 && 100.0 <= e && e - w < 180.0, "{verts:?}");
                // One plain polygon, not an antimeridian split.
                let len = crate::encoding::a5_index_to_wkb(cell, &mut buf).unwrap();
                assert_eq!(buf[1..5], 3u32.to_le_bytes());
                assert_eq!(len, 9 + 4 + 16 * (n + 1));
            }
        }
    }

    const NESTED: [GridKind; 2] = [GridKind::S2, GridKind::Healpix];

    /// Sample points spread over the globe, poles and antimeridian included.
    fn sample_points() -> Vec<(f64, f64)> {
        let mut points = vec![(90.0, 0.0), (-90.0, 0.0), (0.0, 180.0), (0.0, -180.0)];
        let mut lat = -89.5;
        while lat < 90.0 {
            let mut lon = -179.5;
            while lon < 180.0 {
                points.push((lat, lon));
                lon += 13.7;
            }
            lat += 6.1;
        }
        points
    }

    #[test]
    fn nested_grids_parse_and_name_their_columns() {
        assert_eq!(GridKind::parse("S2"), Some(GridKind::S2));
        assert_eq!(GridKind::parse("healpix"), Some(GridKind::Healpix));
        assert_eq!(GridKind::S2.max_resolution(), 30);
        assert_eq!(GridKind::Healpix.max_resolution(), 29);
        assert_eq!(GridKind::S2.index_column_name(), "s2_index");
        assert_eq!(GridKind::Healpix.hex_column_name(), "healpix_hex");
        assert_eq!(
            GridKind::from_cell_column_name("HEALPIX_INDEX"),
            Some(GridKind::Healpix)
        );
        assert_eq!(GridKind::from_cell_column_name("mean"), None);
        assert_eq!(
            serde_json::to_string(&GridKind::Healpix).unwrap(),
            "\"healpix\""
        );
    }

    #[test]
    fn healpix_ids_are_nuniq() {
        // 4 * nside^2 + pixel.
        assert_eq!(healpix_encode(0, 0), 4);
        assert_eq!(healpix_encode(0, 11), 15);
        assert_eq!(healpix_encode(1, 0), 16);
        assert_eq!(healpix_encode(29, 0), 4 << 58);
        for (depth, pixel) in [(0u8, 0u64), (0, 11), (3, 767), (29, 12 * (1 << 58) - 1)] {
            assert_eq!(
                healpix_decode(healpix_encode(depth, pixel)),
                Some((depth, pixel))
            );
        }
        for bad in [0, 3, 1 << 62, u64::MAX] {
            assert_eq!(healpix_decode(bad), None);
            assert!(!GridKind::Healpix.is_valid(bad));
        }
        // Matches the healpix crate's own IVOA uniq encoding.
        assert_eq!(
            healpix::checked::to_uniq_ivoa(7, 1234),
            healpix_encode(7, 1234)
        );
    }

    #[test]
    fn s2_ids_match_the_reference_library() {
        let ll = s2::latlng::LatLng::from_degrees(37.7749, -122.4194);
        for level in [0u8, 1, 12, 30] {
            let expected = S2CellId::from(ll).parent(u64::from(level)).0;
            assert_eq!(
                GridKind::S2.cell_at(37.7749, -122.4194, level),
                Some(expected)
            );
        }
        assert!(!GridKind::S2.is_valid(0));
        assert!(!GridKind::S2.is_valid(u64::MAX));
        assert_eq!(GridKind::S2.cell_at(0.0, 0.0, 31), None);
    }

    #[test]
    fn nested_grid_hierarchy_round_trips() {
        for grid in NESTED {
            for (lat, lon) in sample_points() {
                for res in [0u8, 1, 5, 12, grid.max_resolution()] {
                    let cell = grid.cell_at(lat, lon, res).unwrap();
                    assert!(grid.is_valid(cell), "{grid} {lat} {lon} {res}");
                    assert_eq!(grid.resolution(cell), Some(res));
                    if res == 0 {
                        assert_eq!(grid.immediate_parent(cell), None);
                        continue;
                    }
                    let (parent_res, parent) = grid.immediate_parent(cell).unwrap();
                    assert_eq!(parent_res, res - 1);
                    assert_eq!(grid.cell_at(lat, lon, res - 1), Some(parent));
                    let mut children = Vec::new();
                    grid.children_into(parent, res, &mut children);
                    assert_eq!(
                        children.len(),
                        grid.expected_immediate_children(parent, parent_res)
                    );
                    assert!(children.contains(&cell));
                    assert_eq!(grid.parent(cell, res + 1), None);
                }
            }
        }
    }

    #[test]
    fn nested_grid_centres_index_back_to_their_cell() {
        for grid in NESTED {
            for (lat, lon) in sample_points() {
                for res in [0u8, 3, 9, 20] {
                    let cell = grid.cell_at(lat, lon, res).unwrap();
                    let (c_lat, c_lng) = grid.center_lat_lng(cell).unwrap();
                    assert!((-180.0..=180.0).contains(&c_lng));
                    assert_eq!(grid.cell_at(c_lat, c_lng, res), Some(cell), "{grid} {res}");
                }
            }
        }
    }

    #[test]
    fn nested_grid_south_bound_and_radius_cover_the_cell() {
        use crate::aggregator::horizon_streamer::compute_cell_south_lat_for;
        use crate::pmtiles::max_cell_radius_deg;
        for grid in NESTED {
            for res in 0u8..=6 {
                let radius = max_cell_radius_deg(grid, res);
                for (lat, lon) in sample_points() {
                    let cell = grid.cell_at(lat, lon, res).unwrap();
                    let south = compute_cell_south_lat_for(grid, cell);
                    assert!(south <= lat, "{grid} res {res}: {south} > {lat}");
                    let (c_lat, _) = grid.center_lat_lng(cell).unwrap();
                    assert!((lat - c_lat).abs() <= radius, "{grid} res {res}");
                    let mut verts = [(0.0, 0.0); 10];
                    let n = grid.boundary_lng_lat(cell, &mut verts);
                    assert!((4..=5).contains(&n));
                    for &(v_lng, v_lat) in &verts[..n] {
                        assert!((-180.0..=180.0).contains(&v_lng));
                        assert!(south <= v_lat && (v_lat - c_lat).abs() <= radius);
                    }
                }
            }
        }
    }

    #[test]
    fn nested_grid_pole_cells_are_plain_polygons() {
        let mut buf = [0u8; crate::encoding::WKB_BUF_LEN];
        for grid in NESTED {
            for lat in [89.9999, -89.9999] {
                for res in [1u8, 4, 9] {
                    let cell = grid.cell_at(lat, 100.0, res).unwrap();
                    let mut verts = [(0.0, 0.0); 10];
                    let n = grid.boundary_lng_lat(cell, &mut verts);
                    assert_eq!(n, 5, "{grid} res {res}");
                    assert_eq!(verts[..n].iter().filter(|v| v.1.abs() == 90.0).count(), 2);
                    let (w, e) = verts[..n]
                        .iter()
                        .fold((f64::MAX, f64::MIN), |(w, e), v| (w.min(v.0), e.max(v.0)));
                    assert!(
                        w <= 100.0 && 100.0 <= e && e - w < 180.0,
                        "{grid} {verts:?}"
                    );
                    crate::encoding::cell_index_to_wkb(grid, cell, &mut buf).unwrap();
                    assert_eq!(buf[1..5], 3u32.to_le_bytes());
                }
            }
        }
        // An S2 face holds the pole in its interior; HEALPix base cells meet at it.
        let north_face = GridKind::S2.cell_at(90.0, 0.0, 0).unwrap();
        assert_eq!(GridKind::S2.interior_pole(north_face), Some(true));
        assert!(GridKind::S2.is_south_pole_cell(GridKind::S2.cell_at(-90.0, 0.0, 0).unwrap()));
        let base = GridKind::Healpix.cell_at(90.0, 0.0, 0).unwrap();
        assert_eq!(GridKind::Healpix.interior_pole(base), None);
    }

    #[test]
    fn nested_backends_index_every_resolution_like_cell_at() {
        let resolutions = [0u8, 4, 11, 29];
        let mut cells = [0u64; 4];
        for (lat, lon) in sample_points() {
            let p = S2Backend::make_point(lon, lat).unwrap();
            S2Backend::index_all(&mut (), p, &resolutions, &mut cells);
            for (cell, res) in cells.iter().zip(resolutions) {
                assert_eq!(GridKind::S2.resolution(*cell), Some(res));
                assert_eq!(GridKind::S2.parent(cells[3], res), Some(*cell));
            }
            let p = HealpixBackend::make_point(lon, lat).unwrap();
            HealpixBackend::index_all(&mut (), p, &resolutions, &mut cells);
            for (cell, res) in cells.iter().zip(resolutions) {
                let direct =
                    healpix::get(res).hash([lon.rem_euclid(360.0).to_radians(), lat.to_radians()]);
                assert_eq!(
                    *cell,
                    healpix_encode(res, direct),
                    "{lat} {lon} depth {res}"
                );
            }
        }
        assert!(S2Backend::make_point(f64::NAN, 0.0).is_none());
        assert!(HealpixBackend::make_point(0.0, 91.0).is_none());
    }

    #[test]
    fn spec_names_and_ranges_agree_with_the_grid() {
        assert_eq!(GridSpec::all().len(), GridKind::ALL.len());
        for (grid, spec) in GridKind::ALL.into_iter().zip(GridSpec::all()) {
            assert_eq!(spec.kind, grid);
            assert_eq!(grid.spec(), spec);
            assert_eq!(GridKind::parse(spec.name), Some(grid));
            assert_eq!(GridKind::from_index(grid as u8), grid);
            assert_eq!(spec.index_column, format!("{}_index", spec.name));
            assert_eq!(spec.hex_column, format!("{}_hex", spec.name));
            assert_eq!(
                GridKind::from_cell_column_name(spec.index_column),
                Some(grid)
            );
            assert!(usize::from(spec.max_resolution) < MAX_GRID_RESOLUTIONS);
            assert!(spec.has_resolution(spec.max_resolution));
            assert!(!spec.has_resolution(spec.max_resolution + 1));
            // The range is exactly what indexing accepts.
            let finest = grid.cell_at(10.0, 20.0, spec.max_resolution).unwrap();
            assert!(grid.is_valid(finest));
            assert_eq!(grid.cell_at(10.0, 20.0, spec.max_resolution + 1), None);
            // Areas shrink with resolution and cover the globe at resolution 0.
            let covered = spec.area_m2(0) * f64::from(spec.base_cell_count);
            // H3's table is the average hexagon; its pentagons are smaller.
            let tolerance = if spec.equal_area { 1e-6 } else { 0.05 };
            assert!(
                (covered / EARTH_AREA_M2 - 1.0).abs() < tolerance,
                "{grid}: {covered}"
            );
            for res in 1..=spec.max_resolution {
                assert!(spec.area_m2(res) < spec.area_m2(res - 1), "{grid} {res}");
            }
        }
    }

    #[test]
    fn spec_base_cells_and_aperture_match_the_hierarchy() {
        for grid in GridKind::ALL {
            let spec = grid.spec();
            let mut base = Vec::new();
            grid.base_cells_into(&mut base);
            assert_eq!(base.len(), usize::from(spec.base_cell_count), "{grid}");
            let mut unique = base.clone();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(unique.len(), base.len());
            for &cell in &base {
                assert_eq!(grid.resolution(cell), Some(0), "{grid}");
            }
            // Every point's resolution-0 cell is a base cell.
            for (lat, lon) in sample_points() {
                assert!(base.contains(&grid.cell_at(lat, lon, 0).unwrap()), "{grid}");
            }
            // An ordinary cell has `aperture` children.
            let parent = grid.cell_at(37.7749, -122.4194, 5).unwrap();
            let mut children = Vec::new();
            grid.children_into(parent, 6, &mut children);
            assert_eq!(children.len(), usize::from(spec.aperture), "{grid}");
            assert_eq!(grid.expected_immediate_children(parent, 5), children.len());
        }
    }

    #[test]
    fn spec_sign_bit_flag_matches_the_ids() {
        for grid in GridKind::ALL {
            let spec = grid.spec();
            let mut seen_top_bit = false;
            for (lat, lon) in sample_points() {
                for res in [0, 3, spec.max_resolution - 1] {
                    seen_top_bit |= grid.cell_at(lat, lon, res).unwrap() >> 63 == 1;
                }
            }
            assert_eq!(seen_top_bit, spec.ids_use_sign_bit, "{grid}");
            assert_eq!(spec.needs_unsigned_ids(), spec.ids_use_sign_bit);
            // Ids that use the sign bit are always written unsigned.
            assert!(spec.parquet_index_unsigned || !spec.ids_use_sign_bit);
        }
    }

    #[test]
    fn spec_pole_model_matches_the_cells() {
        for grid in GridKind::ALL {
            let spec = grid.spec();
            for res in [0u8, 1, 4] {
                let north = grid.cell_at(90.0, 0.0, res).unwrap();
                let interior = grid.interior_pole(north) == Some(true);
                let expected = match spec.poles {
                    PoleModel::InteriorAtEveryResolution => true,
                    PoleModel::InteriorAtBaseOnly => res == 0,
                    PoleModel::VertexOnly => false,
                };
                assert_eq!(interior, expected, "{grid} res {res}");
                let mut verts = [(0.0, 0.0); 10];
                let n = grid.boundary_lng_lat(north, &mut verts);
                let on_pole = verts[..n].iter().any(|v| v.1 >= 90.0 - 1e-9);
                assert_eq!(on_pole, !interior, "{grid} res {res}");
                assert!(!on_pole || spec.has_pole_vertices());
            }
        }
    }

    #[test]
    fn spec_edge_model_bounds_the_true_southern_extent() {
        use crate::aggregator::horizon_streamer::compute_cell_south_lat_for;
        for grid in GridKind::ALL {
            let spec = grid.spec();
            assert_eq!(grid.has_geodesic_edges(), spec.edges == EdgeModel::Geodesic);
            assert!(!spec.edge_dips_south(10.0, 20.0) || spec.edges == EdgeModel::Curved);
            assert_eq!(
                spec.edge_dips_south(-10.0, 20.0),
                spec.edges != EdgeModel::IsoLatitudeDiamond
            );
            assert_eq!(
                spec.edge_bulges_north(10.0, -20.0),
                spec.edges != EdgeModel::IsoLatitudeDiamond
            );
            // Points inside a cell are never south of its bound, nor outside its box.
            for res in [1u8, 3, 6] {
                for (lat, lon) in sample_points() {
                    let cell = grid.cell_at(lat, lon, res).unwrap();
                    let south = compute_cell_south_lat_for(grid, cell);
                    assert!(
                        south <= lat,
                        "{grid} {res} ({lat}, {lon}) cell {cell:x}: {south}"
                    );
                    let [w, s, e, n] = grid.wgs84_bbox(cell).unwrap();
                    assert!(s <= lat && lat <= n, "{grid} {res}: {lat} not in {s}..{n}");
                    let inside = if w <= e {
                        w <= lon && lon <= e
                    } else {
                        lon >= w || lon <= e
                    };
                    assert!(inside, "{grid} {res}: lon {lon} not in {w}..{e}");
                    let (c_lat, _) = grid.center_lat_lng(cell).unwrap();
                    assert!((lat - c_lat).abs() <= spec.max_cell_radius_deg(res));
                }
            }
            assert_eq!(grid.wgs84_bbox(0), None);
        }
    }

    #[test]
    fn spec_radius_and_zoom_models() {
        use crate::pmtiles::pyramid::{
            grid_res_for_zoom, grid_res_to_zoom, max_cell_radius_deg, zooms_for_grid_res,
        };
        for grid in GridKind::ALL {
            let spec = grid.spec();
            for res in 0..=spec.max_resolution {
                assert_eq!(
                    max_cell_radius_deg(grid, res),
                    spec.max_cell_radius_deg(res)
                );
                if res > 0 {
                    assert!(spec.max_cell_radius_deg(res) < spec.max_cell_radius_deg(res - 1));
                    assert!(grid_res_to_zoom(grid, res) >= grid_res_to_zoom(grid, res - 1));
                }
                let zooms = zooms_for_grid_res(grid, res, res);
                assert_eq!(
                    zooms.first(),
                    Some(&0),
                    "the coarsest resolution fills from zoom 0"
                );
                assert!(zooms.contains(&grid_res_to_zoom(grid, res)), "{grid} {res}");
            }
            match spec.zoom {
                ZoomModel::OnePerLevel { max_zoom } => {
                    assert_eq!(grid_res_to_zoom(grid, 7), 7);
                    assert_eq!(grid_res_to_zoom(grid, spec.max_resolution), max_zoom);
                    assert_eq!(grid_res_for_zoom(grid, 40), spec.max_resolution);
                }
                ZoomModel::Aperture7Table => {
                    assert_eq!(spec.aperture, 7);
                    assert!(grid_res_for_zoom(grid, 40) <= spec.max_resolution);
                }
            }
        }
        // The H3 table is the published one, entry for entry.
        assert_eq!(GridKind::H3.spec().max_cell_radius_deg(0), 13.5);
        assert_eq!(GridKind::H3.spec().max_cell_radius_deg(15), 0.00001);
        assert_eq!(crate::pmtiles::pyramid::max_hex_radius_deg(200), 0.00001);
    }

    #[test]
    fn spec_capabilities_drive_the_pipeline() {
        use crate::scalar_ops::{grid_polygon_to_cells_wkb_into, ContainmentMode};
        // A small square around San Francisco fills on every grid.
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
        for grid in GridKind::ALL {
            let spec = grid.spec();
            let res = if spec.aperture == 7 { 8 } else { 13 };
            let mut cells = Vec::new();
            grid_polygon_to_cells_wkb_into(
                grid,
                &wkb,
                res,
                ContainmentMode::ContainsCentroid,
                &mut cells,
            )
            .unwrap();
            assert!(!cells.is_empty(), "{grid} ({:?})", spec.polygon_fill);
            assert!(cells.contains(&grid.cell_at(37.775, -122.425, res as u8).unwrap()));
            // Equal-area grids report one area for every cell of a resolution.
            if spec.equal_area {
                let total = spec.area_m2(res as u8) * cells.len() as f64;
                let polygon_m2 = 0.05f64.to_radians()
                    * (37.80f64.to_radians().sin() - 37.75f64.to_radians().sin())
                    * 6_371_007.18f64.powi(2);
                assert!(
                    (total / polygon_m2 - 1.0).abs() < 0.1,
                    "{grid}: {total} vs {polygon_m2}"
                );
            }
        }
        assert!(GridKind::H3.spec().row_space_eviction);
        assert!(
            GridSpec::all()
                .iter()
                .filter(|s| s.row_space_eviction)
                .count()
                == 1
        );
    }

    #[test]
    fn a5_pole_points_do_not_inherit_the_previous_cell() {
        // The `a5` crate's last-cell cache would hand a pole point the polar cell looked
        // up before it, even one at the other pole.
        for res in [1u8, 3, 9, 20] {
            for (first, second) in [(90.0, -90.0), (-90.0, 90.0), (89.99999, -89.9999999999)] {
                for lon in [0.0, 100.0] {
                    let a = GridKind::A5.cell_at(first, lon, res).unwrap();
                    let b = GridKind::A5.cell_at(second, lon, res).unwrap();
                    let (a_lat, _) = GridKind::A5.center_lat_lng(a).unwrap();
                    let (b_lat, _) = GridKind::A5.center_lat_lng(b).unwrap();
                    assert!(a_lat * first > 0.0, "res {res}: {first} -> {a_lat}");
                    assert!(b_lat * second > 0.0, "res {res}: {second} -> {b_lat}");
                    // The scan backend agrees with `cell_at`, whatever came before.
                    let mut cells = [0u64; 2];
                    for (lat, want) in [(first, a), (second, b), (first, a)] {
                        let p = A5Backend::make_point(lon, lat).unwrap();
                        A5Backend::index_all(&mut (), p, &[res, res], &mut cells);
                        assert_eq!(cells, [want, want], "res {res} lat {lat}");
                    }
                }
            }
        }
    }

    #[test]
    fn batched_rows_match_scalar_cells_for_centres_on_cell_boundaries() {
        // The walker steps from the first centre; column 97 of this row is exactly
        // longitude 0 in the scalar formula (an S2 and HEALPix boundary) but about
        // -7e-15 when stepped. The boundary guard must recompute the exact longitude.
        let (c0, a, dx) = (-32.5, 1.0 / 3.0, 0.5);
        for col_offset in [0usize, 7] {
            let exact = ExactLon {
                c0,
                a,
                col_offset,
                dx,
                mercator: false,
            };
            let lon_col0 = c0 + (col_offset as f64 + dx) * a;
            let mut batch = [0u64; 64];
            for lat in [10.0, -10.0, 41.8103148957786] {
                for max_res in [1u8, 2, 5, 12] {
                    let s2 = S2Backend::prepare_row(lat, lon_col0, a, exact, max_res)
                        .expect("S2 row state");
                    let healpix = HealpixBackend::prepare_row(lat, lon_col0, a, exact, max_res);
                    for start in [0usize, 64, 128] {
                        S2Backend::index_row_batch(&s2, start, &mut batch);
                        for (k, &got) in batch.iter().enumerate() {
                            let lon = exact.at(start + k);
                            let want = GridKind::S2.cell_at(lat, lon, max_res).unwrap();
                            assert_eq!(got, want, "S2 res={max_res} lat={lat} lon={lon}");
                        }
                        if let Some(state) = &healpix {
                            HealpixBackend::index_row_batch(state, start, &mut batch);
                            for (k, &got) in batch.iter().enumerate() {
                                let lon = exact.at(start + k);
                                let want = GridKind::Healpix.cell_at(lat, lon, max_res).unwrap();
                                assert_eq!(got, want, "HEALPix res={max_res} lat={lat} lon={lon}");
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn healpix_row_batch_matches_reference_bit_for_bit() {
        let lats = [
            -90.0,
            -89.99995,
            -89.5,
            -75.125,
            -60.0,
            -45.0,
            -41.8103148957786, // south transition -2/3
            -41.7,
            -30.5,
            -12.345,
            0.0,
            12.345,
            30.5,
            41.7,
            41.8103148957786, // north transition +2/3
            45.0,
            60.0,
            75.125,
            89.5,
            89.99995,
            90.0,
        ];
        let lon_starts = [-540.0, -180.0, -90.0, -45.3, 0.0, 89.99, 179.95, 270.0];
        let dlons = [0.0002777777777777778, 0.75, 15.0, -0.25];
        let depths = [0u8, 1, 5, 12, 15, 20, 29];
        let mut batch = [0u64; 64];

        for &max_res in &depths {
            let parent_res: Vec<u8> = depths.iter().copied().filter(|&d| d <= max_res).collect();
            let mut parents_fast = vec![0u64; parent_res.len()];
            let mut parents_ref = vec![0u64; parent_res.len()];
            for &lat in &lats {
                for &lon0 in &lon_starts {
                    for &dlon in &dlons {
                        let prepared = HealpixBackend::prepare_row(
                            lat,
                            lon0,
                            dlon,
                            ExactLon::linear(lon0, dlon),
                            max_res,
                        );
                        if lat.abs() > 89.9999
                            || ((lat.to_radians().sin().abs() - 2.0 / 3.0).abs() < 1e-6)
                        {
                            assert!(prepared.is_none(), "expected fallback near lat {lat}");
                            continue;
                        }
                        let state = prepared.expect("valid healpix row state");
                        for start_col in [0usize, 13, 64] {
                            HealpixBackend::index_row_batch(&state, start_col, &mut batch);
                            for (k, &got) in batch.iter().enumerate() {
                                let col = start_col + k;
                                let lon = lon0 + col as f64 * dlon;
                                let pt = HealpixBackend::make_point(lon, lat).unwrap();
                                HealpixBackend::index_all(
                                    &mut (),
                                    pt,
                                    &parent_res,
                                    &mut parents_ref,
                                );
                                let expected_finest = *parents_ref.last().unwrap();
                                assert_eq!(
                                    got, expected_finest,
                                    "healpix mismatch res={max_res} lat={lat} lon={lon} col={col}"
                                );
                                HealpixBackend::parents_from_finest(
                                    got,
                                    max_res,
                                    &parent_res,
                                    &mut parents_fast,
                                );
                                assert_eq!(
                                    parents_fast, parents_ref,
                                    "healpix parents mismatch max_res={max_res} lat={lat} lon={lon}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn s2_row_batch_matches_reference_bit_for_bit() {
        let lats = [
            -90.0,
            -89.99995,
            -89.5,
            -60.0,
            -45.0,
            -35.264389682754654, // cube corner latitude
            -12.345,
            0.0,
            12.345,
            35.264389682754654,
            45.0,
            60.0,
            89.5,
            89.99995,
            90.0,
        ];
        let lon_starts = [
            -540.0, -180.0, -135.0, -45.0, 0.0, 44.95, 134.95, 179.95, 270.0,
        ];
        let dlons = [0.0002777777777777778, 0.75, 15.0, -0.25];
        let levels = [0u8, 1, 2, 10, 14, 16, 20, 30];
        let mut batch = [0u64; 64];

        for &max_res in &levels {
            let parent_res: Vec<u8> = levels.iter().copied().filter(|&l| l <= max_res).collect();
            let mut parents_fast = vec![0u64; parent_res.len()];
            let mut parents_ref = vec![0u64; parent_res.len()];
            for &lat in &lats {
                for &lon0 in &lon_starts {
                    for &dlon in &dlons {
                        let prepared = S2Backend::prepare_row(
                            lat,
                            lon0,
                            dlon,
                            ExactLon::linear(lon0, dlon),
                            max_res,
                        );
                        if lat.abs() > 89.9999 {
                            assert!(prepared.is_none(), "expected fallback near lat {lat}");
                            continue;
                        }
                        let state = prepared.expect("valid s2 row state");
                        for start_col in [0usize, 13, 64] {
                            S2Backend::index_row_batch(&state, start_col, &mut batch);
                            for (k, &got) in batch.iter().enumerate() {
                                let col = start_col + k;
                                let lon = lon0 + col as f64 * dlon;
                                let pt = S2Backend::make_point(lon, lat).unwrap();
                                S2Backend::index_all(&mut (), pt, &parent_res, &mut parents_ref);
                                let expected_finest = *parents_ref.last().unwrap();
                                assert_eq!(
                                    got, expected_finest,
                                    "s2 mismatch res={max_res} lat={lat} lon={lon} col={col}"
                                );
                                S2Backend::parents_from_finest(
                                    got,
                                    max_res,
                                    &parent_res,
                                    &mut parents_fast,
                                );
                                assert_eq!(
                                    parents_fast, parents_ref,
                                    "s2 parents mismatch max_res={max_res} lat={lat} lon={lon}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
