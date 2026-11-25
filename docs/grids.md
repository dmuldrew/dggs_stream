# Grids: H3, A5, S2 and HEALPix

[← Back to README](../README.md)

`dggs_stream` indexes raster pixels onto four discrete global grids. They all go through the
same pipeline, but their cells, resolutions and cell ids differ. Every engine supports
every grid.

| Grid | Cells | Resolutions | Base cells | Aperture | Equal area | Cell id |
| :--- | :--- | :--- | ---: | ---: | :---: | :--- |
| `h3` (default) | Uber H3 hexagons (12 pentagons per resolution) | 0–15 | 122 | 7 | no | H3 index |
| `a5` | A5 dodecahedral pentagons | 0–30 | 12 | 4 (from resolution 2) | yes | A5 cell id |
| `s2` | Google S2 cube-face quadrilaterals | levels 0–30 | 6 | 4 | no | S2 cell id |
| `healpix` | HEALPix quadrilaterals, nested scheme | depths 0–29 | 12 | 4 | yes | NUNIQ `4 * 4^depth + nested_pixel` |

The resolution scales don't line up across grids: "resolution 9" is a different cell size on
each one. To ask for the same cell size on any grid, use [`auto`](#automatic-resolution).

## A5 index version

A5 uses the 1.0 cell-ID system. IDs generated with A5 0.x can refer to different cells
under 1.0, even when they pass validation. Rebuild raster aggregates and polygon indexes
from their source data together before joining them. To keep existing aggregates, migrate
each stored 0.x ID explicitly with the upstream Rust `a5::migrate(old_id)` function and
regenerate any geometry or archive derived from the IDs. Apply migration only to known
0.x data, because the ID itself carries no version. This project does not automatically
migrate imported IDs.

## Choosing a grid in each engine

| Engine | How to pick the grid |
| :--- | :--- |
| DuckDB | Per-grid functions (`s2_raster_continuous_aggregate`, `healpix_lonlat_to_cell`, ...), or `grid := 's2'` on the `h3_` table functions |
| PostgreSQL | Per-grid functions: `{grid}_raster_continuous_aggregate`, `{grid}_raster_categorical_aggregate`, `{grid}_auto_resolution` and the scalar set |
| Python / Polars | `grid=` on `scan_raster`, `read_raster`, `merge_partials`, the exporters and the scalar helpers (`lonlat_to_cell`, `cell_to_parent`, `polygon_to_cells`, ...); `GRIDS` maps each grid to its finest resolution |
| DataFusion / SedonaDB | `{grid}_raster_{continuous,categorical}_aggregate`, or a grid-name argument (`'s2'`) on any table function; `with_grid(GridKind::S2)` on the providers; per-grid scalar UDFs under DuckDB's names |
| CLI | `--grid h3\|a5\|s2\|healpix`; `-r` is checked against that grid's range |
| Rust (`dggs-stream-core`) | `MultiResolutionConfig::with_grid(GridKind::S2)`; `scalar_ops::grid_*` helpers |

The output always names its cell columns after the grid: `h3_index` / `h3_hex`, `a5_index` /
`a5_hex`, `s2_index` / `s2_hex`, `healpix_index` / `healpix_hex`.

Apache Spark's `dggs_stream.spark` data source still hard-codes `h3_index`.

## Cell ids and their types

Every cell id is a 64-bit unsigned integer. The `*_hex` columns and the `{grid}_to_string`
functions write it as lowercase hexadecimal without leading zeros; `string_to_{grid}` reads
that form back.

- **DuckDB, Arrow, Polars, DataFusion:** `UBIGINT` / `UInt64`.
- **Parquet exports:** A5, S2 and HEALPix index columns are annotated `UINT_64`. H3's stays a
  plain `INT64`, as in every earlier release; H3 ids never use the top bit.
- **PostgreSQL:** there is no unsigned 64-bit type, so ids are `bigint`, the signed reading of
  the same 64 bits. S2 ids on faces 4 and 5, and about half of A5's, are negative there. The
  conversion is lossless, and the hex text is always the unsigned id.

## `GridSpec`: one table of grid properties

Each grid's fixed properties live in one place, `GridKind::spec()` in
[`grid.rs`](../crates/dggs-stream-core/src/grid.rs). Pipeline stages and engine adapters read
the spec instead of matching on the grid:

| Field | Meaning | H3 | A5 | S2 | HEALPix |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `edges` | How edges run between vertices; decides how far south of its vertices a cell can reach | great-circle | curved | great-circle | iso-latitude diamond |
| `poles` | Where the poles sit | inside one cell at every resolution | inside a base cell; finer cells meet there at a vertex | inside a base cell; finer cells meet there at a vertex | always a vertex |
| `row_space_eviction` | Whether cells may be evicted by raster row on Albers rasters | yes | no | no | no |
| `zoom` | PMTiles zoom model | hand-tuned table (1–2 zooms per resolution) | zoom = resolution, at most 24 | zoom = resolution, at most 24 | zoom = resolution, at most 24 |
| `polygon_fill` | Polygon-fill algorithm | `h3o` tiler | `a5` crate | nested descent in lon/lat | nested descent in lon/lat |
| `ids_use_sign_bit` | Whether ids use bit 63 | no | yes | yes | no |

The spec also holds each grid's names, resolution range, base-cell count, aperture,
equal-area flag, the Parquet index annotation, and a bound on how far a cell reaches north or
south of its centre (`max_cell_radius_deg`). From that bound it derives
`max_cell_diameter_km`. Per-cell operations are methods of `GridKind`: `cell_at`,
`center_lat_lng`, `boundary_lng_lat`, `parent`, `children_into`, `wgs84_bbox`,
`base_cells_into` and `interior_pole`.

These bounds matter for correctness. The streaming kernel evicts a cell once the scan has
passed the cell's southernmost possible point. That bound comes from the edge model:
- Great-circle edges in the southern hemisphere and A5's curved edges are padded.
- A HEALPix cell's south vertex is already its southernmost point.
- Cells with a vertex on a pole are treated as reaching the pole.

## Automatic resolution

`auto` picks the resolution whose mean cell area is closest, in ratio, to the raster's ground
pixel area, measured at the raster's centre through its geotransform and CRS. `auto+N` and
`auto-N` move N resolution steps finer or coarser, clamped to the grid's range. Because it
targets a size rather than an index, `auto` gives comparable cells on every grid.

| Engine | Syntax |
| :--- | :--- |
| CLI | `-r auto`, `-r auto+1` |
| DuckDB | `resolutions := 'auto'` |
| Python | `resolutions="auto"`; `auto_resolution(path, grid=...)` returns the choice |
| DataFusion / SedonaDB | an `'auto'` resolution argument; `ResolutionSpec` in Rust |
| PostgreSQL | `{grid}_auto_resolution(path [, offset [, crs]])` |
| Rust | `dggs_stream_core::resolution` (`ResolutionSpec::parse`, `area_matched_resolution`) |

Pair `auto` with `transfer = "auto"` to have the mode follow the resolution: continuous scans
interpolate at `auto+1` and finer and overlay from `auto` (when the cells come out at least a
pixel in size) and coarser; categorical scans overlay at every offset. See
[Transfer Modes](transfer-modes.md#let-the-engine-choose-auto). The CLI reports the chosen
resolution and mode on stderr.

## Poles and the antimeridian

- **Pole cells.** A pole can sit inside a cell (`GridKind::interior_pole`). That cell's
  bounding box spans every longitude, and its geometry is written as a polar cap.
- **Pole vertices.** A vertex on a pole has no longitude of its own. `boundary_lng_lat` turns
  it into two vertices that run along the pole between the neighbouring vertices' meridians,
  so the lon/lat ring still covers the cell. Polygon fills close the ring of a pole-holding
  cell over the full turn before closing it over the pole.
- **A5 near the poles.** Within 0.001° of a pole, A5 points are indexed without the `a5`
  crate's last-cell shortcut. That shortcut could return a stale polar cell
  (`A5_POLE_GUARD_DEG`).
- **Antimeridian.** Bounding boxes of cells that cross ±180° unwrap their longitudes, and
  geometries are split into a `MultiPolygon` at ±180°.

## Polygon fill

`{grid}_polygon_to_cells(geometry, resolution [, containment])` (Python:
`polygon_to_cells(wkb, resolution, containment, grid=...)`) returns the cells of a `POLYGON` or
`MULTIPOLYGON`, sorted and without repeats. The containment modes are the same on every
grid:

| Mode (aliases) | Keeps a cell when |
| :--- | :--- |
| `contains_centroid` (`center`) | the cell's centre is inside the polygon |
| `contains_boundary` (`full`) | the whole cell is inside |
| `intersects_boundary` | the cell overlaps the polygon |
| `covers` (`overlapping`) | the cell overlaps the polygon, plus the enclosing cell of a polygon smaller than one cell |

- **H3** uses `h3o`'s tiler, with geodesic containment.
- **A5** uses the `a5` crate.
- **S2 and HEALPix** descend through the grid's 4-ary hierarchy from the base cells and test
  cells against the polygon in lon/lat degrees, the model their geometry is published in.

A request whose bounding box would hold more than `H3_POLYGON_TO_CELLS_MAX_CELLS` cells is
refused before any cell is generated (10,000,000).

## PMTiles zoom levels

Each grid maps its resolutions to Web Mercator zooms through the spec's zoom model:
- **H3** uses a hand-tuned table in which a resolution covers one or two zooms.
- **The aperture-4 grids** (A5, S2, HEALPix) put each resolution at the zoom of the same
  number, capped at 24.

Archives built from pre-indexed tables give each resolution present a band of zooms, so no
zoom between the coarsest and finest resolution is left empty. See
[PMTiles & Multi-Resolution](pmtiles.md).
