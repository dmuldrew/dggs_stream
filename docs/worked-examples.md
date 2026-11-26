# Worked Examples: One Engine, One Grid, One Question

[← Back to README](../README.md)

The README's [worked example](../README.md#raster-meets-table-a-worked-example) joins a land
cover raster to census tracts in DuckDB on H3. Every engine can also index pixels onto A5, S2
and HEALPix, and each grid is good at something different. The four examples below each pair a
real question with the engine and grid that suit it, and each joins a raster to vector or tabular
data, either on a shared cell id or on cell shapes. Each one also uses a different
[transfer mode](transfer-modes.md), the setting that decides how pixels become cells.

| Question | Engine | Grid | What the grid brings | Transfer mode |
| :--- | :--- | :--- | :--- | :--- |
| [Wetland loss inside vs. outside protected areas](#1-wetland-loss-inside-vs-outside-protected-areas--polars-on-a5) | Python & Polars | A5 | Equal-area cells, so cell fractions add up to true km² | Categorical `overlay`: exact class areas per cell |
| [Flooded area at parcels and roads](#2-flooded-area-at-parcels-and-roads--duckdb-on-h3) | DuckDB | H3 | Hexagons, the grid other datasets are most often indexed on | `auto`, which picks weighted `overlay` |
| [Snowpack per sub-basin, rolled up to the basin](#3-snowpack-per-sub-basin-rolled-up-to-the-basin--sedonadb-on-s2) | SedonaDB | S2 | Exact nesting: four children tile their parent | Weighted `overlay` at about one pixel per cell |
| [Land temperature by latitude band and by city](#4-land-temperature-by-latitude-band-and-by-city--cli-on-healpix) | CLI | HEALPix | Equal-area cells laid out in rings of constant latitude | Ground-area `overlay` with `auto` resolution; `interpolate` onto fine cells, `nearest` vs. `bicubic` |

Without a shared spatial index, each of these needs a GIS overlay: reprojecting rasters, clipping
polygons, rasterizing geometries and managing gigabytes of scratch files. With dggs_stream, the
raster becomes a table of cells and the rest is a join.

## 1. Wetland loss inside vs. outside protected areas — Polars on A5

**The question**: *How much wetland did coastal Louisiana lose between 2001 and 2021, and did
strictly protected land lose less?*

**Data**: two 30-meter NLCD land cover rasters (2001 and 2021) and the PAD-US protected areas
database.

**Why A5**: every A5 cell at a resolution has the same area. A cell straddling a protected-area
boundary goes wholly to one side, by its centre, and because every cell is the same size those
whole-cell assignments don't favour either side the way small and large H3 cells would (H3 cell
area varies by location). Per-cell results are comparable for the same reason: a map of wetland
lost per cell is a density map. The areas themselves come from pixel counts: NLCD is in an
equal-area projection (EPSG:5070), so every pixel covers exactly 900 m².

**Why categorical overlay**: with the default centre assignment, a pixel on a cell's edge counts
wholly for one cell, so each cell's wetland area is off by up to a ring of pixels. With
`transfer="overlay"`, a pixel 40% inside a cell adds 0.4 of a pixel to that cell's count, so
every cell's wetland area is exact. At about 140 pixels per cell, `transfer="auto"` would pick
overlay too.

```python
# pip install "./crates/dggs-stream-python[polars]" geopandas
import geopandas as gpd
import polars as pl
import dggs_stream
import shapely

RES = 14                                        # A5 cells of about 0.127 km² (~140 NLCD pixels)
CELL_KM2 = 510_065_622 / (60 * 4 ** (RES - 1))  # equal area: Earth's area / number of cells
PIXEL_KM2 = 900 / 1e6                           # every NLCD pixel (30 m x 30 m, EPSG:5070)
BBOX = (-93.9, 28.9, -88.8, 30.6)               # coastal Louisiana, lon/lat

# 1. Wetland area in every A5 cell, one lazy scan per year. With overlay, `count` is the
#    wetland area of the cell in pixels, fractions of edge pixels included, so times 900 m²
#    it is the area itself. It stays exact for cells cut by the raster or bbox edge (there,
#    `fraction` is a share of the area observed, not of the whole cell).
def wetland_area(path, year):
    return (
        dggs_stream.scan_raster(
            path, RES, grid="a5", categorical=True, format="long",
            transfer="overlay",               # split edge pixels between cells by area
            remap={90: 1, 95: 1, "else": 0},  # woody + emergent wetland -> 1, everything else -> 0
            bbox=BBOX, columns=["a5_index", "category", "count"],
        )
        .filter(pl.col("category") == 1)
        .select("a5_index", (pl.col("count") * PIXEL_KM2).alias(f"wet_{year}_km2"))
    )

wetland = (
    wetland_area("nlcd_2001_land_cover.tif", 2001)
    .join(wetland_area("nlcd_2021_land_cover.tif", 2021),
          on="a5_index", how="full", coalesce=True)
    .with_columns(pl.col("wet_2001_km2", "wet_2021_km2").fill_null(0.0))  # no wetland = no row
)

# 2. Polyfill strictly protected areas (GAP status 1 and 2) into the same A5 cells.
#    The default containment, 'contains_centroid', gives a cell to a polygon only if the cell's
#    centre is inside it; unique() drops cells shared by overlapping designations.
padus = gpd.read_file(
    "PADUS4_1Geodatabase.gdb", layer="PADUS4_1Fee",
    bbox=gpd.GeoSeries([shapely.box(*BBOX)], crs=4326),
).to_crs(4326)
padus = padus[padus["GAP_Sts"].isin(["1", "2"])]
protected = pl.DataFrame(
    {"a5_index": [c for g in padus.geometry
                  for c in dggs_stream.polygon_to_cells(g.wkb, RES, grid="a5") or []]},
    schema={"a5_index": pl.UInt64},
).unique().with_columns(protected=pl.lit(True))

# 3. Wetland area and loss, inside vs. outside protected land
result = (
    wetland.join(protected.lazy(), on="a5_index", how="left")
    .with_columns(pl.col("protected").fill_null(False))
    .group_by("protected")
    .agg(
        pl.col("wet_2001_km2").sum().alias("wetland_2001_km2"),
        pl.col("wet_2021_km2").sum().alias("wetland_2021_km2"),
    )
    .with_columns(
        lost_km2=pl.col("wetland_2001_km2") - pl.col("wetland_2021_km2"),
        lost_pct=(1 - pl.col("wetland_2021_km2") / pl.col("wetland_2001_km2")) * 100,
    )
    .collect()
)
```

In Polars, both rasters are `LazyFrame`s, read only when `.collect()` runs, so the two-year
comparison is just a join and a `group_by`. Drop the `remap` and `pivot` on `category` to get a
full from→to change matrix in km². `columns=` is pushed into the scan, and `bbox` limits how much
of the raster is read. For a map, `(wet_2001_km2 - wet_2021_km2) / CELL_KM2` is the share of each
cell's ground that stopped being wetland; it compares fairly across cells only because they all
have the same area. Pixel counts are areas here only because every NLCD pixel covers the same
900 m². On a continuous lon/lat raster, whose pixels shrink toward the poles,
`ground_area=True` weights by true m² instead ([example 4](#4-land-temperature-by-latitude-band-and-by-city--cli-on-healpix));
categorical scans are weighted by pixel. See the
[Python scan parameters](engines/python-polars.md#scan-parameters).

## 2. Flooded area at parcels and roads — DuckDB on H3

**The question**: *In a 100-year flood in Houston, which parcels take on more than 30 cm of water,
how much of their ground is under water, and how many kilometres of road become impassable?*

**Data**: a 100-year flood depth GeoTIFF (3 m pixels in a projected CRS, depth in metres, NoData
where the ground stays dry), a county parcel GeoParquet file, and Overture Maps road segments read
directly from S3.

**Why H3**: resolution-11 hexagons (about 2,150 m²) are roughly parcel-sized, and H3 is the grid
other datasets you might join later are most likely to be indexed on already.

**Why `transfer := 'auto'`**: a hexagon holds about 240 of these 3 m pixels, so `auto` picks
weighted `overlay`. Each hexagon then gets every pixel it overlaps, split at its edges by area:
`count` becomes the wet area in pixels, exact to a fraction of a pixel, and `max` sees a deep
pixel even when its centre lies in the next hexagon. Because dry ground is NoData, `count × 9 m²`
is the flooded area of each hexagon. Fed a 30 m flood model, the same call would still pick
`overlay` (about 2.4 pixels per hexagon), and at resolution 13, with cells finer than the
pixels, `interpolate`.

```sql
INSTALL spatial; LOAD spatial;
INSTALL httpfs;  LOAD httpfs;
SET s3_region = 'us-west-2';

-- 1. Flood depth into H3 resolution-11 hexagons. WHERE runs after aggregation,
--    so bbox is what keeps the scan to the city.
CREATE TABLE flood_h3 AS
SELECT h3_index, max AS max_depth_m, p90 AS p90_depth_m,
       count * 9 AS flooded_m2,         -- overlapped wet area in 3 m x 3 m pixels
       geom
FROM h3_raster_continuous_aggregate(
    'flood_depth_100yr.tif', resolution := 11,
    transfer := 'auto',                 -- ~240 pixels per hexagon: area-weighted overlay
    quantiles := 'p90',                 -- streaming 90th percentile per hexagon
    geom := true,                       -- native GEOMETRY column for the road join
    bbox := '-95.80,29.52,-95.01,30.11'
)
WHERE max >= 0.3;                       -- ~30 cm: enough to stall or float a car

-- 2. Parcels from GeoParquet. 'covers' keeps parcels smaller than a hexagon.
CREATE TABLE parcels_h3 AS
SELECT parcel_id, land_use, assessed_value,
       unnest(h3_polygon_to_cells(ST_AsWKB(geometry), 11, 'covers')) AS h3_index
FROM read_parquet('harris_county_parcels.parquet');

-- 3. Road segments straight from Overture's public S3 bucket. The test keeps every road whose
--    bbox overlaps the city's, including roads that cross its edge (step 4b counts only their
--    pieces inside flooded hexagons), and lets DuckDB skip row groups outside the city.
CREATE TABLE roads AS
SELECT id, class, geometry
FROM read_parquet(
    's3://overturemaps-us-west-2/release/<release>/theme=transportation/type=segment/*',
    hive_partitioning = true)
WHERE subtype = 'road'
  AND bbox.xmax > -95.80 AND bbox.xmin < -95.01
  AND bbox.ymax >  29.52 AND bbox.ymin <  30.11;

-- 4a. Flooded parcels: an integer hash join on the shared H3 index. A parcel can span several
--     hexagons and a hexagon several parcels, so depth is summarised per parcel, and flooded
--     ground per land use from distinct hexagons, so no hexagon counts twice in one land use.
WITH parcel_flood AS (
    SELECT p.parcel_id, p.land_use, p.assessed_value, f.h3_index, f.p90_depth_m, f.flooded_m2
    FROM parcels_h3 AS p
    JOIN flood_h3 AS f USING (h3_index)
),
parcel_depth AS (
    SELECT parcel_id,
           any_value(land_use)       AS land_use,
           any_value(assessed_value) AS assessed_value,
           max(p90_depth_m)          AS depth_m
    FROM parcel_flood
    GROUP BY parcel_id
),
land_use_ground AS (
    SELECT land_use, sum(flooded_m2) AS flooded_m2
    FROM (SELECT DISTINCT land_use, h3_index, flooded_m2 FROM parcel_flood)
    GROUP BY land_use
)
SELECT d.land_use,
       count(*)                                  AS parcels_flooded,
       round(avg(d.depth_m), 2)                  AS avg_depth_m,
       round(any_value(g.flooded_m2) / 1e6, 2)   AS flooded_km2,
       sum(d.assessed_value)                     AS value_at_risk
FROM parcel_depth AS d
JOIN land_use_ground AS g USING (land_use)
GROUP BY d.land_use
ORDER BY value_at_risk DESC;

-- 4b. Impassable road length: roads are lines, so join on shape. Hexagons don't overlap,
--     so summing each road's piece inside each hexagon counts no length twice.
SELECT r.class,
       round(sum(ST_Length_Spheroid(ST_FlipCoordinates(
           ST_Intersection(r.geometry, f.geom)))) / 1000, 1) AS km_impassable
FROM roads AS r
JOIN flood_h3 AS f ON ST_Intersects(r.geometry, f.geom)
GROUP BY r.class
ORDER BY km_impassable DESC;
```

DuckDB's advantage is reach: one session reads a local GeoTIFF, a GeoParquet file and a remote
dataset on S3, and joins them with a hash join where polygons can be polyfilled and a spatial join
where lines can't. The same SQL runs from the DuckDB CLI or from Python, Node.js and R. DuckDB
also exposes every engine option, including quantiles, compaction and memory budgets. Unlike
SedonaDB, `WHERE` filters apply after aggregation, so narrow the scan with `bbox := '...'`.
`flooded_km2` is the flooded ground in the hexagons that a land use's parcels touch (parcels were
polyfilled with `'covers'`), each hexagon counted once per land use. It measures exposure around
those parcels, not ground inside them, and a hexagon touching parcels of two land uses counts for
both, so the column does not add up to the city's flooded area.
To keep only hexagons that are mostly under water, add `valid_coverage_threshold := 0.5`: dry
NoData pixels count against a hexagon's coverage, so the filter keeps hexagons at least half
flooded.

## 3. Snowpack per sub-basin, rolled up to the basin — SedonaDB on S2

**The question**: *On April 1, how much snow water does each sub-basin of the Colorado Headwaters
hold, and how much does the basin hold as a whole?*

**Data**: SNODAS daily snow water equivalent (1 km, millimetres, as GeoTIFF) and the USGS
Watershed Boundary Dataset's HUC8 sub-basins.

**Why S2**: an S2 cell's four children tile it exactly (H3's children don't), so fine cells roll up
to coarse cells with no gaps or overlaps. Each cell is also given to exactly one sub-basin, so
sub-basins add up to the basin.

**Why `transfer="overlay"`**: a level-13 cell holds only about two SNODAS pixels (30 arc-seconds,
about 0.66 km² here). With centre assignment that is one, two or three whole pixels depending on
where the centres fall, and a pixel on the edge counts wholly for one side. Overlay splits every
edge pixel by area, so each cell's `mean` is the true area-weighted mean and its `count` the
overlapped area in pixels. Because S2 children tile their parent and each pixel's shares add up
to the whole pixel, the level-9 roll-up in step 4 is the same weighted mean a direct level-9
overlay would give. (`transfer="auto"` picks overlay here too; it is spelled out because the
roll-up depends on it.)

```python
# Build the bindings: cd crates/dggs-stream-sedona/python && maturin develop --release
# pip install "sedonadb==0.5.0" geoarrow-pyarrow pandas pyogrio pyproj geopandas
import geopandas as gpd
import sedonadb
import shapely
import dggs_stream_sedona
from dggs_stream_sedona import RasterTable

sd = sedonadb.connect()
sd.register(dggs_stream_sedona.scalar_functions())

# HUC8 sub-basins of the Colorado Headwaters basin (HUC6 140100)
huc8_path = "wbd_huc8_140100.geojson"
basin_box = shapely.box(*gpd.read_file(huc8_path).to_crs(4326).total_bounds).wkt

# 1. Snow water equivalent on S2 level-13 cells (~1.3 km²), scanned lazily by each query;
#    edge pixels are split between cells by area
sd.create_data_frame(
    RasterTable.continuous("snodas_swe_20260401.tif", 13, grid="s2", nodata=-9999,
                           transfer="overlay")
).to_view("swe_s2")
sd.read_pyogrio(huc8_path).to_view("huc8")

# 2. A filter against a fixed shape limits the scan to the tiles near the basin;
#    the rest of the CONUS raster is never aggregated
sd.sql(f"""
    SELECT s2_index, mean AS swe_mm, min AS min_swe_mm, max AS max_swe_mm, count, wkb
    FROM swe_s2
    WHERE ST_Intersects(wkb, ST_SetSRID(ST_GeomFromWKT('{basin_box}'), 4326))
""").to_view("swe_cells")

# 3. Each cell goes to the one sub-basin that holds its centre. GROUPING SETS gives one row per
#    sub-basin plus one per basin (HUC6 = the first six digits of HUC8), with no grand total.
#    A cell's mean can't say how many of its pixels have snow, so snow cover is bracketed:
#    pixels in cells whose minimum SWE is above 0 are all snow-covered (a lower bound), and
#    pixels in cells whose maximum is above 0 include every snow-covered pixel (an upper bound).
by_basin = sd.sql("""
    WITH cells AS (
        SELECT h.huc8, c.swe_mm, c.min_swe_mm, c.max_swe_mm, c.count
        FROM swe_cells AS c
        JOIN huc8 AS h ON ST_Contains(h.wkb_geometry, ST_Centroid(c.wkb))
    )
    SELECT
        substr(huc8, 1, 6)                                   AS huc6,
        huc8,                                                -- NULL on the basin row
        round(sum(swe_mm * count) / sum(count), 1)           AS mean_swe_mm,
        round(sum(CASE WHEN min_swe_mm > 0 THEN count ELSE 0 END) / sum(count), 2)
                                                             AS snow_share_at_least,
        round(sum(CASE WHEN max_swe_mm > 0 THEN count ELSE 0 END) / sum(count), 2)
                                                             AS snow_share_at_most
    FROM cells
    GROUP BY GROUPING SETS ((substr(huc8, 1, 6), huc8), (substr(huc8, 1, 6)))
    ORDER BY huc6, huc8 NULLS FIRST
""").to_pandas()

# 4. Level-9 parents (~320 km²) are exact unions of their level-13 children. Step 2 kept every
#    child that touches the box, so a parent lying wholly inside the box has all its children
#    here, and its area-weighted mean equals a direct level-9 overlay scan (to overlay's 1/64-pixel
#    edge tolerance). Parents cut by the box's
#    edge would be missing children, so they are left out. Tile the rest for a web map.
dggs_stream_sedona.write_pmtiles(sd.sql(f"""
    SELECT s2_cell_to_parent(s2_index, 9) AS s2_index,
           sum(swe_mm * count) / sum(count) AS mean_swe_mm
    FROM swe_cells
    WHERE ST_Within(s2_to_wkb(s2_cell_to_parent(s2_index, 9)),
                    ST_SetSRID(ST_GeomFromWKT('{basin_box}'), 4326))
    GROUP BY 1
"""), "swe_s2_level9.pmtiles")
```

SedonaDB is a spatial engine, so the sub-basins don't need polyfilling: the join matches cell
shapes to basin shapes directly. It's also the only one of the three engines where a spatial
condition skips raster: `ST_Intersects(wkb, <constant shape>)` limits the scan to the tiles near
that shape, and `count >= n` or `mean >= x` drop cells inside the scan rather than after it.
S2 cells aren't equal-area and SNODAS pixels shrink with latitude, so this example reports
pixel-weighted means and shares, not totals. For the volume of water itself, scan the same file in
DuckDB or Python with `ground_area=True, transfer="overlay"`: SNODAS is a north-up lon/lat
raster, so each cell's `area_weighted_sum` is its SWE integrated over true ground area, in
mm × m², and `area_weighted_sum / 1e12` is km³ of water. To check SNODAS against SNOTEL station
readings, go the other way: `transfer="interpolate"` at a level far finer than the pixels (level
16, about 0.02 km², with a small `bbox`) gives a bilinear SWE at each station's cell, as
[example 4](#4-land-temperature-by-latitude-band-and-by-city--cli-on-healpix) does for cities.
To join on cell ids instead, as DuckDB and Polars do, see [DataFusion &
SedonaDB](engines/datafusion.md#from-sedonadbs-python-api).

## 4. Land temperature by latitude band and by city — CLI on HEALPix

**The question**: *How does average land temperature change from pole to pole, how much land
lies in each latitude band, and what is the mean temperature at each Alpine city?*

**Data**: WorldClim 2.1 annual mean temperature (BIO1), a 10-arc-minute global GeoTIFF in
lon/lat (EPSG:4326), with NoData over the ocean, and Natural Earth's populated places.

**Why HEALPix**: every cell has the same area, and the cells sit in rings of constant latitude, so
cells group cleanly into latitude bands. HEALPix also puts the poles on cell corners, so no cell
encloses one and the ground-area overlay below covers the whole globe.

**Why ground-area overlay**: a lon/lat raster's pixels shrink toward the poles: a 10′ pixel covers
about 342 km² at the equator and 171 km² at 60°. Averaging pixels over-weights high latitudes,
and on a coastline a cell is only partly land. `--ground-area --transfer overlay` clips every
pixel against every cell and weights each piece by its area on the WGS84 ellipsoid, so each cell
reports `retained_area_m2`, the true area of its land, and `area_weighted_sum`, temperature ×
m². Summed over a band, they give the band's land area and its area-weighted mean temperature
exactly, coastal cells included.

**Why `-r auto`**: it picks the resolution whose cells are closest in area to a pixel at the
raster's centre: depth 8 here (786,432 cells of about 649 km²), without looking up either
number.

```bash
# 1. Ground-area overlay on HEALPix, at the depth that matches the pixel size.
#    stderr reports: dggs-stream: -r auto selected HEALPix resolution 8
dggs-stream-cli jsonl wc2.1_10m_bio_1.tif --grid healpix -r auto \
    --ground-area --transfer overlay \
    --columns healpix_index,retained_area_m2,area_weighted_sum > bio1_healpix.jsonl

# 2. The same raster as a web map: one PMTiles archive, zooming from depth 4 to depth 8
dggs-stream-cli pmtiles wc2.1_10m_bio_1.tif --grid healpix -r 4,5,6,7,8 -o bio1_healpix.pmtiles

# 3. Summarise by 10° band in DuckDB; healpix_to_lat comes from the dggs_stream extension.
#    (Installed from a custom repository? Use LOAD dggs_stream FROM <repository> instead.)
duckdb -c "
LOAD dggs_stream;
SELECT floor(healpix_to_lat(healpix_index) / 10) * 10           AS band_south_deg,
       round(sum(retained_area_m2) / 1e6)                       AS land_km2,
       round(sum(area_weighted_sum) / sum(retained_area_m2), 1) AS mean_temp_c
FROM read_json('bio1_healpix.jsonl')
GROUP BY ALL
ORDER BY band_south_deg DESC"

# 4. City-scale cells over the Alps: three depths finer (depth 11, about 10 km², 1/24 of a
#    pixel here). Too fine for centre assignment, so interpolate, once per kernel.
for kernel in nearest bicubic; do
  dggs-stream-cli jsonl wc2.1_10m_bio_1.tif --grid healpix -r auto+3 --bbox 5,43,17,49 \
      --transfer interpolate --interpolation $kernel \
      --columns healpix_index,mean > bio1_alps_$kernel.jsonl
done

# 5. Each city's cell, looked up in both
duckdb -c "
INSTALL spatial; LOAD spatial; LOAD dggs_stream;
WITH cities AS (
    SELECT NAME AS city, healpix_lonlat_to_cell(LONGITUDE, LATITUDE, 11) AS healpix_index
    FROM ST_Read('ne_10m_populated_places.shp')
    WHERE LONGITUDE BETWEEN 5 AND 17 AND LATITUDE BETWEEN 43 AND 49
)
SELECT city,
       round(n.mean, 1)          AS nearest_c,
       round(b.mean, 1)          AS bicubic_c,
       round(b.mean - n.mean, 1) AS difference
FROM cities
LEFT JOIN read_json('bio1_alps_nearest.jsonl') AS n USING (healpix_index)
LEFT JOIN read_json('bio1_alps_bicubic.jsonl') AS b USING (healpix_index)
WHERE n.mean IS NOT NULL OR b.mean IS NOT NULL
ORDER BY abs(b.mean - n.mean) DESC NULLS FIRST"
```

The CLI's advantage is that it needs nothing else installed: one executable (also put on `PATH`
by the Python wheel) that suits shell scripts, cron jobs and CI. It writes GeoParquet or PMTiles
directly, or streams NDJSON to stdout (`dggs-stream-cli jsonl ... | jq`); ground-area scans
stream as `jsonl` or `ipc`. It has no SQL of its own, so the band summary runs in whatever reads
its output next. See the [CLI deep dive](engines/cli.md).

Both columns are additive: a band's land area is the sum of its cells' `retained_area_m2`, and
its mean temperature is the summed integral over the summed area, so cells cut by the coastline
count only for their land. A plain scan (`dggs-stream-cli parquet ... -r 8`) would weight every
pixel the same, and counting cells × 649 km² would count coastal cells in full. Each cell's
centroid latitude lies within a fraction of a cell (about 0.2°) of its ring's, which is plenty for
10° bands; a cell straddling a band's edge counts wholly in one band.

**Why interpolate for the cities**: a 10′ pixel is about 13 × 18 km at the Alps' latitude, far
larger than a city's cell at depth 11. With centre assignment, about 23 of every 24 depth-11
cells would hold no pixel centre and be missing, cities included. `interpolate` gives every cell
whose centre lies on the raster one value, read at that centre:

- `nearest` returns the value of the pixel holding the centre, so the two dozen cells in a pixel
  share one value and a city near a pixel edge takes whichever side its centre falls on. It is
  the only kernel for categorical rasters.
- `bicubic` fits a smooth surface through the 4 × 4 surrounding pixels, so the value changes
  continuously from cell to cell, and the `difference` column shows how much the nearest pixel
  was off. Its catch is overshoot next to sharp steps, such as a ridge between two valleys.
- `bilinear`, the default and what `--transfer auto` picks here, never overshoots;
  `lanczos` is the sharpest and rings most.

On the coast, ocean pixels are NoData: `bilinear` leaves them out and renormalises the rest, and
`bicubic` and `lanczos` fall back to `bilinear` when any tap is NoData, so a harbour city still
gets a value. `nearest` has no such fallback: a city whose cell centre falls on an ocean pixel
gets no `nearest` row, so step 5 uses outer joins, keeps such cities with an empty `nearest_c`,
and lists them first. Interpolation smooths between pixels but adds no detail finer than them. A valley
city still gets the average of its 18 km neighbourhood, mountains included; for that you need a
finer raster or a lapse-rate correction from a DEM.
