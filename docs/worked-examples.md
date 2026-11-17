# Worked Examples: One Engine, One Grid, One Question

[← Back to README](../README.md)

The README's [worked example](../README.md#raster-meets-table-a-worked-example) joins a land
cover raster to census tracts in DuckDB on H3. Every engine can also index pixels onto A5, S2
and HEALPix, and each grid is good at something different. The four examples below each pair a
real question with the engine and grid that suit it, and each joins a raster to vector or tabular
data, either on a shared cell id or on cell shapes.

| Question | Engine | Grid | What the grid brings |
| :--- | :--- | :--- | :--- |
| [Wetland loss inside vs. outside protected areas](#1-wetland-loss-inside-vs-outside-protected-areas--polars-on-a5) | Python & Polars | A5 | Equal-area cells, so cell fractions add up to true km² |
| [Flood depth at parcels and roads](#2-flood-depth-at-parcels-and-roads--duckdb-on-h3) | DuckDB | H3 | Hexagons, the grid other datasets are most often indexed on |
| [Snowpack per sub-basin, rolled up to the basin](#3-snowpack-per-sub-basin-rolled-up-to-the-basin--sedonadb-on-s2) | SedonaDB | S2 | Exact nesting: four children tile their parent |
| [Land temperature by latitude band](#4-land-temperature-by-latitude-band--cli-on-healpix) | CLI | HEALPix | Equal-area cells laid out in rings of constant latitude |

Without a shared spatial index, each of these needs a GIS overlay: reprojecting rasters, clipping
polygons, rasterizing geometries and managing gigabytes of scratch files. With raster_dggs, the
raster becomes a table of cells and the rest is a join.

## 1. Wetland loss inside vs. outside protected areas — Polars on A5

**The question**: *How much wetland did coastal Louisiana lose between 2001 and 2021, and did
strictly protected land lose less?*

**Data**: two 30-meter NLCD land cover rasters (2001 and 2021) and the PAD-US protected areas
database.

**Why A5**: every A5 cell at a resolution has the same area, so a cell's wetland fraction times one
constant is wetland area in km², and summing those gives a total with no grid bias (H3 cells vary
in area by location). NLCD is in an equal-area projection (EPSG:5070), so each pixel's share of a
cell is also its share of the ground: the whole chain conserves area.

```python
# pip install "./crates/raster-h3-python[polars]" geopandas
import geopandas as gpd
import polars as pl
import raster_h3
import shapely

RES = 14                                        # A5 cells of about 0.127 km² (~140 NLCD pixels)
CELL_KM2 = 510_065_622 / (60 * 4 ** (RES - 1))  # equal area: Earth's area / number of cells
BBOX = (-93.9, 28.9, -88.8, 30.6)               # coastal Louisiana, lon/lat

# 1. Wetland share of every A5 cell, one lazy scan per year
def wetland_share(path, year):
    return (
        raster_h3.scan_raster_h3(
            path, RES, grid="a5", categorical=True, format="long",
            remap={90: 1, 95: 1, "else": 0},  # woody + emergent wetland -> 1, everything else -> 0
            bbox=BBOX, columns=["a5_index", "category", "fraction"],
        )
        .filter(pl.col("category") == 1)
        .select("a5_index", pl.col("fraction").alias(f"wet_{year}"))
    )

wetland = (
    wetland_share("nlcd_2001_land_cover.tif", 2001)
    .join(wetland_share("nlcd_2021_land_cover.tif", 2021),
          on="a5_index", how="full", coalesce=True)
    .with_columns(pl.col("wet_2001", "wet_2021").fill_null(0.0))  # no wetland that year = no row
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
                  for c in raster_h3.polygon_to_cells(g.wkb, RES, grid="a5") or []]},
    schema={"a5_index": pl.UInt64},
).unique().with_columns(protected=pl.lit(True))

# 3. Wetland area and loss, inside vs. outside protected land
result = (
    wetland.join(protected.lazy(), on="a5_index", how="left")
    .with_columns(pl.col("protected").fill_null(False))
    .group_by("protected")
    .agg(
        (pl.col("wet_2001").sum() * CELL_KM2).alias("wetland_2001_km2"),
        (pl.col("wet_2021").sum() * CELL_KM2).alias("wetland_2021_km2"),
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
of the raster is read. One caveat: the engine weights statistics by pixel, not by ground area
(see [ground-area weighting](local-grids-and-ground-area.md)). That's exact here only because
every NLCD pixel covers the same 900 m², which also gives a cross-check: `count × 900 m²` from the
same scan should match. See the [Python scan parameters](engines/python-polars.md#scan-parameters).

## 2. Flood depth at parcels and roads — DuckDB on H3

**The question**: *In a 100-year flood in Houston, which parcels take on more than 30 cm of water,
and how many kilometres of road become impassable?*

**Data**: a 100-year flood depth GeoTIFF (metres), a county parcel GeoParquet file, and Overture
Maps road segments read directly from S3.

**Why H3**: resolution-11 hexagons (about 2,150 m²) are roughly parcel-sized, and H3 is the grid
other datasets you might join later are most likely to be indexed on already.

```sql
INSTALL spatial; LOAD spatial;
INSTALL httpfs;  LOAD httpfs;
SET s3_region = 'us-west-2';

-- 1. Flood depth into H3 resolution-11 hexagons. WHERE runs after aggregation,
--    so bbox is what keeps the scan to the city.
CREATE TABLE flood_h3 AS
SELECT h3_index, max AS max_depth_m, p90 AS p90_depth_m, geom
FROM h3_raster_continuous_aggregate(
    'flood_depth_100yr.tif', resolution := 11,
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

-- 3. Road segments straight from Overture's public S3 bucket; the bbox struct
--    lets DuckDB skip row groups outside the city
CREATE TABLE roads AS
SELECT id, class, geometry
FROM read_parquet(
    's3://overturemaps-us-west-2/release/<release>/theme=transportation/type=segment/*',
    hive_partitioning = true)
WHERE subtype = 'road'
  AND bbox.xmin > -95.80 AND bbox.xmax < -95.01
  AND bbox.ymin >  29.52 AND bbox.ymax <  30.11;

-- 4a. Flooded parcels: an integer hash join on the shared H3 index, then one row per parcel
--     (a parcel can span several hexagons)
WITH parcel_depth AS (
    SELECT p.parcel_id,
           any_value(p.land_use)       AS land_use,
           any_value(p.assessed_value) AS assessed_value,
           max(f.p90_depth_m)          AS depth_m
    FROM parcels_h3 AS p
    JOIN flood_h3 AS f USING (h3_index)
    GROUP BY p.parcel_id
)
SELECT land_use,
       count(*)              AS parcels_flooded,
       round(avg(depth_m), 2) AS avg_depth_m,
       sum(assessed_value)   AS value_at_risk
FROM parcel_depth
GROUP BY land_use
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

## 3. Snowpack per sub-basin, rolled up to the basin — SedonaDB on S2

**The question**: *On April 1, how much snow water does each sub-basin of the Colorado Headwaters
hold, and how much does the basin hold as a whole?*

**Data**: SNODAS daily snow water equivalent (1 km, millimetres, as GeoTIFF) and the USGS
Watershed Boundary Dataset's HUC8 sub-basins.

**Why S2**: an S2 cell's four children tile it exactly (H3's children don't), so fine cells roll up
to coarse cells with no gaps or overlaps. Each cell is also given to exactly one sub-basin, so
sub-basins add up to the basin.

```python
# Build the bindings: cd crates/raster-h3-sedona/python && maturin develop --release
# pip install "sedonadb==0.5.0" geoarrow-pyarrow pandas pyogrio pyproj geopandas
import geopandas as gpd
import sedonadb
import shapely
import raster_h3_sedona
from raster_h3_sedona import RasterH3Table

sd = sedonadb.connect()
sd.register(raster_h3_sedona.scalar_functions())

# HUC8 sub-basins of the Colorado Headwaters basin (HUC6 140100)
huc8_path = "wbd_huc8_140100.geojson"
basin_box = shapely.box(*gpd.read_file(huc8_path).to_crs(4326).total_bounds).wkt

# 1. Snow water equivalent on S2 level-13 cells (~1.3 km²), scanned lazily by each query
sd.create_data_frame(
    RasterH3Table.continuous("snodas_swe_20260401.tif", 13, grid="s2", nodata=-9999)
).to_view("swe_s2")
sd.read_pyogrio(huc8_path).to_view("huc8")

# 2. A filter against a fixed shape limits the scan to the tiles near the basin;
#    the rest of the CONUS raster is never aggregated
sd.sql(f"""
    SELECT s2_index, mean AS swe_mm, count, wkb
    FROM swe_s2
    WHERE ST_Intersects(wkb, ST_SetSRID(ST_GeomFromWKT('{basin_box}'), 4326))
""").to_view("swe_cells")

# 3. Each cell goes to the one sub-basin that holds its centre, and ROLLUP adds the
#    basin total (HUC6 = the first six digits of HUC8) from the same rows
by_basin = sd.sql("""
    WITH cells AS (
        SELECT h.huc8, c.swe_mm, c.count
        FROM swe_cells AS c
        JOIN huc8 AS h ON ST_Contains(h.wkb_geometry, ST_Centroid(c.wkb))
    )
    SELECT
        substr(huc8, 1, 6)                                   AS huc6,
        huc8,                                                -- NULL on the basin total row
        round(sum(swe_mm * count) / sum(count), 1)           AS mean_swe_mm,
        round(sum(CASE WHEN swe_mm > 0 THEN count ELSE 0 END) / sum(count), 2)
                                                             AS snow_covered_share
    FROM cells
    GROUP BY ROLLUP (substr(huc8, 1, 6), huc8)
    ORDER BY huc6, huc8 NULLS FIRST
""").to_pandas()

# 4. Level-9 parents (~320 km²) are exact unions of their level-13 children, so these
#    pixel-weighted means equal a direct level-9 scan. Tile them for a web map.
raster_h3_sedona.write_pmtiles(sd.sql("""
    SELECT s2_cell_to_parent(s2_index, 9) AS s2_index,
           sum(swe_mm * count) / sum(count) AS mean_swe_mm
    FROM swe_cells
    GROUP BY 1
"""), "swe_s2_level9.pmtiles")
```

SedonaDB is a spatial engine, so the sub-basins don't need polyfilling: the join matches cell
shapes to basin shapes directly. It's also the only one of the three engines where a spatial
condition skips raster: `ST_Intersects(wkb, <constant shape>)` limits the scan to the tiles near
that shape, and `count >= n` or `mean >= x` drop cells inside the scan rather than after it.
S2 cells aren't equal-area, so this example reports pixel-weighted means and shares, not totals in
km². To join on cell ids instead, as DuckDB and Polars do, see [DataFusion &
SedonaDB](engines/datafusion.md#from-sedonadbs-python-api).

## 4. Land temperature by latitude band — CLI on HEALPix

**The question**: *How does average land temperature change from pole to pole?*

**Data**: WorldClim 2.1 annual mean temperature (BIO1), a 10-arc-minute global GeoTIFF.

**Why HEALPix**: every cell has the same area, and the cells sit in rings of constant latitude.
So a plain average of cells in a latitude band is an area-weighted average. Averaging a lon/lat
raster's pixels directly gets this wrong: pixels shrink toward the poles, so high latitudes get
too much weight.

```bash
# 1. Aggregate the global raster onto HEALPix depth-8 cells (786,432 cells of ~649 km² each),
#    straight to GeoParquet. No database, no Python.
raster-h3-cli parquet wc2.1_10m_bio_1.tif --grid healpix -r 8 -o bio1_healpix8.parquet

# 2. The same scan as a web map: one PMTiles archive, zooming from depth 4 to depth 8
raster-h3-cli pmtiles wc2.1_10m_bio_1.tif --grid healpix -r 4,5,6,7,8 -o bio1_healpix.pmtiles

# 3. Summarise by 10° band with any Parquet reader; here the DuckDB CLI, which needs only
#    its spatial extension to read the cells' latitude, not raster_h3
duckdb -c "
INSTALL spatial; LOAD spatial;
SELECT floor(ST_Y(ST_Centroid(geometry)) / 10) * 10   AS band_south_deg,
       count(*)                                       AS land_cells,
       round(count(*) * 510065622 / (12 * pow(4, 8))) AS land_km2,    -- cells x one constant area
       round(avg(mean), 1)                            AS mean_temp_c  -- area-weighted: equal cells
FROM 'bio1_healpix8.parquet'
GROUP BY ALL
ORDER BY band_south_deg DESC"
```

The CLI's advantage is that it needs nothing else installed: one executable (also put on `PATH`
by the Python wheel) that suits shell scripts, cron jobs and CI. It writes GeoParquet or PMTiles
directly, or streams NDJSON to stdout (`raster-h3-cli jsonl ... | jq`). It has no SQL of its own,
so the band summary runs in whatever reads Parquet next. The CLI exposes the common options
(grid, resolutions, CRS, NoData, bbox, categorical layout); see the [CLI deep dive](engines/cli.md).

WorldClim has no data over the ocean, so only land cells are written. `land_km2` is approximate
because a coastal cell counts in full. Each cell's centroid latitude lands within a fraction of a
cell (a cell is about 0.2° across) of its ring's latitude, which is plenty for 10° bands. Within each cell the
engine still weights by pixel, but at depth 8 (about 25 km across) the latitude change inside a
cell is small except right at the poles.
