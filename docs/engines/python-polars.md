# Python & Polars Deep Dive: `dggs-stream-python`

[← Engines Overview](README.md) · [← Back to README](../../README.md)

`dggs-stream-python` ([`crates/dggs-stream-python`](../../crates/dggs-stream-python)) ships the
`dggs-stream` Python package: a native library with a C ABI, a pure-Python wrapper
([`python/dggs_stream/__init__.py`](../../crates/dggs-stream-python/python/dggs_stream/__init__.py))
that loads it with `ctypes`, and the bundled `dggs-stream-cli` executable.

For every argument, output column, return value and error, see the
[Polars API Reference](../polars-api-reference.md).

Because the library never links against `libpython`, one wheel per platform
(`py3-none-<platform>`) serves every Python ≥ 3.9. Polars and PyArrow are optional.

## Install

The package is not on PyPI yet. Install a wheel from the
[Python Wheels workflow](../../.github/workflows/wheels.yml) artifacts, or build from a
checkout (needs a Rust toolchain):

```bash
pip install "./crates/dggs-stream-python[polars]"   # builds via the hatch hook; extras: polars, pyarrow
# or, for development against target/:
cargo build --release -p dggs-stream-python
```

The wrapper finds the native library in this order: `DGGS_STREAM_PYTHON_LIB` (if the file
exists), the package directory, then the workspace `target/release` and `target/debug`.

## API

```python
import polars as pl
import dggs_stream

# Lazy Polars scan
lf = dggs_stream.scan_raster("dem.tif", resolutions=[7, 8], crs="EPSG:4326")
df = lf.filter(pl.col("count") > 10).select("h3_index", "mean").collect()

# Eager
df = dggs_stream.read_raster("landcover.tif", 8, categorical=True, format="long")

# Arrow PyCapsule producer
stream = dggs_stream.RasterArrowStream("dem.tif", 8, columns=["h3_index", "mean"])
table = stream.to_pyarrow()                 # pyarrow.table(stream)
for batch in stream.iter_batches(batch_size=50_000):
    ...                                     # Polars DataFrames

# Exports
dggs_stream.raster_to_parquet("dem.tif", "dem.parquet", resolutions="7,8")
dggs_stream.raster_to_pmtiles("dem.tif", "dem.pmtiles", resolutions=[6, 7, 8])
```

### Scan parameters

`RasterArrowStream`, `scan_raster` and `read_raster` share these arguments:

| Argument | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `path` | `str \| Path` | — | Raster path, glob, comma-separated list or VRT |
| `grid` | `str` | `"h3"` | `"h3"`, `"a5"`, `"s2"` or `"healpix"`; the cell columns are `{grid}_index` / `{grid}_hex` ([Grids](../grids.md)) |
| `resolutions` | `int \| str \| Sequence[int]` | `8` | `8`, `"7,8"` or `[7, 8]`, within the grid's range (`dggs_stream.GRIDS[grid]` is the finest); or `"auto"`, `"auto+1"`, `"auto-2"` for pixel-sized cells |
| `categorical` | `bool` | `False` | Categorical instead of continuous aggregation |
| `format` | `str` | `"wide"` | Categorical layout: `"wide"` or `"long"` (case-insensitive) |
| `crs` | `str` | `None` | Source CRS override, e.g. `"EPSG:32610"` |
| `nodata` | `float` | `None` | NoData override |
| `bbox` | `str \| Sequence[float]` | `None` | `min_lon,min_lat,max_lon,max_lat`; exactly four numbers |
| `columns` | `Sequence[str]` | `None` | Columns to produce (projection pushed into the engine) |
| `limit` | `int` | `None` | Maximum rows |
| `strict_transforms` | `bool` | `False` | Raise when a valid sample cannot be transformed to WGS84 instead of skipping it with a `RuntimeWarning` |
| `band` | `int` | `None` (1) | 1-indexed band to aggregate |
| `bands` | sequence of integers | `None` | Ordered one-based bands from one interleaved or band-separated TIFF. Continuous center-assignment scans share the grid columns and return statistics per band. |
| `ground_area` | `bool` | `None` (off) | Weight values by WGS84 ellipsoidal area. Center assignment uses the full pixel. Weighted overlay uses the clipped intersection with each cell and accepts `valid_coverage_threshold` in terms of valid area divided by selected overlap area. Needs one north-up, single-band EPSG:4326 raster, omitted NoData and the default center sampling; incompatible with `bands`, quantiles, formulas, partitions and mass normalization. Returns `pixel_count`, `retained_area_m2`, `area_weighted_sum` and `area_weighted_mean` in place of the usual statistics; `min_count` filters on the sum of valid pixel fractions. A `bbox` that misses the raster is an error, as in ordinary scans. |
| `sampling` | `str` | `None` (`"center"`) | Sub-pixel sampling pattern: `center`, `5point`, `gaussian`, `rgss`, `hex`, `8rooks`, `9point`, `16point` |
| `overlap_rule` | `str` | `None` (`"cutline"`) | Mosaic overlaps: `cutline`, `first` or `average` |
| `min_majority_fraction` | `float` | `None` | Categorical only: drop cells whose majority class covers less than this fraction |
| `spectral_formula` | `str` | `None` | Continuous only: `ndvi`, `ndwi`, `nbr` or `evi`, from `nir_band` (4), `red_band` (3), `green_band` (2), `blue_band` (1), `swir_band` (6) |
| `quantiles` | `str \| Sequence` | `None` | Continuous only: quantile columns, e.g. `"p50,p90"`, `[0.05, 0.95]`, `"box"` |
| `remap` | `str \| Mapping` | `None` | Categorical only: class remapping, e.g. `{range(11, 20): 1, "else": None}` |
| `min_count`, `min_mean`, `max_mean` | `float` | `None` | Inclusive cell filters applied in the engine (`min_mean` / `max_mean` continuous only) |
| `nodata_policy` | `str` | `None` (`"omit"`) | `"emit"` keeps cells whose samples are all NoData (`count` 0, null statistics) |
| `emit_nodata_value` | `float` | `None` | Under `"emit"`, the value of those statistics instead of null |
| `decimals` | `int` | `None` | Round floating-point statistics to 0–15 decimals |
| `transfer` | `str` | `None` (`"assign_centers"`) | `"interpolate"`, `"overlay"` or `"auto"`, which picks one from the cell-to-pixel size ratio ([Transfer Modes](../transfer-modes.md)) |
| `interpolation` | `str` | `None` (`"bilinear"`) | With `"interpolate"`: `nearest`, `bilinear`, `bicubic`, `lanczos` |
| `overlay_weighted`, `mass_preserve` | `bool` | `None` | With `"overlay"`: count touched pixels once / keep the raster's total in `sum` |
| `aggregation_budget_mb` | `int` | `None` (64 MiB) | Center-assignment aggregation budget in MiB. Partial cell states spill to disk under memory pressure. A spilled resolution may need to read the full input even with a small row limit. Refused with any other transfer mode, including when `transfer="auto"` resolves to one. |
| `decode_budget_mb` | `int` | `None` (256 MiB) | Decoded buffer budget in MiB, in every transfer mode. A single oversized chunk can proceed alone. |
| `valid_coverage_threshold` | `float` | `None` | Minimum valid fraction (0–1) of selected raster overlap. Applies to overlay. Mass-preserving scans retain all contributions. |
| `histogram` | `str \| Sequence[float]` | `None` | Categorical only: `"values"`, `"4:0:100"` or bin edges `[0, 10, 50]`; the `histogram` column becomes `List(Struct)` |
| `histogram_weight`, `histogram_normalize` | `str` | `None` | `"count"` / `"area"`; `"none"` / `"fraction"` / `"density"` |

With `decimals`, Polars predicates on `mean` and `majority_fraction` are not pushed into the
engine, since it compares unrounded values; Polars applies them to the rows.
`resolution` predicates are pushed for any resolution of the grid.

`profile_raster(path, resolutions, ...)` takes the same arguments, runs the scan with a
runtime profile, and returns `{"rows": ..., "profile": {...}}`
([Scan Options](../scan-options.md#runtime-profile)). `auto_resolution(path, grid, offset=0,
crs=None)` returns the resolution `"auto"` would pick.

An invalid `resolutions`, `format`, `bbox`, `sampling`, `overlap_rule` or `spectral_formula`,
or a `band` the raster does not have, raises `RuntimeError` (as does an invalid
`resolutions` passed to the export functions) rather than being ignored or defaulted.

Columns are the canonical core schemas (see the [overview](README.md#output-schemas)).
Compaction, single-cell pushdown (`h3_cell`) and the `geom` column are
DuckDB-only; see the [fixed behaviour](../polars-api-reference.md#fixed-behaviour) table.

### `RasterArrowStream`

A lazy description of a scan. Nothing runs until a method is called, and each call starts
a new scan.

| Method | Behaviour |
| :--- | :--- |
| `__arrow_c_schema__()` | `"arrow_schema"` PyCapsule for the (projected) output schema |
| `__arrow_c_stream__(requested_schema=None)` | `"arrow_array_stream"` PyCapsule; batches are produced as the consumer pulls them |
| `to_pyarrow()` | `pyarrow.table(self)` |
| `to_polars()` | Polars `DataFrame` through the PyCapsule stream; scan errors propagate |
| `iter_batches(batch_size=None)` | Generator of Polars `DataFrame`s of up to `batch_size` rows (default 2048), one native batch reader for the whole iteration |
| `to_dict()` | `dict[str, list]` via a JSON round trip; needs neither Polars nor PyArrow |

Any consumer of the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
can take the stream directly, e.g. `polars.from_arrow(stream)`, `pyarrow.table(stream)` or
`duckdb.from_arrow(stream)`.

### Polars IO source

When Polars with `polars.io.plugins.register_io_source` is importable, `scan_raster`
returns a `LazyFrame` backed by an IO source; otherwise it returns the
`RasterArrowStream`.

| Polars hint | Handling |
| :--- | :--- |
| `with_columns` (projection) | Pushed into the engine as `columns` |
| `n_rows` (slice) | Pushed into the engine as `limit` when there is no predicate; with a predicate it caps the rows that survive the filter instead |
| `predicate` | Comparisons on `count` / `total_count`, `mean`, `majority_fraction` and `resolution` joined by `&` are pushed into the engine as `min_count`, `min_mean`, `max_mean`, `min_majority_fraction` and a narrower set of resolutions; Polars then applies the full predicate to each batch. See [Query optimisation](../polars-api-reference.md#query-optimisation) |
| `batch_size` | Passed to `iter_batches` |

Before Polars 1.22 neither `predicate` nor `n_rows` reaches the engine: those versions drop
some filters they hand to IO sources, so `scan_raster` keeps all filters in Polars. See
[Query optimisation](../polars-api-reference.md#query-optimisation).

The declared `LazyFrame` schema has no quantile columns, which matches the Python API
(quantiles cannot be requested from Python).

`read_raster(path, resolutions, **kwargs)` is `RasterArrowStream(...).to_polars()`,
falling back to the stream object if Polars is missing.

### Scalar helpers

`h3_to_string`, `string_to_h3`, `h3_to_lat`, `h3_to_lng`, `h3_get_resolution`,
`h3_is_valid(int | str)`, `h3_to_wkb(int | str) -> bytes | None` and
`h3_cell_to_parent(int | str, res)` and
`h3_polygon_to_cells(wkb: bytes, resolution, containment="contains_centroid") -> list[int] | None`
call the shared core helpers. The grid-neutral forms take `grid="h3"` by default:
`lonlat_to_cell(lon, lat, resolution, grid)`, `cell_to_string`, `string_to_cell`,
`cell_is_valid`, `cell_to_lat`, `cell_to_lng`, `cell_resolution`, `cell_to_parent`,
`cell_to_wkb` and `polygon_to_cells(wkb, resolution, containment, grid)` (behaviour on invalid input is listed in the
[overview](README.md#scalar-helpers); containment modes are described in the
[Polars API reference](../polars-api-reference.md#h3_polygon_to_cells)). `h3_to_wkb` returns a
`MULTIPOLYGON` for cells crossing the antimeridian and a polar-cap polygon for the pole
cells (see [cell geometry](../sql-api-reference.md#cell-geometry)). `h3_polygon_to_cells` takes WKB,
e.g. `shapely.to_wkb(geom)`; it raises `ValueError` for an unknown mode or a polygon over the
10,000,000-cell limit. They operate on single Python
values; for whole columns use the Arrow stream or a Polars expression such as
`map_elements`.

### Exporters

| Function | Arguments | Returns |
| :--- | :--- | :--- |
| `raster_to_parquet(input, output, resolutions=8, *, categorical=False, crs=None, geoparquet=True, bands=None, grid=None, ...)` | Parquet or GeoParquet 1.1 | Number of cells written |
| `raster_to_pmtiles(input, output, resolutions=8, *, categorical=False, crs=None, grid=None, ...)` | PMTiles v3 archive | Count reported by the tiler |
| `parquet_to_pmtiles(parquet, output, *, cell_column=None, grid=None, h3_column=None)` | PMTiles v3 archive from any cell-indexed Parquet file; the grid comes from `grid` or the column's name (`s2_index`, ...) | Number of cell features written |

The raster exporters also take `nodata_policy`, `emit_nodata_value`, `decimals`, `transfer`,
`interpolation`, `overlay_weighted`, `mass_preserve` and `valid_coverage_threshold`. Histograms are not available in the
exports.

Errors raise `RuntimeError` with the engine's message.

## Recipes

These mirror the [DuckDB SQL recipes](duckdb.md#sql-usage--recipes) for the parameters the
Python API exposes. Every scan below returns a `LazyFrame` when Polars is installed.

### Basic continuous aggregation

```python
import polars as pl
import dggs_stream

df = dggs_stream.read_raster("elevation.tif", 8)
df.select("h3_index", "h3_hex", "mean", "count", "min", "max", "sum")
```

### CRS, NoData and bounding box

```python
lf = dggs_stream.scan_raster(
    "temperature_global.tif",
    9,
    crs="EPSG:4326",
    nodata=-9999.0,
    bbox=(-122.50, 37.70, -122.35, 37.85),   # min_lon, min_lat, max_lon, max_lat
)
top = (
    lf.select("h3_hex", pl.col("mean").round(2).alias("avg_temp_c"), "count")
      .sort("count", descending=True)
      .collect()
)
```

### Multiple resolutions in one pass

```python
lf = dggs_stream.scan_raster("elevation.tif", [6, 7, 8])
per_res = lf.group_by("resolution").agg(pl.len().alias("cells"), pl.col("mean").mean()).collect()
res8 = lf.filter(pl.col("resolution") == 8).collect()
```

### Other grids, pixel-sized cells and scan options

```python
import polars as pl
import dggs_stream

# S2 cells about one pixel in size
s2 = dggs_stream.scan_raster("dem.tif", "auto", grid="s2").collect()      # s2_index, s2_hex, ...
print(dggs_stream.auto_resolution("dem.tif", grid="healpix"))

# Two steps coarser than a pixel, with the transfer mode chosen from the size ratio (overlay here)
coarse = dggs_stream.scan_raster("dem.tif", "auto-2", transfer="auto")

# Cells finer than the pixels, interpolated; or pixels split by overlap, keeping the total
fine = dggs_stream.scan_raster("dem.tif", "auto+2", transfer="interpolate", interpolation="bicubic")
pop = dggs_stream.scan_raster("population.tif", 7, transfer="overlay", mass_preserve=True)

# Per-cell histograms as a List(Struct) column
hist = dggs_stream.scan_raster(
    "dem.tif", 8, categorical=True, histogram="4:0:2000", histogram_normalize="fraction"
).collect()
hist.explode("histogram").unnest("histogram")                            # lower, upper, weight

# Keep all-NoData cells and round; profile where the time went
lf = dggs_stream.scan_raster("dem.tif", 8, nodata_policy="emit", decimals=2)
report = dggs_stream.profile_raster("dem.tif", 8)["profile"]
```

### Helper scalar functions

The helpers take single values; apply them to a column with `map_elements`:

```python
df = dggs_stream.read_raster("elevation.tif", 8, columns=["h3_index", "mean"])
df = df.with_columns(
    pl.col("h3_index").map_elements(dggs_stream.h3_to_lat, return_dtype=pl.Float64).alias("center_lat"),
    pl.col("h3_index").map_elements(dggs_stream.h3_to_lng, return_dtype=pl.Float64).alias("center_lng"),
    pl.col("h3_index").map_elements(lambda c: dggs_stream.h3_cell_to_parent(c, 6),
                                    return_dtype=pl.UInt64).alias("parent"),
)
dggs_stream.string_to_h3("8828308281fffff")   # 613196570331971583
```

### Joins

`h3_index` is `UInt64`, so it joins directly with any other H3-indexed Polars frame:

```python
population = pl.scan_parquet("population_h3.parquet")   # h3_index: UInt64
joined = (
    dggs_stream.scan_raster("california_elevation.tif", 8, columns=["h3_index", "h3_hex", "mean"])
    .join(population, on="h3_index")
    .filter(pl.col("mean") > 500.0)
    .select("h3_hex", pl.col("mean").alias("avg_elevation"), "total_population", "median_income")
    .collect()
)
```

See [Worked Examples](../worked-examples.md#1-wetland-loss-inside-vs-outside-protected-areas--polars-on-a5)
for a full two-year raster × protected-area join in Polars on the A5 grid.

### Export to Parquet / GeoParquet

```python
# Streaming exporter: WKB geometry + GeoParquet 1.1 metadata
dggs_stream.raster_to_parquet("elevation.tif", "elevation_h3.parquet", 8)

# Preserve the multi-band scan schema, including null statistics and WKB geometry.
dggs_stream.raster_to_parquet("imagery.tif", "bands.parquet", 12, bands=[3, 1])

# Or write a projected frame with Polars itself
(dggs_stream.scan_raster("elevation.tif", 8, columns=["h3_index", "mean", "count"])
    .sink_parquet("elevation_h3_slim.parquet"))
```

### Remote COG & S3 streaming

```python
lf = dggs_stream.scan_raster("https://example.com/rasters/cog_elevation.tif", 8)
lf = dggs_stream.scan_raster("s3://my-geospatial-bucket/landsat/scene_01.tif", 9)
```

### Multi-file mosaics

Globs, comma-separated lists and VRTs are accepted; overlaps use the `cutline` rule unless
`overlap_rule` says otherwise.

```python
lf = dggs_stream.scan_raster("tiles/tile_*.tif", 8)
lf = dggs_stream.scan_raster("tile_west.tif,tile_east.tif", 8, categorical=True)
```

### Bands, sub-pixel sampling and spectral indices

```python
# Band 2 of a multi-band raster
lf = dggs_stream.scan_raster("sentinel2.tif", 9, band=2)

# Ground-area weighting for a north-up EPSG:4326 raster: each pixel counts by its area
lf = dggs_stream.scan_raster("temperature.tif", 8, ground_area=True)
lf.select("h3_index", "retained_area_m2", "area_weighted_mean").collect()

# Integrate population density (people/km²) over the valid portion of each cell.
lf = dggs_stream.scan_raster(
    "population_per_km2.tif", 8, ground_area=True, transfer="overlay", valid_coverage_threshold=0.5
)
lf.select("h3_index", (pl.col("area_weighted_sum") / 1_000_000).alias("population")).collect()

# Read bands 3 and 1 in one pass. Missing samples and scaling are evaluated per band.
lf = dggs_stream.scan_raster("sentinel2.tif", 9, bands=[3, 1])
lf.select("h3_index", "band_3_mean", "band_1_mean").collect()

# NDVI per pixel from bands 8 (NIR) and 4 (red), then aggregated per cell
ndvi = dggs_stream.scan_raster(
    "sentinel2.tif", 9, spectral_formula="ndvi", nir_band=8, red_band=4
)

# Anti-aliased cell edges: four rotated-grid samples per pixel
lf = dggs_stream.scan_raster("dem.tif", 10, sampling="rgss")

# Mosaic where earlier files win in overlaps; categorical cells with a clear majority
lf = dggs_stream.scan_raster(
    "newest.tif,older.tif", 8, categorical=True,
    overlap_rule="first", min_majority_fraction=0.5,
)
```

Ground-area overlay runs serially and adaptively traces grid edges in raster pixel coordinates.
It requires a resolution with a maximum cell diameter of at most 500 km and rejects pole-containing candidate cells. [Geometry and units](core.md#ground-area-weighted-overlay)

The names and defaults match the DuckDB parameters; see the
[scan options](../polars-api-reference.md#keyword-parameters) and
[sub-pixel sampling](../super-sampling.md).

### Partitioned scans (Spark and other distributed engines)

One raster can be split across independent workers that each read a disjoint set of TIFF
tiles or strips, with results identical to a single scan (up to floating-point summation
order). The driver plans from metadata only:

```python
specs = dggs_stream.plan_partitions("dem.tif", target_chunks=256)  # list of JSON strings
```

Each worker scans one spec with the same path, `crs` and `bbox` (and `overlap_rule`, if the
plan was made with one: `plan_partitions(..., overlap_rule="first")`):

```python
cells = pa.table(dggs_stream.RasterArrowStream("dem.tif", 8, partition=spec))
```

The table has the usual columns plus a nullable `partial_state` (binary) column:

- **null**: a final cell, already complete;
- **set**: this partition's share of a cell near a partition edge (its statistics cover
  that share only). Gather these rows from every partition, keyed by `h3_index`, and merge:

```python
final = cells.filter(pc.is_null(cells["partial_state"])).drop(["partial_state"])
merged = dggs_stream.merge_partials(partial_rows_from_all_partitions)  # pyarrow.Table
```

`merge_partials` accepts PyArrow tables or batches, lists of them, or anything exposing
`__arrow_c_stream__`, and casts Spark's signed `h3_index` / `resolution` types. Pass the
scans' `categorical`, `columns`, `quantiles` and filters (`min_count`, `min_mean`,
`max_mean`, `min_majority_fraction`) so the merged rows have the same columns and pass the
same filters; options that act on pixels (`remap`, `band`, `sampling`, ...) are already
applied to the partial states. With Spark,
scan one spec per task (`mapInArrow`), then `repartition("h3_index")` the partial rows and
merge each group with `mapInArrow` as well. On Spark 4.0+, `dggs_stream.spark` wraps all of
this as a data source (`spark.read.format("dggs_stream")`); see
[Spark & Apache Sedona](spark.md). The Apache Sedona demo,
[`examples/sedona_spark/dggs_stream_spark_demo.py`](../../examples/sedona_spark/dggs_stream_spark_demo.py),
is a complete pipeline; its tests in `examples/sedona_spark/tests/` run it on real Spark in
the `sedona` image (CI job `sedona-spark`):

```bash
docker build -f Dockerfile.sedona -t dggs_stream-sedona .
docker run --rm dggs_stream-sedona sh -c \
  "pip3 install --break-system-packages pytest && python3 -m pytest -v /opt/dggs_stream/examples/tests"
```

Partition specs carry a fingerprint of the raster layout (dimensions, chunking,
georeferencing, bbox), so a worker that opens a raster with another layout refuses the spec.
`limit` and the long categorical format cannot be combined with a partition, and a
projection must keep `h3_index` and `resolution`. Boundary rows are typically a few percent
of the output and shrink as partitions grow; partitioning adds no measurable CPU (see
[Partitioned scans](core.md#partitioned-scans)).

### Categorical aggregation

```python
# Wide: majority class and dominance
wide = dggs_stream.read_raster("worldcover_2021.tif", 8, categorical=True)
wide.select(
    "h3_hex", "majority_class",
    (pl.col("majority_fraction") * 100).round(1).alias("dominance_pct"),
    "unique_classes", "total_count", "histogram",
)

# The histogram is JSON text keyed by class code
wide.with_columns(
    pl.col("histogram").str.json_path_match("$['10']").cast(pl.Float64).alias("forest_fraction"),
    pl.col("histogram").str.json_path_match("$['50']").cast(pl.Float64).alias("urban_fraction"),
).filter(pl.col("urban_fraction").is_not_null())

# Long: one row per (cell, class)
urban = (
    dggs_stream.scan_raster("worldcover_2021.tif", 8, categorical=True, format="long")
    .filter((pl.col("category") == 50) & (pl.col("fraction") >= 0.25))
    .sort("fraction", descending=True)
    .collect()
)
```

### PMTiles v3 export

```python
dggs_stream.raster_to_pmtiles("california_elevation.tif", "california_elevation.pmtiles", [6, 7, 8])
dggs_stream.raster_to_pmtiles("worldcover_2021.tif", "worldcover.pmtiles", [5, 6, 7, 8], categorical=True)

# Any H3-indexed Parquet file, e.g. a filtered scan written by Polars
dggs_stream.parquet_to_pmtiles("elevation_h3.parquet", "elevation.pmtiles", h3_column="h3_index")
```

### PyArrow, DuckDB and GeoPandas interop

```python
import duckdb
import geopandas as gpd

stream = dggs_stream.RasterArrowStream("elevation.tif", 8)

tbl = stream.to_pyarrow()                                  # pyarrow.Table
duckdb.sql("SELECT avg(mean) FROM tbl").show()            # DuckDB reads the Arrow table

df = stream.to_polars()
gdf = gpd.GeoDataFrame(
    df.drop("wkb").to_pandas(),
    geometry=gpd.GeoSeries.from_wkb(df["wkb"].to_list()),
    crs="EPSG:4326",
)
```

## Troubleshooting

| Symptom | Cause / fix |
| :--- | :--- |
| `RuntimeError: Could not locate libdggs_stream_python shared library` on first call | The native library is not next to the package. Install a wheel, run `cargo build --release -p dggs-stream-python` in the workspace, or set `DGGS_STREAM_PYTHON_LIB` to the library path. |
| `scan_raster` returns a `RasterArrowStream`, not a `LazyFrame` | Polars is not installed or is too old to have `polars.io.plugins.register_io_source`. Install the `polars` extra. |
| `RuntimeWarning: dggs_stream: N valid sample(s) were dropped ...` from `raster_to_parquet` / `raster_to_pmtiles` / `scan_raster` | Some valid pixels could not be placed because their coordinate transform failed or fell outside the valid latitude/longitude range. Check `crs`; see [transform-failure diagnostics](core.md#transform-failure-diagnostics). |
| `RuntimeError: ... No CRS detected ...` | Pass `crs="EPSG:…"`. See the README's [Troubleshooting](../../README.md#troubleshooting--common-pitfalls). |
| Need compaction or `h3_cell` pushdown | Not exposed in Python. Use [DuckDB](duckdb.md) (for example via `duckdb` from Python) or the Rust [DataFusion](datafusion.md) / [Core](core.md) APIs. |

## Native C ABI

The wrapper calls these exports from `libdggs_stream_python`
([`src/ffi/`](../../crates/dggs-stream-python/src/ffi/mod.rs)). All entry points catch panics and
return errors through an out-parameter string freed with `dggs_stream_py_free_string`.

| Export | Purpose |
| :--- | :--- |
| `dggs_stream_py_create_arrow_schema` / `_free_arrow_schema` | Build / release an `ArrowSchema` |
| `dggs_stream_py_create_arrow_stream` / `_free_arrow_stream` | Build / release an `ArrowArrayStream` for a scan |
| `dggs_stream_py_open_batch_reader` / `_batch_reader_next` / `_free_batch_reader` | Incremental reader; each `next` returns a one-batch stream, or null at EOF |
| `dggs_stream_py_scan_json` | Whole scan as a JSON object of columns (`to_dict`) |
| `dggs_stream_py_plan_partitions`, `dggs_stream_py_create_arrow_{schema,stream}_partitioned`, `dggs_stream_py_open_batch_reader_partitioned` | Partition planning and partitioned scans (`partition=`) |
| `dggs_stream_py_merge_partials` | Merge partial rows; moves its input `ArrowArrayStream` |
| `dggs_stream_py_create_arrow_stream_v2`, `dggs_stream_py_open_batch_reader_v2`, `dggs_stream_py_scan_json_v2`, `dggs_stream_py_create_arrow_schema_v2`, `dggs_stream_py_plan_partitions_v2`, `dggs_stream_py_merge_partials_v2` | The calls above with an optional partition and an `options` JSON object (`band`, `sampling`, `overlap_rule`, `spectral_formula`, `*_band`, `quantiles`, `remap`, `min_count`, `min_mean`, `max_mean`, `min_majority_fraction`); unknown keys are rejected. Older symbols keep their signatures, and `dggs_stream_py_merge_partials` applies no filters or quantiles |
| `dggs_stream_py_quantile_columns` | Output column names of a `quantiles` specification (JSON array) |
| `dggs_stream_py_parquet_to_pmtiles` | Parquet → PMTiles transcoding (returns `-1` on error) |
| `dggs_stream_py_h3_*` | Scalar helpers |
| `dggs_stream_py_export_parquet`, `dggs_stream_py_export_pmtiles` | File exporters (return `-1` on error) |

PyCapsule destructors free the native struct. A consumer that imports the stream moves it
out and clears `release`; otherwise dropping the struct calls `release`, so an unconsumed
capsule does not leak the scan.

## Wheels

[`pyproject.toml`](../../crates/dggs-stream-python/pyproject.toml) uses `hatchling` with a
custom hook ([`hatch_build.py`](../../crates/dggs-stream-python/hatch_build.py)) that:

1. Runs `cargo build --release --locked -p dggs-stream-python -p dggs-stream-cli` against the
   workspace two directories up (override with `DGGS_STREAM_WORKSPACE`), honouring
   `CARGO_BUILD_TARGET` or a single-arch macOS `ARCHFLAGS`.
2. Places `libdggs_stream_python.{so,dylib}` / `dggs_stream_python.dll` inside the `dggs_stream`
   package and `dggs-stream-cli` in the wheel's scripts directory (so it lands on `PATH`).
3. Tags the wheel `py3-none-<platform>`.

[`.github/workflows/wheels.yml`](../../.github/workflows/wheels.yml) runs `cibuildwheel` for
manylinux_2_28 x86_64 and aarch64, macOS arm64 and x86_64 (cross-compiled, tested under
Rosetta), and Windows x86_64, then installs each wheel and runs
[`tests/wheel_smoke.py`](../../crates/dggs-stream-python/tests/wheel_smoke.py). Wheels are
uploaded as workflow artifacts; nothing is published to PyPI yet.

## Testing

```bash
cargo test -p dggs-stream-python                      # Rust-side unit tests
cargo test -p dggs_stream --test it multi_engine_e2e           # spawns python3 against the built cdylib
```
