# dggs-stream

Aggregate GeoTIFF / Cloud-Optimized GeoTIFF rasters into [H3](https://h3geo.org) hexagonal
cells, streamed as Arrow and Polars data. A Rust engine does the work; this package bundles
it for Python along with the `dggs-stream-cli` command-line tool.

```bash
pip install "dggs-stream[polars]"
```

## Python / Polars

```python
import polars as pl
import dggs_stream

# Lazy Polars scan with projection, predicate and row-limit pushdown
lf = dggs_stream.scan_raster_h3("dem.tif", resolutions=[7, 8])
lf.filter(pl.col("count") > 10).select("h3_index", "mean").collect()

# Any Arrow PyCapsule consumer works too (pyarrow, DuckDB, ...)
stream = dggs_stream.RasterH3ArrowStream("landcover.tif", 8, categorical=True)
for batch in stream.iter_batches(batch_size=50_000):
    ...

# H3 helpers and direct exports
dggs_stream.h3_to_string(0x8828308281FFFFF)          # '8828308281fffff'
dggs_stream.raster_to_parquet("dem.tif", "dem.parquet", resolutions=8)
dggs_stream.raster_to_pmtiles("dem.tif", "dem.pmtiles", resolutions=8)
```

## Command line

```bash
dggs-stream-cli jsonl dem.tif -r 8 --columns h3_index,mean
dggs-stream-cli parquet dem.tif -r 7,8 -o dem.parquet
```

Run `dggs-stream-cli --help` for all options.

## Wheels

Each wheel is `py3-none-<platform>` (any Python 3.9+): manylinux_2_28 x86_64 and aarch64,
macOS arm64 and x86_64, and Windows x86_64. Building from source requires a Rust toolchain.

Source and the DuckDB, DataFusion and PostgreSQL integrations:
<https://github.com/dmuldrew/dggs_stream>

## Grids

Every scan, export and scalar helper takes `grid`: `"h3"` (default), `"a5"`, `"s2"` or
`"healpix"`. The cell columns are named for the grid (`{grid}_index`, `{grid}_hex`), and
projection, filter and limit pushdown work the same on each.

```python
import dggs_stream

lf = dggs_stream.scan_raster("dem.tif", 14, grid="healpix")   # same as scan_raster_h3(..., grid=)
lf.filter(pl.col("count") >= 4).select("healpix_index", "mean").collect()

dggs_stream.raster_to_parquet("dem.tif", "cells.parquet", 15, grid="s2")
dggs_stream.parquet_to_pmtiles("cells.parquet", "cells.pmtiles")          # grid from `s2_index`
dggs_stream.parquet_to_pmtiles("other.parquet", "o.pmtiles", cell_column="id", grid="a5")

cell = dggs_stream.lonlat_to_cell(-122.42, 37.77, 12, grid="s2")
dggs_stream.cell_to_parent(cell, 8, grid="s2")
```

Cell ids are unsigned 64-bit integers on every grid (S2 ids on faces 4 and 5 use the
top bit).
