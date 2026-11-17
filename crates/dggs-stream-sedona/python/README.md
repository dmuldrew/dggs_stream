# dggs-stream-sedona (Python)

dggs_stream rasters as [SedonaDB](https://github.com/apache/sedona-db) tables.
`RasterH3Table` implements SedonaDB's `__sedonadb_table_provider__` protocol, so
`create_data_frame` scans it directly, with projection, `LIMIT`, threshold and
spatial filter pushdown. `scalar_functions()` exports the dggs_stream SQL functions
(`h3_polygon_to_cells`, `h3_to_wkb`, `h3_cell_to_parent`, ...) as native SedonaDB kernels.
`write_pmtiles()` writes query results (or any Arrow stream) as a PMTiles v3 archive of
H3 hexagons.

```python
import sedonadb
import dggs_stream_sedona
from dggs_stream_sedona import RasterH3Table

sd = sedonadb.connect()
sd.register(dggs_stream_sedona.scalar_functions())
sd.create_data_frame(RasterH3Table.continuous("dem.tif", [7, 8])).to_view("dem")
sd.sql("""
    SELECT h3_index, mean
    FROM dem
    WHERE count >= 4
      AND ST_Intersects(wkb, ST_SetSRID(ST_GeomFromWKT('POINT (-122.42 37.77)'), 4326))
""").show()

# Polyfill a polygon and join on the shared H3 index
sd.sql("""
    SELECT d.h3_index, d.mean
    FROM (SELECT unnest(h3_polygon_to_cells(
              ST_SetSRID(ST_GeomFromWKT('POLYGON ((-122.45 37.75, -122.40 37.75, -122.40 37.80, -122.45 37.75))'), 4326),
              8, 'covers')) AS h3_index) AS aoi
    JOIN dem AS d ON d.h3_index = aoi.h3_index
""").show()

# Tile any query: one hexagon feature per row, the other columns as properties
dggs_stream_sedona.write_pmtiles(sd.sql("SELECT h3_index, mean FROM dem WHERE count >= 4"), "dem.pmtiles")
```

- `RasterH3Table.continuous(source, resolutions=8, *, crs=None, nodata=None, band=None, bbox=None, grid=None)`
- `RasterH3Table.categorical(source, resolutions=8, *, crs=None, nodata=None, bbox=None, grid=None)`
- `resolutions` is an int, a list of ints, or `"auto"` (`"auto+1"`, `"auto-2"`, ...): the
  resolution whose cells are closest in area to the raster's ground pixels.
- `grid` is `"h3"` (default), `"a5"`, `"s2"` or `"healpix"`. The cell columns are named for
  it (`s2_index`, `s2_hex`, ...) and the scalar functions exist per grid (`s2_to_wkb`,
  `a5_cell_to_parent`, `healpix_polygon_to_cells`, `h3_lonlat_to_cell`, ...).
- `scalar_functions()`: every function under its name and aliases; pass it to
  `sd.register`, or iterate and register single `ScalarFunction`s
- `write_pmtiles(data, path, *, cell_column=None, grid=None, min_zoom=None, max_zoom=None, memory_budget_mb=None)`:
  `data` is anything exposing `__arrow_c_stream__` (SedonaDB DataFrame, PyArrow table, Polars
  DataFrame). The cell column is `cell_column` (`h3_column` still works), else the first of
  `{h3,a5,s2,healpix}_{index,hex}`, a bare grid name, `cell` or `hex`, else the first column;
  the grid is `grid`, else the one the column is named for, else H3. The column may be unsigned or signed 64-bit, or hex strings; null cells are skipped and
  invalid ones raise `ValueError`. Binary columns such as `wkb` are not written as properties.
  Returns the number of features written. In Rust and DataFusion SQL the same writer is
  `COPY (query) TO 'cells.pmtiles'`.

The capsule carries SedonaDB's C extension ABI. This build targets `sedonadb` 0.5.0.

## Build and test

```bash
cd crates/dggs-stream-sedona/python
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop --release --extras test
pytest
```
