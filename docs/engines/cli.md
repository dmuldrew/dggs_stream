# CLI Deep Dive: `dggs-stream-cli`

[← Engines Overview](README.md) · [← Back to README](../../README.md)

`dggs-stream-cli` ([`crates/dggs-stream-cli`](../../crates/dggs-stream-cli)) is a single
executable that streams raster aggregations to stdout or writes Parquet/PMTiles files. It
is meant for shell pipelines, batch jobs and engines that can run an external command,
such as ClickHouse executable table functions.

The same source ([`src/bin/dggs_stream.rs`](../../crates/dggs-stream-cli/src/bin/dggs_stream.rs))
builds two binaries:

| Binary | Built by |
| :--- | :--- |
| `dggs-stream-cli` | `cargo build --release -p dggs-stream-cli`; also installed onto `PATH` by the `dggs-stream` Python wheel |
| `dggs-stream` | the root crate (`cargo build --release` / `cargo install --path .`) |

## Usage

```text
dggs-stream-cli ipc     <input> [options]        Stream RH3IPC01 batches to stdout or -o <file>
dggs-stream-cli jsonl   <input> [options]        Stream JSONEachRow (NDJSON) to stdout or -o <file>
dggs-stream-cli polyfill <input.geojson|-> [options]  Write feature-to-cell rows as JSON Lines
dggs-stream-cli parquet <input> -o <out> [opts]  Write GeoParquet
dggs-stream-cli pmtiles <input> -o <out> [opts]  Write a PMTiles v3 archive
dggs-stream-cli parquet-to-pmtiles <in.parquet> -o <out> [--h3-column NAME]
                                               Transcode any H3-indexed Parquet file to PMTiles v3
dggs-stream-cli plan    <input> [options]        Print partition specs (JSON Lines), see Partitioned scans
dggs-stream-cli merge   <part>... [options]      Combine partitioned `ipc` outputs into final cells
```

For raster commands, `<input>` can be a path, glob, comma-separated list or VRT (it may also be given with
`-i/--input`). Mosaic overlaps use the `cutline` rule.

`polyfill` reads GeoJSON, not rasters, and takes only `--grid`, `-r/--resolution` (a single
resolution), `--containment` and `-o`; it rejects every other option. In the table below, "all"
means the raster commands.

| Option | Applies to | Meaning |
| :--- | :--- | :--- |
| `--grid <h3\|a5\|s2\|healpix>` | all | Grid to index onto (default `h3`); the index columns are `{grid}_index` / `{grid}_hex` |
| `-r, --resolutions <LIST>` (alias `--resolution`) | all; `polyfill` takes one resolution as `-r/--resolution` | Resolutions of the grid, e.g. `8` or `7,8,9` (default `8`); H3 0–15, A5 and S2 0–30, HEALPix 0–29. `auto`, `auto+N`, `auto-N`: the resolution whose cells match the pixel size ([Grids](../grids.md#automatic-resolution)) |
| `-c, --categorical` | all | Categorical aggregation |
| `--format <wide\|long>` | `ipc`, `jsonl` | Categorical layout (default `wide`) |
| `--crs <CRS>` (alias `--source-crs`) | all | Source CRS override, e.g. `EPSG:4326` |
| `--nodata <FLOAT>` | all | NoData override |
| `--nodata-policy <omit\|emit>` | all (ground-area scans require `omit`) | `emit` outputs every cell a pixel falls in, with count 0 and null statistics where every sample is NoData |
| `--emit-nodata-value <NUM>` | all | Under `emit`, the value of those statistics instead of null |
| `--decimals <N>` | all | Round floating-point statistics to `N` decimals (0–15) |
| `--transfer <assign_centers\|interpolate\|overlay\|auto>` | all except partitioned `ipc` | How pixels map onto cells ([Transfer Modes](../transfer-modes.md)). `auto` picks bilinear `interpolate` for continuous scans with cells finer than the pixels and weighted `overlay` otherwise (categorical scans always overlay); with a non-`center` `--sampling` it picks `assign_centers` in place of `overlay`; it is reported on stderr |
| `--interpolation <nearest\|bilinear\|bicubic\|lanczos>` | `--transfer interpolate` | Interpolation (default `bilinear`) |
| `--sampling <center\|rgss\|5point\|gaussian\|hex\|8rooks\|9point\|16point>` | `assign_centers`, `--transfer interpolate`, `--transfer auto` | Quadrature sampling pattern ([Super-Sampling](../super-sampling.md)): sub-pixel for `assign_centers`, sub-cell polygon sampling for `interpolate` (`--transfer overlay` rejects non-`'center'`) |
| `--overlay-weighted <true\|false>` | `--transfer overlay` | `false` counts every touched pixel once |
| `--valid-coverage-threshold <0..1>` | `--transfer overlay` | Minimum valid fraction of selected overlap, measured by area even for unweighted statistics. Mass-preserving scans are exempt. |
| `--mass-preserve` | `--transfer overlay` | Normalise each pixel's weights so `sum` adds up to the raster's total |
| `--histogram <values\|N:min:max\|e0,e1,...>` | `ipc`, `jsonl` | Structured per-cell histogram (implies `-c`) |
| `--histogram-weight <count\|area>` | with `--histogram` | `area` weights by overlap and runs `--transfer overlay` |
| `--histogram-normalize <none\|fraction\|density>` | with `--histogram` | `density` needs bins |
| `--decode-workers <N>` | all | Chunk decode threads (default `(cpus - 2)` clamped to 4–12) |
| `--aggregation-workers <N>` | `--transfer overlay`, `--transfer interpolate` | Threads clipping and accumulating cells; `1` runs serially, the default uses the ambient Rayon pool |
| `--profile` | all | Print a runtime profile on stderr ([Scan Options](../scan-options.md#runtime-profile)) |
| `--raw-values` | all | Aggregate stored values without GDAL band scale/offset calibration |
| `--bbox <min_lon,min_lat,max_lon,max_lat>` | all | WGS84 bounding box |
| `--ground-area` | `ipc`, `jsonl` | Compute area-weighted continuous statistics for one single-band, north-up EPSG:4326 raster. With `--transfer overlay`, integrate over partial pixels. |
| `--compression <codec>` | `parquet` | snappy (default), zstd, lz4 (raw), gzip, brotli or none. Applies to each file in a parent-partitioned dataset and to multiband output. |
| `--bands <list or all>` | `ipc`, `jsonl`, `parquet` | Select one-based bands of an interleaved or band-separated raster with center assignment or `--transfer overlay`, e.g. `--bands 3,1 --columns h3_hex,wkb,band_3_mean,band_1_count`. |
| `--columns <c1,c2,…>` | `ipc`, `jsonl`, multi-band `parquet` | Column projection, in the given order. Include `wkb` when projecting GeoParquet. |
| `--limit <N>` | `ipc`, `jsonl`, multi-band `parquet` | Maximum rows |
| `--partition <SPEC\|@FILE>` | `ipc` | Scan one partition (see [Partitioned scans](#partitioned-scans)) |
| `--partition-index <K>` | `ipc` | Line `K` of the plan file given as `--partition @FILE` |
| `-o, --output <PATH\|->` | all | Output file; `-` or omitted means stdout (required for `parquet`, `pmtiles` and `parquet-to-pmtiles`, which cannot write to stdout) |
| `--h3-column <NAME>` (alias `--h3-col`) | `parquet-to-pmtiles` | Cell column, integer or hex string, matched case-insensitively (default: the first `{grid}_index` / `{grid}_hex` column, else `h3`, `cell` or `hex`). The grid is read from the column's name (`s2_index`, `healpix_hex`, ...), H3 otherwise |
| `-h, --help` | | Help |

Arguments are validated strictly: unknown flags, missing values, resolutions outside
the grid's range, malformed numbers, a bbox without exactly four numbers, an unknown column name
and a second positional input are all errors. Errors go to stderr with exit status `1`.
Every subcommand also prints a warning to stderr when valid samples were dropped because
their coordinate transform failed (see [transform-failure diagnostics](core.md#transform-failure-diagnostics)).

Column names come from the core schemas (see the
[overview](README.md#output-schemas)); `--columns` with a name that does not
exist in the selected schema fails before any raster is read.

## Vector polygon fill

```sh
dggs-stream-cli polyfill parcels.geojson --grid s2 -r 12 --containment covers -o cells.jsonl
```

Supply WGS84 longitude/latitude GeoJSON polygons, multipolygons, Features or FeatureCollections.
Use a single Feature per line and `-` for stdin to process large inputs one feature at a time.
A FeatureCollection is read as one JSON document. The command writes one row per feature and
cell with `feature_id`, `resolution`, and the grid's numeric and hexadecimal index columns.
It preserves string and numeric feature IDs and uses the zero-based input feature position
when the ID is missing. Null geometry yields zero rows. Cells are sorted and deduplicated
within each feature. Overlapping features retain their separate rows.

Select one integer resolution (default 8) and one of the four [containment modes](../grids.md#polygon-fill)
with `--containment` (default `center`). The default grid is H3.

## Output Formats

### `jsonl`

One JSON object per row. Non-finite floats and nulls become `null`; `wkb` is a lowercase
hex string. Keys are emitted in alphabetical order (not `--columns` order); consumers that
match by name, such as ClickHouse `JSONEachRow`, are unaffected.

```bash
dggs-stream-cli jsonl dem.tif -r 8 --columns h3_hex,mean,count --limit 3
# {"count":64.0,"h3_hex":"8828308281fffff","mean":42.0}
```

### `ipc`

`RH3IPC01`, the workspace's binary columnar stream. It is **not** Apache Arrow IPC; read it
with `dggs_stream_core::columnar::ipc::read_ipc_stream` or against the layout in the
[Core deep dive](core.md#rh3ipc01-stream-and-ndjson). Use `jsonl`, or the Python Arrow
stream, when the consumer expects standard formats.

### `parquet` and `pmtiles`

`parquet` always writes GeoParquet 1.1 (WKB geometry, see [cell geometry](../sql-api-reference.md#cell-geometry), + `geo` metadata) and reports nothing
on stdout. `pmtiles` writes one zoom range per requested resolution (see
[PMTiles & Multi-Resolution](../pmtiles.md)).

```bash
dggs-stream-cli parquet dem.tif -r 7,8 -o dem_h3.parquet
dggs-stream-cli pmtiles landcover.tif -c -r 5,6,7,8 -o landcover.pmtiles
```

#### Hive-partitioned H3 output

Pass `--partition-by-parent-res R` to write a new directory. Choose an H3
resolution no finer than the coarsest requested output. For example, this
places resolution 8 and 9 cells in directories named `h3_06=<parent hex>`.

```bash
dggs-stream-cli parquet dem.tif -r 8,9 --partition-by-parent-res 6 -o dem_by_parent
```

DuckDB can filter on the Hive column before opening individual files.

```sql
SELECT h3_index, avg_value
FROM read_parquet('dem_by_parent/*/*.parquet', hive_partitioning = true)
WHERE h3_06 = '862830827ffffff';
```

Each part has the single-file export's columns and GeoParquet metadata, with
bounds for its own geometries. A parent can contain several part files. Coarser
parents reduce the number of small files. The CLI stages the complete dataset
before publishing it and requires a fresh output path for each run. Empty
results include one schema-only part. Categorical and selected-band exports use
the same layout.

### `parquet-to-pmtiles`

Turns any Parquet file with an H3 column into a PMTiles v3 archive, one hexagon feature per
row with the other columns as properties, the same transcoder as DuckDB's
[`h3_parquet_to_pmtiles`](../sql-api-reference.md#parquet-pmtiles-v3-export-h3_parquet_to_pmtilesparquet_path-output_pmtiles-)
and Python's `parquet_to_pmtiles`. The input can come from `dggs-stream-cli parquet`, or from
any engine that writes Parquet (DuckDB `COPY`, Spark, Polars), so cells can be filtered,
joined or aggregated there before tiling. Zoom levels follow the cells' resolutions (see
[PMTiles & Multi-Resolution](../pmtiles.md)). Raster options (`-r`, `--crs`, ...) do not
apply and are rejected.

Rows whose H3 value is null or not a valid cell are skipped, and their count is reported
on stderr; nothing is written to stdout.

```bash
dggs-stream-cli parquet-to-pmtiles census_h3.parquet -o census_h3.pmtiles --h3-column cell_id
```

## Recipes

These mirror the [DuckDB SQL recipes](duckdb.md#sql-usage--recipes) for the options the CLI
exposes.

### Basic continuous aggregation

```bash
dggs-stream-cli jsonl elevation.tif -r 8 --columns h3_index,h3_hex,mean,count,min,max,sum
```

### CRS, NoData and bounding box

```bash
dggs-stream-cli jsonl temperature_global.tif -r 9 \
  --crs EPSG:4326 --nodata -9999 \
  --bbox -122.50,37.70,-122.35,37.85 \
  --columns h3_hex,mean,count
```

### Multiple resolutions in one pass

```bash
dggs-stream-cli jsonl elevation.tif -r 6,7,8 --columns resolution,h3_hex,mean \
  | jq -c 'select(.resolution == 8)'
```

### Other grids and pixel-sized cells

```bash
dggs-stream-cli jsonl elevation.tif --grid s2 -r 15 --columns s2_index,mean
dggs-stream-cli jsonl elevation.tif --grid healpix -r auto      # cells about one pixel in size
dggs-stream-cli parquet elevation.tif --grid a5 -r auto-1 -o a5.parquet
```

### Fine cells, area weighting, histograms and empty cells

```bash
# Cells finer than the pixels: interpolate a value at each cell centre
dggs-stream-cli jsonl elevation.tif -r auto+2 --transfer interpolate --interpolation bicubic
# Crossover cells or intra-cell spread: sub-cell quadrature with bicubic interpolation
dggs-stream-cli jsonl elevation.tif -r auto --transfer interpolate --interpolation bicubic --sampling rgss
# Population counts: split each pixel across the cells it overlaps, keeping the total
dggs-stream-cli parquet population.tif -r 7 --transfer overlay --mass-preserve -o pop.parquet
# Per-cell histogram of a continuous raster in four bins, as fractions
dggs-stream-cli jsonl elevation.tif -r 8 --histogram 4:0:2000 --histogram-normalize fraction
# Keep all-NoData cells (count 0), round statistics, and see where the time went
dggs-stream-cli jsonl elevation.tif -r 8 --nodata-policy emit --decimals 2 --profile
```

### Sampling the output

```bash
dggs-stream-cli jsonl elevation.tif -r 8 --limit 5      # stops aggregation after 5 rows
```

### Categorical aggregation

```bash
# Wide: majority class, dominance and the JSON histogram
dggs-stream-cli jsonl worldcover_2021.tif -c -r 8 \
  --columns h3_hex,majority_class,majority_fraction,unique_classes,total_count,histogram

# Histogram lookups with jq (histogram is a JSON string)
dggs-stream-cli jsonl worldcover_2021.tif -c -r 8 --columns h3_hex,histogram \
  | jq -c '{h3_hex, urban: (.histogram | fromjson | .["50"])} | select(.urban != null)'

# Long: one row per (cell, class)
dggs-stream-cli jsonl worldcover_2021.tif -c --format long -r 8 \
  --columns h3_hex,category,count,fraction \
  | jq -c 'select(.category == 50 and .fraction >= 0.25)'
```

### Export to GeoParquet and PMTiles

```bash
dggs-stream-cli parquet elevation.tif -r 8 -o elevation_h3.parquet
dggs-stream-cli pmtiles california_elevation.tif -r 6,7,8 -o california_elevation.pmtiles
dggs-stream-cli pmtiles worldcover_2021.tif -c -r 5,6,7,8 -o worldcover.pmtiles

# Filter or join in another engine, then tile the result
dggs-stream-cli parquet elevation.tif -r 8 -o elevation_h3.parquet
duckdb -c "COPY (SELECT h3_index, mean FROM 'elevation_h3.parquet' WHERE mean > 500)
           TO 'highlands.parquet'"
dggs-stream-cli parquet-to-pmtiles highlands.parquet -o highlands.pmtiles
```

### Remote COG & S3 streaming

```bash
dggs-stream-cli jsonl https://example.com/rasters/cog_elevation.tif -r 8 --limit 10
dggs-stream-cli parquet s3://my-geospatial-bucket/landsat/scene_01.tif -r 9 -o scene_01_h3.parquet
```

### Multi-file mosaics

Quote globs so the shell does not expand them; overlaps use the `cutline` rule.

```bash
dggs-stream-cli jsonl 'tiles/tile_*.tif' -r 8
dggs-stream-cli jsonl tile_west.tif,tile_east.tif -c -r 8
```

### Joins

The CLI has no join of its own; stream into an engine that does. With DuckDB (no extension
needed):

```bash
dggs-stream-cli jsonl california_elevation.tif -r 8 --columns h3_index,mean | duckdb -c "
  SELECT r.h3_index, r.mean, p.total_population
  FROM read_ndjson_auto('/dev/stdin') r
  JOIN 'population_h3.parquet' p USING (h3_index)
  WHERE r.mean > 500"
```

## Integrations

### ClickHouse executable table function

```sql
SELECT h3_hex, mean, count
FROM executable(
    'dggs-stream-cli jsonl /data/dem.tif -r 8 --columns h3_hex,mean,count',
    JSONEachRow,
    'h3_hex String, mean Float64, count Float64'
)
ORDER BY count DESC
LIMIT 10;
```

For ClickHouse's `executable`, the command must be in the server's `user_scripts`
directory (or referenced through a wrapper script there).

### Partitioned scans

One raster or mosaic can be split across processes (`xargs -P`, GNU parallel, batch-job
arrays), each reading a disjoint set of TIFF tiles or strips. The result has exactly the
cells and values of a single scan.

```bash
# 1. Plan from metadata: one JSON spec per line, ~256 tiles/strips each.
dggs-stream-cli plan dem.tif --target-chunks 256 -o plan.jsonl

# 2. Scan each partition (any process or machine that can read dem.tif).
seq 0 $(( $(wc -l < plan.jsonl) - 1 )) | xargs -P 8 -I{} \
  dggs-stream-cli ipc dem.tif -r 8 --partition @plan.jsonl --partition-index {} -o parts/{}.rh3

# 3. Merge: final rows pass through, cells split across partitions are combined.
dggs-stream-cli merge parts/*.rh3 -o dem_r8.rh3          # or --jsonl
```

- `plan` takes `--target-chunks <N>` (default 256), `--crs`, `--bbox` and `-o`. Plan and
  scan with the same input, `--crs` and `--bbox`: a raster with another layout, or a scan
  with another bbox, refuses the spec (`different mosaic`). Rasters that differ only in
  pixel values share a layout, so one plan serves them all.
- `--partition` takes a spec inline (one line of the plan, e.g. from
  `xargs -d '\n' -I{} … --partition '{}'`) or `@FILE`: a file with one spec, or a plan file
  with `--partition-index`.
- A partition file is a small `RH3PART1` header (magic, `u32` length, JSON naming the
  partition spec, the input and the value-changing options) followed by the normal
  `RH3IPC01` stream with a nullable `partial_state` column: null rows are final cells, set
  rows are this partition's share of a cell that other partitions also reach. Only `ipc`
  output is accepted (that is what `merge` reads);
  `--limit`, `--format long`, `parquet` and `pmtiles` are refused with `--partition`, and
  `--columns` must keep `h3_index` and `resolution`.
- `merge` first checks the headers and columns of all files, and refuses before writing
  anything unless they are every partition of one plan exactly once (it names missing or
  repeated partitions), scanned from the same input with the same resolutions, `--crs`,
  `--nodata`, `--raw-values`, `--bbox`, categorical mode and `--columns`. It then streams
  the files one batch at a time, writing every final row, then the merged rows; only
  partial rows are kept in memory.
  Categorical input is recognised from its columns; pass `-c` when the projection has
  only shared columns (`h3_index`, `h3_hex`, `resolution`, `wkb`). The output's columns
  are the scan's, without `partial_state`, so it can be used as an unpartitioned `ipc`
  result (rows in a different order).

### Shell pipelines

```bash
dggs-stream-cli jsonl dem.tif -r 8 --columns h3_index,mean | duckdb -c \
  "SELECT count(*), avg(mean) FROM read_ndjson_auto('/dev/stdin')"

dggs-stream-cli jsonl dem.tif -r 8 | jq -c 'select(.count > 100)'
```

## Library API

The binary is a thin wrapper over [`src/lib.rs`](../../crates/dggs-stream-cli/src/lib.rs):

| Item | Purpose |
| :--- | :--- |
| `parse_stream_args(args, default_format)` | Parse arguments into `CliStreamOptions` and an optional output path |
| `CliStreamOptions` | `input_path`, `resolutions`, `categorical`, `categorical_format`, `source_crs`, `nodata`, `apply_scale_offset` (true), `bbox`, `columns`, `batch_size` (2048), `limit`, `output_format`, `partition` |
| `stream_raster_to_writer(&opts, &mut impl Write)` | Stream IPC or JSONL to any writer; returns rows written |
| `export_raster_to_parquet(...)`, `export_raster_to_pmtiles(...)` | File exports |
| `plan_raster_partitions(input, target_chunks, crs, bbox)`, `write_plan(&plan, w)` | Plan partitions; write them as JSON Lines |
| `read_partition_spec(spec, index)` | Parse a spec, or `@FILE` (with `index` for a plan file); sets `CliStreamOptions::partition` |
| `merge_partition_files(&CliMergeOptions, w)` | Merge partition files; `parse_plan_args` / `parse_merge_args` parse the subcommands |

## Testing

```bash
cargo test -p dggs-stream-cli        # argument parsing, IPC round trip, JSONL, plan/partition/merge vs one scan
cargo test -p dggs_stream --test it multi_engine_e2e # runs the built binary for all four subcommands
```

### Ground-area-weighted statistics

For a north-up, single-band WGS84 raster, this command assigns each valid pixel
to the grid cell containing its center. Its ellipsoidal footprint supplies the
weight. The same option works with H3, A5, S2 and HEALPix.

```sh
dggs-stream-cli jsonl temperature.tif --ground-area --grid h3 -r 8 \
  --columns h3_hex,pixel_count,retained_area_m2,area_weighted_mean
```

Each row includes the grid-specific index and hex ID, resolution, valid pixel
count, retained area in square metres, area-weighted sum (value × m²), and
area-weighted mean (original units). The mean divides the weighted sum by
the retained area. Ground-area scans require `--nodata-policy omit` (the default). NoData pixels contribute neither count nor area. Use
`--ground-area` with `ipc` to stream the same columns in binary form.
