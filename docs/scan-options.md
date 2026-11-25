# Scan Options: Transfer Modes, Histograms, NoData Policy, Rounding and Profiling

[← Back to README](../README.md)

These options work the same way on every grid ([Grids](grids.md)). Each engine spells them
in its own way; the [table at the end](#option-names-by-engine) lists every spelling.

## Transfer modes

`transfer` chooses how pixels become cell values. [Transfer Modes](transfer-modes.md) covers each
mode in depth; in short:

| Mode | What a cell gets | When to use it |
| :--- | :--- | :--- |
| `auto` | One of the three below, chosen from the cell-to-pixel area ratio and the other options | You'd rather not choose |
| `assign_centers` (default) | Every pixel whose centre it holds (or sub-pixel samples, see [Super-Sampling](super-sampling.md)) | Cells about the size of pixels, where it is the fastest kernel ([comparison](transfer-modes.md#comparing-the-modes)) |
| `interpolate` | The value interpolated at its centre (`interpolation`: `nearest`, `bilinear`, `bicubic`, `lanczos`) | Cells finer than the pixels |
| `overlay` | Every pixel it overlaps, weighted by the fraction it covers | Many pixels per cell, exact edges, conserved totals |

The [overlay modes](transfer-modes.md#the-overlay-modes) refine `overlay`:
`overlay_weighted = false` counts every touched pixel once, `mass_preserve` makes the cells'
`sum` add up to the raster's total, `ground_area` (on EPSG:4326 rasters) weights by true m², and
`valid_coverage_threshold` drops cells with too little valid data. Categorical scans accept
`overlay` (class areas) and `interpolate` with `nearest` only.

Transfer modes exclude compaction, spectral formulas and sub-pixel sampling; partitioned scans
support only single-band continuous weighted overlay
([Spark](engines/spark.md#partitioned-weighted-overlay)). PostgreSQL does not expose them yet.
The code is in [`transfer/`](../crates/dggs-stream-core/src/transfer): `window.rs` reads bands of
rows, `interp.rs` holds the interpolation kernels, `clip.rs` does the exact polygon–pixel overlap,
and `stream.rs` holds the band scheduler and the open-cell bookkeeping.

## Histograms

A `HistogramSpec` adds a per-cell histogram to a categorical scan. It has three parts:

| Part | Values |
| :--- | :--- |
| Classes (`histogram`) | `values`: one class per pixel value. `N:min:max`: N equal bins. `e0,e1,...`: explicit, ascending bin edges (for continuous rasters) |
| Weight (`histogram_weight`) | `count` (the default), or `area`: each pixel's overlapped area, in pixels. `area` runs the weighted `overlay` mode and refuses `overlay_weighted=false` |
| Normalisation (`histogram_normalize`) | `none` (the default), `fraction` (weights sum to 1), or `density` (bins only: weights divided by the bin width, so they integrate to 1) |

How values are binned:
- Calibrated values (scale and offset applied) are binned.
- A bin holds `lower <= v < upper`, and the last bin also holds its upper edge.
- Values outside the edges are not counted.
- At most 4096 bins are accepted.

A binned histogram streams through the categorical kernel with the bin index as the class, so
it keeps the single-pass memory profile.

With a spec, the `histogram` column is a list of structs, `{class, weight}` or
`{lower, upper, weight}`; a binned histogram lists every bin, empty ones with weight 0. Without
a spec it stays JSON text of class fractions.

| Engine | `histogram` column type with a spec |
| :--- | :--- |
| Python / Polars | `List(Struct)` |
| DataFusion / SedonaDB | `List<Struct>` |
| CLI (`ipc`, `jsonl`) | list of structs in `RH3IPC01`, the workspace's own binary stream (not Arrow IPC; see the [CLI output formats](engines/cli.md#ipc)); arrays of objects in JSON Lines |
| DuckDB | `VARCHAR` holding the histogram as JSON: `{"class": weight}` or `[[lower, upper, weight], ...]` |

Histograms are not available in the Parquet and PMTiles exports, in partitioned scans, or in
PostgreSQL. The spec lives in [`histogram.rs`](../crates/dggs-stream-core/src/histogram.rs). The
nested column type is `ArrowDataType::ValueHistogram` / `BinHistogram` with
`ColumnArray::Histogram`, carried through the Arrow C Data Interface, the `RH3IPC01` stream and
JSON Lines.

## NoData policy and rounding

Scans read full-resolution, 1-bit TIFF transparency masks from image directories or
SubIFDs. A zero bit excludes that pixel from every band, even when its stored value is
valid. A set bit keeps the usual per-band NoData and alpha checks, so valid black pixels
still contribute. Mask strips and tiles are decoded on demand for local and remote
rasters. Supported mask compression methods are uncompressed, Deflate, LZW, and ZSTD.

| Option | Values | Effect |
| :--- | :--- | :--- |
| `nodata_policy` | `omit` (default), `emit` | `omit` skips NoData pixels and drops cells without a valid sample. `emit` returns every cell a pixel falls in; a cell with no valid sample has `count` 0 and NULL statistics (`mean`, `min`, `max`, or `majority_class` / `category`) |
| `emit_nodata_value` | a finite number | Under `emit`, the value those NULL statistics take instead |
| `decimals` | 0–15 | Rounds floating-point statistics half away from zero; counts and class codes are left alone |

Details:
- **Formats without NULLs.** Formats that can't hold a NULL statistic (the direct Parquet
  exporter, PMTiles properties) write NaN. A categorical Parquet export is the exception: its
  majority-class column is an integer and can hold neither NULL nor NaN, so under `emit` it
  requires an integer `emit_nodata_value` and refuses the export without one.
- **Limits of `emit`.**
  - It refuses `min_count` and compaction.
  - Chunks a sparse TIFF does not store are not output.
  - DuckDB's long categorical format does not return the empty cells.
  - The class `i64::MIN` stands for NoData pixels under `emit`, so a remap targeting it is
    refused.
- **`decimals` and filter pushdown.** Polars and SedonaDB do not push `mean` or
  `majority_fraction` predicates into the scan, because the scan compares unrounded values
  while the rows show rounded ones. The predicate is applied to the rows instead.
- **Partitioned CLI scans.** Partition files record these settings, and `merge` applies them
  to the boundary cells it builds. Partitions of scans with different settings don't merge.

The policy is `OutputPolicy` in
[`output_policy.rs`](../crates/dggs-stream-core/src/aggregator/output_policy.rs), set through
`MultiResolutionConfig`.

## Runtime profile

An opt-in record of where a scan's time went. It works in release builds, and a scan without
a profile takes no clock readings for it.

- **Phases.** `fetch`, `tiff_decode`, `kernel`, `horizon` (with `spill`), `stall` and
  `serialize` (Parquet and PMTiles writes). On the pixel-centre path, `kernel` is split into
  `crs_transform`, `grid_index` and `accumulate`; that split is a sampled estimate.
- **Times.** Wall time is summed over worker threads. CPU time is reported where the platform
  has per-thread clocks (Unix).
- **Counters.** Pixels read, samples indexed, cells emitted, chunks, stall share, spill runs,
  forced compaction flushes and peak bytes.
- **Overhead.** Timers wrap chunks, windows and batches, never individual pixels.

| Engine | How |
| :--- | :--- |
| CLI | `--profile` (report on stderr) |
| Python | `profile_raster(path, resolutions, ...)` returns `{"rows": ..., "profile": {...}}` |
| DuckDB | `profile := true` (report on stderr); embedders read `run_profile::last_report()` |
| DataFusion / SedonaDB | `with_profile(Arc<RunProfile>)` on the table providers |
| Rust | `MultiResolutionConfig::profile`; `ProfileReport::to_json()` |

The code is in [`run_profile.rs`](../crates/dggs-stream-core/src/run_profile.rs).

## Option names by engine

| Option | CLI | DuckDB | Python / Polars | DataFusion / SedonaDB | PostgreSQL |
| :--- | :--- | :--- | :--- | :--- | :--- |
| grid | `--grid` | per-grid functions, or `grid :=` | `grid=` | per-grid functions, a grid-name argument, `with_grid` | per-grid functions |
| automatic resolution | `-r auto[±N]` | `resolutions := 'auto'` | `resolutions="auto"`, `auto_resolution()` | `'auto'` | `{grid}_auto_resolution()` |
| transfer mode | `--transfer`, `--interpolation`, `--overlay-weighted`, `--mass-preserve` | `transfer :=`, `interpolation :=`, `overlay_weighted :=`, `mass_preserve :=` | `transfer=`, `interpolation=`, `overlay_weighted=`, `mass_preserve=` | `'transfer=overlay'`-style arguments, `with_transfer` | — |
| minimum valid overlay fraction | `--valid-coverage-threshold` | `valid_coverage_threshold :=` | `valid_coverage_threshold=` | — | — |
| histogram | `--histogram`, `--histogram-weight`, `--histogram-normalize` (`ipc`, `jsonl`; implies `-c`) | `histogram :=`, `histogram_weight :=`, `histogram_normalize :=` | `histogram=`, `histogram_weight=`, `histogram_normalize=` | `'histogram=...'` arguments, `with_histogram` | — |
| NoData policy and rounding | `--nodata-policy`, `--emit-nodata-value`, `--decimals` | `nodata_policy :=`, `emit_nodata_value :=`, `decimals :=` | `nodata_policy=`, `emit_nodata_value=`, `decimals=` | `'nodata_policy=emit'`-style arguments, `with_output_policy` | `nodata_policy`, `emit_nodata_value`, `decimals` arguments |
| runtime profile | `--profile` | `profile := true` | `profile_raster()` | `with_profile` | — |
