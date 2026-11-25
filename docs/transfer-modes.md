# Transfer Modes: How Pixels Become Cells

[← Back to README](../README.md)

`transfer` chooses how raster pixels are mapped onto grid cells. It works the same way on every
grid (H3, A5, S2, HEALPix) and in every engine except PostgreSQL. This page covers, in order:

1. [Choosing a mode](#choosing-a-mode), or [letting the engine choose](#let-the-engine-choose-auto)
2. [`assign_centers`](#assign_centers-the-default), the default
3. [`interpolate`](#interpolate-the-value-at-each-cells-centre), for cells finer than the pixels
4. [The overlay modes](#the-overlay-modes), for exact cell edges, conserved totals and true ground areas
5. [What can't be combined](#what-cant-be-combined) and [usage by engine](#usage-by-engine)

## Choosing a mode

The right mode depends mostly on how big a cell is next to a pixel, and then on what the values
mean.

| `transfer` | What each cell gets | Use it when |
| :--- | :--- | :--- |
| `auto` | Whichever of the three below suits the raster and the resolutions | You don't want to work it out yourself |
| `assign_centers` (default) | Every pixel whose centre falls inside the cell | Cells are about the size of a pixel, or you want the fastest scan |
| `interpolate` | One value, interpolated at the cell's centre | Cells are **smaller** than the pixels |
| `overlay` | Every pixel the cell overlaps, weighted by the part of the pixel inside the cell | Cells are much larger than pixels, edges should cut pixels exactly, or totals or ground areas must come out right |

```text
        cell area / pixel area
  0 ──────────── 1 ──────────────────── 16 ─────────────────────────▶
  │ interpolate  │    assign_centers    │          overlay
  │ (cells are   │ (about one pixel     │ (many pixels per cell: edge pixels
  │  sub-pixel)  │  per cell)           │  are a real share of each cell)
```

Within `overlay`, pick the variant by what a pixel value means:

| The raster holds... | Example | Use |
| :--- | :--- | :--- |
| An intensive value, the same whatever the area | temperature, elevation, NDVI, depth | `overlay` (area-weighted mean) |
| An amount per pixel, to be shared out | population count, biomass per pixel | `overlay` + `mass_preserve` |
| An amount per unit area, on a lon/lat raster | people per km², kg/m², mm of water | `overlay` + `ground_area` |
| A class code | land cover, soil type | `overlay` on a categorical scan (class areas) |
| Presence: did anything touch the cell? | burned area, footprints | `overlay` + `overlay_weighted = false` |

## Let the engine choose: `auto`

Two `auto` settings measure the raster before the scan starts, so the same call gives sensible
cells and a sensible mode on a 10 m image and on a 0.25° climate grid alike.

### `transfer = "auto"`

`auto` measures the ground area of a pixel where the scan reads (inside the `bbox`, if one is
given), compares it with the mean cell area at each requested resolution, and picks:

| Cell area / pixel area, at every requested resolution | `auto` runs |
| :--- | :--- |
| below 1 (cells smaller than pixels) | `interpolate`, with `nearest` for categorical scans and `bilinear` for continuous ones |
| from 1 to below 16 | `assign_centers` |
| 16 or more | weighted `overlay` |
| some resolutions below 1, others at or above it | weighted `overlay`, the one mode that is right at both ends |

Scan options take precedence over the ratio:

- `compact`, `spectral_formula` and sub-pixel `sampling` work only with centre assignment, so
  `auto` runs `assign_centers`.
- `valid_coverage_threshold` and `histogram_weight = "area"` need overlap areas, so `auto` runs
  weighted `overlay`.
- Partitioned scans (`plan_partitions`, Spark) run `assign_centers`. Pass `transfer = "overlay"`
  explicitly for a [partitioned overlay](#partitioned-overlay).
- `ground_area` and multi-band (`bands`) scans can't interpolate, so below 1:1 `auto` runs
  `assign_centers` instead.
- Resolutions whose cells are over 500 km across (the coarsest two or three of each grid) run
  `assign_centers`: overlay would take whole pixels by centre there anyway.
- If the pixel size can't be measured, `auto` runs `assign_centers`.

`auto` never adds options of its own: it doesn't turn on `mass_preserve` or `ground_area`, and
`interpolation`, `overlay_weighted` and `mass_preserve` can't be passed with it. When the values
are amounts to conserve, or densities on a lon/lat raster, ask for that overlay explicitly.

The CLI prints its choice on stderr (`dggs-stream: --transfer auto selected overlay`).

### `resolutions = "auto"`

`auto` as a resolution picks the one whose mean cell area is closest, in ratio, to the raster's
ground pixel area, measured at the raster's centre. `auto+N` moves N steps finer and `auto-N` N
steps coarser, clamped to the grid's range. Because it targets a size rather than an index, it
gives comparable cells on every grid ([Grids](grids.md#automatic-resolution)).

The two combine. The table shows where each offset lands with `transfer = "auto"`; a step is
×4 in area on A5, S2 and HEALPix and ×7 on H3.

| Resolution | Cell area / pixel area | `transfer = "auto"` runs |
| :--- | :--- | :--- |
| `auto+1` or finer | below ½ (below 0.38 on H3) | `interpolate` |
| `auto` | about 1 (0.5–2; 0.38–2.6 on H3) | `assign_centers`, or `interpolate` if the cells came out just under a pixel |
| `auto-2` on H3, `auto-3` on the others | at least 18 and 32 | weighted `overlay` |

```bash
dggs-stream-cli jsonl dem.tif --grid s2 -r auto+1 --transfer auto      # interpolate
dggs-stream-cli jsonl dem.tif --grid s2 -r auto-3 --transfer auto      # overlay
```

```python
lf = ds.scan_raster("dem.tif", "auto-2", transfer="auto")              # H3: overlay
```

## `assign_centers` (the default)

Each pixel (or each sub-pixel sample, see [Super-Sampling](super-sampling.md)) is indexed to the
cell that holds its centre and aggregated there. This is the single-pass streaming kernel the rest
of the documentation describes, with the lowest memory use and the highest throughput.

A pixel straddling a cell edge goes wholly to one side. With many pixels per cell this evens out;
with a few it doesn't, and with cells finer than the pixels most cells contain no pixel centre
and are missing from the output. Use `overlay` or `interpolate` for those cases.

`assign`, `centers` and `centres` are accepted as names too.

## `interpolate`: the value at each cell's centre

Every cell whose centre lies on the raster gets the raster's value at that centre, interpolated
from the surrounding pixels. Each cell is filled exactly once, so `count` is 1 and `mean`, `min`,
`max` and `sum` all hold the interpolated value.

`interpolation` picks the kernel:

| `interpolation` | Kernel | Pixels read | Notes |
| :--- | :--- | :--- | :--- |
| `nearest` | Value of the pixel holding the centre | 1 | No new values; the only method for categorical data |
| `bilinear` (default) | Linear in each direction | 2 × 2 | Smooth, never overshoots the neighbouring values |
| `bicubic` | Catmull-Rom cubic convolution | 4 × 4 | Sharper; exact for values varying linearly or quadratically; can overshoot near steps |
| `lanczos` | Lanczos windowed sinc, a = 3, normalised | 6 × 6 | Sharpest; most prone to ringing next to abrupt changes |

The names `near`, `linear`, `cubic` and `lanczos3` are accepted too, and an interpolation name can
be passed directly as the transfer (`transfer := 'bicubic'` is `interpolate` with `bicubic`).

Edges and NoData:

- Taps past the raster's edge repeat the edge pixel.
- `bilinear` leaves out NoData taps and renormalises the remaining weights. It returns no value
  when valid taps have zero total weight, including at the exact centre of a NoData pixel.
- `bicubic` and `lanczos` fall back to `bilinear` when any of their taps is NoData, so NoData never
  leaks into a result.
- Zero-alpha pixels and pixels marked invalid by an internal TIFF mask count as NoData, as in the
  default mode.

## The overlay modes

`overlay` gives each cell every pixel it overlaps. The cell's polygon is clipped against the
pixel grid in raster space, so a pixel that straddles an edge is shared between the cells on
either side in proportion to the area each one covers. Everything else on this page builds on
that one computation:

| Overlay mode | Options | Each pixel contributes | Use it for |
| :--- | :--- | :--- | :--- |
| [Weighted](#weighted-overlay-the-default) | `transfer = "overlay"` | Its value, weighted by the fraction of it inside the cell | Intensive values: temperature, elevation, depth |
| [Touch count](#touch-count-overlay_weighted--false) | `+ overlay_weighted = false` | Its value, at full weight, to every cell it touches | "Any pixel touches the cell" questions |
| [Mass-preserving](#mass-preserving-mass_preserve) | `+ mass_preserve = true` | Its value, split across cells so its shares add up to one | Amounts per pixel: counts, totals |
| [Ground area](#ground-area-overlay-ground_area) | `+ ground_area = true` | Its value × the true m² of the part inside the cell | Densities on lon/lat rasters; true areas |
| [Categorical](#categorical-overlay-class-areas) | categorical scan | The fraction of it inside the cell, to its class | Class areas, area-weighted histograms |

Three modifiers apply on top: a [valid-coverage filter](#valid-coverage-valid_coverage_threshold),
[several bands at once](#several-bands-at-once), and [partitioned scans](#partitioned-overlay).

### What the output columns mean

| Column | Weighted | Touch count | Mass-preserving | Ground area |
| :--- | :--- | :--- | :--- | :--- |
| count | Overlapped area, in pixels (fractional) | Pixels touched | Sum of the pixels' shares | `pixel_count`: valid overlapped area, in pixels |
| mean | Area-weighted mean | Plain mean of touched pixels | Share-weighted mean | `area_weighted_mean`: mean weighted by m² |
| sum | Σ value × overlapped fraction | Σ value of touched pixels | Σ value × share; adds up to the raster's total | `area_weighted_sum`: Σ value × m², the integral |
| min / max | Over every pixel overlapped, even slightly | Over every pixel touched | Over every pixel overlapped | — |
| area | — | — | — | `retained_area_m2`: valid overlapped m² |

### Weighted overlay (the default)

```sql
SELECT h3_index, mean, count
FROM h3_raster_continuous_aggregate('sst.tif', resolution := 5, transfer := 'overlay');
```

`mean` is an area-weighted mean and `count` the overlapped area in pixels, so `count` times the
pixel area is the cell's valid area in the raster's units: exact for a projected raster, whose
pixels all have the same area. A cell partly covered by NoData still reports the mean of its valid
part, and its `count` says how much of it that was (see also
[valid coverage](#valid-coverage-valid_coverage_threshold)).

Use it whenever the values are intensive, i.e. they don't scale with area. It also beats centre
assignment at about one pixel per cell, where centre assignment's fractions are coarse (a cell
holding two pixel centres can only be 0, ½ or 1 forest), and it is never empty for cells finer
than the pixels.

### Touch count: `overlay_weighted = false`

Every pixel the cell touches counts once at full weight: `count` is the number of pixels touched,
and a pixel on an edge counts in every cell it touches. Use it to flag cells that any burned,
built or flooded pixel reaches. It can't be combined with `mass_preserve`.

### Mass-preserving: `mass_preserve`

```python
lf = ds.scan_raster("population_count.tif", 7, transfer="overlay", mass_preserve=True)
lf.select(pl.col("sum").sum()).collect()   # equals the sum of all valid pixels
```

Each pixel's weights are divided by their total across cells, so every pixel contributes exactly
once in total and the cells' `sum` column adds up to the raster's total. Use it for amounts stored
per pixel (population counts, biomass, precipitation volume). It needs area weights, so it can't
be combined with `overlay_weighted = false`. With [valid coverage](#valid-coverage-valid_coverage_threshold)
it keeps every contribution, so nothing is lost from the total.

### Ground-area overlay: `ground_area`

On a lon/lat (EPSG:4326) raster pixels shrink toward the poles: a 0.1° pixel covers about
123 km² at the equator and 62 km² at 60°. Pixel-weighted statistics then over-weight high
latitudes, and pixel counts are not areas. `ground_area = true` weights every overlap by its true
area on the WGS84 ellipsoid instead:

```python
# Population from a people-per-km² density raster: integrate the density over each cell
lf = ds.scan_raster("population_per_km2.tif", 8, ground_area=True, transfer="overlay")
lf.select("h3_index", (pl.col("area_weighted_sum") / 1e6).alias("population"))
```

| Column | Meaning |
| :--- | :--- |
| `pixel_count` | Valid overlapped area, in pixels |
| `retained_area_m2` | Valid overlapped area, in m² on the ellipsoid |
| `area_weighted_sum` | Σ value × m²: the integral of the value over the cell. A density per m² gives the amount itself; per km², divide by 10⁶ |
| `area_weighted_mean` | `area_weighted_sum / retained_area_m2`, in the raster's units |

Without `transfer = "overlay"`, `ground_area` assigns whole pixels by centre, each weighted by its
full ellipsoidal area; with `transfer = "auto"` it picks between the two by the ratio above.

Requirements: one north-up, single-band EPSG:4326 GeoTIFF; a continuous scan; weighted overlay
without `mass_preserve`, quantiles, histograms, formulas, compaction or sub-pixel sampling;
`nodata_policy = "omit"`; cells at most 500 km across; and no cell that encloses a pole. H3
has a cell around each pole at every resolution, so keep a global H3 scan off the poles with a
`bbox`; on A5, S2 and HEALPix the poles are cell corners at every resolution fine enough to clip. It is available in
the Rust core, the CLI (`ipc`, `jsonl`), DuckDB and Python/Polars, not in partitioned scans.
See [ground-area geometry and units](engines/core.md#ground-area-weighted-overlay).

### Categorical overlay: class areas

On a categorical scan, `overlay` turns class counts into overlapped areas in pixels: a pixel of
class 41 that is 30% inside a cell adds 0.3 to that cell's class-41 count, and `fraction` is the
share of the cell's overlapped area. An area-weighted [histogram](scan-options.md#histograms)
(`histogram_weight = "area"`) runs weighted overlay automatically.

On S2 and HEALPix, whose children tile their parent exactly, a weighted categorical overlay over
several resolutions clips only the finest one and rolls its class areas up into the coarser
cells, instead of clipping every resolution again. Engines that push resolution filters into the
scan (Polars, SedonaDB) keep them out of such scans, so filtering doesn't change how the coarser
cells are computed.

### Valid coverage: `valid_coverage_threshold`

`valid_coverage_threshold = 0.75` keeps only cells with valid data across at least 75% of their
overlap with the raster. NoData and masked pixels count in the denominator only; pixels outside
the bbox or owned by another mosaic tile count in neither. Coverage is measured by area even with
`overlay_weighted = false`, and in m² with `ground_area`. The filter applies once a cell is
complete; a positive threshold drops all-NoData cells even under `nodata_policy = "emit"`, and 0
disables it. Category remapping and histogram bins apply after validity is decided.

| Engine | Spelling |
| :--- | :--- |
| CLI | `--transfer overlay --valid-coverage-threshold 0.75` |
| DuckDB | `transfer := 'overlay', valid_coverage_threshold := 0.75` (also on the Parquet and PMTiles exports) |
| Python / Polars | `transfer="overlay", valid_coverage_threshold=0.75` (also on the exports) |
| DataFusion / SedonaDB | `'transfer=overlay', 'valid_coverage_threshold=0.75'` |
| Core | `MultiResolutionConfig::valid_coverage_threshold` |

### Several bands at once

A multi-band overlay (`bands`) clips each pixel–cell intersection once and applies the same weight
to every selected band with a valid value there. Each band has its own count, statistics and
coverage: a cell survives when any band passes the threshold, and a band below it has zero count
and the configured NoData output for its mean (NULL by default). Mass-preserving overlay conserves
each band's valid total separately.

### Partitioned overlay

Partitioned Python and Spark scans run single-band continuous weighted overlay. Workers finish H3
cells whose WGS84 footprints are clear of every other partition; boundary cells carry their valid
and total overlapped area as partial rows, which are merged before the coverage and statistics
filters apply. See [Spark partitioned weighted overlay](engines/spark.md#partitioned-weighted-overlay).

### Overlay geometry

- A `bbox` selects pixels by their centres, as in the default mode, so cells reaching outside the
  box are kept with the pixels inside it.
- On a full-width WGS84 or Web Mercator raster, a cell crossing the longitude seam receives pixel
  contributions from both edges, including when the raster uses 0–360° longitudes.
- Cell edges are traced in raster pixel coordinates before clipping: H3 and S2 great-circle edges
  and the native A5 and HEALPix face edges. A segment is subdivided when a quarter-, half- or
  three-quarter-point sample lies more than 1/64 of a pixel from the projected chord, to at most
  12 bisections (4,096 segments per side). Cells touching a pole keep their straight sides at the
  pole and trace the rest, so a polar cell and its neighbour follow the same curve along a shared
  side.
- Cells whose diameter bound is above 500 km (the coarsest two or three resolutions of each grid)
  take whole pixels by centre: their edges bend too far from straight lines for exact clipping,
  and a single pixel is negligible next to such a cell. Ground-area overlay rejects them instead.
- Zero-alpha pixels and pixels marked invalid by an internal TIFF mask count as NoData.
- Cell polygons are clipped with an edge-walking sweep (Edge-DDA with Green's-theorem prefix
  sums), not pixel by pixel. Overlaps below `1e-12` of a pixel (`1e-14` in ground-area overlay)
  are rounding noise and are skipped.
- Runs of whole pixels inside a cell are accumulated as contiguous slices, whose variance can
  differ from per-pixel accumulation in the last bits. When `min_mean` or `max_mean` is set,
  pixels are accumulated one at a time so a mean exactly on an inclusive bound passes.

## Categorical rasters

Class codes can't be interpolated or averaged, so categorical scans accept:

- `overlay` (with any options): class counts become overlapped areas in pixels
  ([above](#categorical-overlay-class-areas)).
- `interpolate` with `interpolation = 'nearest'` only. Other methods are rejected.
- `auto`, which picks between those two and `assign_centers`.

## What can't be combined

`interpolate` and `overlay` read the raster in bands of rows and hold only one band plus the cells
still open across band edges, so memory stays flat as the raster gets taller. They are a different
kernel from the default, and can't be combined with:

| Option | Why |
| :--- | :--- |
| `sampling` other than `center` | The transfer mode replaces sub-pixel sampling |
| `compact` | Not supported by the transfer kernel |
| `spectral_formula` | Not supported by the transfer kernel |
| Partitioned scans, except single-band continuous weighted overlay | Only that case [merges across partitions](#partitioned-overlay) |
| `interpolation` without `transfer = 'interpolate'` | The option belongs to the other mode |
| `overlay_weighted` / `mass_preserve` without `transfer = 'overlay'` | The options belong to the other mode |

Each of these is rejected with an error, not ignored (`transfer = 'auto'` steers around them
instead). A resolution so fine that one block of the raster would hold too many cells is also
rejected, with a message asking for a coarser resolution.

### `transfer` vs. `sampling`

The two work in opposite directions:

- `sampling` (`'rgss'`, `'5point'`, ...) works **from the pixels**: it takes several points inside
  each pixel and assigns each to the cell holding it, for anti-aliased edges in the default mode.
  It approximates what `overlay` computes exactly.
- `transfer = 'interpolate'` works **from the cells**: it reads the raster's value at each cell's
  centre.

`transfer = 'sample'` is not a mode; it is rejected with a pointer to `'interpolate'` and to
`sampling`.

## Usage by engine

**CLI** (every subcommand that scans a raster: `ipc`, `jsonl`, `parquet`, `pmtiles`; `--ground-area` on `ipc` and `jsonl`)

```bash
dggs-stream-cli jsonl dem.tif -r auto --transfer auto
dggs-stream-cli jsonl dem.tif -r 12 --transfer interpolate --interpolation bicubic
dggs-stream-cli parquet population.tif -o population.parquet -r 7 --transfer overlay --mass-preserve
dggs-stream-cli ipc landcover.tif -r 9 -c --transfer overlay --overlay-weighted false -o landcover.rh3
dggs-stream-cli jsonl density_per_km2.tif -r 8 --ground-area --transfer overlay
```

**DuckDB** (named parameters of the raster scan and export functions)

```sql
SELECT * FROM h3_raster_continuous_aggregate('dem.tif', resolutions := 'auto', transfer := 'auto');

SELECT * FROM h3_raster_continuous_aggregate('dem.tif', resolution := 12,
    transfer := 'interpolate', interpolation := 'bicubic');

SELECT sum(sum) FROM h3_raster_continuous_aggregate('population.tif', resolution := 7,
    transfer := 'overlay', mass_preserve := true);

SELECT sum(area_weighted_sum) / 1e6 AS population
FROM raster_h3('density_per_km2.tif', resolution := 8, ground_area := true, transfer := 'overlay');
```

**Python / Polars** (keyword arguments of `scan_raster` and the exporters)

```python
import dggs_stream as ds

lf = ds.scan_raster("dem.tif", "auto", transfer="auto")
lf = ds.scan_raster("dem.tif", resolutions=12, transfer="interpolate", interpolation="lanczos")
lf = ds.scan_raster("population.tif", resolutions=7, transfer="overlay", mass_preserve=True)
lf = ds.scan_raster("density_per_km2.tif", 8, ground_area=True, transfer="overlay")
```

**DataFusion / SedonaDB**: option strings on the table functions, `transfer=` on the SedonaDB
`RasterTable`, or `with_transfer` on the table providers. Ground area is not available here.

```sql
SELECT * FROM h3_raster_continuous_aggregate('dem.tif', 'auto', 'transfer=auto');
SELECT * FROM h3_raster_continuous_aggregate('dem.tif', 12, 'transfer=interpolate', 'interpolation=bicubic');
SELECT * FROM h3_raster_continuous_aggregate('population.tif', 7, 'transfer=overlay', 'mass_preserve=true');
```

```rust
use dggs_stream_core::transfer::{Interpolation, TransferMode};

let provider = provider.with_transfer(TransferMode::Auto);
let provider = provider.with_transfer(TransferMode::Interpolate(Interpolation::Bicubic));
let provider = provider.with_transfer(TransferMode::from_options(
    Some("overlay"), None, None, Some(true),
)?);
```

**Core**: set `MultiResolutionConfig::transfer`, built with `TransferMode::from_options` or
directly (`TransferMode::Auto`, `TransferMode::Interpolate(Interpolation::Bilinear)`,
`TransferMode::OVERLAY`); `resolve_auto_transfer` exposes the `auto` rule. Ground-area overlay is
`transfer::scan_ground_area_overlay`. See [`transfer/mod.rs`](../crates/dggs-stream-core/src/transfer/mod.rs).

**PostgreSQL**: not available yet.

## Performance

`interpolate` and `overlay` keep memory flat in the raster's height (see
[What can't be combined](#what-cant-be-combined)). Ordinary overlay buffers overlaps one cell at a
time, and runs of whole pixels inside a cell are accumulated as slices, so cells spanning many
pixels cost little more than centre assignment.

### Parallel overlap calculation

Eligible overlay blocks project and clip cells in parallel. The pool is set by
`MultiResolutionConfig::aggregation_workers` in Rust, `--aggregation-workers` in the CLI and
`workers` in DuckDB: unset uses the caller's ambient Rayon pool, `n` a dedicated `n`-thread pool
and `1` runs serially. Ordinary overlay without `mass_preserve` clips and accumulates each cell on
a worker; mass-preserving overlay has workers prepare intersections that the caller applies in the
original cell and pixel order, including normalisation and per-band coverage filtering. Results
are identical either way. DuckDB's ground-area overlay runs serially whatever `workers` says.

The mass-preserving path uses the pool for blocks with at least 64 candidate cells and at most
512 estimated source pixels per cell. Coarse-cell fallbacks and callers already on a Rayon worker
use the serial path. Prepared lists hold at most 128 × 1,024 pixel–area pairs (2 MiB of entries);
past that, the worker stops and the serial reducer clips the full cell using its reusable buffer.

### Benchmarks

Build the CLI with `cargo build --release -p dggs-stream-cli`. With GDAL's `gdal_create` on PATH,
run the overlay benchmark from the repository root on macOS or Linux; the baseline argument takes
a separately saved release binary.

```sh
python3 scripts/benchmark_overlay.py --baseline-cli /path/to/baseline-cli --runs 9
```

Each run scanned a local, tiled, Deflate-compressed Float32 raster covering the same one-degree
square and wrote IPC to the null device, with the coverage filter disabled. The runner warms both
binaries, alternates their order, and measures wall time and peak resident memory in a fresh
process for each scan. Medians on arm64 macOS: first per-cell overlap buffering against revision
`63f69cf`, then the interior-pixel shortcut against revision `0dd44e8`. At H3 resolution 6 a cell
contains many whole pixels; at resolution 9 on the smaller raster, cells are mostly smaller than
one pixel.

| Change | Raster size | H3 resolution | Mass preservation | Time, baseline → updated | Peak RSS, baseline → updated |
| --- | --- | --- | --- | --- | --- |
| Overlap buffering | 1024 × 1024 | 6 | Off | 75.3 → 59.6 ms | 44.1 → 25.0 MiB |
| Overlap buffering | 1024 × 1024 | 6 | On | 81.9 → 65.5 ms | 51.9 → 51.9 MiB |
| Overlap buffering | 128 × 128 | 9 | Off | 341.9 → 332.8 ms | 39.9 → 38.8 MiB |
| Overlap buffering | 128 × 128 | 9 | On | 342.1 → 340.5 ms | 40.0 → 39.8 MiB |
| Interior pixels | 1024 × 1024 | 6 | Off | 57.9 → 32.4 ms | 25.4 → 25.2 MiB |
| Interior pixels | 1024 × 1024 | 6 | On | 63.6 → 37.8 ms | 51.9 → 52.2 MiB |
| Interior pixels | 128 × 128 | 9 | Off | 322.8 → 322.8 ms | 38.8 → 38.9 MiB |
| Interior pixels | 128 × 128 | 9 | On | 327.2 → 326.5 ms | 39.5 → 39.6 MiB |

To compare worker counts on a local TIFF, build the release example:

```sh
cargo build --release -p dggs-stream-core --example overlay_workers
target/release/examples/overlay_workers raster.tif 9 1
target/release/examples/overlay_workers raster.tif 9 4
```

It reports emitted cells and elapsed milliseconds, including reader and stream construction. To
verify exact output, append `ordinary serial.bin` and `ordinary parallel.bin` to the two commands
and compare the files with `cmp` (`mass` checks mass-preserving overlay). State files hold
resolution, cell ID and the serialized accumulator in emission order; time separate runs without
them.

Both inputs below were single-band 2,048 × 2,048 Float32 windows with floating-point predictor 3:
Hawaii CFL (LZW, 256 × 256 tiles, upper-left source pixel (17800, 8850) of
`CFL_HI_wgs84_cog.tif`) and CONUS burn probability (DEFLATE, 128 × 128 tiles, (21600, 39700) of
`BP_CONUS_wgs84_cog.tif`). Medians of five alternating warm-cache release runs per worker count
on arm64 macOS, each in a fresh process; peak RSS covers the whole process. H3 resolution 7
exercises the serial fallback.

| Raster | H3 resolution | Cells | Scan ms, 1 / 4 workers | Peak RSS MiB, 1 / 4 workers |
| --- | ---: | ---: | ---: | ---: |
| Hawaii CFL | 7 | 694 | 156.1 / 157.1 | 83.2 / 83.4 |
| Hawaii CFL | 9 | 31,453 | 345.1 / 245.1 | 96.1 / 96.8 |
| CONUS burn probability | 7 | 867 | 123.7 / 124.2 | 81.6 / 81.7 |
| CONUS burn probability | 9 | 39,643 | 334.0 / 224.8 | 94.1 / 95.1 |

At H3 resolution 10, ordinary weighted overlay of the same windows (five warm-cache scans per
worker count, worker order reversed on alternate runs) gave the medians below. Ordinary and
mass-preserving runs produced byte-identical output states at all four worker counts.

| Raster | Cells | Workers | Median scan, ms | Median peak RSS, MiB |
| --- | ---: | ---: | ---: | ---: |
| Hawaii CFL | 218,370 | 1 | 961.9 | 108.0 |
| Hawaii CFL | 218,370 | 2 | 705.9 | 108.3 |
| Hawaii CFL | 218,370 | 4 | 604.0 | 111.5 |
| Hawaii CFL | 218,370 | 8 | 649.9 | 109.3 |
| CONUS burn probability | 275,405 | 1 | 1118.9 | 115.2 |
| CONUS burn probability | 275,405 | 2 | 793.5 | 115.8 |
| CONUS burn probability | 275,405 | 4 | 668.4 | 119.2 |
| CONUS burn probability | 275,405 | 8 | 720.6 | 116.4 |
