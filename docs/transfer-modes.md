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
| `auto` | `interpolate` or `overlay`, whichever suits the raster and the resolutions | You don't want to work it out yourself |
| `assign_centers` (default) | Every pixel whose centre falls inside the cell | Speed matters more than edge accuracy at about one to a few pixels per cell, where it is fastest |
| `interpolate` | One value, interpolated at the cell's centre | Cells are **smaller** than the pixels |
| `overlay` | Every pixel the cell overlaps, weighted by the part of the pixel inside the cell | Cells are much larger than pixels (where it is also the fastest mode), edges should cut pixels exactly, or totals or ground areas must come out right |

```text
        cell area / pixel area
  0 ──────────── 1 ─────────────────────────────────────────────────▶
  │ interpolate  │                       overlay
  │ (cells are   │  exact at every size; from about 10–60 pixels per cell
  │  sub-pixel)  │  also the fastest mode. assign_centers is faster below
  │              │  that, at the cost of 4–38% per-cell area errors
```

Within a mode, [choosing a kernel](#choosing-a-kernel) and
[choosing an overlay mode](#choosing-an-overlay-mode) pick the options by what the pixel values
mean.

## Let the engine choose: `auto`

Two `auto` settings measure the raster before the scan starts, so the same call gives sensible
cells and a sensible mode on a 10 m image and on a 0.25° climate grid alike.

### `transfer = "auto"`

`auto` measures the ground area of a pixel where the scan reads (inside the `bbox`, if one is
given), compares it with the mean cell area at each requested resolution, and picks:

| Scan | Cell area / pixel area | `auto` runs |
| :--- | :--- | :--- |
| Continuous | below 1 at every requested resolution (cells smaller than pixels) | `interpolate` with `bilinear` |
| Continuous | 1 or more at any requested resolution | weighted `overlay` |
| Categorical | any | weighted `overlay`: class codes can't be blended, and `nearest` is only a lookup at the cell's centre, while overlay gives every class its share of the cell |

Centre assignment is never chosen by ratio. The [mode comparison](#comparing-the-modes) found it
at most about 2.6× faster than overlay from 1 to 16 pixels per cell, where it left up to 14% of
the cells empty and put their areas 4–38% off; from about 10 (H3) to 60 (S2) pixels per cell,
overlay was faster too. Ask for `assign_centers` explicitly when speed matters more than edges.

Scan options take precedence over the ratio:

- `compact`, `spectral_formula` and sub-pixel `sampling` work only with centre assignment, so
  `auto` runs `assign_centers`.
- `valid_coverage_threshold` and `histogram_weight = "area"` need overlap areas, so `auto` runs
  weighted `overlay`.
- Partitioned scans (`plan_partitions`, Spark) run `assign_centers`. Pass `transfer = "overlay"`
  explicitly for a [partitioned overlay](#partitioned-overlay).
- `ground_area` and multi-band (`bands`) scans can't interpolate, so below 1:1 `auto` runs
  weighted `overlay` for them too.
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
| `auto+1` or finer | below ½ (below 0.38 on H3) | `interpolate` (continuous) |
| `auto` | about 1 (0.5–2; 0.38–2.6 on H3) | `overlay`, or `interpolate` for a continuous scan whose cells came out just under a pixel |
| `auto-1` or coarser | 2 or more (2.6 on H3) | weighted `overlay` |

Categorical scans run `overlay` at every offset.

```bash
dggs-stream-cli jsonl dem.tif --grid s2 -r auto+1 --transfer auto      # interpolate
dggs-stream-cli jsonl dem.tif --grid s2 -r auto-1 --transfer auto      # overlay
```

```python
lf = ds.scan_raster("dem.tif", "auto-1", transfer="auto")              # overlay
```

## `assign_centers` (the default)

Each pixel (or each sub-pixel sample, see [Super-Sampling](super-sampling.md)) is indexed to the
cell that holds its centre and aggregated there. This is the single-pass streaming kernel the rest
of the documentation describes, with the lowest memory use. It indexes every pixel, so its time
grows with the raster's size whatever the resolution: it is the fastest mode at about one to a
few pixels per cell, and slower than `overlay` once cells hold dozens of pixels
([comparison](#comparing-the-modes)).

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

### Choosing a kernel

Start from what the raster holds:

| Your raster | Use | Why |
| :--- | :--- | :--- |
| Class codes (land cover, soil, zoning) | `nearest` | Codes can't be blended; a categorical scan rejects every other kernel, including the `bilinear` default, so name it |
| Values that must stay within their neighbours' range (percentages, probabilities, counts, depths that can't go negative) | `bilinear` | It never overshoots, so you won't get 102% cover or a negative depth |
| Smooth fields (temperature, pressure, elevation in rolling terrain) | `bicubic` | Sharper than `bilinear`, and exact where values vary linearly or quadratically |
| Very smooth fields where sharpness matters, without NoData nearby | `lanczos` | The sharpest kernel, but it rings next to abrupt changes |
| Sharp steps (cliffs, coastlines, flood edges, burn scars) | `bilinear`, or `nearest` to keep the step | `bicubic` and `lanczos` overshoot on both sides of a step |
| Raw pixel values, just on finer cells | `nearest` | It adds no new values |

Things that decide close calls:

- **Overshoot is not clamped.** `bicubic` and `lanczos` can return values slightly above the
  highest or below the lowest neighbouring pixel; the engine does not clip them. If a bound
  matters (0–100%, depth ≥ 0), use `bilinear` or clip the result yourself.
- **NoData narrows the choice.** Whenever any of their taps is NoData, `bicubic` and `lanczos`
  fall back to `bilinear`, so on a patchy raster (clouds, coasts) most edge cells are bilinear
  whichever you pick.
- **Cost grows with the window**: 1, 4, 16 and 36 pixel reads per cell. That matters at very fine
  resolutions, where the scan emits many cells for every pixel.
- **No kernel adds detail.** Interpolation only smooths between pixel values: features smaller
  than a pixel stay invisible, and cells much finer than about a tenth of a pixel across mostly
  add rows, not information.
- **Defaults.** Without `interpolation`, `interpolate` uses `bilinear`, so a categorical scan
  must name `nearest` explicitly or it is rejected. `transfer = "auto"` picks `bilinear` for
  continuous scans below one pixel per cell, the kernel that is never wrong if not always the
  sharpest; for categorical scans it runs `overlay` instead of `nearest`.

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

[Worked example 4](worked-examples.md#4-land-temperature-by-latitude-band-and-by-city--cli-on-healpix)
interpolates a 10′ climate raster onto city-scale cells and compares `nearest` with `bicubic`.

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

### Choosing an overlay mode

Start from what one pixel value means:

| The raster holds... | Example | Use | Read |
| :--- | :--- | :--- | :--- |
| A value that doesn't scale with area | temperature, elevation, NDVI, depth | weighted `overlay` | `mean` |
| An amount per pixel | population count, biomass per pixel | `overlay` + `mass_preserve` | `sum` |
| An amount per unit area, on a lon/lat (EPSG:4326) raster | people per km², kg/m², mm of water | `overlay` + `ground_area` | `area_weighted_sum` (÷ 10⁶ for per-km²) |
| An amount per unit area, on an equal-area projected raster | people per km² in EPSG:5070 or LAEA | weighted `overlay` | `sum × pixel area` |
| A class code | land cover, soil type | `overlay` on a categorical scan | `fraction`, or `count × pixel area` |
| Presence: did anything touch the cell? | burned, built or flooded pixels | `overlay` + `overlay_weighted = false` | `count > 0`, `max` |

Then check the questions that change the answer:

- **Is the total the answer?** Weighted overlay splits each pixel across cells by area, so its
  `sum` comes close to the raster's total, but only `mass_preserve` guarantees it, by
  normalising every pixel's shares to exactly one. Use `mass_preserve` whenever you will add cells
  up or compare the result with a published total.
- **Do pixels differ in area?** Pixel-weighted statistics treat every pixel the same. That is
  exact on an equal-area projection, close on a small region of any projection, and wrong for
  totals and densities on a lon/lat raster, whose pixels shrink with cos(latitude): use
  `ground_area` there. Web Mercator pixels vary even more (with cos²) and `ground_area` doesn't
  accept them; reproject densities to an equal-area CRS or to EPSG:4326 first. For means of
  intensive values, the error inside one cell is small unless the cell spans many degrees of
  latitude.
- **What does NoData mean?** If it means *not observed* (clouds, gaps, outside a survey), a cell
  that is mostly NoData reports a mean from a sliver: add `valid_coverage_threshold` (e.g. 0.5)
  to drop it. If it means *none here* (dry ground in a flood depth grid, no population), the
  valid `count` is the extent of what you are measuring, so read it rather than filtering on it,
  or use the threshold deliberately to keep cells that are mostly covered.
- **Should slivers count in `min` and `max`?** They do: a pixel that overlaps a cell by a
  fraction of a percent still sets its `min` or `max`. That is the conservative choice for
  hazards (the deepest water touching a parcel). For a value typical of the cell, use `mean`.
- **Will you add counts across cells?** Not with `overlay_weighted = false`: a pixel on an edge
  counts in every cell it touches. Use it to flag cells, not to total them.
- **Are the cells near pixel size?** Overlay is right at any size. From about 1 to 16 pixels per
  cell, centre assignment is 1.2–2.6× faster, but each cell's area comes out 4–38% off on average
  (see [comparing the modes](#comparing-the-modes)); means are close only where values vary
  smoothly. That is why `transfer = "auto"` runs overlay there. Above about 10–60 pixels per cell,
  overlay is both exact and faster.
- **Very coarse cells.** Above 500 km across, overlay takes whole pixels by centre (and
  ground-area overlay refuses), so the choice between overlay and centre assignment stops
  mattering.

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
full ellipsoidal area; `transfer = "auto"` runs the overlay at every ratio, as ground area can't interpolate.

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
- `auto`, which runs `overlay` at every ratio (unless an option forces centre assignment).

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
time, and runs of whole pixels inside a cell are accumulated as slices without indexing each
pixel, so cells spanning many pixels cost less than centre assignment.

### Comparing the modes

`scripts/benchmark_transfer_modes.py` times every mode in a fresh process and scores it against
weighted overlay, which clips every cell against every pixel exactly. It needs only Python's
standard library and one release example, and writes its own tiled, Deflate-compressed Float32
GeoTIFFs in EPSG:4326 near 37.5° N. Continuous rasters hold a 0–100 west–east gradient plus a
50-unit, 0.01° checkerboard, so cells see both smooth variation and sharp edges; categorical
rasters hold eight classes, the checkerboard's two colours in four west–east bands.

```sh
cargo build --release -p dggs-stream-core --example transfer_compare
python3 scripts/benchmark_transfer_modes.py --runs 5 --markdown                       # fixed pixel size
python3 scripts/benchmark_transfer_modes.py --sweep ratio --runs 5 --markdown         # fixed cells, S2
python3 scripts/benchmark_transfer_modes.py --sweep ratio --grid h3 --resolution 11 --markdown
python3 scripts/benchmark_transfer_modes.py --sweep ratio --kind categorical --markdown
```

The columns:

- **Cells missing**: overlay's cells that the mode leaves out.
- **Error**: for continuous scans, the average absolute difference of cell means from overlay's,
  on the 0–150 field; for categorical scans, the average class-fraction distance from overlay's
  (half the summed absolute differences of the fractions: 0 is identical, 1 is disjoint).
- **Area error**: average relative difference of a cell's covered area (`count`, in pixels) from
  overlay's. Not meaningful for interpolation, whose count is always 1.

All results are medians of five runs, in alternating order after a warm-up, on a 4-vCPU Intel
Xeon (2.1 GHz) Linux VM with default worker settings. Peak RSS is the scan process's own
(`VmHWM`).

#### Fixed pixel size

H3 resolutions 7–13 over rasters with one pixel size (1/2048°, about 2,350 m²): 2,048 pixels
square at resolutions 7–9, then 1,024, 512, 256 and 128. The ratio changes 7× per resolution and
the cell count grows with it, so this shows how each mode scales with the raster and with the
number of cells. `auto` runs `overlay` at resolutions 7–10 and `bilinear` at 11–13.

| H3 res | Pixels per cell | Mode | Cells | Scan ms | Peak RSS MiB | Cells missing | Mean abs. error | Area error |
| ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 7 | 2,209 | `center` | 1,910 | 473 | 13 | 0.1% | 0.032 | 0.09% |
| 7 | 2,209 | `rgss` | 1,912 | 1,164 | 12 | 0.0% | 0.012 | 0.12% |
| 7 | 2,209 | `9point` | 1,911 | 2,453 | 13 | 0.1% | 0.006 | 0.02% |
| 7 | 2,209 | `16point` | 1,912 | 4,008 | 12 | 0.0% | 0.002 | 0.02% |
| 7 | 2,209 | `overlay` | 1,912 | 23 | 24 | — | — | — |
| 7 | 2,209 | `bilinear` | 1,809 | 10 | 23 | 5.4% | 25.045 | — |
| 8 | 316 | `center` | 12,914 | 476 | 13 | 0.1% | 0.093 | 0.28% |
| 8 | 316 | `rgss` | 12,924 | 1,173 | 13 | 0.1% | 0.039 | 0.16% |
| 8 | 316 | `9point` | 12,924 | 2,378 | 13 | 0.1% | 0.024 | 0.10% |
| 8 | 316 | `16point` | 12,927 | 4,073 | 13 | 0.0% | 0.010 | 0.05% |
| 8 | 316 | `overlay` | 12,931 | 56 | 26 | — | — | — |
| 8 | 316 | `bilinear` | 12,656 | 14 | 24 | 2.1% | 16.099 | — |
| 9 | 45 | `center` | 89,221 | 514 | 15 | 0.1% | 0.353 | 3.56% |
| 9 | 45 | `rgss` | 89,289 | 1,310 | 15 | 0.0% | 0.122 | 0.78% |
| 9 | 45 | `9point` | 89,293 | 2,474 | 15 | 0.0% | 0.069 | 0.51% |
| 9 | 45 | `16point` | 89,303 | 4,253 | 15 | 0.0% | 0.033 | 0.19% |
| 9 | 45 | `overlay` | 89,329 | 208 | 47 | — | — | — |
| 9 | 45 | `bilinear` | 88,611 | 50 | 30 | 0.8% | 6.291 | — |
| 10 | 6.4 | `center` | 155,941 | 248 | 20 | 0.2% | 0.657 | 10.35% |
| 10 | 6.4 | `rgss` | 156,166 | 493 | 18 | 0.1% | 0.223 | 2.93% |
| 10 | 6.4 | `9point` | 156,167 | 820 | 18 | 0.1% | 0.122 | 1.77% |
| 10 | 6.4 | `16point` | 156,211 | 1,305 | 17 | 0.1% | 0.055 | 0.89% |
| 10 | 6.4 | `overlay` | 156,297 | 317 | 51 | — | — | — |
| 10 | 6.4 | `bilinear` | 155,350 | 74 | 21 | 0.6% | 1.743 | — |
| 11 | 0.92 | `center` | 236,618 | 155 | 38 | 13.4% | 0.908 | 15.16% |
| 11 | 0.92 | `rgss` | 272,859 | 238 | 31 | 0.2% | 0.354 | 18.66% |
| 11 | 0.92 | `9point` | 272,873 | 333 | 29 | 0.2% | 0.234 | 10.23% |
| 11 | 0.92 | `16point` | 273,046 | 511 | 27 | 0.1% | 0.143 | 6.56% |
| 11 | 0.92 | `overlay` | 273,378 | 476 | 61 | — | — | — |
| 11 | 0.92 | `bilinear` | 272,124 | 128 | 29 | 0.5% | 0.131 | — |
| 12 | 0.13 | `center` | 65,536 | 55 | 33 | 86.3% | 0.000 | 627% |
| 12 | 0.13 | `rgss` | 253,825 | 130 | 38 | 46.9% | 0.456 | 88% |
| 12 | 0.13 | `9point` | 441,014 | 251 | 43 | 7.8% | 0.322 | 33.6% |
| 12 | 0.13 | `16point` | 476,981 | 335 | 37 | 0.2% | 0.226 | 21.2% |
| 12 | 0.13 | `overlay` | 478,094 | 876 | 60 | — | — | — |
| 12 | 0.13 | `bilinear` | 476,436 | 225 | 28 | 0.3% | 0.746 | — |
| 13 | 0.019 | `center` | 16,384 | 21 | 14 | 98.0% | 0.000 | 4,990% |
| 13 | 0.019 | `rgss` | 65,536 | 51 | 32 | 92.2% | 0.025 | 1,173% |
| 13 | 0.019 | `9point` | 147,456 | 84 | 39 | 82.4% | 0.000 | 466% |
| 13 | 0.019 | `16point` | 262,144 | 140 | 39 | 68.6% | 0.026 | 218% |
| 13 | 0.019 | `overlay` | 836,161 | 1,515 | 59 | — | — | — |
| 13 | 0.019 | `bilinear` | 833,971 | 413 | 28 | 0.3% | 1.009 | — |


#### Fixed cell count

One resolution over a fixed region of about 60,000 cells, with only the pixel size changing
between rows (`--sweep ratio`), in steps fine enough to place the thresholds. `auto runs` is the
mode the current rule picks.

**S2 level 13 (about 1.27 km²), continuous, about 69,400 cells**

| Pixels per cell | `center` ms | `center` missing | `center` error | `center` area error | `16point` ms | `16point` error | `overlay` ms | `bilinear` ms | `bilinear` error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.245 | 10.7 | 78.6% | 0.006 | 362.6% | 41.7 | 1.439 | 74.3 | 24.1 | 3.096 | `bilinear` |
| 0.492 | 16.8 | 56.9% | 2.714 | 130.1% | 57.1 | 1.517 | 65.8 | 18.1 | 3.161 | `bilinear` |
| 0.987 | 30.4 | 14.4% | 14.761 | 16.3% | 66.7 | 1.995 | 71.0 | 18.1 | 2.048 | `bilinear` |
| 1.48 | 49.1 | 0.4% | 14.767 | 33.8% | 85.1 | 1.425 | 73.3 | 22.4 | 3.405 | `overlay` |
| 1.97 | 36.0 | 0.4% | 10.885 | 37.9% | 92.8 | 1.049 | 77.0 | 20.3 | 4.523 | `overlay` |
| 2.96 | 34.0 | 0.3% | 6.869 | 29.6% | 115.0 | 0.911 | 68.9 | 19.0 | 6.446 | `overlay` |
| 3.95 | 42.5 | 0.3% | 5.154 | 15.2% | 154.2 | 0.749 | 74.6 | 21.8 | 8.204 | `overlay` |
| 5.92 | 43.4 | 0.2% | 4.144 | 20.2% | 225.3 | 0.514 | 84.6 | 22.3 | 9.844 | `overlay` |
| 7.9 | 47.1 | 0.2% | 2.676 | 9.1% | 256.0 | 0.469 | 86.0 | 20.8 | 10.935 | `overlay` |
| 11.9 | 51.3 | 0.1% | 2.650 | 14.2% | 415.8 | 0.431 | 94.8 | 21.3 | 12.363 | `overlay` |
| 15.8 | 48.5 | 0.1% | 1.746 | 3.8% | 466.0 | 0.341 | 82.8 | 22.5 | 13.255 | `overlay` |
| 23.7 | 60.7 | 0.1% | 1.485 | 3.1% | 664.6 | 0.274 | 103.6 | 25.2 | 13.947 | `overlay` |
| 31.6 | 68.7 | 0.1% | 1.154 | 7.0% | 912.0 | 0.230 | 90.1 | 22.3 | 15.014 | `overlay` |
| 63.2 | 118.3 | 0.1% | 0.763 | 2.2% | 1,696.2 | 0.162 | 120.3 | 36.6 | 16.652 | `overlay` |

**H3 resolution 11 (about 2,150 m²), continuous, about 57,900 cells**

| Pixels per cell | `center` ms | `center` missing | `center` error | `center` area error | `16point` ms | `16point` error | `overlay` ms | `bilinear` ms | `bilinear` error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.249 | 20.5 | 74.3% | 0.095 | 285.3% | 65.5 | 0.408 | 125.3 | 32.6 | 1.179 | `bilinear` |
| 0.501 | 32.7 | 48.3% | 0.643 | 91.8% | 89.8 | 0.216 | 129.4 | 35.2 | 0.490 | `bilinear` |
| 1 | 47.8 | 8.8% | 1.002 | 16.6% | 140.6 | 0.136 | 126.6 | 29.0 | 0.136 | `overlay` |
| 1.51 | 64.2 | 0.8% | 0.794 | 31.9% | 187.0 | 0.109 | 128.9 | 38.6 | 0.209 | `overlay` |
| 2 | 69.7 | 0.7% | 0.654 | 19.8% | 223.6 | 0.081 | 130.7 | 33.3 | 0.321 | `overlay` |
| 3.01 | 74.0 | 0.6% | 0.464 | 20.2% | 287.3 | 0.063 | 124.0 | 31.7 | 0.459 | `overlay` |
| 4.02 | 73.5 | 0.5% | 0.403 | 9.6% | 347.3 | 0.051 | 115.7 | 30.6 | 0.546 | `overlay` |
| 6.03 | 94.4 | 0.4% | 0.277 | 13.1% | 509.9 | 0.034 | 141.8 | 33.3 | 0.646 | `overlay` |
| 8.04 | 118.2 | 0.4% | 0.209 | 11.3% | 607.4 | 0.024 | 145.6 | 32.8 | 0.707 | `overlay` |
| 12.1 | 153.1 | 0.3% | 0.156 | 8.4% | 895.9 | 0.020 | 125.4 | 30.4 | 0.780 | `overlay` |
| 16.1 | 179.8 | 0.3% | 0.133 | 5.3% | 1,182.0 | 0.015 | 148.2 | 41.0 | 0.822 | `overlay` |
| 24.1 | 257.7 | 0.2% | 0.108 | 4.3% | 1,641.8 | 0.007 | 133.8 | 38.8 | 0.874 | `overlay` |
| 32.2 | 330.2 | 0.2% | 0.080 | 4.7% | 2,135.5 | 0.006 | 142.4 | 33.5 | 0.903 | `overlay` |
| 64.3 | 475.6 | 0.1% | 0.051 | 2.0% | 4,013.3 | 0.004 | 169.1 | 37.6 | 0.962 | `overlay` |

**S2 level 13, categorical (eight classes), about 69,400 cells; error is the class-fraction distance**

| Pixels per cell | `center` ms | `center` missing | `center` error | `center` area error | `16point` ms | `16point` error | `overlay` ms | `nearest` ms | `nearest` error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.245 | 10.1 | 78.6% | 0.000 | 362.6% | 43.9 | 0.029 | 78.3 | 21.5 | 0.071 | `overlay` |
| 0.492 | 17.5 | 56.9% | 0.055 | 130.1% | 57.2 | 0.031 | 87.0 | 18.3 | 0.108 | `overlay` |
| 0.987 | 32.5 | 14.4% | 0.296 | 16.3% | 75.8 | 0.040 | 86.6 | 18.8 | 0.323 | `overlay` |
| 1.48 | 43.0 | 0.4% | 0.296 | 33.8% | 89.5 | 0.029 | 81.6 | 22.0 | 0.394 | `overlay` |
| 1.97 | 37.4 | 0.4% | 0.218 | 37.9% | 113.4 | 0.021 | 78.2 | 21.9 | 0.392 | `overlay` |
| 2.96 | 41.2 | 0.3% | 0.138 | 29.6% | 144.4 | 0.018 | 83.2 | 22.9 | 0.397 | `overlay` |
| 3.95 | 46.7 | 0.3% | 0.104 | 15.2% | 170.7 | 0.015 | 103.1 | 22.8 | 0.394 | `overlay` |
| 5.92 | 49.0 | 0.2% | 0.084 | 20.2% | 224.9 | 0.010 | 105.9 | 26.2 | 0.395 | `overlay` |
| 7.9 | 46.7 | 0.2% | 0.054 | 9.1% | 303.7 | 0.009 | 116.8 | 26.3 | 0.395 | `overlay` |
| 11.9 | 43.3 | 0.1% | 0.053 | 14.2% | 373.9 | 0.009 | 106.1 | 23.3 | 0.395 | `overlay` |
| 15.8 | 51.9 | 0.1% | 0.036 | 3.8% | 553.2 | 0.007 | 100.6 | 25.7 | 0.393 | `overlay` |
| 23.7 | 63.9 | 0.1% | 0.030 | 3.1% | 736.4 | 0.006 | 102.3 | 23.9 | 0.396 | `overlay` |
| 31.6 | 71.7 | 0.1% | 0.023 | 7.0% | 914.4 | 0.005 | 110.9 | 23.8 | 0.394 | `overlay` |
| 63.2 | 105.8 | 0.1% | 0.015 | 2.2% | 1,942.3 | 0.003 | 138.3 | 31.6 | 0.395 | `overlay` |

#### What the numbers show

- **Overlay costs about 1–2 µs per cell, almost independent of the ratio**, so it is exact at every
  size for a nearly flat cost: 66–120 ms for 69,000 S2 cells and 116–169 ms for 58,000 H3 cells
  from a quarter of a pixel to 64 pixels per cell. It used up to three times the memory of
  centre assignment (24–61 MiB against 12–40).
- **Centre assignment indexes every pixel, so its cost grows with the ratio.** Overlay became
  faster from about 10 pixels per cell on H3 and about 60 on S2 (S2 indexes a pixel more
  cheaply), and 2.5–20× faster with hundreds or thousands of pixels per cell.
- **Between 1 and 16 pixels per cell, centre assignment is fast but coarse.** It was 1.2–2.6×
  faster than overlay, but put each cell's area 4–38% off on average, left up to 14% of the cells
  empty at about one pixel per cell, and had its largest mean errors between 1 and 6 pixels per
  cell. On categorical rasters its class fractions were 0.04–0.30 away from overlay's.
- **Below one pixel per cell, `bilinear` is the fast way to fill every cell.** It ran in a quarter
  to a third of overlay's time and left about 1% of the cells empty, against 14–98% for centre
  assignment. Overlay and `bilinear` answer different questions here: overlay gives each tiny
  cell the pixels under it, a step function, while `bilinear` gives a smooth surface, so their
  difference near the checkerboard's 50-unit steps is not an error of either.
- **For categorical rasters, `nearest` is fast but blunt.** It looks up one class at the cell's
  centre, so its class fractions were 0.07–0.11 from overlay's at a quarter to half a pixel per
  cell and 0.32–0.40 from about a pixel up, where overlay splits cells between classes. Overlay
  cost about 4× as much (78–87 ms against 18–22 ms below one pixel per cell).
- **Sub-pixel sampling rarely came out ahead.** Above a few pixels per cell, `overlay` was both
  faster and exact; `rgss` cost 2–4× centre assignment and `16point` 5–14×. Its one niche is
  somewhat below one pixel per cell, where `16point` reached nearly every cell and stayed closer to
  overlay than `bilinear` or `nearest`, in 40–95% of overlay's time.

These results set [`transfer = "auto"`](#transfer--auto): `bilinear` below 1:1 for continuous
scans, weighted `overlay` from 1:1 up and for categorical scans at every ratio. `auto` gives up
at most about 2.6× in speed against centre assignment, in the band where centre assignment's
errors are largest.

Caveats: one machine, synthetic fields, H3 and S2 only, single-band scans of local files, and
default worker pools (overlay clips cells on the ambient Rayon pool). Pixel-dense real rasters,
remote COGs and the other grids may shift the crossovers.

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
