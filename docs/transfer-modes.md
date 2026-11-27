# Transfer Modes: How Pixels Become Cells

[← Back to README](../README.md)

`transfer` chooses how raster pixels are mapped onto grid cells. It works the same way on every
grid (H3, A5, S2, HEALPix) and in every engine except PostgreSQL. This page covers, in order:

1. [Choosing a mode](#choosing-a-mode), or [letting the engine choose](#let-the-engine-choose-auto)
2. [`assign_centers`](#assign_centers-the-default), the default
3. [`interpolate`](#interpolate-the-value-at-or-across-each-cell), for cells finer than the pixels, optionally with sub-cell `sampling`
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
| `overlay` | Every pixel the cell overlaps, weighted by the part of the pixel inside the cell | Cells are larger than pixels, edges should cut pixels exactly, or totals or ground areas must come out right; with many pixels per cell it can also be the fastest mode |

```text
        cell area / pixel area
  0 ──────────── 1 ─────────────────────────────────────────────────▶
  │ interpolate  │                       overlay
  │ (cells are   │  exact at every size. assign_centers is up to 2–4x faster from
  │  sub-pixel)  │  about 1 to 12 pixels per cell (more on S2 and HEALPix), at the
  │              │  cost of 4–38% per-cell area errors and some empty cells
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
2.2–3.9× faster than overlay at about one pixel per cell, where it left 9–26% of the cells empty
and put their areas 16–35% off, and still 1.7–2.7× faster at 16 pixels per cell on S2 and
HEALPix; on H3 and A5 overlay was also faster from about 12 pixels per cell. Ask for
`assign_centers` explicitly when speed matters more than edges.

Scan options take precedence over the ratio:

- `compact` and `spectral_formula` work only with centre assignment, so `auto` runs
  `assign_centers`.
- A non-`center` `sampling` rules out `overlay`, which refuses it, so `auto` chooses between the
  two modes that accept it: when every requested resolution's cells are finer than the pixels,
  `interpolate` with sub-cell samples (`nearest` for categorical scans, `bilinear` otherwise);
  otherwise `assign_centers` with sub-pixel samples. Below one pixel per cell, sparse sub-pixel
  presets leave cells empty and, for classes, sub-pixel samples sat further from overlay's shares
  than sub-cell ones. From one pixel per cell up, sub-pixel samples were as close to overlay as
  sub-cell ones or closer for classes, and closer at 2 pixels per cell for continuous scans
  ([sub-cell sampling](#sub-cell-sampling-and-the-two-references)).
- `valid_coverage_threshold` and `histogram_weight = "area"` need overlap areas, so `auto` runs
  weighted `overlay`.
- Partitioned scans (`plan_partitions`, Spark) run `assign_centers`. Pass `transfer = "overlay"`
  explicitly for a [partitioned overlay](#partitioned-overlay).
- `ground_area` and multi-band (`bands`) scans can't interpolate, so below 1:1 `auto` runs
  weighted `overlay` for them too.
- Resolutions whose cells are over 500 km across (the coarsest two or three of each grid) run
  `assign_centers`: overlay would take whole pixels by centre there anyway. A continuous scan
  whose cells are all smaller than its pixels still interpolates, however large both are.
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
few pixels per cell. Overlay overtakes it from about 12 pixels per cell on H3 and A5, but not by
64 pixels per cell on S2 and HEALPix ([comparison](#comparing-the-modes)).

A pixel straddling a cell edge goes wholly to one side. With many pixels per cell this evens out;
with a few it doesn't, and with cells finer than the pixels most cells contain no pixel centre
and are missing from the output. Use `overlay` or `interpolate` for those cases.

`assign`, `centers` and `centres` are accepted as names too.

## `interpolate`: the value at (or across) each cell

Every cell whose centre lies on the raster gets the raster's value at that centre, interpolated
from the surrounding pixels. By default (`sampling = 'center'`), each cell is evaluated once at its
centre, so `count` is 1, `mean`, `min`, `max` and `sum` all hold that interpolated value, and
`stddev` is `0`.

With a non-default [`sampling`](super-sampling.md) pattern (`'rgss'`, `'8rooks'`, `'16point'`, etc.),
`interpolate` projects the cell's boundary polygon into pixel space once and maps the `[0, 1]^2`
offsets across the cell interior with constant-Jacobian star-triangle mapping (`CellPolygonSampler`).
Each valid sample point adds its normalized weight to the cell, so a fully covered cell keeps
`count = 1.0` (edge or partial-NoData cells receive their valid tap weight in `(0, 1]`), while `min`,
`max`, `stddev`, and `quantiles` reflect intra-cell variation across the smooth interpolated surface.
Pairing `interpolation = 'bicubic'` with `'rgss'` or `'16point'` gives the closest cell means to the
field's true cell means when pixels sample a smooth field: in the benchmark, 2–13× closer than
overlay from 1 to 8 pixels per cell. It does not help against overlay's pixel-area answer, or on
fields with sharp edges, where overlay and sub-pixel sampling stay closer; see
[sub-cell sampling](#sub-cell-sampling-and-the-two-references).

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
  cell, centre assignment is up to 2.2–3.9× faster, but each cell's area comes out 4–38% off on
  average and up to a quarter of the cells can be empty at about one pixel per cell (see
  [comparing the modes](#comparing-the-modes)); means are close only where values vary smoothly.
  That is why `transfer = "auto"` runs overlay there. With more pixels per cell overlay is both
  exact and, on H3 and A5, faster; on S2 and HEALPix centre assignment stayed faster through 64
  pixels per cell.
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
| `sampling` other than `center` with `overlay` | Overlay already integrates each pixel's exact overlap; `assign_centers` and `interpolate` accept `sampling` |
| `compact` | Not supported by the transfer kernel |
| `spectral_formula` | Not supported by the transfer kernel |
| Partitioned scans, except single-band continuous weighted overlay | Only that case [merges across partitions](#partitioned-overlay) |
| `interpolation` without `transfer = 'interpolate'` | The option belongs to the other mode |
| `overlay_weighted` / `mass_preserve` without `transfer = 'overlay'` | The options belong to the other mode |

Each of these is rejected with an error, not ignored (`transfer = 'auto'` steers around them
instead). A resolution so fine that one block of the raster would hold too many cells is also
rejected, with a message asking for a coarser resolution.

### `transfer` vs. `sampling`

`sampling` (`'rgss'`, `'8rooks'`, `'16point'`, ...) works as a quadrature pattern in `[0, 1]^2` and pairs with **two** transfer modes in opposite directions:

- **With `assign_centers` (sub-pixel sampling):** evaluates multiple points inside each **pixel** and assigns each sample's weight to the cell containing it. Useful when `assign_centers` features are required (`spectral_formula`, compaction, partitions, `> 500 km` cells); otherwise `overlay` computes pixel splitting analytically.
- **With `interpolate` (sub-cell sampling):** maps the pattern inside each **cell** polygon and interpolates the raster at each sample point. Useful with `bicubic` from about one pixel per cell up when pixels sample a smooth field, for classes with `nearest` below one pixel per cell, and whenever intra-cell `stddev`/`min`/`max`/`quantiles` are needed ([sub-cell sampling](#sub-cell-sampling-and-the-two-references)).
- **With `overlay`:** refused (non-`'center'` `sampling` is rejected with an error).

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
50-unit checkerboard, so cells see both smooth variation and sharp edges; categorical rasters
hold eight classes, the checkerboard's two colours in four west–east bands.

```sh
cargo build --release -p dggs-stream-core --example transfer_compare
python3 scripts/benchmark_transfer_modes.py --runs 5 --markdown                       # fixed pixel size, H3
python3 scripts/benchmark_transfer_modes.py --sweep ratio --grid s2 --resolution 13 --markdown
python3 scripts/benchmark_transfer_modes.py --sweep ratio --grid a5 --resolution 12 --kind categorical --markdown
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
number of cells. Its checkerboard squares are a fixed 0.01° (about 1 km), so the value errors depend on how
that compares with each resolution's cells: compare modes within a resolution, not errors across
resolutions. The fixed-cell-count sweeps below scale the field with the cell. `auto` runs `overlay` at resolutions 7–10 and `bilinear` at 11–13.

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


On H3, with 45 to 2,200 pixels per cell (resolutions 7–9), overlay ran 2.5–20× faster than
centre assignment. Whole-pixel runs inside a cell are summed without indexing each pixel, while
centre assignment indexes all 4.2 million pixels whatever the resolution (about 475 ms here).
Overlay used two to three times the memory (24–47 MiB against 13–15). Whether it wins at a given
ratio depends on the grid, as the next section shows.

#### Fixed cell count

One resolution per grid over a fixed region of about 60,000 cells (H3 resolution 11, S2 level 13,
A5 resolution 12, HEALPix depth 12), with only the pixel size changing between rows
(`--sweep ratio`). The checkerboard squares are one cell edge wide on every grid
(`--feature-cells 1`), so the field varies on the same scale relative to the cell everywhere and
value errors can be compared across grids; area errors never depend on the field. Rows are target
ratios; the actual ratio is within 2%. Continuous and categorical scans run on all four grids.

**Centre assignment's scan time divided by overlay's.** Below 1, centre assignment is faster; bold
marks overlay faster.

| Pixels per cell | H3 cont. | S2 cont. | A5 cont. | HEALPix cont. | H3 cat. | S2 cat. | A5 cat. | HEALPix cat. |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.25 | 0.18 | 0.14 | 0.21 | 0.13 | 0.14 | 0.12 | 0.14 | 0.10 |
| 0.5 | 0.25 | 0.23 | 0.24 | 0.19 | 0.21 | 0.23 | 0.21 | 0.20 |
| 1 | 0.34 | 0.45 | 0.35 | 0.26 | 0.36 | 0.40 | 0.34 | 0.27 |
| 1.5 | 0.54 | 0.61 | 0.55 | 0.33 | 0.47 | 0.50 | 0.45 | 0.33 |
| 2 | 0.53 | 0.55 | **1.09** | 0.40 | 0.50 | 0.45 | 0.67 | 0.40 |
| 3 | 0.53 | 0.45 | 0.89 | 0.32 | 0.54 | 0.49 | 0.59 | 0.36 |
| 4 | 0.58 | 0.46 | 0.94 | 0.35 | 0.55 | 0.48 | 0.66 | 0.35 |
| 6 | 0.65 | 0.54 | **1.42** | 0.42 | 0.72 | 0.61 | 0.99 | 0.39 |
| 8 | 0.85 | 0.51 | 0.82 | 0.42 | 0.76 | 0.51 | **1.15** | 0.38 |
| 12 | **1.09** | 0.65 | **1.34** | 0.41 | **1.11** | 0.47 | **3.07** | 0.38 |
| 16 | **1.29** | 0.59 | **1.55** | 0.46 | **1.12** | 0.48 | **3.32** | 0.37 |
| 24 | **1.69** | 0.61 | **4.43** | 0.45 | **1.25** | 0.64 | **4.07** | 0.45 |
| 32 | **1.67** | 0.70 | **4.68** | 0.56 | **1.76** | 0.66 | **4.08** | 0.49 |
| 64 | **2.90** | 0.92 | **5.58** | 0.64 | **2.73** | 0.80 | **5.63** | 0.65 |


**Centre assignment's accuracy against overlay**, as the range over the four grids and both kinds
(class-fraction error over the four categorical sweeps, mean absolute error over the four
continuous ones):

| Pixels per cell | Cells missing | Area error | Class-fraction error | Mean abs. error |
| ---: | ---: | ---: | ---: | ---: |
| 0.25 | 74.3–78.6% | 285.3–362.6% | 0.000–0.002 | 0.0–0.1 |
| 0.5 | 48.3–56.9% | 91.8–130.1% | 0.077–0.119 | 3.8–5.9 |
| 1 | 8.8–26.2% | 16.3–34.6% | 0.270–0.326 | 13.5–16.3 |
| 1.5 | 0.4–7.6% | 31.9–34.8% | 0.203–0.271 | 10.1–13.5 |
| 2 | 0.4–0.8% | 4.9–37.9% | 0.116–0.199 | 5.7–9.9 |
| 3 | 0.2–0.6% | 20.0–29.6% | 0.098–0.132 | 4.8–6.6 |
| 4 | 0.1–0.5% | 9.6–23.5% | 0.083–0.108 | 4.1–5.4 |
| 6 | 0.0–0.4% | 13.1–20.2% | 0.057–0.082 | 2.8–4.1 |
| 8 | 0.0–0.4% | 3.7–11.3% | 0.047–0.057 | 2.3–2.8 |
| 12 | 0.0–0.3% | 7.4–14.2% | 0.034–0.051 | 1.7–2.5 |
| 16 | 0.0–0.3% | 3.8–9.4% | 0.033–0.040 | 1.6–2.0 |
| 24 | 0.0–0.2% | 3.1–5.1% | 0.021–0.033 | 1.1–1.6 |
| 32 | 0.0–0.2% | 2.6–7.0% | 0.015–0.024 | 0.8–1.2 |
| 64 | 0.0–0.1% | 1.9–2.8% | 0.011–0.017 | 0.6–0.8 |


The full tables follow. Each compares `center`, `16point` (the densest sampling preset),
`overlay` and the interpolation `auto` would use, and names the mode `auto` ran.

<details>
<summary>H3 resolution 11, continuous, 57,917 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` mean abs. error | `center` area error | `16point` ms | `16point` mean abs. error | `overlay` ms | `bilinear` ms | `bilinear` mean abs. error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 20.7 | 74.3% | 0.02 | 285.3% | 62.1 | 0.07 | 112.2 | 27.5 | 0.19 | `bilinear` |
| 0.5 | 26.9 | 48.3% | 5.26 | 91.8% | 85.9 | 1.52 | 109.4 | 27.9 | 3.18 | `bilinear` |
| 1 | 37.2 | 8.8% | 16.20 | 16.6% | 127.3 | 1.63 | 107.9 | 29.1 | 0.91 | `overlay` |
| 1.5 | 59.3 | 0.8% | 10.08 | 31.9% | 161.0 | 1.03 | 109.7 | 28.4 | 3.26 | `overlay` |
| 2 | 59.8 | 0.7% | 6.65 | 19.8% | 186.3 | 0.79 | 112.0 | 28.0 | 4.90 | `overlay` |
| 3 | 60.8 | 0.6% | 4.83 | 20.2% | 266.6 | 0.63 | 114.5 | 29.3 | 7.08 | `overlay` |
| 4 | 70.6 | 0.5% | 4.39 | 9.6% | 331.5 | 0.56 | 121.2 | 32.2 | 8.52 | `overlay` |
| 6 | 90.5 | 0.4% | 2.81 | 13.1% | 478.0 | 0.33 | 138.2 | 29.4 | 10.29 | `overlay` |
| 8 | 97.7 | 0.4% | 2.36 | 11.3% | 576.2 | 0.38 | 114.9 | 29.8 | 11.35 | `overlay` |
| 12 | 144.0 | 0.3% | 1.68 | 8.4% | 840.2 | 0.17 | 132.4 | 29.2 | 12.74 | `overlay` |
| 16 | 154.5 | 0.3% | 1.63 | 5.3% | 1032.9 | 0.13 | 119.5 | 32.2 | 13.58 | `overlay` |
| 24 | 214.6 | 0.2% | 1.05 | 4.3% | 1512.3 | 0.13 | 127.0 | 30.4 | 14.59 | `overlay` |
| 32 | 246.5 | 0.2% | 0.76 | 4.7% | 2094.2 | 0.10 | 147.6 | 35.2 | 15.20 | `overlay` |
| 64 | 465.9 | 0.1% | 0.56 | 2.0% | 3821.6 | 0.05 | 160.7 | 36.5 | 16.40 | `overlay` |

</details>

<details>
<summary>H3 resolution 11, categorical, 57,917 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` class-fraction error | `center` area error | `16point` ms | `16point` class-fraction error | `overlay` ms | `nearest` ms | `nearest` class-fraction error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 18.4 | 74.3% | 0.001 | 285.3% | 71.0 | 0.002 | 134.2 | 30.0 | 0.005 | `overlay` |
| 0.5 | 26.7 | 48.3% | 0.106 | 91.8% | 92.7 | 0.031 | 128.5 | 35.7 | 0.188 | `overlay` |
| 1 | 43.7 | 8.8% | 0.324 | 16.6% | 132.6 | 0.033 | 121.7 | 30.8 | 0.387 | `overlay` |
| 1.5 | 65.9 | 0.8% | 0.203 | 31.9% | 178.0 | 0.021 | 140.5 | 32.2 | 0.389 | `overlay` |
| 2 | 67.9 | 0.7% | 0.134 | 19.8% | 208.5 | 0.016 | 135.9 | 32.8 | 0.390 | `overlay` |
| 3 | 68.8 | 0.6% | 0.098 | 20.2% | 266.3 | 0.013 | 128.2 | 31.7 | 0.390 | `overlay` |
| 4 | 70.5 | 0.5% | 0.088 | 9.6% | 333.4 | 0.011 | 127.8 | 40.1 | 0.390 | `overlay` |
| 6 | 94.4 | 0.4% | 0.057 | 13.1% | 507.6 | 0.007 | 131.5 | 36.6 | 0.390 | `overlay` |
| 8 | 109.8 | 0.4% | 0.047 | 11.3% | 624.8 | 0.008 | 144.2 | 35.0 | 0.390 | `overlay` |
| 12 | 152.3 | 0.3% | 0.034 | 8.4% | 908.0 | 0.003 | 137.7 | 32.7 | 0.391 | `overlay` |
| 16 | 166.8 | 0.3% | 0.033 | 5.3% | 1083.8 | 0.003 | 149.1 | 33.5 | 0.390 | `overlay` |
| 24 | 201.9 | 0.2% | 0.021 | 4.3% | 1559.3 | 0.003 | 161.5 | 34.9 | 0.390 | `overlay` |
| 32 | 282.1 | 0.2% | 0.015 | 4.7% | 2103.1 | 0.002 | 160.5 | 34.0 | 0.390 | `overlay` |
| 64 | 437.8 | 0.1% | 0.011 | 2.0% | 3911.8 | 0.001 | 160.4 | 39.4 | 0.390 | `overlay` |

</details>

<details>
<summary>S2 resolution 13, continuous, 69,420 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` mean abs. error | `center` area error | `16point` ms | `16point` mean abs. error | `overlay` ms | `bilinear` ms | `bilinear` mean abs. error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 8.6 | 78.6% | 0.00 | 362.6% | 38.2 | 0.10 | 60.1 | 16.9 | 0.17 | `bilinear` |
| 0.5 | 14.3 | 56.9% | 3.84 | 130.1% | 45.0 | 2.05 | 61.6 | 18.1 | 3.91 | `bilinear` |
| 1 | 28.5 | 14.4% | 16.27 | 16.3% | 64.1 | 1.95 | 63.3 | 16.3 | 2.49 | `bilinear` |
| 1.5 | 43.1 | 0.4% | 13.50 | 33.8% | 76.6 | 1.47 | 70.2 | 21.4 | 3.05 | `overlay` |
| 2 | 34.4 | 0.4% | 9.92 | 37.9% | 82.1 | 1.10 | 62.9 | 18.2 | 4.22 | `overlay` |
| 3 | 31.9 | 0.3% | 6.58 | 29.6% | 111.5 | 1.06 | 70.8 | 18.8 | 6.27 | `overlay` |
| 4 | 31.9 | 0.3% | 5.38 | 15.2% | 132.2 | 0.84 | 68.9 | 18.8 | 7.90 | `overlay` |
| 6 | 38.4 | 0.2% | 4.08 | 20.2% | 188.8 | 0.58 | 70.8 | 18.5 | 9.50 | `overlay` |
| 8 | 39.0 | 0.2% | 2.82 | 9.1% | 252.2 | 0.51 | 75.7 | 19.4 | 10.40 | `overlay` |
| 12 | 46.4 | 0.1% | 2.52 | 14.2% | 339.6 | 0.48 | 70.9 | 19.5 | 11.89 | `overlay` |
| 16 | 47.8 | 0.1% | 1.95 | 3.8% | 441.0 | 0.38 | 80.7 | 22.0 | 13.08 | `overlay` |
| 24 | 48.3 | 0.1% | 1.63 | 3.1% | 611.0 | 0.30 | 79.7 | 21.4 | 13.20 | `overlay` |
| 32 | 59.6 | 0.1% | 1.21 | 7.0% | 875.8 | 0.25 | 85.4 | 22.1 | 13.83 | `overlay` |
| 64 | 91.2 | 0.1% | 0.84 | 2.2% | 1636.4 | 0.18 | 98.9 | 27.7 | 14.64 | `overlay` |

</details>

<details>
<summary>S2 resolution 13, categorical, 69,420 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` class-fraction error | `center` area error | `16point` ms | `16point` class-fraction error | `overlay` ms | `nearest` ms | `nearest` class-fraction error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 8.9 | 78.6% | 0.000 | 362.6% | 41.8 | 0.002 | 73.9 | 18.5 | 0.006 | `overlay` |
| 0.5 | 16.9 | 56.9% | 0.077 | 130.1% | 52.7 | 0.041 | 71.8 | 18.6 | 0.170 | `overlay` |
| 1 | 32.5 | 14.4% | 0.326 | 16.3% | 71.9 | 0.040 | 81.0 | 19.1 | 0.358 | `overlay` |
| 1.5 | 38.5 | 0.4% | 0.271 | 33.8% | 85.5 | 0.030 | 76.2 | 19.4 | 0.359 | `overlay` |
| 2 | 36.2 | 0.4% | 0.199 | 37.9% | 93.5 | 0.022 | 80.3 | 19.1 | 0.356 | `overlay` |
| 3 | 40.2 | 0.3% | 0.132 | 29.6% | 128.1 | 0.021 | 81.6 | 20.7 | 0.355 | `overlay` |
| 4 | 40.0 | 0.3% | 0.108 | 15.2% | 173.4 | 0.017 | 82.7 | 22.0 | 0.358 | `overlay` |
| 6 | 49.0 | 0.2% | 0.082 | 20.2% | 206.6 | 0.012 | 80.8 | 23.5 | 0.352 | `overlay` |
| 8 | 42.8 | 0.2% | 0.057 | 9.1% | 254.5 | 0.010 | 83.2 | 20.1 | 0.353 | `overlay` |
| 12 | 42.5 | 0.1% | 0.051 | 14.2% | 379.0 | 0.010 | 90.0 | 22.1 | 0.353 | `overlay` |
| 16 | 48.8 | 0.1% | 0.040 | 3.8% | 511.2 | 0.008 | 101.1 | 26.6 | 0.360 | `overlay` |
| 24 | 58.6 | 0.1% | 0.033 | 3.1% | 676.9 | 0.006 | 92.0 | 24.4 | 0.352 | `overlay` |
| 32 | 69.1 | 0.1% | 0.024 | 7.0% | 850.0 | 0.005 | 105.2 | 23.1 | 0.350 | `overlay` |
| 64 | 94.1 | 0.1% | 0.017 | 2.2% | 1737.5 | 0.004 | 117.4 | 26.8 | 0.346 | `overlay` |

</details>

<details>
<summary>A5 resolution 12, continuous, 61,720 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` mean abs. error | `center` area error | `16point` ms | `16point` mean abs. error | `overlay` ms | `bilinear` ms | `bilinear` mean abs. error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 35.7 | 75.9% | 0.09 | 310.6% | 133.7 | 0.08 | 172.1 | 20.2 | 0.16 | `bilinear` |
| 0.5 | 43.2 | 53.6% | 5.88 | 113.5% | 148.2 | 1.92 | 176.3 | 21.8 | 2.96 | `bilinear` |
| 1 | 58.6 | 26.2% | 13.46 | 34.6% | 235.0 | 1.70 | 169.6 | 20.4 | 2.50 | `bilinear` |
| 1.5 | 93.9 | 7.6% | 10.87 | 34.8% | 434.1 | 1.10 | 170.9 | 21.8 | 4.62 | `overlay` |
| 2 | 195.0 | 0.8% | 7.73 | 15.6% | 802.0 | 0.98 | 179.3 | 21.9 | 5.89 | `overlay` |
| 3 | 176.8 | 0.6% | 5.38 | 20.0% | 1208.7 | 0.71 | 197.6 | 23.4 | 7.89 | `overlay` |
| 4 | 164.0 | 0.5% | 4.10 | 23.5% | 1551.5 | 0.56 | 174.8 | 21.8 | 9.53 | `overlay` |
| 6 | 258.5 | 0.4% | 3.39 | 13.7% | 2048.0 | 0.40 | 181.6 | 23.1 | 11.25 | `overlay` |
| 8 | 161.5 | 0.4% | 2.70 | 7.5% | 2371.7 | 0.32 | 196.3 | 22.6 | 12.31 | `overlay` |
| 12 | 268.9 | 0.3% | 1.83 | 9.7% | 2958.4 | 0.23 | 200.1 | 24.0 | 13.68 | `overlay` |
| 16 | 299.4 | 0.3% | 1.72 | 5.8% | 3564.1 | 0.17 | 193.2 | 26.3 | 14.49 | `overlay` |
| 24 | 901.3 | 0.2% | 1.12 | 5.1% | 4448.5 | 0.12 | 203.4 | 24.5 | 15.48 | `overlay` |
| 32 | 929.4 | 0.2% | 0.99 | 3.6% | 5285.3 | 0.09 | 198.7 | 25.8 | 16.08 | `overlay` |
| 64 | 1284.9 | 0.1% | 0.56 | 1.9% | 7777.0 | 0.05 | 230.3 | 31.2 | 17.27 | `overlay` |

</details>

<details>
<summary>A5 resolution 12, categorical, 61,720 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` class-fraction error | `center` area error | `16point` ms | `16point` class-fraction error | `overlay` ms | `nearest` ms | `nearest` class-fraction error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 26.0 | 75.9% | 0.002 | 310.6% | 116.1 | 0.002 | 182.9 | 21.8 | 0.006 | `overlay` |
| 0.5 | 38.3 | 53.6% | 0.119 | 113.5% | 141.0 | 0.039 | 185.3 | 22.2 | 0.195 | `overlay` |
| 1 | 60.3 | 26.2% | 0.270 | 34.6% | 202.8 | 0.034 | 178.2 | 22.7 | 0.405 | `overlay` |
| 1.5 | 87.6 | 7.6% | 0.219 | 34.8% | 530.8 | 0.022 | 195.8 | 25.8 | 0.406 | `overlay` |
| 2 | 120.2 | 0.8% | 0.156 | 15.6% | 793.3 | 0.020 | 179.6 | 23.2 | 0.403 | `overlay` |
| 3 | 114.1 | 0.6% | 0.109 | 20.0% | 1186.8 | 0.014 | 194.1 | 25.9 | 0.399 | `overlay` |
| 4 | 124.9 | 0.5% | 0.083 | 23.5% | 1497.2 | 0.011 | 189.2 | 23.0 | 0.407 | `overlay` |
| 6 | 192.1 | 0.4% | 0.068 | 13.7% | 2048.8 | 0.008 | 193.4 | 24.7 | 0.407 | `overlay` |
| 8 | 240.5 | 0.4% | 0.055 | 7.5% | 2421.1 | 0.006 | 209.3 | 28.2 | 0.406 | `overlay` |
| 12 | 655.4 | 0.3% | 0.037 | 9.7% | 3045.2 | 0.005 | 213.3 | 28.3 | 0.407 | `overlay` |
| 16 | 723.0 | 0.3% | 0.035 | 5.8% | 3690.1 | 0.004 | 218.0 | 25.3 | 0.408 | `overlay` |
| 24 | 875.0 | 0.2% | 0.023 | 5.1% | 4660.2 | 0.002 | 214.8 | 26.8 | 0.407 | `overlay` |
| 32 | 965.7 | 0.2% | 0.020 | 3.6% | 5492.0 | 0.002 | 236.8 | 28.9 | 0.407 | `overlay` |
| 64 | 1305.3 | 0.1% | 0.011 | 1.9% | 7938.5 | 0.001 | 231.8 | 32.0 | 0.408 | `overlay` |

</details>

<details>
<summary>HEALPix resolution 12, continuous, 61,740 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` mean abs. error | `center` area error | `16point` ms | `16point` mean abs. error | `overlay` ms | `bilinear` ms | `bilinear` mean abs. error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 9.9 | 75.9% | 0.01 | 311.4% | 33.1 | 0.05 | 78.2 | 13.9 | 0.21 | `bilinear` |
| 0.5 | 14.1 | 51.5% | 4.66 | 104.7% | 41.3 | 1.90 | 73.4 | 13.2 | 3.80 | `bilinear` |
| 1 | 20.3 | 18.7% | 14.18 | 22.3% | 45.1 | 1.64 | 79.4 | 14.0 | 0.57 | `bilinear` |
| 1.5 | 26.5 | 4.8% | 10.46 | 34.4% | 45.6 | 1.06 | 79.4 | 14.5 | 3.08 | `overlay` |
| 2 | 35.2 | 0.5% | 5.71 | 4.9% | 61.4 | 0.86 | 87.4 | 18.1 | 5.77 | `overlay` |
| 3 | 29.0 | 0.2% | 5.55 | 21.1% | 72.9 | 0.76 | 91.5 | 16.2 | 7.10 | `overlay` |
| 4 | 29.1 | 0.1% | 4.57 | 17.1% | 83.7 | 0.67 | 83.4 | 16.2 | 8.32 | `overlay` |
| 6 | 34.1 | 0.0% | 3.22 | 15.1% | 110.4 | 0.49 | 80.7 | 14.9 | 10.22 | `overlay` |
| 8 | 34.8 | 0.0% | 2.31 | 3.7% | 131.8 | 0.39 | 83.7 | 14.9 | 11.00 | `overlay` |
| 12 | 36.5 | 0.0% | 1.91 | 7.4% | 174.1 | 0.34 | 88.7 | 15.2 | 13.33 | `overlay` |
| 16 | 42.1 | 0.0% | 1.64 | 9.4% | 222.9 | 0.29 | 91.9 | 16.1 | 13.39 | `overlay` |
| 24 | 47.7 | 0.0% | 1.17 | 4.6% | 337.3 | 0.21 | 105.3 | 16.4 | 14.60 | `overlay` |
| 32 | 56.4 | 0.0% | 0.94 | 2.6% | 408.3 | 0.16 | 100.2 | 19.5 | 14.98 | `overlay` |
| 64 | 79.8 | 0.0% | 0.69 | 2.8% | 754.9 | 0.09 | 125.6 | 23.2 | 16.09 | `overlay` |

</details>

<details>
<summary>HEALPix resolution 12, categorical, 61,740 cells</summary>

| Pixels per cell | `center` ms | `center` missing | `center` class-fraction error | `center` area error | `16point` ms | `16point` class-fraction error | `overlay` ms | `nearest` ms | `nearest` class-fraction error | `auto` runs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 0.25 | 8.9 | 75.9% | 0.000 | 311.4% | 33.9 | 0.002 | 85.0 | 15.3 | 0.005 | `overlay` |
| 0.5 | 17.9 | 51.5% | 0.093 | 104.7% | 46.2 | 0.038 | 89.5 | 14.9 | 0.172 | `overlay` |
| 1 | 22.9 | 18.7% | 0.284 | 22.3% | 60.9 | 0.033 | 83.9 | 14.4 | 0.385 | `overlay` |
| 1.5 | 28.0 | 4.8% | 0.210 | 34.4% | 47.8 | 0.021 | 85.5 | 16.3 | 0.385 | `overlay` |
| 2 | 34.5 | 0.5% | 0.116 | 4.9% | 58.6 | 0.017 | 87.3 | 15.3 | 0.379 | `overlay` |
| 3 | 33.5 | 0.2% | 0.112 | 21.1% | 76.1 | 0.015 | 93.4 | 16.6 | 0.386 | `overlay` |
| 4 | 32.4 | 0.1% | 0.092 | 17.1% | 87.1 | 0.014 | 91.9 | 16.4 | 0.386 | `overlay` |
| 6 | 35.7 | 0.0% | 0.065 | 15.1% | 115.4 | 0.010 | 90.9 | 15.8 | 0.386 | `overlay` |
| 8 | 34.3 | 0.0% | 0.047 | 3.7% | 140.9 | 0.008 | 89.7 | 16.6 | 0.387 | `overlay` |
| 12 | 38.6 | 0.0% | 0.038 | 7.4% | 191.1 | 0.007 | 101.6 | 16.7 | 0.383 | `overlay` |
| 16 | 38.3 | 0.0% | 0.033 | 9.4% | 239.3 | 0.006 | 102.2 | 16.9 | 0.387 | `overlay` |
| 24 | 47.6 | 0.0% | 0.024 | 4.6% | 343.0 | 0.004 | 106.4 | 19.4 | 0.386 | `overlay` |
| 32 | 55.9 | 0.0% | 0.019 | 2.6% | 437.4 | 0.003 | 114.4 | 19.0 | 0.387 | `overlay` |
| 64 | 98.2 | 0.0% | 0.014 | 2.8% | 854.8 | 0.002 | 150.6 | 23.1 | 0.387 | `overlay` |

</details>

#### What the numbers show

- **Centre assignment is fastest from about one to a few pixels per cell, by 2.2–3.9×, and
  coarse.** At about one pixel per cell it ran in 26–45% of overlay's time. At that size it left
  9–26% of the cells empty (H3 9%, S2 14%, HEALPix 19%, A5 26%) and put cell areas 16–35% off.
  At every ratio from 1 to 16 pixels per cell the mean area error was 4–38%, while the
  class-fraction error fell from 0.27–0.33 to 0.03–0.04 and the continuous mean error from 13–16
  to about 2 on a 0–150 field.
- **Where overlay becomes faster depends on the grid.** From about 12 pixels per cell on H3 and
  A5, overlay was faster than centre assignment, 1.1–5.6× at 12–64 pixels per cell. On S2 and
  HEALPix, centre assignment stayed faster through the largest ratio tested: overlay cost 1.1–1.3×
  as much at 64 pixels per cell on S2 and 1.5–1.6× on HEALPix. Centre assignment indexes every
  pixel, so how its cost grows depends on how expensive a grid is to index. A5's timings were also
  the noisiest, so its crossover, between 6 and 12, is approximate.
- **Overlay's cost is steady.** It took 0.9–3.8 µs per cell on the four grids (S2 cheapest, A5
  dearest), rising at most 1.8× across a 256× range of ratios. It used about 1.5–3× the memory of
  centre assignment (26–52 MiB against 12–35 on these scans).
- **Below one pixel per cell, interpolation fills every cell for an eighth to a third of overlay's
  time.** `bilinear` and `nearest` left about 1% of the cells empty (those whose centre lies off
  the raster), against 48–57% for centre assignment at 0.5 pixels per cell and 74–79% at 0.25. They answer a
  different question from overlay: overlay gives each tiny cell the pixels under it, a step
  function, while `bilinear` gives a smooth surface, so their difference is not an error of
  either.
- **`nearest` is fast but blunt for classes.** It looks up one class at the cell centre, so its
  class fractions were 0.17–0.20 from overlay's at half a pixel per cell and 0.35–0.41 from about
  one pixel up, on all four grids, however many pixels the cell held.
- **Sub-pixel sampling rarely comes out ahead.** From about 4 pixels per cell up, `rgss` cost
  1.2–5.7× centre assignment and `16point` 2.7–18.5×, and overlay was exact for 1–4 µs per cell.
  Its one niche is a quarter to half a pixel per cell, where `16point` ran in 40–85% of overlay's
  time and stayed closer to it than interpolation did (class-fraction error 0.04 against 0.17–0.20
  for `nearest` at half a pixel per cell), while missing 0–0.6% of the cells.

These results set [`transfer = "auto"`](#transfer--auto): `bilinear` below 1:1 for continuous
scans, weighted `overlay` from 1:1 up and for categorical scans at every ratio. That costs up to
2.2–3.9× in speed against centre assignment at about one pixel per cell, and 1.7–2.7× at 16 pixels
per cell on S2 and HEALPix, in return for no empty cells and exact areas and class shares.
On H3 and A5, `auto` is also the faster choice from about 12 pixels per cell. Where speed matters
more than edges, ask for `assign_centers`.

Caveats: one machine, synthetic fields, single-band scans of local files, and default worker pools
(overlay clips cells on the ambient Rayon pool). Value errors depend on how fast the field varies
within a cell: the squares here are one cell wide, the hardest case for centre assignment, while
a smooth field would make it look better; area and empty-cell errors do not depend on the field.
Pixel-dense real rasters and remote COGs may shift the crossovers.

#### Sub-cell sampling and the two references

Interpolation with a sub-cell [`sampling`](super-sampling.md) preset (`bicubic+16point`,
`nearest+rgss`, ...) answers a different question from overlay. Overlay treats each pixel as a
uniform area, so it is exact if pixel values are area averages. Interpolation treats pixels as
point samples of a continuous field and reconstructs that field between them. The benchmark
scores both against two references:

- **vs. overlay**: overlay on the scanned raster, the exact answer for pixels as area averages.
- **vs. field**: overlay on a raster of the same synthetic field 8× finer each way
  (`--field-reference 8`), which approaches the field's true mean, or class shares, over each
  cell. This is the right answer when pixels are point samples of the field.

Each sweep ran on H3 resolution 11 and S2 level 13 (about 20,000 cells, three runs) with two
fields at four cell edges per feature (`--feature-cells 4`): `smooth`, a sinusoid spanning
340–660, and `checker`, the gradient-plus-checkerboard field spanning 0–150 with sharp edges. The
tables give the range over the two grids. Mean absolute error of cell means, continuous scans,
against the field:

| Pixels per cell | Field | `overlay` | `bilinear` | `bicubic` | `bilinear+16point` | `bicubic+16point` | `16point` (sub-pixel) |
| ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.25 | smooth | 24.33–25.99 | 17.90–18.75 | 6.93–7.54 | 19.94–20.23 | 7.65–8.04 | 26.61–30.01 |
| 0.5 | smooth | 12.11–12.92 | 9.02–9.67 | 2.46–2.71 | 11.39–11.66 | 2.23–2.49 | 13.20–15.24 |
| 1 | smooth | 4.22–4.50 | 3.27–4.19 | 2.87–2.98 | 5.84–5.99 | 0.40–0.59 | 4.88–6.62 |
| 2 | smooth | 2.24–2.47 | 0.96–1.52 | 3.21–3.30 | 2.84–2.97 | 0.18–0.21 | 2.47–3.04 |
| 4 | smooth | 0.94–1.12 | 1.83–1.96 | 3.36–3.43 | 1.30–1.41 | 0.20–0.27 | 1.23–1.96 |
| 8 | smooth | 0.55 | 2.64–2.65 | 3.42–3.47 | 0.52–0.63 | 0.21–0.28 | 0.67–0.86 |
| 0.25 | checker | 6.33–9.68 | 7.62–9.90 | 9.19–12.07 | 8.09–10.18 | 9.00–11.66 | 7.02–10.12 |
| 0.5 | checker | 7.05–7.26 | 7.13–7.36 | 9.12–9.32 | 7.38–7.61 | 8.52–8.79 | 7.21–7.52 |
| 1 | checker | 4.55–5.70 | 4.57–5.75 | 5.93–7.48 | 4.83–5.75 | 5.60–6.62 | 4.92–5.75 |
| 2 | checker | 3.63–3.66 | 3.93–4.09 | 5.41–5.57 | 3.73–3.77 | 4.16–4.18 | 3.69–3.84 |
| 4 | checker | 0.65–2.00 | 2.89–3.16 | 4.23–4.37 | 0.84–2.05 | 1.03–2.25 | 0.77–2.14 |
| 8 | checker | 1.82–1.85 | 3.74–4.06 | 4.73–4.99 | 1.85–1.87 | 1.99–2.04 | 1.84–1.87 |

The same modes against overlay:

| Pixels per cell | Field | `bilinear` | `bicubic` | `bilinear+16point` | `bicubic+16point` | `16point` (sub-pixel) |
| ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| 0.25 | smooth | 16.73–18.89 | 17.31–18.89 | 18.41–20.12 | 17.48–19.17 | 6.50–8.36 |
| 0.5 | smooth | 7.78–8.80 | 10.68–11.37 | 9.41–10.17 | 9.84–10.68 | 3.63–5.29 |
| 1 | smooth | 1.81–1.84 | 6.15–6.17 | 3.23–3.31 | 3.56–3.85 | 2.23–4.31 |
| 2 | smooth | 2.39–2.69 | 5.04 | 1.76–2.22 | 2.10–2.34 | 1.05–1.69 |
| 4 | smooth | 2.62–2.79 | 4.23–4.24 | 0.56–0.76 | 1.05–1.16 | 0.76–1.50 |
| 8 | smooth | 3.02–3.05 | 3.80–3.87 | 0.27–0.37 | 0.66–0.71 | 0.39–0.67 |
| 0.25 | checker | 5.04–5.73 | 5.45–6.00 | 5.76–6.27 | 5.58–6.13 | 1.85–2.35 |
| 0.5 | checker | 2.21–2.49 | 2.99–3.26 | 3.32–3.45 | 3.16–3.41 | 1.01–1.36 |
| 1 | checker | 0.48–0.59 | 2.46–2.58 | 1.69–1.72 | 1.48–1.68 | 0.65–1.18 |
| 2 | checker | 1.28–1.63 | 3.07–3.49 | 0.80–0.95 | 0.75–0.87 | 0.37–0.52 |
| 4 | checker | 2.70–2.81 | 4.02–4.16 | 0.36–0.39 | 0.59–0.62 | 0.24–0.44 |
| 8 | checker | 3.44–3.67 | 4.46–4.64 | 0.17–0.23 | 0.40–0.44 | 0.12–0.25 |

Categorical scans with cells finer than the pixels, where a sampling preset decides between
sub-pixel samples (`rgss`, `16point` under centre assignment) and sub-cell samples (`nearest+…`).
Errors are class-fraction distances. At a quarter of a pixel per cell every mode, overlay
included, is about as far from the field, because the raster cannot resolve it:

| Pixels per cell | Field | Reference | `rgss` (sub-pixel) | `nearest+rgss` | `16point` (sub-pixel) | `nearest+16point` | `overlay` |
| ---: | :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| 0.25 | smooth | cells missing | 13.1–23.0% | 0.8–1.7% | 0.4–0.9% | 0.8–1.7% | 0.0% |
| 0.25 | smooth | vs. overlay | 0.152–0.160 | 0.055–0.065 | 0.058–0.073 | 0.020–0.029 | — |
| 0.25 | smooth | vs. field | 0.486–0.493 | 0.440–0.451 | 0.447–0.460 | 0.439–0.449 | 0.439–0.448 |
| 0.25 | checker | cells missing | 13.1–23.0% | 0.8–1.7% | 0.4–0.9% | 0.8–1.7% | 0.0% |
| 0.25 | checker | vs. overlay | 0.104–0.108 | 0.036–0.042 | 0.037–0.048 | 0.013–0.019 | — |
| 0.25 | checker | vs. field | 0.175–0.243 | 0.142–0.209 | 0.146–0.210 | 0.141–0.206 | 0.134–0.202 |
| 0.5 | smooth | cells missing | 1.1–1.6% | 0.8–1.7% | 0.3–0.6% | 0.8–1.7% | 0.0% |
| 0.5 | smooth | vs. overlay | 0.117–0.140 | 0.065–0.077 | 0.037–0.052 | 0.024–0.035 | — |
| 0.5 | smooth | vs. field | 0.334–0.353 | 0.318–0.321 | 0.316–0.326 | 0.313–0.316 | 0.310–0.315 |
| 0.5 | checker | cells missing | 1.1–1.6% | 0.8–1.7% | 0.3–0.6% | 0.8–1.7% | 0.0% |
| 0.5 | checker | vs. overlay | 0.066–0.084 | 0.033–0.043 | 0.020–0.028 | 0.013–0.016 | — |
| 0.5 | checker | vs. field | 0.157–0.169 | 0.148–0.151 | 0.144–0.151 | 0.144–0.149 | 0.141–0.145 |

Categorical scans from one pixel per cell up:

| Pixels per cell | Field | `16point` vs. overlay | `nearest+16point` vs. overlay | `16point` vs. field | `nearest+16point` vs. field | `overlay` vs. field |
| ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| 1 | smooth | 0.027–0.049 | 0.027–0.048 | 0.195–0.208 | 0.197–0.208 | 0.191–0.196 |
| 2 | smooth | 0.016–0.023 | 0.029–0.044 | 0.127–0.134 | 0.130–0.137 | 0.125–0.131 |
| 4 | smooth | 0.010–0.018 | 0.031–0.042 | 0.067–0.075 | 0.074–0.082 | 0.065–0.071 |
| 8 | smooth | 0.005–0.009 | 0.029–0.038 | 0.057–0.062 | 0.065–0.076 | 0.056–0.060 |
| 1 | checker | 0.013–0.024 | 0.013–0.023 | 0.103–0.120 | 0.101–0.121 | 0.096–0.119 |
| 2 | checker | 0.008–0.010 | 0.013–0.017 | 0.074–0.077 | 0.076–0.079 | 0.073 |
| 4 | checker | 0.005–0.009 | 0.013–0.019 | 0.019–0.045 | 0.023–0.048 | 0.016–0.043 |
| 8 | checker | 0.003–0.005 | 0.013–0.021 | 0.037–0.038 | 0.039–0.040 | 0.037 |

What this shows:

- **Smooth fields sampled at points favour bicubic.** Against the field, `bicubic+16point` was
  the closest mode from one pixel per cell up, 2–13× closer than overlay, and below one pixel per
  cell `bicubic`, with or without sub-cell samples, was 2.4–3.9× closer than `bilinear`. Sub-cell
  samples made `bilinear` worse below about 4 pixels per cell, since it already flattens the field
  and averaging flattens it more, and better from there up.
- **Sharp edges favour overlay.** On the checker field overlay was as close to the field as any
  mode, or closer, at every ratio. `bicubic+16point` was 9–60% further away and `bicubic` alone
  further still (ringing at the edges). Sub-pixel `16point` and `bilinear+16point` came within a
  few percent of overlay from 2 pixels per cell up.
- **Against overlay, neither kind of sample wins everywhere for continuous scans.** Plain
  `bilinear` was closest at about one pixel per cell. Of the sampled modes, sub-pixel `16point`
  was closest below one pixel per cell and at 2 pixels per cell, though sparser sub-pixel presets
  leave cells empty below one pixel per cell (`rgss`: 13–23% at a quarter of a pixel per cell).
  From 4 pixels per cell, `bilinear+16point` was as close or closer.
- **For classes below one pixel per cell, sub-cell samples are the better use of a preset.**
  `nearest+16point` came 1.5–3× closer to overlay than sub-pixel `16point`, and `nearest+rgss`
  about 2–3× closer than `rgss`. Both matched overlay against the field and left only edge cells
  (whose centre lies off the raster) without a value. From one pixel per cell up, sub-pixel
  `16point` was as close or closer to both references.
- **Cost.** `bicubic+16point` took 25–44 ms and `nearest+16point` 14–29 ms on these 20,000-cell
  scans, against 17–52 ms for overlay. Sub-pixel `16point` grew with the ratio, to 78–215 ms at
  8 pixels per cell.

`transfer = "auto"` follows the overlay reference, which needs no assumption about the data, and
adds no options of its own. With a sampling preset it uses sub-cell samples where they win
(cells finer than the pixels, `nearest` for classes) and sub-pixel samples elsewhere. When pixels
are point samples of a smooth field, such as model output or an interpolated surface, ask for
`interpolate` with `interpolation = "bicubic"` and `sampling = "16point"` (or `"rgss"`) instead;
when the field has sharp edges or the pixels are area averages, keep `overlay`.

```sh
python3 scripts/benchmark_transfer_modes.py --sweep ratio --grid h3 --resolution 11 --field smooth \
    --feature-cells 4 --field-reference 8 --cells 20000 --runs 3 --ratios 0.25,0.5,1,2,4,8 \
    --modes overlay,bilinear,bicubic,bilinear+16point,bicubic+16point,16point --markdown
```

### Parallel overlap calculation

Eligible overlay blocks project and clip cells in parallel. The pool is set by
`MultiResolutionConfig::aggregation_workers` in Rust, `--aggregation-workers` in the CLI and
`workers` in DuckDB: unset uses the caller's ambient Rayon pool, `n` a dedicated `n`-thread pool
and `1` runs serially. Ordinary overlay without `mass_preserve` clips and accumulates each cell on
a worker; mass-preserving overlay has workers prepare intersections that the caller applies in the
original cell and pixel order, including normalisation and per-band coverage filtering. Results
are identical either way. Ground-area overlay uses the same pool: it processes row windows in
order and clips the cells of each window with at least 16 candidates in parallel.

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
