# Transfer Modes: How Pixels Become Cells

[← Back to README](../README.md)

`transfer` chooses how raster pixels are mapped onto grid cells. It works the same way for every
grid (H3, A5, S2, HEALPix) and in every engine except PostgreSQL.

| `transfer` | What each cell gets | Use it when | Options |
| :--- | :--- | :--- | :--- |
| `assign_centers` (default) | Every pixel whose centre falls inside the cell | Cells are about the size of a pixel or larger | — |
| `interpolate` | One value, interpolated at the cell's centre | Cells are **smaller** than the pixels | `interpolation` |
| `overlay` | Every pixel the cell overlaps, weighted by the overlapped part of each pixel | Cell edges should cut pixels exactly, or totals must be preserved | `overlay_weighted`, `mass_preserve` |

## `assign_centers` (default)

Each pixel (or each sub-pixel sample, see [Super-Sampling](super-sampling.md)) is indexed to the
cell that holds its centre and aggregated there. This is the single-pass streaming kernel the rest
of the documentation describes, with the lowest memory use and the highest throughput.

When the cells are much finer than the pixels, most cells contain no pixel centre and are missing
from the output. Use `interpolate` for that case.

`assign`, `centers` and `centres` are accepted as names too.

## `interpolate`: the value at each cell's centre

Every cell whose centre lies on the raster gets the raster's value at that centre, interpolated
from the surrounding pixels. Each cell is filled exactly once, so `count` is 1 and `mean`, `min`,
`max` and `sum` all hold the interpolated value.

`interpolation` picks the interpolation kernel:

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
- Zero-alpha pixels count as NoData, as in the default mode.

## `overlay`: area-weighted aggregation

For each cell, `overlay` aggregates every pixel the cell overlaps. A pixel's weight is the fraction
of it inside the cell, computed exactly by clipping the cell's polygon in pixel space. A pixel that
straddles an edge contributes to each cell in proportion to its share, so `mean` becomes an
area-weighted mean and `count` the overlapped area in pixels.

- `overlay_weighted` (default `true`). With `false`, every pixel the cell touches counts once at
  full weight: `count` is the number of pixels touched, and pixels on an edge count in every
  cell they touch.
- `mass_preserve` (default `false`). Divides each pixel's weights by their total across cells,
  so that every pixel contributes exactly once in total and the cells' `sum` column adds up to
  the raster's total. Use it for quantities that must be conserved (population, biomass,
  precipitation volume). It needs area weights, so it can't be combined with
  `overlay_weighted = false`.

Notes:

- A `bbox` selects pixels by their centres, as in the default mode, so cells reaching outside the
  box are kept with the pixels inside it.
- On a full-width WGS84 geographic or Web Mercator raster, a cell crossing the longitude seam
  receives pixel contributions from both edges, including when the raster uses 0–360° longitudes.
- Cells whose diameter bound is above 500 km (the coarsest two or three resolutions of each grid)
  take whole pixels by centre: their edges bend too far from straight lines for exact clipping,
  and a single pixel is negligible next to such a cell.
- Zero-alpha pixels count as NoData.

## Categorical rasters

Class codes can't be interpolated or averaged, so categorical scans accept:

- `overlay` (with any options): class counts become overlapped areas in pixels.
- `interpolate` with `interpolation = 'nearest'` only. Other methods are rejected.

An area-weighted [histogram](engines/core.md) (`histogram_weight = 'area'`) runs `overlay`
automatically.

## What can't be combined

`interpolate` and `overlay` read the raster in bands of rows and hold only one band plus the cells still
open across band edges, so memory stays flat as the raster gets taller. They are a different
kernel from the default, and can't be combined with:

| Option | Why |
| :--- | :--- |
| `sampling` other than `center` | The transfer mode replaces sub-pixel sampling |
| Partitioned scans (`plan_partitions`, Spark) | Not supported by the transfer kernel |
| `compact` | Not supported by the transfer kernel |
| `spectral_formula` | Not supported by the transfer kernel |
| `interpolation` without `transfer = 'interpolate'` | The option belongs to the other mode |
| `overlay_weighted` / `mass_preserve` without `transfer = 'overlay'` | The options belong to the other mode |

Each of these is rejected with an error, not ignored. A resolution so fine that one block of the
raster would hold too many cells is also rejected, with a message asking for a coarser resolution.

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

**CLI** (every subcommand that scans a raster: `ipc`, `jsonl`, `parquet`, `pmtiles`)

```bash
dggs-stream-cli jsonl dem.tif -r 12 --transfer interpolate --interpolation bicubic
dggs-stream-cli parquet population.tif -o population.parquet -r 7 --transfer overlay --mass-preserve
dggs-stream-cli ipc landcover.tif -r 9 -c --transfer overlay --overlay-weighted false -o landcover.rh3
```

**DuckDB** (named parameters of the raster scan and export functions)

```sql
SELECT * FROM h3_raster_continuous_aggregate('dem.tif', resolution := 12,
    transfer := 'interpolate', interpolation := 'bicubic');

SELECT sum(sum) FROM h3_raster_continuous_aggregate('population.tif', resolution := 7,
    transfer := 'overlay', mass_preserve := true);
```

**Python / Polars** (keyword arguments of `scan_raster` and the exporters)

```python
import dggs_stream as ds

lf = ds.scan_raster("dem.tif", resolutions=12, transfer="interpolate", interpolation="lanczos")
lf = ds.scan_raster("population.tif", resolutions=7, transfer="overlay", mass_preserve=True)
```

**DataFusion / SedonaDB**: option strings on the table functions, or `with_transfer` on the table
providers.

```sql
SELECT * FROM h3_raster_continuous_aggregate('dem.tif', 12, 'transfer=interpolate', 'interpolation=bicubic');
SELECT * FROM h3_raster_continuous_aggregate('population.tif', 7, 'transfer=overlay', 'mass_preserve=true');
```

```rust
use dggs_stream_core::transfer::{Interpolation, TransferMode};

let provider = provider.with_transfer(TransferMode::Interpolate(Interpolation::Bicubic));
let provider = provider.with_transfer(TransferMode::from_options(
    Some("overlay"), None, None, Some(true),
)?);
```

**Core**: set `MultiResolutionConfig::transfer`, built with `TransferMode::from_options` or
directly (`TransferMode::Interpolate(Interpolation::Bilinear)`, `TransferMode::OVERLAY`). See
[`transfer/mod.rs`](../crates/dggs-stream-core/src/transfer/mod.rs).

**PostgreSQL**: not available yet.
