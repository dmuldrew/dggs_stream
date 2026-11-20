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
- Zero-alpha pixels and pixels marked invalid by an internal TIFF mask count as NoData, as in the default mode.

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
- Zero-alpha pixels and pixels marked invalid by an internal TIFF mask count as NoData.

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

### Valid coverage

Set `MultiResolutionConfig::valid_coverage_threshold` or pass
`--valid-coverage-threshold 0.75 --transfer overlay` to keep cells with valid
band data across at least 75% of their selected raster overlap. Coverage uses
pixel-space overlap fractions even with `--overlay-weighted false`. Pixels
outside the bbox or owned by a different mosaic tile are excluded from both
areas. NoData and masked pixels contribute only to the denominator. Category
remapping and histogram bin selection happen after band validity is determined.

DuckDB accepts `transfer := 'overlay', valid_coverage_threshold := 0.75`. In Python use
`scan_raster(path, transfer="overlay", valid_coverage_threshold=0.75)`. Both adapters also
accept the option on raster-to-Parquet and raster-to-PMTiles exports.

DataFusion and SedonaDB SQL accept `'transfer=overlay', 'valid_coverage_threshold=0.75'`.

The filter applies when a cell is complete. A positive threshold drops
all-NoData cells even under `--nodata-policy emit`. Zero disables the filter.
Mass-preserving scans accept the threshold and retain every contribution.

In a multi-band overlay, each pixel–cell intersection supplies the same geometric weight to every selected band with a valid observation. A coverage threshold applies to each band independently. A cell survives when any band passes. A band that falls below the threshold has zero count and uses the configured NoData output for its mean (NULL by default). Mass-preserving overlay conserves the sum of valid pixel values independently for each band.

### Reproducing the overlay benchmark

Build the CLI with `cargo build --release -p dggs-stream-cli`. With GDAL's
`gdal_create` on PATH, run the following from the repository root on macOS or
Linux. The baseline argument takes a separately saved release binary.

```sh
python3 scripts/benchmark_overlay.py --baseline-cli /path/to/baseline-cli --runs 9
```

On arm64 macOS, the following medians compare revision `63f69cf` with per-cell
overlap buffering. Each run scanned a local, tiled, Deflate-compressed Float32
raster covering the same one-degree square and wrote IPC to the null device.
The runner warms both binaries, alternates their order, and measures wall time
and peak resident memory in a fresh process for each scan. The coverage filter
is disabled in both versions.

| Raster size | H3 resolution | Mass preservation | Time, baseline → updated | Peak RSS, baseline → updated |
| --- | --- | --- | --- | --- |
| 1024 × 1024 | 6 | Off | 75.3 → 59.6 ms | 44.1 → 25.0 MiB |
| 1024 × 1024 | 6 | On | 81.9 → 65.5 ms | 51.9 → 51.9 MiB |
| 128 × 128 | 9 | Off | 341.9 → 332.8 ms | 39.9 → 38.8 MiB |
| 128 × 128 | 9 | On | 342.1 → 340.5 ms | 40.0 → 39.8 MiB |
