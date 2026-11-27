# Sampling Guide: Sub-Pixel (`assign_centers`) & Sub-Cell (`interpolate`)

[← Back to README](../README.md)

`sampling` defines a `[0, 1]^2` quadrature pattern (`SamplePoint` offsets and weights summing to `1.0`). Depending on [`transfer`](transfer-modes.md), those offsets are applied either **inside each pixel** or **inside each DGGS cell**:

| Mode | Where `sampling` runs | Supported? | Notes |
| :--- | :--- | :---: | :--- |
| **`assign_centers`** *(default)* | **Sub-pixel** (inside each pixel's `[0, 1]^2` square) | Yes | Splits each pixel across the cells holding its sub-pixel sample offsets. |
| **`interpolate`** | **Sub-cell** (inside each cell's projected polygon) | Yes | Maps `[0, 1]^2` offsets via an area-preserving star-polygon map (`CellPolygonSampler`), then interpolates each point from surrounding pixels. |
| **`overlay`** | Analytical polygon–pixel intersection | `center` only | Non-default `sampling` is rejected: `overlay` already computes the exact pixel overlap integral. |
| **`auto`** | Resolves per scale (`< 1:1` vs `>= 1:1`) | Yes | Keeps non-default `sampling` when resolving to `interpolate`, and falls back to `assign_centers` (not `overlay`) when cells are larger than pixels. |

![Sub-Pixel Super-Sampling Patterns](../assets/sampling_patterns.svg)

## Where Each Mode Applies

### 1. Sub-Pixel Sampling (`transfer = 'assign_centers'`)
When a raster pixel straddles two or more cells, single-point `'center'` sampling sends 100% of the pixel to whichever cell contains `(0.5, 0.5)`. With sub-pixel sampling, multiple offsets `(dx_i, dy_i)` inside each pixel box are projected and indexed so edge pixels split their weight across cells.

> **When to prefer `overlay` instead:** For ordinary continuous or categorical area-weighted averages when cells are comparable to or larger than pixels (`>= 1:1`), [`transfer = 'overlay'`](transfer-modes.md#overlay-area-weighted-aggregation) computes exact analytical pixel–cell intersections and is usually preferred over sub-pixel supersampling. Sub-pixel sampling remains essential in `assign_centers` when using features that `overlay` does not support (`spectral_formula`, H3 `compact`, partitioned scans, or ultra-coarse `> 500 km` cells) or when running `assign_centers` for maximum speed with light edge anti-aliasing.

### 2. Sub-Cell Quadrature (`transfer = 'interpolate'`)
With `transfer = 'interpolate'` and `sampling != 'center'`, each cell whose center falls on the raster projects its boundary corners (`4..10` vertices across H3, A5, S2, and HEALPix) into pixel space once and maps the `[0, 1]^2` pattern algebraically via `CellPolygonSampler` (constant-Jacobian star-triangle fan mapping):

- **All sample points stay strictly inside the cell polygon**, avoiding extra per-sample CRS/PROJ transforms.
- **Smooth continuous crossover (`~1×–4×` px/cell):** Pairing `interpolation = 'bicubic'` with `'rgss'` (4-pt) or `'16point'` (16-pt) integrates the smooth interpolated surface across the cell instead of piecewise-constant pixel squares, beating both 1-point `'center'` interpolation and `overlay` on smooth curved fields.
- **Intra-cell spread (`stddev`, `min`, `max`, `quantiles`):** Single-point `'center'` `interpolate` produces only 1 observation per cell (`stddev = 0`, `min == max == mean`). Sub-cell quadrature recovers intra-cell variation across `rgss` / `8rooks` / `16point` while keeping cell `count <= 1.0`.
- **Categorical sub-cell fractions:** With categorical data and `interpolation = 'nearest'`, dense sub-cell sampling (e.g., `'16point'`) estimates each class's fraction within the cell (converging toward `overlay` as sample count increases).
- **Cells containing a pole** on geographic (EPSG:4326) and Web Mercator rasters fall back to a single sample at the cell centre, so they report one observation (`stddev = 0`) rather than a sub-cell spread.
- **Mosaics and 360° rasters:** a cell takes all of its samples from the raster file that owns its centre (under the mosaic's overlap rule), as single-point interpolation does. When its stencil would reach data that file's block cannot read (across the west/east seam of a raster spanning 360° of longitude, or into another file of the mosaic), the cell falls back to its centre sample, as pole cells do. Samples beyond every file are off the data and lower the count, as at the edge of a single raster.
- **Stencils wider than a block's window:** the scan is refused when a cell's stencil at the coarsest requested resolution would reach more rows beyond its block than the window budget holds (very wide rasters with cells many pixels across), rather than returning cells with missing samples. Use a finer resolution, `overlay`, or `sampling = 'center'`.
- **Very fine cells (`<= 0.5×` px/cell):** If you only need cell `mean` (not `stddev`/`min`/`max`/`quantiles`), 1-point `'center'` with `'bicubic'` gives essentially the same `mean` at 4×–16× higher throughput.

## Sampling Preset Reference Table

| Preset Name | Points | Weighting | Geometric Rationale | Why & When to Use |
| :--- | :---: | :--- | :--- | :--- |
| **`'center'`** *(default)* | 1 | 1.0 (Center) | Centroid evaluation | **Maximum speed**: Best for `assign_centers` when pixels are much smaller than cells, and for `interpolate` well below `1:1` (`<= 0.5×`) when only `mean` is needed. |
| **`'rgss'`** / `'rotated4'` ⭐ | 4 | 0.25 each | 26.6° rotated grid (`arctan 0.5`) | **Best speed/accuracy balance**: Eliminates axis-aligned aliasing with 4 points; captures most intra-cell `stddev` and improves `bicubic` `mean` around `1×–2×` at 1/4 the cost of `16point`. |
| **`'8rooks'`** / `'stratified8'` | 8 | 1/8 each | Latin Hypercube non-attacking rooks | **Balanced stratification**: Cleaner 1D axis coverage and intra-cell `quantiles` / `min` / `max` than `rgss` at half the cost of `16point`. |
| **`'16point'`** / `'4x4'` | 16 | 1/16 each | Regular 4 × 4 stratified grid | **Highest precision**: Best continuous `mean` (with `bicubic`) in the `1×–4×` crossover zone, tightest intra-cell `range`/`quantiles`, and `nearest` categorical fractions. |
| **`'5point'`** / `'quincunx'` | 5 | 0.20 each | Center + 4 diagonal corners | **Legacy 5-point stencil**: Center plus inset corners; generally outperformed by `rgss` or `8rooks`. |
| **`'gaussian'`** / `'psf'` | 5 | Center 0.50, Edges 0.125 | Gaussian Point Spread Function | **Optical PSF emulation**: Center-biased sensor modeling in `assign_centers` (not intended for uniform cell-area `variance` or `quantiles`). |
| **`'hex'`** / `'7point'` | 7 | 1/7 each | Inscribed regular hexagon | **Radial 7-point stencil**: Matches H3 symmetry in pixel space (`assign_centers`); in `interpolate`, `CellPolygonSampler` already adapts any `[0, 1]^2` pattern to the cell polygon. |
| **`'9point'`** / `'3x3'` | 9 | 1/9 each | Regular 3 × 3 grid | **Dense axis-aligned grid**: Generally superseded by `8rooks` or `16point`. |

## What Sampling Does and Does Not Compute

- **In `assign_centers` (sub-pixel):** Counts and means are **sample-weighted** across each pixel's sub-pixel points. Supersampling approximates how a pixel splits across cell boundaries; it is not an exact pixel–cell area intersection (`overlay`), and does not weight by ground area. `stddev` treats each pixel as **one observation** per cell weighted by the sum of its sample weights in that cell (`w_i`); the reliability-weighted sample variance `M2 / (W - sum(w_i^2) / W)` is invariant to scaling all weights, so a pixel wholly inside a cell still has weight `1.0`.
- **In `interpolate` (sub-cell):** Normalized point weights inside a cell sum to `1.0`, so a cell whose entire stencil lies inside valid raster data has `count = 1.0` (edge or partial-NoData cells receive the sum of valid tap weights in `(0, 1]`). `stddev`, `min`, `max`, and `quantiles` summarize the interpolated values across the cell's interior sample points, using the same reliability-weighted sample variance across taps.

## Performance & Precision Trade-Off Guide

| Sampling Mode | Samples / Pixel | Relative Runtime | Boundary Precision | Recommended Use Case |
| :--- | :---: | :---: | :--- | :--- |
| **`center`** | 1 | **$1.0\times$** (Fastest) | Baseline | Fast exploratory scans, massive high-resolution rasters (10m pixels into Res 6–8 cells) |
| **`rgss`** *(Recommended)* | 4 | **$\sim 0.75\times$** | High Anti-Aliasing | Default for production analytical queries; eliminates axis-aligned blind spots |
| **`hex`** | 7 | **$\sim 0.60\times$** | Hexagonally symmetric sample layout | When a symmetric sub-pixel pattern is preferred (still sample-weighted, not area-weighted) |
| **`gaussian`** | 5 | **$\sim 0.68\times$** | Optical PSF Emulation | Remote sensing satellite imagery where pixel centers dominate sensor response |
| **`8rooks`** | 8 | **$\sim 0.55\times$** | Full Stratified Anti-Aliasing | Highly complex boundary contours with diagonal edges |
| **`16point`** | 16 | **$\sim 0.35\times$** | Sub-Grid Reconstruction | Coarse rasters (e.g. 1km climate grids) aggregated into fine H3 cells (Res 9–11) |

## The Recommended Compromise: Why RGSS is the Optimal Default

For production workflows, **`'rgss'` (Rotated Grid Super-Sampling, 4-point)** provides the ideal compromise between boundary accuracy and throughput.

### 1. Geometric Efficiency
Standard axis-aligned grids (such as a regular $2 \times 2$ grid or 5-point quincunx) suffer from collinear blind spots: when a hexagonal boundary cuts parallel to an axis, multiple sample points fall on the same side of the cut line, degrading anti-aliasing efficiency. 

RGSS rotates the sub-pixel grid by $\arctan(0.5) \approx 26.57^\circ$:
$$\left( \frac{3}{8}, \frac{1}{8} \right), \quad \left( \frac{7}{8}, \frac{3}{8} \right), \quad \left( \frac{1}{8}, \frac{5}{8} \right), \quad \left( \frac{5}{8}, \frac{7}{8} \right)$$

Because **no two points share the same horizontal ($X$) or vertical ($Y$) coordinate**, RGSS provides 4 distinct 1D projection slices across any arbitrary hexagon edge with only 4 evaluation points.

### 2. Fused Sample Evaluation

For ordinary non-overlap scans, each valid pixel's sample locations are transformed
once, then indexed once per requested H3 resolution. Small reusable buffers retain
these assignments. Continuous and categorical engines combine the weights of a pixel's samples that
share a cell into one update per cell (in the order of each cell's first sample),
so a pixel wholly inside one cell is a single update, and the stored assignments are
replayed without repeating projection or indexing. Bounding-box
clipping preserves only the included sample weights, including partial pixels.

Quantile sketches receive the same combined weight. The
mosaic overlap path keeps its ownership-aware sample walker. Center sampling
continues to use sequential span accumulation. No geometric containment shortcut
skips sample indexing, and performance depends on the CRS and sampling pattern.

### 3. Selection Heuristic

```
Do you need exact pixel-cell area overlap or conserved totals (>= 1:1 px/cell, or categorical)?
 ├── YES ──> Use transfer='overlay' (leave sampling='center')
 └── NO (smooth continuous field <= 4× px/cell, OR assign_centers-only features like spectral_formula/compact)
      ├── Using transfer='interpolate'?
      │    ├── Need only cell mean well below 1:1 (<= 0.5×)? ──> sampling='center' + interpolation='bicubic'
      │    ├── Need intra-cell spread (stddev/quantiles) or smooth 1×–2× crossover? ──> sampling='rgss' + interpolation='bicubic'
      │    └── Need highest crossover accuracy / tightest cell range or quantiles? ──> sampling='16point' (or '8rooks') + interpolation='bicubic'
      └── Using transfer='assign_centers' (e.g. spectral_formula, compact, partitions, or coarse > 500 km)?
           ├── Pixels much smaller than cells (many pixels/cell)? ──> sampling='center'
           └── Mixed / edge-sensitive or cells <= pixels? ──> sampling='rgss' (4-pt) or '16point' (coarse -> fine)
```

## Performance Tuning Tips
1. **Match `chunk_size` to Tile Dimensions**: For tiled GeoTIFFs (e.g., $256 \times 256$ or $512 \times 512$ tiles), set `chunk_size := 512` to align DuckDB decompressor buffers with native TIFF block boundaries.
2. **Region of Interest (ROI) Pruning**: Always specify `min_lon`, `min_lat`, `max_lon`, `max_lat` when analyzing spatial subsets. Non-overlapping GeoTIFF blocks are discarded instantly before reading from disk.
3. **Multi-Resolution Single Passes**: When creating multi-zoom web layers, use `resolutions := [6, 7, 8]` or `h3_raster_to_pmtiles(...)` rather than separate SQL queries to read the underlying GeoTIFF only once.

