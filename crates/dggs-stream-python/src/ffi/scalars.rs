//! Scalar H3 helpers exposed to Python.

use std::ffi::{c_char, c_int, CString};
use std::ptr;

use dggs_stream_core::error::catch_unwind_or;
use dggs_stream_core::scalar_ops::{
    h3_cell_to_parent_str, h3_cell_to_parent_u64, h3_get_resolution, h3_is_valid_str,
    h3_is_valid_u64, h3_polygon_to_cells_wkb, h3_to_lat, h3_to_lng, h3_to_string, h3_to_wkb_u64,
    parse_containment_mode, string_to_h3, ContainmentMode, PolygonToCellsError,
};

use dggs_stream_core::grid::GridKind;
use dggs_stream_core::scalar_ops::{
    grid_cell_to_parent_u64, grid_get_resolution, grid_lonlat_to_cell,
    grid_polygon_to_cells_wkb_into, grid_to_lat, grid_to_lng, grid_to_wkb_u64,
};

use crate::ffi::opt_cstr;

/// C-ABI scalar helper: `h3_to_string(cell) -> *mut c_char` (NULL if invalid/0).
#[no_mangle]
pub extern "C" fn raster_h3_py_h3_to_string(cell: u64) -> *mut c_char {
    catch_unwind_or(ptr::null_mut(), || match h3_to_string(cell) {
        Some(s) => CString::new(s).unwrap_or_default().into_raw(),
        None => ptr::null_mut(),
    })
}

/// C-ABI scalar helper: `string_to_h3(hex) -> u64`.
///
/// # Safety
/// `hex` must be a valid NUL-terminated string or NULL.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_string_to_h3(hex: *const c_char) -> u64 {
    // SAFETY: `hex` is null or NUL-terminated (caller contract), as `opt_cstr` requires.
    unsafe {
        catch_unwind_or(0, || match opt_cstr(hex) {
            Some(s) => string_to_h3(&s),
            None => 0,
        })
    }
}

/// C-ABI scalar helper: `h3_to_lat(cell) -> f64`.
#[no_mangle]
pub extern "C" fn raster_h3_py_h3_to_lat(cell: u64) -> f64 {
    catch_unwind_or(f64::NAN, || h3_to_lat(cell))
}

/// C-ABI scalar helper: `h3_to_lng(cell) -> f64`.
#[no_mangle]
pub extern "C" fn raster_h3_py_h3_to_lng(cell: u64) -> f64 {
    catch_unwind_or(f64::NAN, || h3_to_lng(cell))
}

/// C-ABI scalar helper: `h3_get_resolution(cell) -> i64`.
#[no_mangle]
pub extern "C" fn raster_h3_py_h3_get_resolution(cell: u64) -> i64 {
    catch_unwind_or(-1, || h3_get_resolution(cell))
}

/// C-ABI scalar helper: `h3_is_valid_u64(cell) -> i32`.
#[no_mangle]
pub extern "C" fn raster_h3_py_h3_is_valid_u64(cell: u64) -> c_int {
    catch_unwind_or(0, || c_int::from(h3_is_valid_u64(cell)))
}

/// C-ABI scalar helper: `h3_is_valid_str(hex) -> i32`.
///
/// # Safety
/// `hex` must be a valid NUL-terminated string or NULL.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_h3_is_valid_str(hex: *const c_char) -> c_int {
    // SAFETY: `hex` is null or NUL-terminated (caller contract), as `opt_cstr` requires.
    unsafe {
        catch_unwind_or(0, || match opt_cstr(hex) {
            Some(s) => c_int::from(h3_is_valid_str(&s)),
            None => 0,
        })
    }
}

/// C-ABI scalar helper: `h3_cell_to_parent_u64(cell, parent_res) -> u64` (0 if invalid).
#[no_mangle]
pub extern "C" fn raster_h3_py_h3_cell_to_parent_u64(cell: u64, parent_res: i64) -> u64 {
    catch_unwind_or(0, || h3_cell_to_parent_u64(cell, parent_res).unwrap_or(0))
}

/// C-ABI scalar helper: `h3_cell_to_parent_str(hex, parent_res) -> *mut c_char`.
///
/// # Safety
/// `hex` must be a valid NUL-terminated string or NULL.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_h3_cell_to_parent_str(
    hex: *const c_char,
    parent_res: i64,
) -> *mut c_char {
    // SAFETY: `hex` is null or NUL-terminated (caller contract), as `opt_cstr` requires;
    // the returned string is freed with `raster_h3_py_free_string`.
    unsafe {
        catch_unwind_or(ptr::null_mut(), || {
            let Some(s) = opt_cstr(hex) else {
                return ptr::null_mut();
            };
            match h3_cell_to_parent_str(&s, parent_res) {
                Some(out) => CString::new(out).unwrap_or_default().into_raw(),
                None => ptr::null_mut(),
            }
        })
    }
}

/// C-ABI helper: the WKB buffer capacity that always suffices for
/// [`raster_h3_py_h3_to_wkb`] (`dggs_stream_core::encoding::WKB_BUF_LEN`).
#[no_mangle]
pub extern "C" fn raster_h3_py_wkb_buf_len() -> usize {
    dggs_stream_core::encoding::WKB_BUF_LEN
}

/// C-ABI helper: write WKB polygon for `cell` into `out_buf` (capacity of
/// `dggs_stream_core::encoding::WKB_BUF_LEN` bytes always suffices).
/// Returns number of bytes written, or `0` if `cell` is invalid or `buf_cap` is too small.
///
/// # Safety
/// `out_buf` must point to at least `buf_cap` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_h3_to_wkb(
    cell: u64,
    out_buf: *mut u8,
    buf_cap: usize,
) -> usize {
    // SAFETY: non-null `out_buf` has `buf_cap` writable bytes (caller contract) and
    // `wkb.len() <= buf_cap` was checked, so the copy stays in bounds; `wkb` is a separate
    // Rust allocation, so the regions cannot overlap.
    unsafe {
        if out_buf.is_null() {
            return 0;
        }
        let Some(wkb) = catch_unwind_or(None, || h3_to_wkb_u64(cell)) else {
            return 0;
        };
        if wkb.len() > buf_cap {
            return 0;
        }
        ptr::copy_nonoverlapping(wkb.as_ptr(), out_buf, wkb.len());
        wkb.len()
    }
}

/// C-ABI scalar helper: `h3_polygon_to_cells(wkb, res, containment)`.
///
/// On success (`0`) writes a heap array of `*out_len` cells to `*out_cells`; release it with
/// [`raster_h3_py_free_cells`]. A NULL `containment` means `contains_centroid`. Other return
/// codes leave the outputs untouched: `1` invalid geometry or resolution (or an internal
/// panic), `2` unknown containment mode, `3` more cells than `H3_POLYGON_TO_CELLS_MAX_CELLS`,
/// `-1` NULL pointers.
///
/// # Safety
/// `wkb` must point to `wkb_len` readable bytes, `containment` must be NULL or a valid
/// NUL-terminated string, and `out_cells` / `out_len` must be writable.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_h3_polygon_to_cells(
    wkb: *const u8,
    wkb_len: usize,
    res: i64,
    containment: *const c_char,
    out_cells: *mut *mut u64,
    out_len: *mut usize,
) -> c_int {
    // SAFETY: `containment` is null or NUL-terminated, `wkb` addresses `wkb_len` readable
    // bytes that stay unchanged during the call, and the out-pointers are non-null
    // (checked) and writable (caller contract). The boxed slice handed out is freed by
    // `raster_h3_py_free_cells` with the same length.
    unsafe {
        if wkb.is_null() || out_cells.is_null() || out_len.is_null() {
            return -1;
        }
        let mode = match opt_cstr(containment) {
            None => ContainmentMode::ContainsCentroid,
            Some(name) => match parse_containment_mode(&name) {
                Some(mode) => mode,
                None => return 2,
            },
        };
        let bytes = std::slice::from_raw_parts(wkb, wkb_len);
        let Some(result) =
            catch_unwind_or(None, || Some(h3_polygon_to_cells_wkb(bytes, res, mode)))
        else {
            return 1;
        };
        match result {
            Ok(cells) => {
                let cells = cells.into_boxed_slice();
                *out_len = cells.len();
                *out_cells = Box::into_raw(cells) as *mut u64;
                0
            }
            Err(PolygonToCellsError::TooManyCells { .. }) => 3,
            Err(_) => 1,
        }
    }
}

/// Free a cell array returned by [`raster_h3_py_h3_polygon_to_cells`].
///
/// # Safety
/// `cells` / `len` must come from one successful `raster_h3_py_h3_polygon_to_cells` call
/// and not have been freed already.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_free_cells(cells: *mut u64, len: usize) {
    // SAFETY: non-null `cells`/`len` came from one `Box<[u64]>` leaked by
    // `raster_h3_py_h3_polygon_to_cells` (caller contract), so rebuilding the fat pointer
    // with the same length reproduces that box, which is freed once.
    unsafe {
        if !cells.is_null() {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(cells, len)));
        }
    }
}

// -----------------------------------------------------------------------------
// Grid-neutral scalar helpers. `grid` is a grid code: 0 = H3, 1 = A5, 2 = S2,
// 3 = HEALPix (`GridKind as u8`); see `raster_h3_py_grid_code`.
// -----------------------------------------------------------------------------

/// The grid of a grid code, `None` if there is no such grid.
fn grid_of_code(code: c_int) -> Option<GridKind> {
    GridKind::ALL.into_iter().find(|g| *g as c_int == code)
}

/// C-ABI helper: the grid code of a grid name (`h3`, `a5`, `s2`, `healpix`,
/// case-insensitive), or `-1` if the name is NULL or unknown.
///
/// # Safety
/// `name` must be a valid NUL-terminated string or NULL.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_grid_code(name: *const c_char) -> c_int {
    // SAFETY: `name` is null or NUL-terminated (caller contract), as `opt_cstr` requires.
    unsafe {
        catch_unwind_or(-1, || {
            opt_cstr(name)
                .and_then(|name| GridKind::parse(&name))
                .map_or(-1, |grid| grid as c_int)
        })
    }
}

/// C-ABI helper: the finest resolution of grid `grid`, or `-1` for an unknown grid code.
#[no_mangle]
pub extern "C" fn raster_h3_py_grid_max_resolution(grid: c_int) -> c_int {
    grid_of_code(grid).map_or(-1, |g| c_int::from(g.max_resolution()))
}

/// C-ABI scalar helper: the cell of `grid` holding `(lon, lat)` degrees at `res`
/// (`0` if the grid code, coordinate or resolution is invalid).
#[no_mangle]
pub extern "C" fn raster_h3_py_grid_lonlat_to_cell(
    grid: c_int,
    lon: f64,
    lat: f64,
    res: i64,
) -> u64 {
    catch_unwind_or(0, || {
        grid_of_code(grid)
            .and_then(|g| grid_lonlat_to_cell(g, lon, lat, res))
            .unwrap_or(0)
    })
}

/// C-ABI scalar helper: whether `cell` is a valid cell of `grid`.
#[no_mangle]
pub extern "C" fn raster_h3_py_grid_is_valid(grid: c_int, cell: u64) -> c_int {
    catch_unwind_or(0, || {
        c_int::from(grid_of_code(grid).is_some_and(|g| g.is_valid(cell)))
    })
}

/// C-ABI scalar helper: centre latitude of `cell` in degrees (NaN if invalid).
#[no_mangle]
pub extern "C" fn raster_h3_py_grid_to_lat(grid: c_int, cell: u64) -> f64 {
    catch_unwind_or(f64::NAN, || {
        grid_of_code(grid).map_or(f64::NAN, |g| grid_to_lat(g, cell))
    })
}

/// C-ABI scalar helper: centre longitude of `cell` in degrees (NaN if invalid).
#[no_mangle]
pub extern "C" fn raster_h3_py_grid_to_lng(grid: c_int, cell: u64) -> f64 {
    catch_unwind_or(f64::NAN, || {
        grid_of_code(grid).map_or(f64::NAN, |g| grid_to_lng(g, cell))
    })
}

/// C-ABI scalar helper: resolution of `cell` (`-1` if invalid).
#[no_mangle]
pub extern "C" fn raster_h3_py_grid_get_resolution(grid: c_int, cell: u64) -> i64 {
    catch_unwind_or(-1, || {
        grid_of_code(grid).map_or(-1, |g| grid_get_resolution(g, cell))
    })
}

/// C-ABI scalar helper: ancestor of `cell` at `parent_res` (`0` if invalid or finer).
#[no_mangle]
pub extern "C" fn raster_h3_py_grid_cell_to_parent(grid: c_int, cell: u64, parent_res: i64) -> u64 {
    catch_unwind_or(0, || {
        grid_of_code(grid)
            .and_then(|g| grid_cell_to_parent_u64(g, cell, parent_res))
            .unwrap_or(0)
    })
}

/// C-ABI helper: write the WKB polygon of `cell` into `out_buf`, as
/// [`raster_h3_py_h3_to_wkb`] does for H3.
///
/// # Safety
/// `out_buf` must point to at least `buf_cap` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_grid_to_wkb(
    grid: c_int,
    cell: u64,
    out_buf: *mut u8,
    buf_cap: usize,
) -> usize {
    // SAFETY: non-null `out_buf` has `buf_cap` writable bytes (caller contract) and
    // `wkb.len() <= buf_cap` was checked, so the copy stays in bounds; `wkb` is a separate
    // Rust allocation, so the regions cannot overlap.
    unsafe {
        if out_buf.is_null() {
            return 0;
        }
        let Some(wkb) = catch_unwind_or(None, || {
            grid_of_code(grid).and_then(|g| grid_to_wkb_u64(g, cell))
        }) else {
            return 0;
        };
        if wkb.len() > buf_cap {
            return 0;
        }
        ptr::copy_nonoverlapping(wkb.as_ptr(), out_buf, wkb.len());
        wkb.len()
    }
}

/// C-ABI scalar helper: fill a WKB polygon with cells of `grid`, with the outputs and
/// return codes of [`raster_h3_py_h3_polygon_to_cells`]; an unknown grid code returns `4`.
///
/// # Safety
/// As for [`raster_h3_py_h3_polygon_to_cells`].
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_grid_polygon_to_cells(
    grid: c_int,
    wkb: *const u8,
    wkb_len: usize,
    res: i64,
    containment: *const c_char,
    out_cells: *mut *mut u64,
    out_len: *mut usize,
) -> c_int {
    // SAFETY: as for `raster_h3_py_h3_polygon_to_cells`.
    unsafe {
        if wkb.is_null() || out_cells.is_null() || out_len.is_null() {
            return -1;
        }
        let Some(grid) = grid_of_code(grid) else {
            return 4;
        };
        let mode = match opt_cstr(containment) {
            None => ContainmentMode::ContainsCentroid,
            Some(name) => match parse_containment_mode(&name) {
                Some(mode) => mode,
                None => return 2,
            },
        };
        let bytes = std::slice::from_raw_parts(wkb, wkb_len);
        let Some(result) = catch_unwind_or(None, || {
            let mut cells = Vec::new();
            Some(grid_polygon_to_cells_wkb_into(grid, bytes, res, mode, &mut cells).map(|()| cells))
        }) else {
            return 1;
        };
        match result {
            Ok(cells) => {
                let cells = cells.into_boxed_slice();
                *out_len = cells.len();
                *out_cells = Box::into_raw(cells) as *mut u64;
                0
            }
            Err(PolygonToCellsError::TooManyCells { .. }) => 3,
            Err(_) => 1,
        }
    }
}

/// C-ABI helper: the `auto` resolution of the raster at `path` on grid `grid` (a grid
/// code), moved `offset` resolution steps (positive is finer) and clamped to the grid's
/// range: the resolution whose cells are closest in area to the raster's ground pixels.
/// `crs` overrides the raster's CRS (NULL to use its own). Returns `-1` with `out_err`
/// set if the grid code is unknown or the raster cannot be opened or located.
///
/// # Safety
/// `path` and `crs` must be valid NUL-terminated strings or NULL; `out_err` must be NULL
/// or a writable `char **`.
#[no_mangle]
pub unsafe extern "C" fn raster_h3_py_auto_resolution(
    path: *const c_char,
    grid: c_int,
    offset: c_int,
    crs: *const c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    use dggs_stream_core::error::DggsStreamError;
    use dggs_stream_core::raster::mosaic::resolve_raster_sources;
    use dggs_stream_core::resolution::ResolutionSpec;
    // SAFETY: the C string arguments are null or NUL-terminated and `out_err` is null or
    // a writable `char **` (caller contract), as `opt_cstr` and `ffi_call` require.
    unsafe {
        crate::ffi::ffi_call("auto_resolution", out_err, || {
            let grid = grid_of_code(grid)
                .ok_or_else(|| DggsStreamError::InvalidParameter("unknown grid code".into()))?;
            let path = opt_cstr(path)
                .ok_or_else(|| DggsStreamError::InvalidParameter("Missing raster path".into()))?;
            let paths = resolve_raster_sources(&path)?;
            let resolutions = ResolutionSpec::Auto { offset }.resolve_paths(
                grid,
                &paths,
                opt_cstr(crs).as_deref(),
            )?;
            Ok(c_int::from(resolutions[0]))
        })
        .unwrap_or(-1)
    }
}
