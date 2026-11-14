-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION raster_h3" to load this file. \quit

-- ============================================================================
-- H3 Scalar Helper Functions
-- ============================================================================

CREATE OR REPLACE FUNCTION h3_to_string(cell bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_h3_to_string'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION string_to_h3(hex text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_string_to_h3'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_to_lat(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_h3_to_lat'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_to_lng(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_h3_to_lng'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_get_resolution(cell bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_h3_get_resolution'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_is_valid(cell bigint)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_h3_is_valid'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_is_valid(hex text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_h3_is_valid_text'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_to_wkb(cell bigint)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_h3_to_wkb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_cell_to_parent(cell bigint, parent_res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_h3_cell_to_parent'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- ============================================================================
-- Set-Returning Functions (SRFs) for Raster Aggregation
-- ============================================================================

CREATE OR REPLACE FUNCTION h3_raster_continuous_aggregate(
    raster_path text,
    h3_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    h3_index bigint,
    h3_hex text,
    mean double precision,
    stddev double precision,
    count double precision,
    min double precision,
    max double precision,
    sum double precision,
    resolution smallint,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_h3_raster_continuous_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_raster_categorical_aggregate(
    raster_path text,
    h3_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    h3_index bigint,
    h3_hex text,
    majority_class bigint,
    majority_fraction double precision,
    majority_count double precision,
    unique_classes bigint,
    total_count double precision,
    histogram text,
    resolution smallint,
    shannon_entropy double precision,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_h3_raster_categorical_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

-- The raster SRFs open server-side files with the server's OS privileges, so
-- restrict them like COPY FROM a file: superusers and pg_read_server_files only.
REVOKE EXECUTE ON FUNCTION h3_raster_continuous_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION h3_raster_categorical_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION h3_raster_continuous_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;
GRANT EXECUTE ON FUNCTION h3_raster_categorical_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;

CREATE OR REPLACE FUNCTION h3_lonlat_to_cell(lon double precision, lat double precision, res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_h3_lonlat_to_cell'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- ============================================================================
-- Other grids: A5 (resolutions 0..30), S2 (0..30) and HEALPix (0..29, nested
-- scheme, NUNIQ ids)
--
-- Cell ids are bigint, as for H3: the signed reading of the id's 64 bits. A5
-- and S2 ids use all 64 bits (an S2 id on cube faces 4 and 5 has the top bit
-- set), so some read as negative bigints. The conversion loses nothing: equal
-- cells have equal bigints, every function below takes and returns that form,
-- and {grid}_to_string / the {grid}_hex column give the unsigned hexadecimal
-- id. Only ORDER BY differs from the unsigned order. For the unsigned decimal
-- value use: CASE WHEN id < 0 THEN id::numeric + 18446744073709551616 ELSE id END.
-- ============================================================================

-- ============================================================================
-- A5 scalar helper functions and raster SRFs
-- ============================================================================

CREATE OR REPLACE FUNCTION a5_to_string(cell bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_a5_to_string'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION string_to_a5(hex text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_string_to_a5'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_to_lat(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_a5_to_lat'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_to_lng(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_a5_to_lng'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_get_resolution(cell bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_a5_get_resolution'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_is_valid(cell bigint)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_a5_is_valid'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_is_valid(hex text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_a5_is_valid_text'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_to_wkb(cell bigint)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_a5_to_wkb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_cell_to_parent(cell bigint, parent_res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_a5_cell_to_parent'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_lonlat_to_cell(lon double precision, lat double precision, res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_a5_lonlat_to_cell'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_raster_continuous_aggregate(
    raster_path text,
    a5_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    a5_index bigint,
    a5_hex text,
    mean double precision,
    stddev double precision,
    count double precision,
    min double precision,
    max double precision,
    sum double precision,
    resolution smallint,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_a5_raster_continuous_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION a5_raster_categorical_aggregate(
    raster_path text,
    a5_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    a5_index bigint,
    a5_hex text,
    majority_class bigint,
    majority_fraction double precision,
    majority_count double precision,
    unique_classes bigint,
    total_count double precision,
    histogram text,
    resolution smallint,
    shannon_entropy double precision,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_a5_raster_categorical_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION a5_raster_continuous_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION a5_raster_categorical_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION a5_raster_continuous_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;
GRANT EXECUTE ON FUNCTION a5_raster_categorical_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;

-- ============================================================================
-- S2 scalar helper functions and raster SRFs
-- ============================================================================

CREATE OR REPLACE FUNCTION s2_to_string(cell bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_s2_to_string'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION string_to_s2(hex text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_string_to_s2'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_to_lat(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_s2_to_lat'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_to_lng(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_s2_to_lng'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_get_resolution(cell bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_s2_get_resolution'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_is_valid(cell bigint)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_s2_is_valid'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_is_valid(hex text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_s2_is_valid_text'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_to_wkb(cell bigint)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_s2_to_wkb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_cell_to_parent(cell bigint, parent_res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_s2_cell_to_parent'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_lonlat_to_cell(lon double precision, lat double precision, res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_s2_lonlat_to_cell'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_raster_continuous_aggregate(
    raster_path text,
    s2_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    s2_index bigint,
    s2_hex text,
    mean double precision,
    stddev double precision,
    count double precision,
    min double precision,
    max double precision,
    sum double precision,
    resolution smallint,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_s2_raster_continuous_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION s2_raster_categorical_aggregate(
    raster_path text,
    s2_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    s2_index bigint,
    s2_hex text,
    majority_class bigint,
    majority_fraction double precision,
    majority_count double precision,
    unique_classes bigint,
    total_count double precision,
    histogram text,
    resolution smallint,
    shannon_entropy double precision,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_s2_raster_categorical_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION s2_raster_continuous_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION s2_raster_categorical_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION s2_raster_continuous_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;
GRANT EXECUTE ON FUNCTION s2_raster_categorical_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;

-- ============================================================================
-- HEALPix scalar helper functions and raster SRFs
-- ============================================================================

CREATE OR REPLACE FUNCTION healpix_to_string(cell bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_healpix_to_string'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION string_to_healpix(hex text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_string_to_healpix'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_to_lat(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_healpix_to_lat'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_to_lng(cell bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_healpix_to_lng'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_get_resolution(cell bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_healpix_get_resolution'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_is_valid(cell bigint)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_healpix_is_valid'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_is_valid(hex text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_healpix_is_valid_text'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_to_wkb(cell bigint)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_healpix_to_wkb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_cell_to_parent(cell bigint, parent_res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_healpix_cell_to_parent'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_lonlat_to_cell(lon double precision, lat double precision, res bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_healpix_lonlat_to_cell'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_raster_continuous_aggregate(
    raster_path text,
    healpix_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    healpix_index bigint,
    healpix_hex text,
    mean double precision,
    stddev double precision,
    count double precision,
    min double precision,
    max double precision,
    sum double precision,
    resolution smallint,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_healpix_raster_continuous_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION healpix_raster_categorical_aggregate(
    raster_path text,
    healpix_res integer DEFAULT 8,
    source_crs text DEFAULT NULL,
    nodata double precision DEFAULT NULL,
    bbox text DEFAULT NULL
)
RETURNS TABLE (
    healpix_index bigint,
    healpix_hex text,
    majority_class bigint,
    majority_fraction double precision,
    majority_count double precision,
    unique_classes bigint,
    total_count double precision,
    histogram text,
    resolution smallint,
    shannon_entropy double precision,
    wkb bytea
)
AS 'MODULE_PATHNAME', 'pg_healpix_raster_categorical_aggregate'
LANGUAGE C VOLATILE PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION healpix_raster_continuous_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION healpix_raster_categorical_aggregate(text, integer, text, double precision, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION healpix_raster_continuous_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;
GRANT EXECUTE ON FUNCTION healpix_raster_categorical_aggregate(text, integer, text, double precision, text) TO pg_read_server_files;

-- ============================================================================
-- Automatic resolution
--
-- {grid}_auto_resolution(raster_path [, res_offset [, source_crs]]) returns the
-- resolution whose cells are closest in area to the raster's ground pixels,
-- moved res_offset resolution steps (positive is finer) and clamped to the
-- grid's range. The grids number their resolutions on unrelated scales, so
-- this is how to ask for comparable cells on each:
--
--   SELECT * FROM s2_raster_continuous_aggregate(
--       '/data/dem.tif', s2_auto_resolution('/data/dem.tif'));
--
-- It reads the raster's header from the server's file system, so it is
-- restricted like the SRFs.
-- ============================================================================

CREATE OR REPLACE FUNCTION h3_auto_resolution(
    raster_path text,
    res_offset integer DEFAULT 0,
    source_crs text DEFAULT NULL
)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_h3_auto_resolution'
LANGUAGE C STABLE PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION h3_auto_resolution(text, integer, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION h3_auto_resolution(text, integer, text) TO pg_read_server_files;

CREATE OR REPLACE FUNCTION a5_auto_resolution(
    raster_path text,
    res_offset integer DEFAULT 0,
    source_crs text DEFAULT NULL
)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_a5_auto_resolution'
LANGUAGE C STABLE PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION a5_auto_resolution(text, integer, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION a5_auto_resolution(text, integer, text) TO pg_read_server_files;

CREATE OR REPLACE FUNCTION s2_auto_resolution(
    raster_path text,
    res_offset integer DEFAULT 0,
    source_crs text DEFAULT NULL
)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_s2_auto_resolution'
LANGUAGE C STABLE PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION s2_auto_resolution(text, integer, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION s2_auto_resolution(text, integer, text) TO pg_read_server_files;

CREATE OR REPLACE FUNCTION healpix_auto_resolution(
    raster_path text,
    res_offset integer DEFAULT 0,
    source_crs text DEFAULT NULL
)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_healpix_auto_resolution'
LANGUAGE C STABLE PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION healpix_auto_resolution(text, integer, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION healpix_auto_resolution(text, integer, text) TO pg_read_server_files;
