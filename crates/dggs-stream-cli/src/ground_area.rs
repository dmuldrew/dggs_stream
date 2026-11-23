//! Stream the core's area-weighted records through the CLI's IPC and JSONL writers.
use super::*;
use dggs_stream_core::columnar::ground_area::{ground_area_schema, GroundAreaBatchReader};
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;

pub(super) fn stream<W: Write>(
    opts: &CliStreamOptions,
    paths: &[PathBuf],
    config: &MultiResolutionConfig,
    writer: &mut W,
) -> Result<usize> {
    let [path] = paths else {
        return Err(DggsStreamError::InvalidParameter(
            "--ground-area requires one raster".into(),
        ));
    };
    if opts.bands.is_some()
        || opts.categorical
        || opts.histogram.is_some()
        || opts.partition.is_some()
    {
        return Err(DggsStreamError::InvalidParameter(
            "--ground-area requires an unpartitioned single-band continuous scan".into(),
        ));
    }
    let schema = ground_area_schema(config.grid, false);
    let projection = resolve_projection_indices(&schema, &opts.columns)?
        .unwrap_or_else(|| (0..schema.fields.len()).collect());
    let mut reader = GroundAreaBatchReader::open(
        GeoTiffStreamReader::open(path)?,
        config,
        projection,
        opts.batch_size,
        opts.limit,
    )?;
    let count = match opts.output_format {
        StreamOutputFormat::Ipc => write_ipc_stream(writer, &reader.schema(), &mut reader),
        StreamOutputFormat::Jsonl => write_jsonl_stream(writer, &mut reader),
    };
    warn_transform_failures(reader.transform_failures());
    count
}
