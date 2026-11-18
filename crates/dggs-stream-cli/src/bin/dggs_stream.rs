//! Unified `dggs-stream-cli` executable supporting `ipc` (Arrow/Columnar IPC to stdout or file),
//! `jsonl` (`JSONEachRow` to stdout for ClickHouse executable table functions),
//! `parquet`, and `pmtiles` exports, `parquet-to-pmtiles` transcoding, plus `plan` / `merge`
//! for partitioned scans.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use dggs_stream_cli::{
    export_raster_to_parquet_with_transfer, export_raster_to_pmtiles_with_transfer,
    merge_partition_files, parse_merge_args, parse_parquet_to_pmtiles_args, parse_plan_args,
    parse_stream_args, plan_raster_partitions, stream_raster_to_writer,
    transcode_parquet_to_pmtiles, write_plan, StreamOutputFormat,
};
use dggs_stream_core::error::Result;

/// Run `f` on a buffered writer for `path`, or stdout when `None`.
fn with_output<T>(path: Option<String>, f: impl FnOnce(&mut dyn Write) -> Result<T>) -> Result<T> {
    match path {
        Some(path) => f(&mut BufWriter::new(File::create(path)?)),
        None => f(&mut BufWriter::new(io::stdout().lock())),
    }
}

fn print_help() {
    eprintln!(
        "dggs-stream-cli: High-performance GeoTIFF/COG to H3 CLI\n\n\
        Usage:\n  \
          dggs-stream-cli ipc <input.tif> [options]        Stream binary Columnar IPC to stdout or -o <file>\n  \
          dggs-stream-cli jsonl <input.tif> [options]      Stream JSONEachRow (NDJSON) to stdout or -o <file>\n  \
          dggs-stream-cli parquet <input.tif> -o <out>     Export directly to Parquet / GeoParquet\n  \
          dggs-stream-cli pmtiles <input.tif> -o <out>     Export directly to PMTiles v3 archive\n  \
          dggs-stream-cli parquet-to-pmtiles <in.parquet> -o <out>  Transcode H3-indexed Parquet to PMTiles v3\n  \
          dggs-stream-cli plan <input.tif> [options]       Print partition specs (JSON Lines) for parallel scans\n  \
          dggs-stream-cli merge <part.rh3>... [-o <out>]   Combine partitioned `ipc` outputs into final cells\n\n\
        Options:\n  \
          --grid <h3|a5|s2|healpix>  Discrete global grid system (default: h3)\n  \
          -r, --resolutions <LIST>   Grid resolutions (H3: 0..=15, A5 and S2: 0..=30,\n                                     \
                                     HEALPix: 0..=29; default: 8), or `auto` for the\n                                     \
                                     resolution whose cells match the pixel size\n                                     \
                                     (`auto+1` one step finer, `auto-1` coarser)\n  \
          -c, --categorical          Enable categorical aggregation\n  \
          --format <wide|long>       Categorical output format (default: wide)\n  \
          --crs <CRS>                Override source CRS (e.g. EPSG:4326)\n  \
          --nodata <FLOAT>           Override nodata value\n  \
          --nodata-policy <omit|emit> omit (default): skip NoData and drop cells without\n                                     \
                                     a valid sample; emit: output every cell a pixel\n                                     \
                                     falls in (count 0 where all samples are NoData)\n  \
          --emit-nodata-value <NUM>  Value of an all-NoData cell's statistics under\n                                     \
                                     `--nodata-policy emit` (default: null)\n  \
          --decimals <N>             Round output statistics to N decimals (0..=15)\n  \
          --transfer <MODE>          How pixels map onto cells: assign_centers (default,\n                                     \
                                     each pixel into the cell holding its centre),\n                                     \
                                     interpolate (each cell takes the value interpolated\n                                     \
                                     at its centre; for cells finer than pixels) or\n                                     \
                                     overlay (each cell aggregates the pixels it\n                                     \
                                     overlaps, weighted by overlap area)\n  \
          --interpolation <M>        nearest, bilinear (default), bicubic or lanczos\n  \
          --overlay-weighted <BOOL>  false counts every touched pixel once (default true)\n  \
          --mass-preserve            Overlay: normalise each pixel's weights so `sum` is\n                                     \
                                     conserved exactly\n  \
          --histogram <SPEC>         Structured per-cell histogram (implies -c): `values`\n                                     \
                                     (a weight per pixel value), `N:min:max` (N equal\n                                     \
                                     bins) or `e0,e1,...` (bin edges). `ipc` and `jsonl`\n                                     \
                                     only\n  \
          --histogram-weight <W>     count (default) or area (overlap area, in pixels;\n                                     \
                                     runs --transfer overlay)\n  \
          --histogram-normalize <N>  none (default), fraction or density (bins only)\n  \
          --profile                  Print where the time went (decode, kernel, horizon,\n                                     \
                                     serialization; pixels, cells, stalls) on stderr\n  \
          --raw-values               Skip GDAL band scale/offset calibration\n  \
          --bbox <minx,miny,maxx,maxy> WGS84 bounding box filter\n  \
          --columns <col1,col2,...>  Column projection pushdown\n  \
          --limit <N>                Maximum number of rows to emit\n  \
          -o, --output <PATH|->      Output file path (default: '-' for stdout on ipc/jsonl/plan/merge)\n  \
          --h3-column <NAME>         parquet-to-pmtiles: H3 column, integer or hex (default: h3_index,\n  \
          \x20                          h3_hex, h3, cell or hex, whichever comes first)\n\n\
        Partitioned scans (shell fan-out; plan and scan with the same --crs and --bbox):\n  \
          plan --target-chunks <N>       TIFF tiles/strips per partition (default: 256)\n  \
          ipc --partition <SPEC|@FILE>   Scan one partition (adds a partial_state column)\n  \
          ipc --partition-index <K>      Line K of the plan file given as --partition @FILE\n  \
          merge -c                       Categorical partitions (else inferred from columns)\n  \
          merge --jsonl                  Write JSONEachRow instead of IPC\n\n  \
          dggs-stream-cli plan dem.tif -o plan.jsonl\n  \
          seq 0 $(( $(wc -l < plan.jsonl) - 1 )) | xargs -P 8 -I{{}} \\\n      \
            dggs-stream-cli ipc dem.tif -r 8 --partition @plan.jsonl --partition-index {{}} -o part-{{}}.rh3\n  \
          dggs-stream-cli merge part-*.rh3 -o dem.rh3"
    );
}

fn reject_partition(opts: &dggs_stream_cli::CliStreamOptions, subcmd: &str) -> Result<()> {
    if opts.partition.is_some() {
        return Err(dggs_stream_core::error::RasterH3Error::InvalidParameter(format!(
            "{subcmd} does not take --partition: scan partitions with `ipc` and combine them with `merge`"
        )));
    }
    Ok(())
}

fn reject_histogram(opts: &dggs_stream_cli::CliStreamOptions, subcmd: &str) -> Result<()> {
    if opts.histogram.is_some() {
        return Err(dggs_stream_core::error::RasterH3Error::InvalidParameter(format!(
            "{subcmd} does not take --histogram: its files have no histogram column; use `ipc` or `jsonl`"
        )));
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        if args.len() < 2 {
            std::process::exit(1);
        }
        return;
    }

    let subcmd = args[1].as_str();
    let rest = &args[2..];

    let result = match subcmd {
        "ipc" => parse_stream_args(rest, StreamOutputFormat::Ipc).and_then(|(opts, out)| {
            with_output(out, |mut w| stream_raster_to_writer(&opts, &mut w))
        }),
        "jsonl" => parse_stream_args(rest, StreamOutputFormat::Jsonl).and_then(|(opts, out)| {
            with_output(out, |mut w| stream_raster_to_writer(&opts, &mut w))
        }),
        "plan" => parse_plan_args(rest).and_then(|args| {
            let plan = plan_raster_partitions(
                &args.input_path,
                args.target_chunks,
                args.source_crs.as_deref(),
                args.bbox,
            )?;
            with_output(args.output, |mut w| write_plan(&plan, &mut w))?;
            Ok(plan.len())
        }),
        "merge" => parse_merge_args(rest)
            .and_then(|(opts, out)| with_output(out, |mut w| merge_partition_files(&opts, &mut w))),
        "parquet" => parse_stream_args(rest, StreamOutputFormat::Ipc).and_then(|(opts, out)| {
            reject_partition(&opts, "parquet")?;
            reject_histogram(&opts, "parquet")?;
            let out_path = out.ok_or_else(|| {
                dggs_stream_core::error::RasterH3Error::InvalidParameter(
                    "Missing -o / --output path for parquet export".into(),
                )
            })?;
            export_raster_to_parquet_with_transfer(
                opts.grid,
                &opts.input_path,
                Path::new(&out_path),
                opts.resolutions,
                opts.categorical,
                opts.source_crs,
                opts.nodata,
                opts.bbox,
                true,
                opts.apply_scale_offset,
                &opts.output,
                opts.transfer,
                opts.profile.clone(),
            )
        }),
        "pmtiles" => parse_stream_args(rest, StreamOutputFormat::Ipc).and_then(|(opts, out)| {
            reject_partition(&opts, "pmtiles")?;
            reject_histogram(&opts, "pmtiles")?;
            let out_path = out.ok_or_else(|| {
                dggs_stream_core::error::RasterH3Error::InvalidParameter(
                    "Missing -o / --output path for pmtiles export".into(),
                )
            })?;
            export_raster_to_pmtiles_with_transfer(
                opts.grid,
                &opts.input_path,
                Path::new(&out_path),
                opts.resolutions,
                opts.categorical,
                opts.source_crs,
                opts.nodata,
                opts.bbox,
                opts.apply_scale_offset,
                &opts.output,
                opts.transfer,
                opts.profile.clone(),
            )
        }),
        "parquet-to-pmtiles" => parse_parquet_to_pmtiles_args(rest).and_then(|args| {
            let summary = transcode_parquet_to_pmtiles(
                Path::new(&args.input_path),
                Path::new(&args.output_path),
                args.h3_column.as_deref(),
            )?;
            if summary.invalid_features_dropped > 0 {
                eprintln!(
                    "dggs-stream-cli: skipped {} of {} rows whose H3 value was null or not a valid cell",
                    summary.invalid_features_dropped, summary.total_features
                );
            }
            Ok(summary.valid_features)
        }),
        other => {
            eprintln!("Unknown subcommand: '{}'", other);
            print_help();
            std::process::exit(1);
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
