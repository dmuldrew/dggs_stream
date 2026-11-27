//! GeoJSON polygons to one JSON Lines record per feature and cell.
use std::fs::File;
use std::io::{self, BufReader, Read, Write};

use dggs_stream_core::error::{DggsStreamError, Result};
use dggs_stream_core::grid::GridKind;
use dggs_stream_core::scalar_ops::{
    grid_polygon_to_cells_into, parse_containment_mode, ContainmentMode,
};
use geo_types::{Coord, LineString, MultiPolygon, Polygon};
use geojson::{Feature, GeoJson, GeometryValue};
use serde_json::{json, Value};

/// Parsed `polyfill` arguments: the GeoJSON source (`-` for stdin), grid, single
/// resolution and containment mode.
pub struct Options {
    input: String,
    grid: GridKind,
    resolution: u8,
    mode: ContainmentMode,
}

fn invalid(message: impl Into<String>) -> DggsStreamError {
    DggsStreamError::InvalidParameter(message.into())
}

/// Parse the arguments after `polyfill`: one input path (or `-`), `--grid`, `-r/--resolution`,
/// `--containment` and `-o/--output`. Returns the options and the output path, `None` for
/// stdout. Any other argument, a missing input or a resolution beyond the grid's range is an
/// error.
pub fn parse_args(args: &[String]) -> Result<(Options, Option<String>)> {
    let mut input = None;
    let mut output = None;
    let mut grid = GridKind::H3;
    let mut resolution = 8;
    let mut mode = ContainmentMode::ContainsCentroid;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut value = || {
            iter.next()
                .ok_or_else(|| invalid(format!("missing value for {arg}")))
        };
        match arg.as_str() {
            "--grid" => {
                grid = GridKind::parse(value()?).ok_or_else(|| invalid(GridKind::EXPECTED))?
            }
            "-r" | "--resolution" => {
                resolution = value()?
                    .parse()
                    .map_err(|_| invalid("expected an integer resolution"))?
            }
            "--containment" => {
                mode = parse_containment_mode(value()?).ok_or_else(|| {
                    invalid("expected containment center, full, intersects_boundary or covers")
                })?
            }
            "-o" | "--output" => {
                let path = value()?;
                output = (path != "-").then(|| path.clone());
            }
            path if (path == "-" || !path.starts_with('-')) && input.is_none() => {
                input = Some(arg.clone())
            }
            _ => return Err(invalid(format!("unexpected polyfill argument {arg}"))),
        }
    }
    if resolution > grid.max_resolution() {
        return Err(invalid(format!(
            "resolution {resolution} exceeds {} maximum {}",
            grid.as_str(),
            grid.max_resolution()
        )));
    }
    Ok((
        Options {
            input: input
                .ok_or_else(|| invalid("polyfill requires a GeoJSON file or - for stdin"))?,
            grid,
            resolution,
            mode,
        },
        output,
    ))
}

fn polygon(rings: geojson::PolygonType) -> std::result::Result<Polygon<f64>, &'static str> {
    let mut rings = rings.into_iter().map(|ring| {
        if ring.len() < 4 || ring.first() != ring.last() {
            return Err("expected a closed linear ring with at least four positions");
        }
        ring.into_iter()
            .map(|position| match position.as_slice() {
                [x, y, ..] if (-90.0..=90.0).contains(y) && (-180.0..=180.0).contains(x) => {
                    Ok(Coord { x: *x, y: *y })
                }
                _ => Err("expected WGS84 longitude and latitude"),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(LineString::new)
    });
    let exterior = rings.next().transpose()?.ok_or("missing exterior ring")?;
    Ok(Polygon::new(
        exterior,
        rings.collect::<std::result::Result<_, _>>()?,
    ))
}

/// Fill every Polygon and MultiPolygon feature of `opts.input` (a Feature, FeatureCollection,
/// bare Geometry or a stream of them) and write one JSON object per `(feature, cell)` to
/// `writer`: `feature_id` (the feature's `id`, or its ordinal), the grid's hex and index
/// columns and `resolution`. Features without geometry are skipped; other geometry types and
/// invalid coordinates are errors naming the feature's ordinal. Returns the rows written.
pub fn run(opts: &Options, writer: &mut dyn Write) -> Result<usize> {
    let input: Box<dyn Read> = if opts.input == "-" {
        Box::new(io::stdin().lock())
    } else {
        Box::new(File::open(&opts.input)?)
    };
    // Row keys in sorted order, as serde_json writes an object: where the grid's column
    // names fall relative to `feature_id` and `resolution` depends on the grid.
    #[derive(Clone, Copy)]
    enum Field {
        Id,
        Hex,
        Index,
        Resolution,
    }
    let mut fields = [
        ("feature_id", Field::Id),
        (opts.grid.hex_column_name(), Field::Hex),
        (opts.grid.index_column_name(), Field::Index),
        ("resolution", Field::Resolution),
    ];
    fields.sort_unstable_by_key(|&(name, _)| name);
    let mut cells = Vec::new();
    let mut features = 0usize;
    let mut rows = 0usize;
    let mut fill = |feature: Feature| -> Result<()> {
        let ordinal = features;
        features += 1;
        let Some(geometry) = feature.geometry else {
            return Ok(());
        };
        let context = |message: String| invalid(format!("feature {ordinal}: {message}"));
        let polygons = match geometry.value {
            GeometryValue::Polygon { coordinates } => vec![coordinates],
            GeometryValue::MultiPolygon { coordinates } => coordinates,
            _ => return Err(context("expected Polygon or MultiPolygon".into())),
        };
        let polygons = polygons
            .into_iter()
            .filter(|p| !p.is_empty())
            .map(polygon)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(MultiPolygon::new)
            .map_err(|message| context(message.into()))?;
        grid_polygon_to_cells_into(opts.grid, polygons, opts.resolution, opts.mode, &mut cells)
            .map_err(|e| context(e.to_string()))?;
        // Serialize the ID once and format each row straight into the writer.
        let id = feature
            .id
            .map_or_else(|| Value::from(ordinal), |id| json!(id));
        let id = serde_json::to_string(&id).map_err(io::Error::from)?;
        for &cell in &cells {
            writer.write_all(b"{")?;
            for (i, &(name, field)) in fields.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b",")?;
                }
                write!(writer, "\"{name}\":")?;
                match field {
                    Field::Id => writer.write_all(id.as_bytes())?,
                    Field::Hex => write!(writer, "\"{cell:x}\"")?,
                    Field::Index => write!(writer, "{cell}")?,
                    Field::Resolution => write!(writer, "{}", opts.resolution)?,
                }
            }
            writer.write_all(b"}\n")?;
        }
        rows += cells.len();
        Ok(())
    };
    for item in serde_json::Deserializer::from_reader(BufReader::new(input)).into_iter::<GeoJson>()
    {
        match item.map_err(|e| invalid(format!("invalid GeoJSON: {e}")))? {
            GeoJson::Feature(feature) => fill(feature)?,
            GeoJson::Geometry(geometry) => fill(Feature::from(geometry))?,
            GeoJson::FeatureCollection(collection) => {
                for feature in collection.features {
                    fill(feature)?;
                }
            }
        }
    }
    Ok(rows)
}
