//! Hive-partitioned H3 datasets. Group one bounded window at a time and close
//! each part before writing the next, regardless of the number of parents.
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{ParquetExportConfig, ParquetRowGroupBuffer, ParquetStreamer};
use crate::columnar::{multiband::MultiBandBatchReader, ColumnArray, RecordBatch};
use h3o::{CellIndex, Resolution};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Dataset {
    staging: tempfile::TempDir,
    resolution: Resolution,
    part: usize,
}

impl Dataset {
    fn new(destination: &Path, resolution: u8) -> Result<Self> {
        let resolution = Resolution::try_from(resolution)?;
        if destination.try_exists()? {
            return Err(format!("dataset output already exists: {}", destination.display()).into());
        }
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;
        Ok(Self {
            staging: tempfile::tempdir_in(parent)?,
            resolution,
            part: 0,
        })
    }

    fn parent(&self, id: u64) -> Result<CellIndex> {
        CellIndex::try_from(id)?
            .parent(self.resolution)
            .ok_or_else(|| "partition resolution is finer than the output cell".into())
    }

    fn next_file(&mut self, parent: Option<CellIndex>) -> Result<PathBuf> {
        // An empty dataset still has a discoverable schema and Hive column.
        let value = parent
            .map(|p| p.to_string())
            .unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".into());
        let directory = self
            .staging
            .path()
            .join(format!("h3_{:02}={value}", u8::from(self.resolution)));
        std::fs::create_dir_all(&directory)?;
        let path = directory.join(format!("part-{:06}.parquet", self.part));
        self.part += 1;
        Ok(path)
    }

    fn publish(self, destination: &Path) -> Result<()> {
        std::fs::rename(self.staging.path(), destination)?;
        Ok(())
    }
}

/// Write completed H3 records with the same columns, geometry and footer as a
/// single-file export. Each window contributes at most one file per parent.
pub fn write_parent_records<S: ParquetStreamer, B: ParquetRowGroupBuffer<Record = S::Record>>(
    mut source: S,
    destination: &Path,
    resolution: u8,
    config: ParquetExportConfig,
    cell_id: impl Fn(&S::Record) -> u64,
) -> Result<usize> {
    if source.grid() != crate::GridKind::H3 {
        return Err("parent-partitioned export requires H3".into());
    }
    let mut dataset = Dataset::new(destination, resolution)?;
    let policy = source.output_policy();
    B::check_output_policy(&policy)?;
    let limit = config.row_group_size.max(1);
    let mut total = 0;
    loop {
        let mut rows = Vec::new();
        while rows.len() < limit {
            let n = source.drain_completed_into(limit - rows.len(), |_, row| rows.push(row))?;
            if n == 0 {
                break;
            }
        }
        let finished = rows.len() < limit;
        let clock = crate::run_profile::SerializeClock::start(source.run_profile());
        let mut groups = BTreeMap::<Option<CellIndex>, Vec<S::Record>>::new();
        for row in rows {
            groups
                .entry(Some(dataset.parent(cell_id(&row))?))
                .or_default()
                .push(row);
        }
        if total == 0 && groups.is_empty() {
            groups.insert(None, Vec::new());
        }
        for (parent, rows) in groups {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(dataset.next_file(parent)?)?;
            let mut footer = file.try_clone()?;
            let mut writer = super::pipeline::open_writer::<B>(file, source.grid(), &config)?;
            let mut buffer = B::with_capacity_and_grid_options(
                rows.len(),
                config.should_omit_redundant_columns(),
                config.geoparquet,
                source.grid(),
            );
            buffer.set_output_policy(policy);
            for row in rows {
                buffer.push_record(row);
            }
            if !buffer.is_empty() {
                buffer.sort_by_h3_index();
                buffer.flush_to_row_group(&mut writer)?;
                clock.rows(buffer.len());
                total += buffer.len();
            }
            let bounds = config.geoparquet.then(|| {
                buffer
                    .geometry_bounds()
                    .or_else(|| source.bounds_wgs84())
                    .unwrap_or([-180., -90., 180., 90.])
            });
            super::pipeline::close_writer(writer, &mut footer, bounds)?;
        }
        if finished {
            break;
        }
    }
    dataset.publish(destination)?;
    Ok(total)
}

/// Export projected wide batches by H3 parent. When the routing index was
/// appended specifically for partitioning, omit it from the published schema.
pub fn write_parent_multiband(
    source: &mut MultiBandBatchReader,
    destination: &Path,
    resolution: u8,
    hidden_index: bool,
) -> Result<usize> {
    let mut dataset = Dataset::new(destination, resolution)?;
    let input = source.schema();
    let key = input
        .index_of("h3_index")
        .ok_or("parent-partitioned export needs h3_index")?;
    let fields: Vec<_> = (0..input.fields.len())
        .filter(|&i| !(hidden_index && i == key))
        .collect();
    let schema = Arc::new(input.project(&fields)?);
    let config = ParquetExportConfig {
        geoparquet: true,
        ..Default::default()
    };
    let limit = super::multiband::group_rows(&input, config.row_group_size);
    let mut groups = BTreeMap::<Option<CellIndex>, Vec<RecordBatch>>::new();
    let mut pending = 0;
    let mut total = 0;
    let flush =
        |dataset: &mut Dataset, groups: &mut BTreeMap<_, Vec<RecordBatch>>| -> Result<usize> {
            let mut n = 0;
            for (parent, batches) in std::mem::take(groups) {
                n += super::multiband::write_batches(
                    schema.clone(),
                    batches.into_iter().map(Ok),
                    &dataset.next_file(parent)?,
                    &config,
                )?;
            }
            Ok(n)
        };
    for batch in source {
        let batch = batch?;
        let ColumnArray::UInt64 { values, .. } = &batch.columns[key] else {
            return Err("invalid h3_index column".into());
        };
        let mut offset = 0;
        while offset < batch.num_rows {
            let end = batch.num_rows.min(offset.saturating_add(limit - pending));
            let mut by_parent = BTreeMap::<CellIndex, Vec<usize>>::new();
            for (idx, &id) in values[offset..end].iter().enumerate() {
                by_parent
                    .entry(dataset.parent(id)?)
                    .or_default()
                    .push(offset + idx);
            }
            for (parent, indices) in by_parent {
                let cols = fields
                    .iter()
                    .map(|&i| select_wide_rows(&batch.columns[i], &indices))
                    .collect::<Result<_>>()?;
                groups
                    .entry(Some(parent))
                    .or_default()
                    .push(RecordBatch::try_new(schema.clone(), cols)?);
            }
            pending += end - offset;
            offset = end;
            if pending == limit {
                total += flush(&mut dataset, &mut groups)?;
                pending = 0;
            }
        }
    }
    if pending == 0 && total == 0 {
        groups.insert(None, Vec::new());
    }
    total += flush(&mut dataset, &mut groups)?;
    dataset.publish(destination)?;
    Ok(total)
}

fn select_wide_rows(array: &ColumnArray, indices: &[usize]) -> Result<ColumnArray> {
    fn take<T: Clone>(values: &[T], indices: &[usize]) -> Vec<T> {
        indices.iter().map(|&i| values[i].clone()).collect()
    }
    let valid = array.validity().map(|bits| take(bits, indices));
    macro_rules! select {
        ($kind:ident,$values:expr) => {
            ColumnArray::$kind {
                values: take($values, indices),
                validity: valid,
            }
        };
    }
    Ok(match array {
        ColumnArray::UInt64 { values, .. } => select!(UInt64, values),
        ColumnArray::UInt8 { values, .. } => select!(UInt8, values),
        ColumnArray::Float64 { values, .. } => select!(Float64, values),
        ColumnArray::Binary { values, .. } => select!(Binary, values),
        ColumnArray::Utf8 { values, .. } => select!(Utf8, values),
        _ => return Err("unexpected column in wide GeoParquet export".into()),
    })
}
