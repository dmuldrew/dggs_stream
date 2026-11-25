//! Private, lossless accumulator runs. Two-way merges bound open readers and memory;
//! binary-carry levels avoid rewriting the entire history on every spill.
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::Path;

use super::sharded_map::AccumulatorMerge;
use crate::aggregator::{
    accumulator::ContinuousAccumulator, categorical::CategoricalAccumulator,
    quantiles::QuantileSketch,
};

pub trait SpillAccumulator: AccumulatorMerge {
    fn write_state(&self, out: &mut impl Write) -> io::Result<()>;
    fn read_state(input: &mut impl Read, limit: usize) -> io::Result<Self>;
    fn memory_bytes(&self) -> usize {
        size_of::<Self>() + self.heap_bytes()
    }
}

pub(crate) fn table_bytes<K, V>(capacity: usize) -> usize {
    // Conservative allowance for bucket slack, control bytes and alignment.
    if capacity == 0 {
        0
    } else {
        (capacity + 1).saturating_mul(2 * (size_of::<(K, V)>() + 1)) + 64
    }
}
#[inline]
fn spill_buf_capacity(limit: usize) -> usize {
    (limit / 4).clamp(4096, 64 * 1024)
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn write_u64(out: &mut impl Write, v: u64) -> io::Result<()> {
    out.write_all(&v.to_le_bytes())
}
fn read_u64(input: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}
fn check_total(v: f64, what: &str) -> io::Result<f64> {
    if v.is_finite() && v >= 0.0 {
        Ok(v)
    } else {
        Err(invalid(&format!("invalid {what} {v} in accumulator state")))
    }
}
fn check_weight(v: f64, what: &str) -> io::Result<f64> {
    if v.is_finite() && v > 0.0 {
        Ok(v)
    } else {
        Err(invalid(&format!(
            "invalid {what} weight {v} in accumulator state"
        )))
    }
}
#[inline]
fn f64_from_le_slice(bytes: &[u8]) -> f64 {
    f64::from_bits(u64::from_le_bytes(bytes.try_into().unwrap()))
}
#[inline]
fn u64_from_le_slice(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().unwrap())
}
fn count(input: &mut impl Read, limit: usize) -> io::Result<usize> {
    let n = usize::try_from(read_u64(input)?).map_err(|_| invalid("spill length overflow"))?;
    if n > limit / 64 {
        return Err(invalid("spill accumulator exceeds aggregation budget"));
    }
    Ok(n)
}

impl SpillAccumulator for ContinuousAccumulator {
    fn write_state(&self, out: &mut impl Write) -> io::Result<()> {
        let mut hdr = [0u8; 56];
        hdr[0..8].copy_from_slice(&self.sum.to_bits().to_le_bytes());
        hdr[8..16].copy_from_slice(&self.count.to_bits().to_le_bytes());
        hdr[16..24].copy_from_slice(&self.sum_w2.to_bits().to_le_bytes());
        hdr[24..32].copy_from_slice(&self.min.to_bits().to_le_bytes());
        hdr[32..40].copy_from_slice(&self.max.to_bits().to_le_bytes());
        hdr[40..48].copy_from_slice(&self.m2.to_bits().to_le_bytes());
        hdr[48..56].copy_from_slice(&u64::from(self.quantiles.is_some()).to_le_bytes());
        out.write_all(&hdr)?;
        if let Some(q) = &self.quantiles {
            let mut qhdr = [0u8; 16];
            qhdr[0..8].copy_from_slice(&q.zero_count.to_bits().to_le_bytes());
            qhdr[8..16].copy_from_slice(&q.total_count.to_bits().to_le_bytes());
            out.write_all(&qhdr)?;
            for bins in [&q.pos_bins, &q.neg_bins] {
                write_u64(out, bins.len() as u64)?;
                for (&k, &v) in bins {
                    let mut entry = [0u8; 12];
                    entry[0..4].copy_from_slice(&k.to_le_bytes());
                    entry[4..12].copy_from_slice(&v.to_bits().to_le_bytes());
                    out.write_all(&entry)?;
                }
            }
        }
        Ok(())
    }
    /// Rejects states the accumulator cannot reach: a negative, NaN or infinite count or
    /// `sum_w2`, a negative `m2`, or `min > max` (or a NaN bound) once anything is counted.
    /// `sum`, a NaN `m2` and infinite bounds stay allowed: extreme inputs can overflow them.
    fn read_state(input: &mut impl Read, limit: usize) -> io::Result<Self> {
        let mut hdr = [0u8; 56];
        input.read_exact(&mut hdr)?;
        let sum = f64_from_le_slice(&hdr[0..8]);
        let count = check_total(f64_from_le_slice(&hdr[8..16]), "count")?;
        let sum_w2 = check_total(f64_from_le_slice(&hdr[16..24]), "sum of squared weights")?;
        let (min, max, m2) = (
            f64_from_le_slice(&hdr[24..32]),
            f64_from_le_slice(&hdr[32..40]),
            f64_from_le_slice(&hdr[40..48]),
        );
        if m2 < 0.0 {
            return Err(invalid("negative m2 in accumulator state"));
        }
        if count > 0.0 && (min.is_nan() || max.is_nan() || min > max) {
            return Err(invalid("accumulator state has min > max"));
        }
        let mut acc = Self::from_weighted_stats(sum, count, sum_w2, min, max, m2);
        match u64_from_le_slice(&hdr[48..56]) {
            0 => {}
            1 => {
                let mut qhdr = [0u8; 16];
                input.read_exact(&mut qhdr)?;
                let mut q = QuantileSketch::new();
                q.zero_count = check_total(f64_from_le_slice(&qhdr[0..8]), "quantile zero count")?;
                q.total_count =
                    check_total(f64_from_le_slice(&qhdr[8..16]), "quantile total count")?;
                for bins in [&mut q.pos_bins, &mut q.neg_bins] {
                    let n = self::count(input, limit)?;
                    for _ in 0..n {
                        let mut entry = [0u8; 12];
                        input.read_exact(&mut entry)?;
                        let key = i32::from_le_bytes(entry[0..4].try_into().unwrap());
                        let weight =
                            check_weight(f64_from_le_slice(&entry[4..12]), "quantile bucket")?;
                        if bins.insert(key, weight).is_some() {
                            return Err(invalid("duplicate quantile bucket in accumulator state"));
                        }
                    }
                }
                acc.quantiles = Some(Box::new(q));
            }
            _ => return Err(invalid("invalid quantile marker in spill")),
        }
        if acc.memory_bytes() > limit {
            return Err(invalid("quantile accumulator exceeds aggregation budget"));
        }
        Ok(acc)
    }
}

impl SpillAccumulator for CategoricalAccumulator {
    fn write_state(&self, out: &mut impl Write) -> io::Result<()> {
        let mut hdr = [0u8; 16];
        hdr[0..8].copy_from_slice(&self.total_count.to_bits().to_le_bytes());
        hdr[8..16].copy_from_slice(&(self.unique_classes() as u64).to_le_bytes());
        out.write_all(&hdr)?;
        // Do not round-trip through JSON: it cannot preserve arbitrary f64 states.
        let mut result = Ok(());
        self.for_each_class(|k, v| {
            if result.is_ok() {
                let mut entry = [0u8; 16];
                entry[0..8].copy_from_slice(&k.to_le_bytes());
                entry[8..16].copy_from_slice(&v.to_bits().to_le_bytes());
                result = out.write_all(&entry);
            }
        });
        result
    }
    /// Rejects a NaN, negative or infinite total, a class weight that is not finite and
    /// positive, and a class listed twice.
    fn read_state(input: &mut impl Read, limit: usize) -> io::Result<Self> {
        let mut hdr = [0u8; 16];
        input.read_exact(&mut hdr)?;
        let total = check_total(f64_from_le_slice(&hdr[0..8]), "total count")?;
        // The compact list stores a class in 16 bytes, so the generic `count` bound
        // (64 bytes per entry) would refuse states that `push_sorted` accepted. Bound
        // the count by the smallest per-class size and check the real size as
        // classes arrive, so an oversized state still fails before it grows far.
        let n = usize::try_from(u64_from_le_slice(&hdr[8..16]))
            .map_err(|_| invalid("spill length overflow"))?;
        if n > limit / size_of::<(i64, f64)>() {
            return Err(invalid("spill accumulator exceeds aggregation budget"));
        }
        let mut acc = Self::new();
        for _ in 0..n {
            let mut entry = [0u8; 16];
            input.read_exact(&mut entry)?;
            let class = i64::from_le_bytes(entry[0..8].try_into().unwrap());
            let weight = check_weight(f64_from_le_slice(&entry[8..16]), "class")?;
            if acc.get_class_count(class) != 0.0 {
                return Err(invalid("duplicate class in accumulator state"));
            }
            acc.update_weighted(class, weight);
            if acc.memory_bytes() > limit {
                return Err(invalid(
                    "categorical accumulator exceeds aggregation budget",
                ));
            }
        }
        acc.total_count = total;
        if acc.memory_bytes() > limit {
            return Err(invalid(
                "categorical accumulator exceeds aggregation budget",
            ));
        }
        Ok(acc)
    }
}

struct Run {
    file: File,
    len: u64,
}

struct SingleRunReader<A> {
    input: BufReader<File>,
    remaining: u64,
    limit: usize,
    _acc: PhantomData<A>,
}

impl<A: SpillAccumulator> SingleRunReader<A> {
    fn new(mut run: Run, limit: usize) -> io::Result<Self> {
        run.file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            input: BufReader::with_capacity(spill_buf_capacity(limit), run.file),
            remaining: run.len,
            limit,
            _acc: PhantomData,
        })
    }

    fn next_record(&mut self) -> io::Result<Option<(u64, A)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        // Truncation is an error, never successful EOF.
        let key = read_u64(&mut self.input)?;
        let value = A::read_state(&mut self.input, self.limit)?;
        self.remaining -= 1;
        Ok(Some((key, value)))
    }
}

pub struct RunReader<A> {
    readers: Vec<SingleRunReader<A>>,
    heads: Vec<Option<(u64, A)>>,
    initialized: bool,
    limit: usize,
}

impl<A: SpillAccumulator> RunReader<A> {
    fn from_runs(runs: Vec<Run>, limit: usize) -> io::Result<Self> {
        let mut readers = Vec::with_capacity(runs.len());
        for run in runs {
            readers.push(SingleRunReader::new(run, limit)?);
        }
        Ok(Self {
            readers,
            heads: Vec::new(),
            initialized: false,
            limit,
        })
    }

    pub fn next_record(&mut self) -> io::Result<Option<(u64, A)>> {
        if self.readers.len() == 1 {
            return self.readers[0].next_record();
        }
        if !self.initialized {
            self.initialized = true;
            self.heads = Vec::with_capacity(self.readers.len());
            for reader in &mut self.readers {
                self.heads.push(reader.next_record()?);
            }
        }
        let Some(min_key) = self
            .heads
            .iter()
            .filter_map(|h| h.as_ref().map(|(k, _)| *k))
            .min()
        else {
            return Ok(None);
        };
        // Readers run from the oldest level to the newest. Merge each older state with
        // the combined newer ones (`older.merge(newer)`), the grouping a carry-merge of
        // the levels produced, so floating-point sums do not depend on the reader.
        let mut merged: Option<A> = None;
        for (head, reader) in self.heads.iter_mut().zip(&mut self.readers).rev() {
            if head.as_ref().is_some_and(|(k, _)| *k == min_key) {
                let (_, mut acc) = head.take().unwrap();
                *head = reader.next_record()?;
                if let Some(newer) = merged.take() {
                    acc.merge(&newer);
                    if acc.memory_bytes() > self.limit {
                        return Err(invalid("merged accumulator exceeds aggregation budget"));
                    }
                }
                merged = Some(acc);
            }
        }
        Ok(merged.map(|acc| (min_key, acc)))
    }
}

pub struct SpillRuns<A> {
    levels: Vec<Option<Run>>,
    limit: usize,
    directory: Option<std::path::PathBuf>,
    pub runs_written: u64,
    pub bytes_written: u64,
    _acc: PhantomData<A>,
}
impl<A: SpillAccumulator> SpillRuns<A> {
    pub fn new(limit: usize, directory: Option<&Path>) -> Self {
        Self {
            levels: Vec::new(),
            limit,
            directory: directory.map(Path::to_owned),
            runs_written: 0,
            bytes_written: 0,
            _acc: PhantomData,
        }
    }
    pub fn has_spilled(&self) -> bool {
        self.runs_written != 0
    }
    fn file(&self) -> io::Result<File> {
        match &self.directory {
            Some(dir) => tempfile::tempfile_in(dir),
            None => tempfile::tempfile(),
        }
    }
    pub fn push_sorted(&mut self, records: impl IntoIterator<Item = (u64, A)>) -> io::Result<()> {
        let mut out = BufWriter::with_capacity(spill_buf_capacity(self.limit), self.file()?);
        let mut len = 0;
        let mut previous = None;
        for (key, acc) in records {
            if previous.is_some_and(|p| p >= key) {
                return Err(invalid("spill input is not strictly sorted"));
            }
            if acc.memory_bytes() > self.limit {
                return Err(invalid("single accumulator exceeds aggregation budget"));
            }
            write_u64(&mut out, key)?;
            acc.write_state(&mut out)?;
            previous = Some(key);
            len += 1;
        }
        out.flush()?;
        if len == 0 {
            return Ok(());
        }
        let file = out.into_inner().map_err(|e| e.into_error())?;
        self.bytes_written += file.metadata()?.len();
        let mut run = Run { file, len };
        self.runs_written += 1;
        let mut level = 0;
        loop {
            if level == 64 {
                return Err(invalid("spill run counter exhausted"));
            }
            if level == self.levels.len() {
                self.levels.push(None);
            }
            if let Some(other) = self.levels[level].take() {
                run = self.merge(other, run)?;
                level += 1;
            } else {
                self.levels[level] = Some(run);
                break;
            }
        }
        Ok(())
    }
    fn merge(&mut self, left: Run, right: Run) -> io::Result<Run> {
        let mut left = SingleRunReader::<A>::new(left, self.limit)?;
        let mut right = SingleRunReader::<A>::new(right, self.limit)?;
        let mut a = left.next_record()?;
        let mut b = right.next_record()?;
        let mut out = BufWriter::with_capacity(spill_buf_capacity(self.limit), self.file()?);
        let mut len = 0;
        while a.is_some() || b.is_some() {
            let (key, acc) = if b.is_none()
                || a.as_ref().zip(b.as_ref()).is_some_and(|(a, b)| a.0 < b.0)
            {
                let v = a.take().unwrap();
                a = left.next_record()?;
                v
            } else if a.is_none() || a.as_ref().zip(b.as_ref()).is_some_and(|(a, b)| a.0 > b.0) {
                let v = b.take().unwrap();
                b = right.next_record()?;
                v
            } else {
                let (key, mut acc) = a.take().unwrap();
                acc.merge(&b.take().unwrap().1);
                if acc.memory_bytes() > self.limit {
                    return Err(invalid("merged accumulator exceeds aggregation budget"));
                }
                a = left.next_record()?;
                b = right.next_record()?;
                (key, acc)
            };
            write_u64(&mut out, key)?;
            acc.write_state(&mut out)?;
            len += 1;
        }
        out.flush()?;
        let file = out.into_inner().map_err(|e| e.into_error())?;
        self.bytes_written += file.metadata()?.len();
        Ok(Run { file, len })
    }
    pub fn finish(&mut self) -> io::Result<Option<RunReader<A>>> {
        let mut runs = Vec::new();
        for i in (0..self.levels.len()).rev() {
            if let Some(run) = self.levels[i].take() {
                runs.push(run);
            }
        }
        if runs.is_empty() {
            return Ok(None);
        }
        RunReader::from_runs(runs, self.limit).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn binary_codec_preserves_quantile_state_and_nonfinite_statistics() {
        let mut a = ContinuousAccumulator::with_quantiles();
        for value in [-100.0, -1.0, 0.0, 0.1, 12.0, 1000.0] {
            a.update_weighted(value, 0.25);
        }
        let mut bytes = Vec::new();
        a.write_state(&mut bytes).unwrap();
        let b = ContinuousAccumulator::read_state(&mut Cursor::new(bytes), 65536).unwrap();
        assert_eq!(a, b);
        let a = ContinuousAccumulator::default();
        let mut bytes = Vec::new();
        a.write_state(&mut bytes).unwrap();
        assert_eq!(
            a,
            ContinuousAccumulator::read_state(&mut Cursor::new(bytes), 65536).unwrap()
        );
    }

    fn write_f64(out: &mut impl Write, v: f64) -> io::Result<()> {
        write_u64(out, v.to_bits())
    }

    /// A quantile sketch's zero count, total count and positive buckets.
    type Sketch<'a> = Option<(f64, f64, &'a [(i32, f64)])>;

    /// A continuous state as `write_state` lays it out.
    fn continuous_bytes(stats: [f64; 6], sketch: Sketch<'_>) -> Vec<u8> {
        let mut out = Vec::new();
        for v in stats {
            write_f64(&mut out, v).unwrap();
        }
        write_u64(&mut out, u64::from(sketch.is_some())).unwrap();
        if let Some((zero, total, bins)) = sketch {
            write_f64(&mut out, zero).unwrap();
            write_f64(&mut out, total).unwrap();
            write_u64(&mut out, bins.len() as u64).unwrap();
            for &(k, w) in bins {
                out.extend_from_slice(&k.to_le_bytes());
                write_f64(&mut out, w).unwrap();
            }
            write_u64(&mut out, 0).unwrap();
        }
        out
    }

    /// A categorical state as `write_state` lays it out.
    fn categorical_bytes(total: f64, classes: &[(i64, f64)]) -> Vec<u8> {
        let mut out = Vec::new();
        write_f64(&mut out, total).unwrap();
        write_u64(&mut out, classes.len() as u64).unwrap();
        for &(k, w) in classes {
            out.extend_from_slice(&k.to_le_bytes());
            write_f64(&mut out, w).unwrap();
        }
        out
    }

    #[test]
    fn decoder_accepts_states_the_accumulator_can_reach() {
        let read = |b: Vec<u8>| ContinuousAccumulator::read_state(&mut Cursor::new(b), 65536);
        let (nan, inf) = (f64::NAN, f64::INFINITY);
        // [sum, count, sum_w2, min, max, m2]
        // Empty: min and max keep their infinite sentinels.
        read(continuous_bytes([0.0, 0.0, 0.0, inf, -inf, 0.0], None)).unwrap();
        // Extreme inputs can overflow the sum, and then turn m2 into NaN.
        read(continuous_bytes([inf, 2.0, 2.0, 1e308, 1e308, nan], None)).unwrap();
        read(continuous_bytes(
            [3.0, 2.0, 2.0, 1.0, 2.0, 0.5],
            Some((0.0, 2.0, &[(5, 2.0)])),
        ))
        .unwrap();
        CategoricalAccumulator::read_state(
            &mut Cursor::new(categorical_bytes(3.0, &[(1, 2.0), (-7, 1.0)])),
            65536,
        )
        .unwrap();
    }

    #[test]
    fn decoder_rejects_states_the_accumulator_cannot_reach() {
        let (nan, inf) = (f64::NAN, f64::INFINITY);
        let ok = [3.0, 2.0, 2.0, 1.0, 2.0, 0.5];
        let with = |i: usize, v: f64| {
            let mut stats = ok;
            stats[i] = v;
            stats
        };
        let continuous: Vec<(&str, Vec<u8>)> = vec![
            ("NaN count", continuous_bytes(with(1, nan), None)),
            ("negative count", continuous_bytes(with(1, -1.0), None)),
            ("infinite count", continuous_bytes(with(1, inf), None)),
            ("NaN sum_w2", continuous_bytes(with(2, nan), None)),
            ("min > max", continuous_bytes(with(3, 5.0), None)),
            ("NaN min", continuous_bytes(with(3, nan), None)),
            ("negative m2", continuous_bytes(with(5, -1e-9), None)),
            (
                "NaN zero count",
                continuous_bytes(ok, Some((nan, 2.0, &[]))),
            ),
            (
                "negative total",
                continuous_bytes(ok, Some((0.0, -2.0, &[]))),
            ),
            (
                "zero bucket",
                continuous_bytes(ok, Some((0.0, 2.0, &[(5, 0.0)]))),
            ),
            (
                "NaN bucket",
                continuous_bytes(ok, Some((0.0, 2.0, &[(5, nan)]))),
            ),
            (
                "infinite bucket",
                continuous_bytes(ok, Some((0.0, 2.0, &[(5, inf)]))),
            ),
            (
                "duplicate bucket",
                continuous_bytes(ok, Some((0.0, 2.0, &[(5, 1.0), (5, 1.0)]))),
            ),
        ];
        for (case, bytes) in continuous {
            let err =
                ContinuousAccumulator::read_state(&mut Cursor::new(bytes), 65536).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
        }

        let categorical = [
            ("NaN total", categorical_bytes(nan, &[(1, 1.0)])),
            ("infinite total", categorical_bytes(inf, &[(1, 1.0)])),
            ("NaN class weight", categorical_bytes(1.0, &[(1, nan)])),
            ("zero class weight", categorical_bytes(1.0, &[(1, 0.0)])),
            ("infinite class weight", categorical_bytes(1.0, &[(1, inf)])),
            (
                "duplicate class",
                categorical_bytes(2.0, &[(1, 1.0), (1, 1.0)]),
            ),
        ];
        for (case, bytes) in categorical {
            let err =
                CategoricalAccumulator::read_state(&mut Cursor::new(bytes), 65536).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
        }
    }

    #[test]
    fn corrupt_partial_states_are_rejected_on_decode() {
        use super::super::partial::{decode_partial_state, encode_partial_state};
        let mut acc = ContinuousAccumulator::default();
        acc.update(4.0);
        let mut bytes = encode_partial_state(&acc);
        decode_partial_state::<ContinuousAccumulator>(&bytes, 65536).unwrap();
        // The count follows the 4-byte magic, the version, the kind and the 8-byte sum.
        bytes[14..22].copy_from_slice(&f64::NAN.to_bits().to_le_bytes());
        let err = decode_partial_state::<ContinuousAccumulator>(&bytes, 65536).unwrap_err();
        assert!(err.to_string().contains("invalid count"), "{err}");
    }

    #[test]
    fn binary_codec_preserves_categorical_heap() {
        let mut a = CategoricalAccumulator::new();
        for k in -30..30 {
            a.update_weighted(k, 0.25);
        }
        let mut bytes = Vec::new();
        a.write_state(&mut bytes).unwrap();
        let b = CategoricalAccumulator::read_state(&mut Cursor::new(bytes), 65536).unwrap();
        assert_eq!(a.total_count, b.total_count);
        a.for_each_class(|k, v| assert_eq!(v, b.get_class_count(k)));
    }

    #[test]
    fn repeated_runs_merge_late_contributions_once_and_clean_up() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut runs = SpillRuns::<ContinuousAccumulator>::new(65536, Some(dir.path()));
            for n in 1..=37 {
                runs.push_sorted((0..100).map(|k| {
                    let mut acc = ContinuousAccumulator::with_quantiles();
                    acc.update(n as f64);
                    (k, acc)
                }))
                .unwrap();
            }
            assert_eq!(runs.runs_written, 37);
            assert!(runs.levels.len() <= 6);
            let mut reader = runs.finish().unwrap().unwrap();
            for k in 0..100 {
                let (key, acc) = reader.next_record().unwrap().unwrap();
                assert_eq!(key, k);
                assert_eq!(acc.count, 37.0);
                assert_eq!(acc.sum, 703.0);
                assert_eq!(acc.quantiles.unwrap().count(), 37.0);
            }
            assert!(reader.next_record().unwrap().is_none());
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn finishing_three_levels_merges_each_older_level_with_the_newer_ones() {
        // Seven runs fill levels 2 (runs 1-4), 1 (runs 5-6) and 0 (run 7). A carry-merge
        // computes oldest + (middle + newest); grouping oldest-first would lose the 1.
        let mut runs = SpillRuns::<ContinuousAccumulator>::new(65536, None);
        for value in [1.0, 0.0, 0.0, 0.0, 1e16, 0.0, -1e16] {
            runs.push_sorted([(7, ContinuousAccumulator::new(value))])
                .unwrap();
        }
        assert!(runs.levels.iter().take(3).all(Option::is_some));
        let mut reader = runs.finish().unwrap().unwrap();
        let (key, acc) = reader.next_record().unwrap().unwrap();
        assert_eq!(key, 7);
        assert_eq!(acc.count, 7.0);
        assert_eq!(acc.sum, 1.0);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn truncated_runs_are_errors_not_eof() {
        let mut runs = SpillRuns::<ContinuousAccumulator>::new(65536, None);
        runs.push_sorted([(1, ContinuousAccumulator::new(2.0))])
            .unwrap();
        runs.levels[0].as_ref().unwrap().file.set_len(9).unwrap();
        let mut reader = runs.finish().unwrap().unwrap();
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn compact_categorical_states_round_trip_under_a_small_limit() {
        // 40 classes use the 16-byte list tier: well under 2 KiB, but more than the
        // generic 64-bytes-per-entry bound (32 entries) would allow.
        let mut acc = CategoricalAccumulator::new();
        for k in 0..40 {
            acc.update_weighted(k, 1.0 + k as f64);
        }
        assert!(acc.memory_bytes() <= 2048);
        let mut bytes = Vec::new();
        acc.write_state(&mut bytes).unwrap();
        let back = CategoricalAccumulator::read_state(&mut Cursor::new(bytes), 2048).unwrap();
        assert_eq!(back.unique_classes(), 40);
        assert_eq!(back.total_count.to_bits(), acc.total_count.to_bits());
        for k in 0..40 {
            assert_eq!(back.get_class_count(k), acc.get_class_count(k));
        }
        let partial = crate::aggregator::multi_horizon::partial::encode_partial_state(&acc);
        let decoded: CategoricalAccumulator =
            crate::aggregator::multi_horizon::partial::decode_partial_state(&partial, 2048)
                .unwrap();
        assert_eq!(decoded.unique_classes(), 40);
    }

    #[test]
    fn oversized_categorical_state_is_refused_while_decoding() {
        let mut acc = CategoricalAccumulator::new();
        for k in 0..2000 {
            acc.update(k);
        }
        let mut bytes = Vec::new();
        acc.write_state(&mut bytes).unwrap();
        assert!(CategoricalAccumulator::read_state(&mut Cursor::new(bytes), 2048).is_err());
    }

    #[test]
    fn oversized_merged_state_fails_without_discarding_categories() {
        // 40 classes fit the limit; the 80 of the merged state do not.
        let mut runs = SpillRuns::<CategoricalAccumulator>::new(2048, None);
        for offset in [0, 100] {
            let mut acc = CategoricalAccumulator::new();
            for k in offset..offset + 40 {
                acc.update(k);
            }
            // Each input decodes on its own, so the error below comes from the merge.
            let mut bytes = Vec::new();
            acc.write_state(&mut bytes).unwrap();
            CategoricalAccumulator::read_state(&mut Cursor::new(bytes), 2048).unwrap();
            let result = runs.push_sorted([(1, acc)]);
            if offset == 0 {
                result.unwrap();
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn streaming_finish_merges_multiple_levels_and_enforces_limit() {
        let dir = tempfile::tempdir().unwrap();
        {
            // 13 runs occupy binary-carry levels 0, 2, 3 (multiple non-empty levels at finish).
            let mut runs = SpillRuns::<ContinuousAccumulator>::new(65536, Some(dir.path()));
            for n in 1..=13 {
                runs.push_sorted((0..50).map(|k| {
                    let mut acc = ContinuousAccumulator::with_quantiles();
                    acc.update((k * 10 + n) as f64);
                    (k, acc)
                }))
                .unwrap();
            }
            assert!(runs.levels.iter().filter(|l| l.is_some()).count() == 3);
            let mut reader = runs.finish().unwrap().unwrap();
            for k in 0..50u64 {
                let (key, acc) = reader.next_record().unwrap().unwrap();
                assert_eq!(key, k);
                assert_eq!(acc.count, 13.0);
                let expected_sum: f64 = (1..=13).map(|n| (k * 10 + n) as f64).sum();
                assert!((acc.sum - expected_sum).abs() < 1e-12);
                assert_eq!(acc.quantiles.unwrap().count(), 13.0);
            }
            assert!(reader.next_record().unwrap().is_none());
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        // Two levels (3 runs -> level 1 has keys 10, 20; level 0 has key 10) whose
        // combined categorical state on key 10 exceeds the 2048-byte limit on finish.
        let mut cat_limit = SpillRuns::<CategoricalAccumulator>::new(2048, None);
        for (i, offset) in [0i64, 200, 50].into_iter().enumerate() {
            let mut acc = CategoricalAccumulator::new();
            for c in offset..offset + 40 {
                acc.update(c);
            }
            let key = if i == 1 { 20 } else { 10 };
            cat_limit.push_sorted([(key, acc)]).unwrap();
        }
        let mut reader = cat_limit.finish().unwrap().unwrap();
        let err = reader.next_record().unwrap_err();
        assert!(
            err.to_string().contains("exceeds aggregation budget"),
            "{err}"
        );
    }
}
