//! Synthetic GeoTIFF builders, in-memory remote transport, and local HTTP range server
//! shared by the `streaming` Criterion benchmarks.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dggs_stream_core::error::{DggsStreamError, Result};
use dggs_stream_core::raster::http_range::{
    ProbeResult, RemoteHttpSource, RemoteSourceFactory, RemoteTransport,
};
use tempfile::NamedTempFile;
use weezl::encode::Encoder as LzwEncoder;
use weezl::BitOrder;

#[derive(Clone, Copy)]
pub enum TiledCodec {
    UncompressedF32,
    DeflateF32 { fp_predictor: bool },
    LzwU16Horiz,
}

fn push_aligned_bytes(meta: &mut Vec<u8>, bytes: &[u8]) -> u32 {
    if !meta.len().is_multiple_of(2) {
        meta.push(0);
    }
    let at = 8 + meta.len();
    meta.extend_from_slice(bytes);
    at as u32
}

fn push_u16s(meta: &mut Vec<u8>, values: &[u16]) -> u32 {
    let mut bytes = Vec::with_capacity(values.len() * 2);
    for &v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    push_aligned_bytes(meta, &bytes)
}

fn push_u32s(meta: &mut Vec<u8>, values: &[u32]) -> u32 {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for &v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    push_aligned_bytes(meta, &bytes)
}

fn push_f64s(meta: &mut Vec<u8>, values: &[f64]) -> u32 {
    let mut bytes = Vec::with_capacity(values.len() * 8);
    for &v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    push_aligned_bytes(meta, &bytes)
}

fn short_tag(meta: &mut Vec<u8>, tag: u16, values: &[u16]) -> (u16, u16, u32, u32) {
    let count = values.len() as u32;
    let value_or_offset = match values {
        [v0] => u32::from(*v0),
        [v0, v1] => u32::from(*v0) | (u32::from(*v1) << 16),
        _ => push_u16s(meta, values),
    };
    (tag, 3, count, value_or_offset)
}

fn long_tag(meta: &mut Vec<u8>, tag: u16, values: &[u32]) -> (u16, u16, u32, u32) {
    let count = values.len() as u32;
    let value_or_offset = match values {
        [v0] => *v0,
        _ => push_u32s(meta, values),
    };
    (tag, 4, count, value_or_offset)
}

struct TiffSpec {
    width: u32,
    height: u32,
    tile_w: u32,
    tile_h: u32,
    bands: u16,
    bits_per_sample: u16,
    sample_format: u16,
    compression: u16,
    predictor: u16,
    planar: bool,
}

fn assemble_tiled_tiff(spec: &TiffSpec, chunks: &[Vec<u8>], tile_data_start: usize) -> Vec<u8> {
    let mut buf = vec![0u8; tile_data_start];
    buf[..8].copy_from_slice(b"II\x2a\x00\0\0\0\0");

    let mut offsets = Vec::with_capacity(chunks.len());
    let mut counts = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        offsets.push(buf.len() as u32);
        counts.push(chunk.len() as u32);
        buf.extend_from_slice(chunk);
    }

    let mut meta = Vec::with_capacity(1024);
    let bits_vec = vec![spec.bits_per_sample; usize::from(spec.bands)];
    let fmt_vec = vec![spec.sample_format; usize::from(spec.bands)];
    let scale_off = push_f64s(&mut meta, &[0.001, 0.001, 0.0]);
    let tie_off = push_f64s(&mut meta, &[0.0, 0.0, 0.0, -122.45, 37.80, 0.0]);
    let geokeys_off = push_u16s(&mut meta, &[1, 1, 0, 2, 1024, 0, 1, 2, 2048, 0, 1, 4326]);

    let mut entries = vec![
        long_tag(&mut meta, 256, &[spec.width]),
        long_tag(&mut meta, 257, &[spec.height]),
        short_tag(&mut meta, 258, &bits_vec),
        short_tag(&mut meta, 259, &[spec.compression]),
        short_tag(&mut meta, 262, &[1]), // BlackIsZero
        short_tag(&mut meta, 277, &[spec.bands]),
        short_tag(&mut meta, 284, &[if spec.planar { 2 } else { 1 }]),
        short_tag(&mut meta, 317, &[spec.predictor]),
        long_tag(&mut meta, 322, &[spec.tile_w]),
        long_tag(&mut meta, 323, &[spec.tile_h]),
        long_tag(&mut meta, 324, &offsets),
        long_tag(&mut meta, 325, &counts),
    ];
    if spec.bands > 1 {
        let extra = vec![0u16; usize::from(spec.bands - 1)];
        entries.push(short_tag(&mut meta, 338, &extra));
    }
    entries.push(short_tag(&mut meta, 339, &fmt_vec));
    entries.push((33550, 12, 3, scale_off));
    entries.push((33922, 12, 6, tie_off));
    entries.push((34735, 3, 12, geokeys_off));
    entries.sort_by_key(|e| e.0);

    if !meta.len().is_multiple_of(2) {
        meta.push(0);
    }
    let ifd_offset = (8 + meta.len()) as u32;
    meta.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    for (tag, ty, count, value) in entries {
        meta.extend_from_slice(&tag.to_le_bytes());
        meta.extend_from_slice(&ty.to_le_bytes());
        meta.extend_from_slice(&count.to_le_bytes());
        meta.extend_from_slice(&value.to_le_bytes());
    }
    meta.extend_from_slice(&0u32.to_le_bytes());

    assert!(
        8 + meta.len() <= tile_data_start,
        "IFD metadata ({}) exceeds tile_data_start ({tile_data_start})",
        8 + meta.len()
    );
    buf[4..8].copy_from_slice(&ifd_offset.to_le_bytes());
    buf[8..8 + meta.len()].copy_from_slice(&meta);
    buf
}

fn encode_f32_tile_bytes(
    width: u32,
    height: u32,
    x0: u32,
    y0: u32,
    tile_w: u32,
    tile_h: u32,
    fp_predictor: bool,
) -> Vec<u8> {
    let w = tile_w as usize;
    let h = tile_h as usize;
    let mut raw = vec![0u8; w * h * 4];
    for r in 0..h {
        for c in 0..w {
            let x = x0 + c as u32;
            let y = y0 + r as u32;
            let val = if x < width && y < height {
                ((x * 7 + y * 11) % 4096) as f32
            } else {
                0.0f32
            };
            let off = (r * w + c) * 4;
            raw[off..off + 4].copy_from_slice(&val.to_le_bytes());
        }
    }
    if !fp_predictor {
        return raw;
    }
    let mut pred = vec![0u8; raw.len()];
    for r in 0..h {
        let row_in = &raw[r * w * 4..(r + 1) * w * 4];
        let row_out = &mut pred[r * w * 4..(r + 1) * w * 4];
        for i in 0..w {
            let be = f32::from_le_bytes([
                row_in[i * 4],
                row_in[i * 4 + 1],
                row_in[i * 4 + 2],
                row_in[i * 4 + 3],
            ])
            .to_bits()
            .to_be_bytes();
            row_out[i] = be[0];
            row_out[w + i] = be[1];
            row_out[2 * w + i] = be[2];
            row_out[3 * w + i] = be[3];
        }
        for i in (1..row_out.len()).rev() {
            row_out[i] = row_out[i].wrapping_sub(row_out[i - 1]);
        }
    }
    pred
}

pub fn write_tiled_codec_bytes(
    width: u32,
    height: u32,
    tile_w: u32,
    tile_h: u32,
    codec: TiledCodec,
    tile_data_start: usize,
) -> Vec<u8> {
    let across = width.div_ceil(tile_w);
    let down = height.div_ceil(tile_h);
    let total = across * down;
    let mut chunks = Vec::with_capacity(total as usize);
    let mut compressor = libdeflater::Compressor::new(libdeflater::CompressionLvl::default());

    for idx in 0..total {
        let x0 = (idx % across) * tile_w;
        let y0 = (idx / across) * tile_h;
        let chunk = match codec {
            TiledCodec::UncompressedF32 => {
                encode_f32_tile_bytes(width, height, x0, y0, tile_w, tile_h, false)
            }
            TiledCodec::DeflateF32 { fp_predictor } => {
                let raw =
                    encode_f32_tile_bytes(width, height, x0, y0, tile_w, tile_h, fp_predictor);
                let mut comp = vec![0u8; compressor.zlib_compress_bound(raw.len())];
                let len = compressor.zlib_compress(&raw, &mut comp).unwrap();
                comp.truncate(len);
                comp
            }
            TiledCodec::LzwU16Horiz => {
                let w = tile_w as usize;
                let h = tile_h as usize;
                let mut samples = Vec::with_capacity(w * h);
                for r in 0..h {
                    for c in 0..w {
                        let x = x0 + c as u32;
                        let y = y0 + r as u32;
                        let val = if x < width && y < height {
                            ((x * 7 + y * 11) % 4096) as u16
                        } else {
                            0u16
                        };
                        samples.push(val);
                    }
                }
                for r in 0..h {
                    let row = &mut samples[r * w..(r + 1) * w];
                    for c in (1..w).rev() {
                        row[c] = row[c].wrapping_sub(row[c - 1]);
                    }
                }
                let mut raw = Vec::with_capacity(samples.len() * 2);
                for s in samples {
                    raw.extend_from_slice(&s.to_le_bytes());
                }
                let mut encoder = LzwEncoder::with_tiff_size_switch(BitOrder::Msb, 8);
                encoder.encode(&raw).unwrap()
            }
        };
        chunks.push(chunk);
    }

    let (bits_per_sample, sample_format, compression, predictor) = match codec {
        TiledCodec::UncompressedF32 => (32, 3, 1, 1),
        TiledCodec::DeflateF32 { fp_predictor } => (32, 3, 8, if fp_predictor { 3 } else { 1 }),
        TiledCodec::LzwU16Horiz => (16, 1, 5, 2),
    };
    let spec = TiffSpec {
        width,
        height,
        tile_w,
        tile_h,
        bands: 1,
        bits_per_sample,
        sample_format,
        compression,
        predictor,
        planar: false,
    };
    assemble_tiled_tiff(&spec, &chunks, tile_data_start)
}

pub fn write_tiled_codec_tempfile(
    width: u32,
    height: u32,
    tile_w: u32,
    tile_h: u32,
    codec: TiledCodec,
) -> (NamedTempFile, PathBuf) {
    let bytes = write_tiled_codec_bytes(width, height, tile_w, tile_h, codec, 4096);
    write_bytes_tempfile(&bytes)
}

pub fn write_tiled_multiband_u16_bytes(
    width: u32,
    height: u32,
    tile_w: u32,
    tile_h: u32,
    bands: u16,
    planar: bool,
    tile_data_start: usize,
) -> Vec<u8> {
    let across = width.div_ceil(tile_w);
    let down = height.div_ceil(tile_h);
    let tiles_per_plane = across * down;
    let w = tile_w as usize;
    let h = tile_h as usize;

    let sample = |x: u32, y: u32, b: u16| -> u16 {
        if x < width && y < height {
            ((x * 7 + y * 11 + u32::from(b) * 53) % 4096) as u16
        } else {
            0
        }
    };

    let chunks = if !planar {
        let mut out = Vec::with_capacity(tiles_per_plane as usize);
        for idx in 0..tiles_per_plane {
            let x0 = (idx % across) * tile_w;
            let y0 = (idx / across) * tile_h;
            let mut raw = Vec::with_capacity(w * h * usize::from(bands) * 2);
            for r in 0..h {
                for c in 0..w {
                    for b in 0..bands {
                        raw.extend_from_slice(
                            &sample(x0 + c as u32, y0 + r as u32, b).to_le_bytes(),
                        );
                    }
                }
            }
            out.push(raw);
        }
        out
    } else {
        let mut out = Vec::with_capacity(usize::from(bands) * tiles_per_plane as usize);
        for b in 0..bands {
            for idx in 0..tiles_per_plane {
                let x0 = (idx % across) * tile_w;
                let y0 = (idx / across) * tile_h;
                let mut raw = Vec::with_capacity(w * h * 2);
                for r in 0..h {
                    for c in 0..w {
                        raw.extend_from_slice(
                            &sample(x0 + c as u32, y0 + r as u32, b).to_le_bytes(),
                        );
                    }
                }
                out.push(raw);
            }
        }
        out
    };

    let spec = TiffSpec {
        width,
        height,
        tile_w,
        tile_h,
        bands,
        bits_per_sample: 16,
        sample_format: 1,
        compression: 1,
        predictor: 1,
        planar,
    };
    assemble_tiled_tiff(&spec, &chunks, tile_data_start)
}

pub fn write_tiled_multiband_u16_tempfile(
    width: u32,
    height: u32,
    tile_w: u32,
    tile_h: u32,
    bands: u16,
    planar: bool,
) -> (NamedTempFile, PathBuf) {
    let bytes = write_tiled_multiband_u16_bytes(width, height, tile_w, tile_h, bands, planar, 4096);
    write_bytes_tempfile(&bytes)
}

fn write_bytes_tempfile(bytes: &[u8]) -> (NamedTempFile, PathBuf) {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
    let path = file.path().to_path_buf();
    (file, path)
}

struct MemoryRangeTransport {
    data: Arc<Vec<u8>>,
    requests: Arc<AtomicUsize>,
}

impl RemoteTransport for MemoryRangeTransport {
    fn fetch_range(&self, start: u64, end: u64, expected_total: Option<u64>) -> Result<Vec<u8>> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let total = self.data.len() as u64;
        if let Some(expected) = expected_total {
            if expected != total {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "unexpected total size {expected} != {total}"
                )));
            }
        }
        let start = start as usize;
        let end = end as usize;
        if start > end || end >= self.data.len() {
            return Err(DggsStreamError::InvalidParameter(format!(
                "range {start}-{end} out of bounds for {}",
                self.data.len()
            )));
        }
        Ok(self.data[start..=end].to_vec())
    }

    fn probe(&self, initial_fetch_len: usize) -> Result<ProbeResult> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let total_size = self.data.len() as u64;
        if initial_fetch_len >= self.data.len() {
            Ok(ProbeResult::Complete {
                data: (*self.data).clone(),
                total_size,
            })
        } else {
            Ok(ProbeResult::Partial {
                data: self.data[..initial_fetch_len].to_vec(),
                total_size,
            })
        }
    }
}

pub fn memory_source_factory(
    data: Arc<Vec<u8>>,
    requests: Arc<AtomicUsize>,
) -> RemoteSourceFactory {
    RemoteSourceFactory::new(move |path| {
        Ok(Arc::new(RemoteHttpSource::from_transport(
            path.to_string(),
            Box::new(MemoryRangeTransport {
                data: Arc::clone(&data),
                requests: Arc::clone(&requests),
            }),
        )?))
    })
}

pub struct BenchHttpServer {
    pub url: String,
    pub requests: Arc<AtomicUsize>,
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl BenchHttpServer {
    pub fn try_start(file_bytes: Vec<u8>) -> Option<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let probe = TcpStream::connect_timeout(&addr, Duration::from_millis(50)).ok()?;
        let (accepted_probe, _) = listener.accept().ok()?;
        drop(probe);
        drop(accepted_probe);

        let file = Arc::new(file_bytes);
        let requests = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = {
            let file = Arc::clone(&file);
            let requests = Arc::clone(&requests);
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                while let Ok((stream, _)) = listener.accept() {
                    if shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    let file = Arc::clone(&file);
                    let requests = Arc::clone(&requests);
                    thread::spawn(move || serve_connection(stream, &file, &requests));
                }
            })
        };

        Some(Self {
            url: format!("http://{addr}/raster.tif"),
            requests,
            addr,
            shutdown,
            handle: Some(handle),
        })
    }
}

impl Drop for BenchHttpServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(100));
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve_connection(stream: TcpStream, file: &[u8], requests: &AtomicUsize) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new(&stream);

    loop {
        let mut request_line = String::new();
        match reader.read_line(&mut request_line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if request_line.trim().is_empty() {
            continue;
        }
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("");
        let _path = parts.next().unwrap_or("");
        let version = parts.next().unwrap_or("HTTP/1.1");

        let mut range: Option<(usize, usize)> = None;
        let mut connection_close = version.eq_ignore_ascii_case("HTTP/1.0");
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() || line.trim().is_empty() {
                break;
            }
            let lower = line.to_ascii_lowercase();
            if let Some(val) = lower
                .strip_prefix("range:")
                .and_then(|v| v.trim().strip_prefix("bytes="))
            {
                if let Some((a, b)) = val.split_once('-') {
                    if let (Ok(start), Ok(end)) =
                        (a.trim().parse::<usize>(), b.trim().parse::<usize>())
                    {
                        if !file.is_empty() {
                            range = Some((start, end.min(file.len() - 1)));
                        }
                    }
                }
            } else if let Some(val) = lower.strip_prefix("connection:") {
                let val = val.trim();
                if val == "close" {
                    connection_close = true;
                } else if val == "keep-alive" {
                    connection_close = false;
                }
            }
        }

        requests.fetch_add(1, Ordering::SeqCst);
        let conn_hdr = if connection_close {
            "close"
        } else {
            "keep-alive"
        };
        let mut w: &TcpStream = reader.get_ref();
        let ok = match range {
            Some((start, end)) if start <= end && end < file.len() => {
                let slice = &file[start..=end];
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nETag: \"bench-v1\"\r\nConnection: {conn_hdr}\r\n\r\n",
                    file.len(),
                    slice.len(),
                );
                w.write_all(head.as_bytes()).is_ok()
                    && (method == "HEAD" || w.write_all(slice).is_ok())
            }
            Some(_) => {
                let head = format!(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\nConnection: {conn_hdr}\r\n\r\n",
                    file.len(),
                );
                w.write_all(head.as_bytes()).is_ok()
            }
            None => {
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"bench-v1\"\r\nConnection: {conn_hdr}\r\n\r\n",
                    file.len(),
                );
                w.write_all(head.as_bytes()).is_ok()
                    && (method == "HEAD" || w.write_all(file).is_ok())
            }
        };
        if !ok || connection_close {
            break;
        }
    }
}
