//! RGB sample semantics for ordinary full-range JPEG/YCbCr imagery.
use crate::error::{DggsStreamError, Result};
use std::io::{Read, Seek};
use tiff::decoder::{ifd::Value, Decoder, DecodingResult};
use tiff::tags::Tag;

// TIFF's generic JPEG decoder upsamples chroma but deliberately retains the
// JPEG colour space. IJG's full-range BT.601 transform uses 16 fractional bits.
pub(super) fn to_rgb(samples: &mut DecodingResult) -> Result<()> {
    let DecodingResult::U8(bytes) = samples else {
        return Err(DggsStreamError::InvalidMetadata(
            "expected 8-bit JPEG/YCbCr samples".into(),
        ));
    };
    let (pixels, remainder) = bytes.as_chunks_mut::<3>();
    if !remainder.is_empty() {
        return Err(DggsStreamError::InvalidMetadata(
            "incomplete JPEG/YCbCr pixel".into(),
        ));
    }
    for pixel in pixels {
        let y = i32::from(pixel[0]);
        let cb = i32::from(pixel[1]) - 128;
        let cr = i32::from(pixel[2]) - 128;
        let channel = |delta: i32| (y + ((delta + 32768) >> 16)).clamp(0, 255) as u8;
        pixel[0] = channel(91881 * cr);
        pixel[1] = channel(-22554 * cb - 46802 * cr);
        pixel[2] = channel(116130 * cb);
    }
    Ok(())
}

// Explicit nonstandard coefficients or black/white levels require a different transform.
pub(super) fn validate_color_tags<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<()> {
    let positioning = decoder
        .find_tag_unsigned::<u16>(Tag::ChromaPositioning)?
        .unwrap_or(1);
    let subsampling = decoder.find_tag_unsigned_vec::<u16>(Tag::ChromaSubsampling)?;
    if positioning != 1 && !(positioning == 2 && subsampling.as_deref() == Some(&[1, 1])) {
        return Err(DggsStreamError::InvalidMetadata(
            "unsupported JPEG/YCbCr chroma positioning for subsampled data".into(),
        ));
    }
    for (tag, expected, scale) in [
        (Tag::Unknown(529), &[299u32, 587, 114][..], 1000u64),
        (Tag::Unknown(532), &[0, 255, 128, 255, 128, 255][..], 1),
    ] {
        let Some(value) = decoder.find_tag(tag)? else {
            continue;
        };
        let standard = match value {
            Value::List(values) if values.len() == expected.len() => {
                values.iter().zip(expected).all(|(v, &e)| match v {
                    Value::Rational(n, d) => {
                        // libtiff writes coefficients through single-precision floats.
                        if *d == 0 {
                            return false;
                        }
                        if tag == Tag::Unknown(529) {
                            (f64::from(*n) / f64::from(*d)) as f32 == e as f32 / scale as f32
                        } else {
                            u64::from(*n) * scale == u64::from(e) * u64::from(*d)
                        }
                    }
                    _ => false,
                })
            }
            _ => false,
        };
        if !standard {
            return Err(DggsStreamError::InvalidMetadata(format!(
                "unsupported JPEG/YCbCr color parameters in TIFF tag {tag:?}"
            )));
        }
    }
    Ok(())
}
