//! Independent bounded codecs for wire 005 metadata blocks and data frames.

use super::wire::{PackedResult, PackedWireError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PackedCodec {
    Raw = 0,
    Zstd = 1,
}

impl PackedCodec {
    pub fn from_u8(value: u8) -> PackedResult<Self> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Zstd),
            _ => Err(PackedWireError::UnsupportedFormat(
                "unknown packed block codec".into(),
            )),
        }
    }
}

pub fn encode_block(codec: PackedCodec, raw: &[u8], limit: usize) -> PackedResult<Vec<u8>> {
    if raw.len() > limit {
        return Err(PackedWireError::LimitExceeded(
            "packed block raw bytes exceed budget".into(),
        ));
    }
    match codec {
        PackedCodec::Raw => Ok(raw.to_vec()),
        PackedCodec::Zstd => zstd::bulk::compress(raw, 3)
            .map_err(|_| PackedWireError::Invalid("packed zstd encoding failed".into())),
    }
}

pub fn decode_block(
    codec: PackedCodec,
    stored: &[u8],
    raw_len: usize,
    stored_limit: usize,
    raw_limit: usize,
) -> PackedResult<Vec<u8>> {
    if stored.len() > stored_limit || raw_len > raw_limit {
        return Err(PackedWireError::LimitExceeded(
            "packed block bytes exceed budget".into(),
        ));
    }
    match codec {
        PackedCodec::Raw if stored.len() == raw_len => Ok(stored.to_vec()),
        PackedCodec::Raw => Err(PackedWireError::Invalid(
            "raw packed block length mismatch".into(),
        )),
        PackedCodec::Zstd => {
            let frame_len = zstd::zstd_safe::find_frame_compressed_size(stored)
                .map_err(|_| PackedWireError::Invalid("invalid packed zstd frame".into()))?;
            if frame_len != stored.len() {
                return Err(PackedWireError::Invalid(
                    "packed zstd block has trailing frames or bytes".into(),
                ));
            }
            let content_len = zstd::zstd_safe::get_frame_content_size(stored).map_err(|_| {
                PackedWireError::Invalid("invalid packed zstd content length".into())
            })?;
            if content_len.is_some_and(|length| length != raw_len as u64) {
                return Err(PackedWireError::Invalid(
                    "packed zstd content length mismatch".into(),
                ));
            }
            // Static single-shot DCtx cannot call malloc. Its exact workspace
            // requirement comes from the linked zstd build, so a history-window
            // cap is never misrepresented as the decoder's allocation bound.
            use zstd::zstd_safe::zstd_sys;
            let workspace_len = decode_workspace_bytes(codec)?;
            let mut workspace = vec![0u64; workspace_len.div_ceil(8)];
            let decoder = unsafe {
                zstd_sys::ZSTD_initStaticDCtx(workspace.as_mut_ptr().cast(), workspace.len() * 8)
            };
            if decoder.is_null() {
                return Err(PackedWireError::LimitExceeded(
                    "static zstd workspace unavailable".into(),
                ));
            }
            // The destination bound alone does not bound zstd's history window.
            let window_log = usize::BITS - raw_limit.max(1024).saturating_sub(1).leading_zeros();
            let parameter = unsafe {
                zstd_sys::ZSTD_DCtx_setParameter(
                    decoder,
                    zstd_sys::ZSTD_dParameter::ZSTD_d_windowLogMax,
                    window_log as i32,
                )
            };
            if unsafe { zstd_sys::ZSTD_isError(parameter) } != 0 {
                return Err(PackedWireError::Invalid(
                    "packed zstd window limit is invalid".into(),
                ));
            }
            let mut raw = vec![0u8; raw_len];
            let actual = unsafe {
                zstd_sys::ZSTD_decompressDCtx(
                    decoder,
                    raw.as_mut_ptr().cast(),
                    raw.len(),
                    stored.as_ptr().cast(),
                    stored.len(),
                )
            };
            if unsafe { zstd_sys::ZSTD_isError(actual) } != 0 {
                return Err(PackedWireError::Invalid(
                    "packed zstd block decode failed".into(),
                ));
            }
            if actual != raw_len {
                return Err(PackedWireError::Invalid(
                    "decoded packed block length mismatch".into(),
                ));
            }
            Ok(raw)
        }
    }
}

pub fn decode_workspace_bytes(codec: PackedCodec) -> PackedResult<usize> {
    match codec {
        PackedCodec::Raw => Ok(0),
        PackedCodec::Zstd => {
            let size = unsafe { zstd::zstd_safe::zstd_sys::ZSTD_estimateDCtxSize() };
            if size == 0 || size > 8 * 1024 * 1024 {
                return Err(PackedWireError::LimitExceeded(
                    "zstd static workspace bound is invalid".into(),
                ));
            }
            Ok(size.div_ceil(8) * 8)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_codecs_roundtrip() {
        let raw = vec![0x55; 65536];
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let stored = encode_block(codec, &raw, raw.len()).unwrap();
            assert_eq!(
                decode_block(codec, &stored, raw.len(), 131072, 65536).unwrap(),
                raw
            );
        }
    }

    #[test]
    fn decoder_rejects_zstd_tail_concat_and_wrong_raw_length() {
        let first = encode_block(PackedCodec::Zstd, b"first", 1024).unwrap();
        let second = encode_block(PackedCodec::Zstd, b"second", 1024).unwrap();
        for tail in [b"garbage".to_vec(), second] {
            let mut stored = first.clone();
            stored.extend(tail);
            assert!(matches!(
                decode_block(PackedCodec::Zstd, &stored, 5, 1024, 1024),
                Err(PackedWireError::Invalid(_))
            ));
        }
        assert!(matches!(
            decode_block(PackedCodec::Zstd, &first, 6, 1024, 1024),
            Err(PackedWireError::Invalid(_))
        ));
        assert!(matches!(
            decode_block(PackedCodec::Zstd, &first[..first.len() - 1], 5, 1024, 1024),
            Err(PackedWireError::Invalid(_))
        ));
    }

    #[test]
    fn codec_limits_apply_before_allocation() {
        assert!(matches!(
            encode_block(PackedCodec::Raw, &[0; 2], 1),
            Err(PackedWireError::LimitExceeded(_))
        ));
        assert!(matches!(
            decode_block(PackedCodec::Zstd, &[], usize::MAX, 1024, 1024),
            Err(PackedWireError::LimitExceeded(_))
        ));
        assert!(matches!(
            decode_block(PackedCodec::Raw, &[0; 2], 2, 1, 2),
            Err(PackedWireError::LimitExceeded(_))
        ));
        assert!(PackedCodec::from_u8(2).is_err());
    }
}
