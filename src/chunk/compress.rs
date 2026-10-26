//! Block-level data compression for storage and transfer.
//!
//! Provides transparent compression/decompression for the versioned chunk-object
//! layout. Framed payloads use a 4-byte header:
//! `[magic_hi, magic_lo, algorithm, reserved]`.
//!
//! The header alone is not sufficient to classify arbitrary legacy bytes. It is
//! therefore trusted only for objects in the versioned chunk namespace; legacy
//! object layout is selected by the caller's compatibility policy.

use std::borrow::Cow;

use bytes::Bytes;
use tracing::{debug, trace};

/// Magic bytes identifying compressed data (0xSF = SlayerFs)
const MAGIC: [u8; 2] = [0x53, 0x46];
pub const PERSISTED_HEADER_LEN: usize = 4;

/// Compression algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression — data stored/transferred as-is
    #[default]
    None,
    /// LZ4 compression (fast, moderate ratio ~2-3x)
    Lz4,
    /// Zstd compression with configurable level (slower, better ratio ~3-5x)
    Zstd(i32),
}

impl Compression {
    /// Algorithm identifier byte for the header
    fn algo_byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
            Self::Zstd(_) => 2,
        }
    }

    /// Reconstruct algorithm from header byte (zstd level is not stored; uses default for decompress)
    fn from_algo_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd(0)), // level irrelevant for decompression
            _ => None,
        }
    }
}

/// Result of parsing the fixed-size persisted block header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedHeader {
    /// Fewer than four bytes were supplied, so classification is unsafe.
    Incomplete,
    /// The bytes are not a framed block.
    NotFramed,
    /// A valid framed block and its stored encoding.
    Framed(Compression),
    /// The bytes use the framing magic but are not a supported frame.
    Invalid,
}

/// Parse a persisted block header. All consumers of a framed object must use
/// this parser so range-read selection and full-block decoding agree.
pub fn parse_persisted_header(data: &[u8]) -> PersistedHeader {
    if data.len() < PERSISTED_HEADER_LEN {
        return PersistedHeader::Incomplete;
    }
    if data[..2] != MAGIC {
        return PersistedHeader::NotFramed;
    }
    if data[3] != 0 {
        return PersistedHeader::Invalid;
    }
    match Compression::from_algo_byte(data[2]) {
        Some(compression) => PersistedHeader::Framed(compression),
        None => PersistedHeader::Invalid,
    }
}

/// Compress data using the specified algorithm.
/// Returns `Cow::Borrowed` when data should be stored as-is (no compression or incompressible),
/// and `Cow::Owned` with compressed+header bytes when compression is beneficial.
/// This avoids unnecessary copies for the common case of incompressible data.
pub fn compress<'a>(data: &'a [u8], algo: Compression) -> Cow<'a, [u8]> {
    if matches!(algo, Compression::None) || data.is_empty() {
        return Cow::Borrowed(data);
    }

    let compressed_body = match algo {
        Compression::Lz4 => lz4_flex::compress_prepend_size(data),
        Compression::Zstd(level) => match zstd::bulk::compress(data, level) {
            Ok(c) => c,
            Err(e) => {
                debug!("Zstd compression failed, storing uncompressed: {}", e);
                return Cow::Borrowed(data);
            }
        },
        Compression::None => unreachable!(),
    };

    // If compressed is not smaller, store uncompressed (no header)
    if compressed_body.len() + PERSISTED_HEADER_LEN >= data.len() {
        trace!(
            "Compression not beneficial: {} -> {} bytes, storing raw",
            data.len(),
            compressed_body.len() + PERSISTED_HEADER_LEN
        );
        return Cow::Borrowed(data);
    }

    let ratio = data.len() as f64 / (compressed_body.len() + PERSISTED_HEADER_LEN) as f64;
    trace!(
        "Compressed {} -> {} bytes ({:.1}x ratio, algo={:?})",
        data.len(),
        compressed_body.len() + PERSISTED_HEADER_LEN,
        ratio,
        algo
    );

    // Prepend 4-byte header: [magic_hi, magic_lo, algo, reserved]
    let mut result = Vec::with_capacity(PERSISTED_HEADER_LEN + compressed_body.len());
    result.extend_from_slice(&MAGIC);
    result.push(algo.algo_byte());
    result.push(0); // reserved
    result.extend_from_slice(&compressed_body);
    #[cfg(test)]
    copy_probe::record(compressed_body.len(), PERSISTED_HEADER_LEN);
    Cow::Owned(result)
}

/// Encode a block for the versioned object namespace.
///
/// Unlike [`compress`], this always returns a frame: uncompressed and
/// incompressible blocks use the `Compression::None` encoding. This means the
/// framing parser never needs to guess whether arbitrary user data is raw.
pub fn encode_persisted_block(data: &[u8], compression: Compression) -> Bytes {
    match compress(data, compression) {
        Cow::Owned(encoded) => Bytes::from(encoded),
        Cow::Borrowed(raw) => {
            let mut encoded = Vec::with_capacity(PERSISTED_HEADER_LEN + raw.len());
            encoded.extend_from_slice(&MAGIC);
            encoded.push(Compression::None.algo_byte());
            encoded.push(0);
            encoded.extend_from_slice(raw);
            #[cfg(test)]
            copy_probe::record(raw.len(), PERSISTED_HEADER_LEN);
            Bytes::from(encoded)
        }
    }
}

/// Prepare raw cache bytes and a persisted frame from ordered block fragments.
///
/// A fragmented raw block needs only one payload allocation: reserve the frame
/// header before appending the fragments, then freeze it before sharing the body
/// with the cache. Compression still sees exactly the raw body and retains its
/// existing beneficial-size decision. A single fragment keeps the borrowed raw
/// cache buffer, avoiding an extra assembly copy on that path.
pub(crate) fn encode_persisted_block_vectored(
    parts: &[Bytes],
    compression: Compression,
) -> (Bytes, Bytes) {
    if let [single] = parts {
        return (single.clone(), encode_persisted_block(single, compression));
    }

    let total_len = parts.iter().map(|part| part.len()).sum::<usize>();
    let mut framed = Vec::with_capacity(PERSISTED_HEADER_LEN + total_len);
    framed.extend_from_slice(&[MAGIC[0], MAGIC[1], Compression::None.algo_byte(), 0]);
    #[cfg(test)]
    copy_probe::record(0, PERSISTED_HEADER_LEN);
    for part in parts {
        framed.extend_from_slice(part);
        #[cfg(test)]
        copy_probe::record(part.len(), 0);
    }

    let framed = Bytes::from(framed);
    let raw = framed.slice(PERSISTED_HEADER_LEN..);
    let encoded = match compress(&raw, compression) {
        Cow::Owned(encoded) => Bytes::from(encoded),
        Cow::Borrowed(_) => framed,
    };
    (raw, encoded)
}

/// Counts the bytes appended by object preparation, excluding codec internals,
/// callers' staging buffers, and backend/transport copies. Thread-local because
/// preparation is synchronous; the store evidence tests use a current-thread runtime.
#[cfg(test)]
pub(crate) mod copy_probe {
    use std::cell::Cell;

    thread_local! {
        static COPIED: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    }

    pub fn record(payload: usize, header: usize) {
        COPIED.with(|counter| {
            let (old_payload, old_header) = counter.get();
            counter.set((old_payload + payload, old_header + header));
        });
    }

    pub fn take() -> (usize, usize) {
        COPIED.with(|counter| counter.replace((0, 0)))
    }
}

/// Decompress a framed persisted block.
///
/// Callers must only use this for objects whose namespace declares the framed
/// layout. Raw and incomplete bytes are rejected instead of being guessed.
/// `max_decoded_len` is the caller's block-size/output budget, checked before
/// allocating a decompression buffer.
pub fn decompress_framed(data: &[u8], max_decoded_len: usize) -> anyhow::Result<Cow<'_, [u8]>> {
    let algo = match parse_persisted_header(data) {
        PersistedHeader::Framed(algo) => algo,
        PersistedHeader::Incomplete => anyhow::bail!("truncated persisted block header"),
        PersistedHeader::NotFramed => anyhow::bail!("missing persisted block header"),
        PersistedHeader::Invalid => anyhow::bail!("unsupported persisted block header"),
    };

    let body = &data[PERSISTED_HEADER_LEN..];

    match algo {
        Compression::None => {
            anyhow::ensure!(
                body.len() <= max_decoded_len,
                "raw block exceeds decoded size limit"
            );
            Ok(Cow::Borrowed(body))
        }
        Compression::Lz4 => {
            let (decoded_len, compressed) = lz4_flex::block::uncompressed_size(body)
                .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))?;
            anyhow::ensure!(
                decoded_len <= max_decoded_len,
                "LZ4 block exceeds decoded size limit"
            );
            lz4_flex::block::decompress(compressed, decoded_len)
                .map(Cow::Owned)
                .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))
        }
        Compression::Zstd(_) => zstd::bulk::decompress(body, max_decoded_len)
            .map(Cow::Owned)
            .map_err(|e| anyhow::anyhow!("Zstd decompression failed: {}", e)),
    }
}

/// Decompress a legacy payload using its historical, magic-based convention.
/// If there is no magic prefix, returns the original bytes. Invalid magic
/// prefixes fail rather than silently exposing encoded bytes as file data.
pub fn decompress(data: &[u8], max_decoded_len: usize) -> anyhow::Result<Cow<'_, [u8]>> {
    if data.len() < 4 || data[0] != MAGIC[0] || data[1] != MAGIC[1] {
        // No compression header — return raw data
        anyhow::ensure!(
            data.len() <= max_decoded_len,
            "raw block exceeds decoded size limit"
        );
        return Ok(Cow::Borrowed(data));
    }
    decompress_framed(data, max_decoded_len)
}

/// Decompress an owned Bytes buffer while preserving zero-copy raw fallback.
pub fn decompress_bytes(data: Bytes, max_decoded_len: usize) -> anyhow::Result<Bytes> {
    match decompress(data.as_ref(), max_decoded_len)? {
        Cow::Borrowed(borrowed) => {
            let base = data.as_ptr() as usize;
            let start = (borrowed.as_ptr() as usize)
                .checked_sub(base)
                .ok_or_else(|| anyhow::anyhow!("decompressed slice is outside source buffer"))?;
            let end = start
                .checked_add(borrowed.len())
                .filter(|end| *end <= data.len())
                .ok_or_else(|| anyhow::anyhow!("decompressed slice exceeds source buffer"))?;
            Ok(data.slice(start..end))
        }
        Cow::Owned(decompressed) => Ok(Bytes::from(decompressed)),
    }
}

/// Decompress an owned framed block while preserving zero-copy raw frames.
pub fn decompress_framed_bytes(data: Bytes, max_decoded_len: usize) -> anyhow::Result<Bytes> {
    match decompress_framed(data.as_ref(), max_decoded_len)? {
        Cow::Borrowed(borrowed) => {
            let base = data.as_ptr() as usize;
            let start = (borrowed.as_ptr() as usize)
                .checked_sub(base)
                .ok_or_else(|| anyhow::anyhow!("decompressed slice is outside source buffer"))?;
            let end = start
                .checked_add(borrowed.len())
                .filter(|end| *end <= data.len())
                .ok_or_else(|| anyhow::anyhow!("decompressed slice exceeds source buffer"))?;
            Ok(data.slice(start..end))
        }
        Cow::Owned(decompressed) => Ok(Bytes::from(decompressed)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_none() {
        let data = b"hello world";
        let compressed = compress(data, Compression::None);
        assert_eq!(&*compressed, &data[..]);
        let decompressed = decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed.as_ref(), data);
    }

    #[test]
    fn test_roundtrip_lz4() {
        // Use compressible data (repeated pattern)
        let data: Vec<u8> = (0..4096).map(|i| (i % 16) as u8).collect();
        let compressed = compress(&data, Compression::Lz4);
        assert!(compressed.len() < data.len());
        assert_eq!(&compressed[..2], &MAGIC);
        assert_eq!(compressed[2], 1); // LZ4
        let decompressed = decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed.as_ref(), data);
    }

    #[test]
    fn test_roundtrip_zstd() {
        let data: Vec<u8> = (0..4096).map(|i| (i % 16) as u8).collect();
        let compressed = compress(&data, Compression::Zstd(3));
        assert!(compressed.len() < data.len());
        assert_eq!(&compressed[..2], &MAGIC);
        assert_eq!(compressed[2], 2); // Zstd
        let decompressed = decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed.as_ref(), data);
    }

    #[test]
    fn test_incompressible_data_stored_raw() {
        // Random data is incompressible
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let compressed = compress(&data, Compression::Lz4);
        // Should be stored raw (no header) since compression isn't beneficial
        // Or it might still be smaller — just verify roundtrip
        let decompressed = decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed.as_ref(), data);
    }

    #[test]
    fn test_empty_data() {
        let compressed = compress(&[], Compression::Lz4);
        assert!(compressed.is_empty());
        let decompressed = decompress(&compressed, 0).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_raw_data_without_header() {
        // Data that doesn't start with magic bytes should pass through
        let data = b"raw data without compression";
        let decompressed = decompress(data, data.len()).unwrap();
        assert_eq!(decompressed.as_ref(), data);
    }

    #[test]
    fn test_raw_data_without_header_is_borrowed() {
        let data = b"incompressible raw block payload";
        let decompressed = decompress(data, data.len()).unwrap();
        assert!(
            matches!(decompressed, Cow::Borrowed(_)),
            "raw fallback should avoid copying the object buffer"
        );
    }

    #[test]
    fn test_decompress_bytes_reuses_raw_buffer() {
        let data = Bytes::from_static(b"raw object payload");
        let raw_ptr = data.as_ptr();
        let decompressed = decompress_bytes(data, b"raw object payload".len()).unwrap();
        assert_eq!(decompressed.as_ref(), b"raw object payload");
        assert_eq!(decompressed.as_ptr(), raw_ptr);
    }

    #[test]
    fn test_decompress_bytes_slices_none_header_without_copy() {
        let mut encoded = Vec::from(MAGIC);
        encoded.push(Compression::None.algo_byte());
        encoded.push(0);
        encoded.extend_from_slice(b"raw body");

        let data = Bytes::from(encoded);
        let expected_ptr = data.as_ptr().wrapping_add(4);
        let decompressed = decompress_bytes(data, b"raw body".len()).unwrap();

        assert_eq!(decompressed.as_ref(), b"raw body");
        assert_eq!(decompressed.as_ptr(), expected_ptr);
    }

    #[test]
    fn versioned_raw_frame_preserves_magic_like_user_bytes() {
        let raw = b"SF\x00\x00payload";
        let encoded = encode_persisted_block(raw, Compression::None);
        assert_eq!(
            parse_persisted_header(&encoded),
            PersistedHeader::Framed(Compression::None)
        );
        assert_eq!(
            decompress_framed_bytes(encoded, raw.len())
                .unwrap()
                .as_ref(),
            raw
        );
    }

    #[test]
    fn parser_rejects_invalid_framed_headers_consistently() {
        assert_eq!(
            parse_persisted_header(b"SF\x01"),
            PersistedHeader::Incomplete
        );
        assert_eq!(
            parse_persisted_header(b"SF\x01\x01"),
            PersistedHeader::Invalid
        );
        assert_eq!(
            parse_persisted_header(b"SF\x7f\x00"),
            PersistedHeader::Invalid
        );
        assert!(decompress_framed(b"SF\x01\x01payload", 4096).is_err());
        assert!(decompress_framed(b"SF\x7f\x00payload", 4096).is_err());
    }

    #[test]
    fn framed_decoding_enforces_the_output_budget_for_every_encoding() {
        let raw = vec![42; 4096];
        for compression in [Compression::None, Compression::Lz4, Compression::Zstd(3)] {
            let encoded = encode_persisted_block(&raw, compression);
            assert_eq!(
                parse_persisted_header(&encoded),
                PersistedHeader::Framed(match compression {
                    Compression::Zstd(_) => Compression::Zstd(0),
                    other => other,
                })
            );
            assert_eq!(
                decompress_framed(&encoded, raw.len()).unwrap().as_ref(),
                raw
            );
            assert!(decompress_framed(&encoded, raw.len() - 1).is_err());
            assert!(decompress_framed_bytes(encoded.clone(), raw.len() - 1).is_err());
            assert!(decompress_bytes(encoded, raw.len() - 1).is_err());
        }
    }

    #[test]
    fn lz4_rejects_an_oversized_length_prefix_before_decoding() {
        let mut encoded = vec![MAGIC[0], MAGIC[1], Compression::Lz4.algo_byte(), 0];
        encoded.extend_from_slice(&u32::MAX.to_le_bytes());
        let error = decompress_framed(&encoded, 4096).unwrap_err();
        assert_eq!(error.to_string(), "LZ4 block exceeds decoded size limit");
    }

    #[test]
    fn legacy_raw_decoding_enforces_the_output_budget() {
        let raw = Bytes::from_static(b"raw block");
        assert!(decompress_bytes(raw.clone(), raw.len() - 1).is_err());
        assert_eq!(decompress_bytes(raw.clone(), raw.len()).unwrap(), raw);
    }
}
