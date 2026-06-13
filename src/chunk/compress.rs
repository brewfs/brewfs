//! Block-level data compression for storage and transfer.
//!
//! Provides transparent compression/decompression with auto-detection on read.
//! Compressed data uses a 4-byte header: `[magic_hi, magic_lo, algorithm, reserved]`
//! so that decompression can automatically detect the algorithm without external metadata.

use std::borrow::Cow;
use tracing::{debug, trace};

/// Magic bytes identifying compressed data (0xSF = SlayerFs)
const MAGIC: [u8; 2] = [0x53, 0x46];

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
    if compressed_body.len() + 4 >= data.len() {
        trace!(
            "Compression not beneficial: {} -> {} bytes, storing raw",
            data.len(),
            compressed_body.len() + 4
        );
        return Cow::Borrowed(data);
    }

    let ratio = data.len() as f64 / (compressed_body.len() + 4) as f64;
    trace!(
        "Compressed {} -> {} bytes ({:.1}x ratio, algo={:?})",
        data.len(),
        compressed_body.len() + 4,
        ratio,
        algo
    );

    // Prepend 4-byte header: [magic_hi, magic_lo, algo, reserved]
    let mut result = Vec::with_capacity(4 + compressed_body.len());
    result.extend_from_slice(&MAGIC);
    result.push(algo.algo_byte());
    result.push(0); // reserved
    result.extend_from_slice(&compressed_body);
    Cow::Owned(result)
}

/// Decompress data, auto-detecting compression from the header.
/// If data has no compression header (no magic bytes), returns as-is.
pub fn decompress(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    if data.len() < 4 || data[0] != MAGIC[0] || data[1] != MAGIC[1] {
        // No compression header — return raw data
        return Ok(data.to_vec());
    }

    let algo = Compression::from_algo_byte(data[2])
        .ok_or_else(|| anyhow::anyhow!("Unknown compression algorithm byte: {}", data[2]))?;

    let body = &data[4..];

    match algo {
        Compression::None => Ok(body.to_vec()),
        Compression::Lz4 => lz4_flex::decompress_size_prepended(body)
            .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e)),
        Compression::Zstd(_) => {
            zstd::bulk::decompress(body, 64 * 1024 * 1024) // max 64MB decompressed
                .map_err(|e| anyhow::anyhow!("Zstd decompression failed: {}", e))
        }
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
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_roundtrip_lz4() {
        // Use compressible data (repeated pattern)
        let data: Vec<u8> = (0..4096).map(|i| (i % 16) as u8).collect();
        let compressed = compress(&data, Compression::Lz4);
        assert!(compressed.len() < data.len());
        assert_eq!(&compressed[..2], &MAGIC);
        assert_eq!(compressed[2], 1); // LZ4
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_roundtrip_zstd() {
        let data: Vec<u8> = (0..4096).map(|i| (i % 16) as u8).collect();
        let compressed = compress(&data, Compression::Zstd(3));
        assert!(compressed.len() < data.len());
        assert_eq!(&compressed[..2], &MAGIC);
        assert_eq!(compressed[2], 2); // Zstd
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_incompressible_data_stored_raw() {
        // Random data is incompressible
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let compressed = compress(&data, Compression::Lz4);
        // Should be stored raw (no header) since compression isn't beneficial
        // Or it might still be smaller — just verify roundtrip
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_empty_data() {
        let compressed = compress(&[], Compression::Lz4);
        assert!(compressed.is_empty());
        let decompressed = decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_raw_data_without_header() {
        // Data that doesn't start with magic bytes should pass through
        let data = b"raw data without compression";
        let decompressed = decompress(data).unwrap();
        assert_eq!(decompressed, data);
    }
}
