//! CRC32C integrity verification for disk cache files.
//!
//! Cache files are stored as `[data][checksums][data_len_u64]`.
//! Checksums are computed per 32KB block using CRC32C (hardware-accelerated
//! on x86_64 via SSE4.2). The trailing 8-byte `data_len` allows the decoder
//! to split data from checksums without knowing the original size upfront.

/// Disk cache integrity mode.
///
/// `Full` preserves the current behavior: every cached block is wrapped with
/// CRC32C framing and verified on load. `None` stores raw cache bytes and
/// relies on atomic rename plus cache miss recovery if local cache data is
/// corrupted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheIntegrityMode {
    #[default]
    Full,
    None,
}

/// Block size for checksum calculation (32KB, matching JuiceFS)
const CS_BLOCK: usize = 32 * 1024;

/// Magic bytes to identify integrity-wrapped cache files.
const MAGIC: [u8; 4] = [0x53, 0x46, 0x43, 0x31]; // "SFC1"

/// Header: [MAGIC(4)][data_len(4 bytes LE)]
const HEADER_LEN: usize = 8;

/// Encode data with CRC32C checksums for disk storage.
/// Format: [MAGIC(4)][data_len_u32(4)][data][checksums]
/// Each checksum covers CS_BLOCK bytes (last block may be smaller).
#[allow(dead_code)]
pub fn encode(data: &[u8]) -> Vec<u8> {
    let num_blocks = data.len().div_ceil(CS_BLOCK);
    let checksum_bytes = num_blocks * 4;
    let total = HEADER_LEN + data.len() + checksum_bytes;
    let mut out = Vec::with_capacity(total);

    // Header
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());

    // Data
    out.extend_from_slice(data);

    // Checksums
    for i in 0..num_blocks {
        let start = i * CS_BLOCK;
        let end = (start + CS_BLOCK).min(data.len());
        let cs = crc32c::crc32c(&data[start..end]);
        out.extend_from_slice(&cs.to_le_bytes());
    }

    out
}

/// Compute the header and checksums without copying data.
/// Returns (header_bytes, checksum_bytes) for use with vectored I/O.
pub fn compute_framing(data: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let num_blocks = data.len().div_ceil(CS_BLOCK);

    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(&MAGIC);
    header.extend_from_slice(&(data.len() as u32).to_le_bytes());

    let mut checksums = Vec::with_capacity(num_blocks * 4);
    for i in 0..num_blocks {
        let start = i * CS_BLOCK;
        let end = (start + CS_BLOCK).min(data.len());
        let cs = crc32c::crc32c(&data[start..end]);
        checksums.extend_from_slice(&cs.to_le_bytes());
    }

    (header, checksums)
}

/// Decode and verify a cache file. Returns the data if checksums pass.
/// Returns None if the file is corrupted or has an invalid format.
/// Legacy files (without magic header) are returned as-is without verification.
pub fn decode(raw: &[u8]) -> Option<Vec<u8>> {
    // Check for magic header
    if raw.len() < HEADER_LEN || raw[..4] != MAGIC {
        // Legacy format: no checksums, return as-is
        return Some(raw.to_vec());
    }

    let data_len = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
    let num_blocks = data_len.div_ceil(CS_BLOCK);
    let checksum_bytes = num_blocks * 4;
    let expected_total = HEADER_LEN + data_len + checksum_bytes;

    if raw.len() < expected_total {
        return None;
    }

    let data = &raw[HEADER_LEN..HEADER_LEN + data_len];
    let checksums = &raw[HEADER_LEN + data_len..HEADER_LEN + data_len + checksum_bytes];

    // Verify each block
    for i in 0..num_blocks {
        let start = i * CS_BLOCK;
        let end = (start + CS_BLOCK).min(data_len);
        let expected = u32::from_le_bytes([
            checksums[i * 4],
            checksums[i * 4 + 1],
            checksums[i * 4 + 2],
            checksums[i * 4 + 3],
        ]);
        let actual = crc32c::crc32c(&data[start..end]);
        if actual != expected {
            return None;
        }
    }

    Some(data.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_small() {
        let data = vec![0xABu8; 100];
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn roundtrip_exact_block() {
        let data = vec![0x42u8; CS_BLOCK];
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn roundtrip_multi_block() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn detects_corruption() {
        let data = vec![0xABu8; 50_000];
        let mut encoded = encode(&data);
        // Corrupt a byte in the middle of the data
        encoded[HEADER_LEN + 25_000] ^= 0xFF;
        assert!(decode(&encoded).is_none());
    }

    #[test]
    fn detects_truncation() {
        let data = vec![0xABu8; 50_000];
        let encoded = encode(&data);
        // Truncate 10 bytes from the end
        let truncated = &encoded[..encoded.len() - 10];
        assert!(decode(truncated).is_none());
    }

    #[test]
    fn legacy_format_accepted() {
        // Data without magic header is returned as-is
        let data = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let decoded = decode(&data).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn empty_data() {
        let data = vec![];
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn large_block_4mb() {
        let data = vec![0x77u8; 4 * 1024 * 1024];
        let encoded = encode(&data);
        // Overhead: 8 byte header + (4MB/32KB)*4 = 8 + 512 = 520 bytes
        assert_eq!(encoded.len(), data.len() + HEADER_LEN + 128 * 4);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }
}
