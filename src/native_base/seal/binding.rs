//! BlockBinding (spec 04 §3): `decoded_len:u32 + content_hash[32]`.
//!
//! `content_hash` is SHA-256 of the complete decoded block bytes. Physical
//! zero prefixes inside the decoded domain are hash content and must never
//! be trimmed by packing.

use sha2::{Digest, Sha256};

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockBinding {
    pub decoded_len: u32,
    pub content_hash: [u8; 32],
}

impl BlockBinding {
    pub const ENCODED_LEN: usize = 36;

    pub fn of_decoded(decoded: &[u8]) -> BlockBinding {
        BlockBinding {
            decoded_len: decoded.len() as u32,
            content_hash: Sha256::digest(decoded).into(),
        }
    }

    pub fn encode_into(&self, w: &mut Writer) {
        w.u32(self.decoded_len);
        w.put(&self.content_hash);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.encode_into(&mut w);
        w.into_bytes()
    }

    /// Structural decode. `decoded_len == 0` is rejected here; the
    /// `decoded_len <= block_size` bound needs the volume's block size and
    /// is enforced by the reader.
    pub fn decode(r: &mut Reader<'_>) -> WireResult<BlockBinding> {
        let what = "block binding";
        let decoded_len = r.u32(what)?;
        if decoded_len == 0 {
            return Err(WireError::invalid(what, "decoded_len must be > 0"));
        }
        let content_hash: [u8; 32] = r.take(32, what)?.try_into().unwrap();
        Ok(BlockBinding {
            decoded_len,
            content_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_is_exactly_36_bytes_and_roundtrips() {
        let b = BlockBinding::of_decoded(b"hello block");
        assert_eq!(b.decoded_len, 11);
        assert_eq!(b.encode().len(), BlockBinding::ENCODED_LEN);
        let encoded = b.encode();
        let mut r = Reader::new(&encoded);
        assert_eq!(BlockBinding::decode(&mut r).unwrap(), b);
        assert!(r.is_empty());
    }

    #[test]
    fn zero_length_binding_rejected_and_truncated_rejected() {
        // decoded_len 0 is structurally invalid (spec 04 §3).
        let mut w = Writer::new();
        w.u32(0);
        w.put(&[0u8; 32]);
        assert!(BlockBinding::decode(&mut Reader::new(w.as_slice())).is_err());
        // Truncated value.
        let b = BlockBinding::of_decoded(b"x");
        assert!(BlockBinding::decode(&mut Reader::new(&b.encode()[..20])).is_err());
    }

    #[test]
    fn hash_covers_leading_zeros_of_decoded_domain() {
        // A block whose decoded bytes start with zeros: the hash must be
        // over the zeros too, so a trimmed re-encode cannot match.
        let decoded = [0u8, 0, 0, 1, 2];
        let b = BlockBinding::of_decoded(&decoded);
        let trimmed = BlockBinding::of_decoded(&decoded[3..]);
        assert_ne!(b.content_hash, trimmed.content_hash);
        assert_eq!(b.decoded_len, 5);
    }
}
