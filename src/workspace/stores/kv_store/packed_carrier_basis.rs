//! Immutable packed carrier descriptor codec candidate.
//! A decoded descriptor is identity data, never publication or fork authority.
//! Only an authenticated final publication CAS may install descriptor + claim;
//! fork must also fence exact history, registry root, native base and epochs.

use super::*;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_CARRIER_BASIS_BYTES: usize = 16 << 10;
const CARRIER_CLAIM_MAGIC: &[u8; 4] = b"PSC3";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PackedCarrierBasis {
    pub(super) carrier_revision: BaseRevision,
    pub(super) native_sealed_source_revision: BaseRevision,
    pub(super) source_binding: PackedLowerBindingRecord,
    pub(super) registry_incarnation: Uuid,
}

fn invalid_basis() -> WorkspaceError {
    WorkspaceError::CorruptMetadata("packed carrier basis identity/codec".into())
}

fn encode_revision(bytes: &mut Vec<u8>, revision: &BaseRevision) {
    bytes.extend_from_slice(revision.layer_id.as_bytes());
    bytes.extend_from_slice(&revision.sealed_version.to_le_bytes());
    bytes.extend_from_slice(&revision.root_hash);
}

struct BasisCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BasisCursor<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], WorkspaceError> {
        let end = self.offset.checked_add(length).ok_or_else(invalid_basis)?;
        let value = self.bytes.get(self.offset..end).ok_or_else(invalid_basis)?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], WorkspaceError> {
        self.bytes(N)?.try_into().map_err(|_| invalid_basis())
    }

    fn revision(&mut self) -> Result<BaseRevision, WorkspaceError> {
        Ok(BaseRevision {
            layer_id: LayerId::from_uuid(Uuid::from_bytes(self.array()?)),
            sealed_version: u64::from_le_bytes(self.array()?),
            root_hash: self.array()?,
        })
    }
}

pub(super) fn packed_carrier_basis_key(layer: LayerId) -> Vec<u8> {
    format!("packed/sealed-carrier/{layer}").into_bytes()
}

pub(super) fn packed_carrier_claim_key(layer: LayerId) -> Vec<u8> {
    format!("packed/carrier-claim/{layer}").into_bytes()
}

impl PackedCarrierBasis {
    fn validate(&self) -> Result<(), WorkspaceError> {
        self.source_binding.encode()?;
        if self.carrier_revision != self.source_binding.base_revision
            || self.carrier_revision.layer_id != self.source_binding.binding.base_layer_id
            || self.carrier_revision.layer_id.as_uuid().is_nil()
            || self.carrier_revision.sealed_version == 0
            || self
                .native_sealed_source_revision
                .layer_id
                .as_uuid()
                .is_nil()
            || self.native_sealed_source_revision.sealed_version == 0
            || self.native_sealed_source_revision.layer_id == self.carrier_revision.layer_id
            || self.native_sealed_source_revision.layer_id == self.source_binding.head_layer_id
            || self.registry_incarnation.is_nil()
        {
            return Err(invalid_basis());
        }
        Ok(())
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        self.validate()?;
        let binding = self.source_binding.encode()?;
        let mut bytes = Vec::with_capacity(169 + binding.len());
        bytes.extend_from_slice(b"PSB3");
        bytes.push(1); // Independent descriptor codec, not a packed wire version.
        encode_revision(&mut bytes, &self.carrier_revision);
        encode_revision(&mut bytes, &self.native_sealed_source_revision);
        bytes.extend_from_slice(self.registry_incarnation.as_bytes());
        bytes.extend_from_slice(&(binding.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&binding);
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(&digest);
        if bytes.len() > MAX_CARRIER_BASIS_BYTES {
            return Err(invalid_basis());
        }
        Ok(bytes)
    }

    pub(super) fn claim(encoded: &[u8]) -> Result<Vec<u8>, WorkspaceError> {
        // Refuse opaque/raw caller bytes even though the claim itself is tiny.
        Self::decode(encoded)?;
        let mut claim = CARRIER_CLAIM_MAGIC.to_vec();
        claim.extend_from_slice(&Sha256::digest(encoded));
        Ok(claim)
    }

    pub(super) fn decode(encoded: &[u8]) -> Result<Self, WorkspaceError> {
        if encoded.len() > MAX_CARRIER_BASIS_BYTES || encoded.len() < 169 {
            return Err(invalid_basis());
        }
        let end = encoded.len() - 32;
        let digest: [u8; 32] = Sha256::digest(&encoded[..end]).into();
        if encoded[end..] != digest {
            return Err(invalid_basis());
        }
        let mut cursor = BasisCursor {
            bytes: &encoded[..end],
            offset: 0,
        };
        if cursor.array::<4>()? != *b"PSB3" || cursor.array::<1>()? != [1] {
            return Err(invalid_basis());
        }
        let carrier_revision = cursor.revision()?;
        let native_sealed_source_revision = cursor.revision()?;
        let registry_incarnation = Uuid::from_bytes(cursor.array()?);
        let length = u32::from_le_bytes(cursor.array()?) as usize;
        let source_binding = PackedLowerBindingRecord::decode(cursor.bytes(length)?)?;
        if cursor.offset != end {
            return Err(invalid_basis());
        }
        let basis = Self {
            carrier_revision,
            native_sealed_source_revision,
            source_binding,
            registry_incarnation,
        };
        basis.validate()?;
        Ok(basis)
    }

    pub(super) fn decode_pair(
        revision: &BaseRevision,
        descriptor: &Option<Vec<u8>>,
        claim: &Option<Vec<u8>>,
    ) -> Result<Option<Self>, WorkspaceError> {
        let (Some(encoded), Some(claim)) = (descriptor, claim) else {
            return if descriptor.is_none() && claim.is_none() {
                Ok(None)
            } else {
                Err(invalid_basis())
            };
        };
        let basis = Self::decode(encoded)?;
        if &basis.carrier_revision != revision || *claim != Self::claim(encoded)? {
            return Err(invalid_basis());
        }
        Ok(Some(basis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::packed::snapshot::{SnapshotObjectKind, SnapshotObjectRef};

    fn basis() -> PackedCarrierBasis {
        let carrier_revision = BaseRevision {
            layer_id: LayerId::from_uuid(Uuid::from_u128(3)),
            sealed_version: 5,
            root_hash: [6; 32],
        };
        PackedCarrierBasis {
            carrier_revision: carrier_revision.clone(),
            native_sealed_source_revision: BaseRevision {
                layer_id: LayerId::from_uuid(Uuid::from_u128(4)),
                sealed_version: 7,
                root_hash: [8; 32],
            },
            source_binding: PackedLowerBindingRecord {
                workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(1)),
                head_layer_id: LayerId::from_uuid(Uuid::from_u128(2)),
                head_epoch: 9,
                base_revision: carrier_revision.clone(),
                highest_inode: 400,
                binding: PackedLowerBinding {
                    binding_version: 2,
                    base_layer_id: carrier_revision.layer_id,
                    manifest: SnapshotObjectRef {
                        key: "fixture/manifest".into(),
                        kind: SnapshotObjectKind::Manifest,
                        object_len: 8192,
                        digest: [10; 32],
                    },
                },
            },
            registry_incarnation: Uuid::from_u128(11),
        }
    }

    fn rehash(encoded: &mut [u8]) {
        let end = encoded.len() - 32;
        let digest: [u8; 32] = Sha256::digest(&encoded[..end]).into();
        encoded[end..].copy_from_slice(&digest);
    }

    #[test]
    fn carrier_pair_roundtrip_preserves_distinct_native_source_revision() {
        let basis = basis();
        let encoded = basis.encode().unwrap();
        let claim = PackedCarrierBasis::claim(&encoded).unwrap();
        assert_eq!(PackedCarrierBasis::decode(&encoded).unwrap(), basis);
        assert_eq!(
            PackedCarrierBasis::decode_pair(&basis.carrier_revision, &Some(encoded), &Some(claim))
                .unwrap(),
            Some(basis.clone()),
        );
        assert_ne!(basis.carrier_revision, basis.native_sealed_source_revision);
    }

    #[test]
    fn carrier_pair_refuses_missing_half_and_exact_revision_substitution() {
        let basis = basis();
        let encoded = basis.encode().unwrap();
        let claim = PackedCarrierBasis::claim(&encoded).unwrap();
        assert!(
            PackedCarrierBasis::decode_pair(&basis.carrier_revision, &Some(encoded.clone()), &None)
                .is_err()
        );
        assert!(
            PackedCarrierBasis::decode_pair(&basis.carrier_revision, &None, &Some(claim.clone()))
                .is_err()
        );
        assert!(
            PackedCarrierBasis::decode_pair(
                &basis.native_sealed_source_revision,
                &Some(encoded),
                &Some(claim)
            )
            .is_err()
        );
        assert_eq!(
            PackedCarrierBasis::decode_pair(&basis.carrier_revision, &None, &None).unwrap(),
            None
        );
        let mut mismatched = basis.clone();
        mismatched.carrier_revision.root_hash[0] ^= 1;
        assert!(mismatched.encode().is_err());
    }

    #[test]
    fn carrier_codec_refuses_tamper_unknown_codec_oversized_length_and_trailing_data() {
        let encoded = basis().encode().unwrap();
        let mut changed = encoded.clone();
        changed[5] ^= 1;
        assert!(PackedCarrierBasis::decode(&changed).is_err());
        changed = encoded.clone();
        changed[4] = 2;
        rehash(&mut changed);
        assert!(PackedCarrierBasis::decode(&changed).is_err());
        changed = encoded.clone();
        changed[133..137].copy_from_slice(&u32::MAX.to_le_bytes());
        rehash(&mut changed);
        assert!(PackedCarrierBasis::decode(&changed).is_err());
        changed = encoded;
        changed.insert(changed.len() - 32, 0);
        rehash(&mut changed);
        assert!(PackedCarrierBasis::decode(&changed).is_err());
        assert!(PackedCarrierBasis::decode(&vec![0; MAX_CARRIER_BASIS_BYTES + 1]).is_err());
    }
}
