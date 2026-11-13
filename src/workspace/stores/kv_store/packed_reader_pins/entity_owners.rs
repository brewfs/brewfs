//! Durable immutable-lower ownership for current-entity namespaces.
//!
//! Promotion and release update the same root epoch consumed by packed and
//! native collectors. A reader lease can expire; this owner cannot. Release
//! needs the entity lifecycle's terminal, quiescent deletion certificate.

use super::*;
use crate::workspace::packed::{Reader, Writer};

const OWNER_PREFIX: &[u8] = b"packed/entity-owner/";
const ACTIVE_OWNER_PREFIX: &[u8] = b"packed/entity-active/";
const OWNER_COUNT_KEY: &[u8] = b"packed/entity-owner-count";

/// Only a successful collector-fenced CAS constructs this retention token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedEntityLowerOwner {
    volume_id: Uuid,
    namespace_id: Uuid,
    binding: PackedLowerBindingRecord,
    released: bool,
}

impl PackedEntityLowerOwner {
    pub fn volume_id(&self) -> Uuid {
        self.volume_id
    }
    pub fn namespace_id(&self) -> Uuid {
        self.namespace_id
    }
    pub fn binding(&self) -> &PackedLowerBindingRecord {
        &self.binding
    }

    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.volume_id.is_nil() || self.namespace_id.is_nil() {
            return Err(pin_error("nil entity lower owner identity"));
        }
        let binding = self.binding.encode()?;
        let mut writer = Writer::default();
        writer.bytes(b"PEO1");
        writer.bytes(self.volume_id.as_bytes());
        writer.bytes(self.namespace_id.as_bytes());
        writer.u8(u8::from(self.released));
        writer.u32(binding.len() as u32);
        writer.bytes(&binding);
        Ok(writer.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut reader = Reader::new(bytes);
        if reader.take(4).map_err(pin_error)? != b"PEO1" {
            return Err(pin_error("entity owner version"));
        }
        let volume_id = Uuid::from_bytes(reader.array().map_err(pin_error)?);
        let namespace_id = Uuid::from_bytes(reader.array().map_err(pin_error)?);
        let released = match reader.u8().map_err(pin_error)? {
            0 => false,
            1 => true,
            _ => return Err(pin_error("entity owner state")),
        };
        let size = reader.u32().map_err(pin_error)? as usize;
        if size > BINDING_BYTES {
            return Err(pin_error("entity owner binding bound"));
        }
        let binding = PackedLowerBindingRecord::decode(reader.take(size).map_err(pin_error)?)?;
        if !reader.is_empty() {
            return Err(pin_error("entity owner trailing bytes"));
        }
        let owner = Self {
            volume_id,
            namespace_id,
            binding,
            released,
        };
        owner.encode()?;
        Ok(owner)
    }

    fn key(&self, active: bool) -> Vec<u8> {
        let prefix = if active {
            ACTIVE_OWNER_PREFIX
        } else {
            OWNER_PREFIX
        };
        let mut key = prefix.to_vec();
        key.extend_from_slice(self.volume_id.as_bytes());
        key.extend_from_slice(self.namespace_id.as_bytes());
        key
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Restart lookup grants retention only for the exact still-active owner.
    /// It cannot revive a released namespace or create a missing owner.
    pub async fn load_packed_entity_lower(
        &self,
        volume_id: Uuid,
        namespace_id: Uuid,
    ) -> Result<PackedEntityLowerOwner, WorkspaceError> {
        if volume_id.is_nil() || namespace_id.is_nil() {
            return Err(WorkspaceError::Fenced);
        }
        let _permit = self.reader_pin_admission(OPERATION_BYTES)?;
        let mut key = OWNER_PREFIX.to_vec();
        key.extend_from_slice(volume_id.as_bytes());
        key.extend_from_slice(namespace_id.as_bytes());
        let (rows, _) = self.pin_values(&[key]).await?;
        let owner =
            PackedEntityLowerOwner::decode(rows[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if owner.volume_id != volume_id || owner.namespace_id != namespace_id {
            return Err(WorkspaceError::Fenced);
        }
        self.validate_packed_entity_lower(&owner).await?;
        Ok(owner)
    }

    /// O(1) ownership transfer from an authenticated, still-live reader. The
    /// source remains separately owned until its normal release/reap. Terminal
    /// owner records prohibit namespace UUID reuse and make release replay safe.
    pub async fn retain_packed_entity_lower(
        &self,
        volume_id: Uuid,
        namespace_id: Uuid,
        source: &PackedReaderPin,
    ) -> Result<PackedEntityLowerOwner, WorkspaceError> {
        source.validate()?;
        let _permit = self.reader_pin_admission(OPERATION_BYTES)?;
        let owner = PackedEntityLowerOwner {
            volume_id,
            namespace_id,
            binding: source.binding.clone(),
            released: false,
        };
        let bytes = owner.encode()?;
        let keys = vec![
            owner.key(false),
            owner.key(true),
            OWNER_COUNT_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            pin_key(source.slot),
            active_key(source.slot),
            FEATURE_KEY.to_vec(),
        ];
        for _ in 0..CAS_MAX_RETRIES {
            let (values, now) = self.pin_values(&keys).await?;
            if let Some(existing) = &values[0] {
                let observed = PackedEntityLowerOwner::decode(existing)?;
                if observed != owner || values[1] != values[0] {
                    return Err(WorkspaceError::Fenced);
                }
                if self
                    .backend
                    .compare_and_swap(&checks_for(&keys[..4], &values[..4]), &[])
                    .await?
                {
                    return Ok(owner);
                }
                continue;
            }
            if values[1].is_some() {
                return Err(pin_error("entity mirror without owner"));
            }
            let live = values[4]
                .as_deref()
                .map(PackedReaderPin::decode)
                .transpose()?
                .ok_or(WorkspaceError::Fenced)?;
            if !live.same_identity(source)
                || live.state != PackedReaderPinState::Active
                || values[4] != values[5]
                || values[6].as_deref() != Some(FEATURE)
                || now >= live.expires_at_ns
            {
                return Err(WorkspaceError::Fenced);
            }
            let owners = count(&values[2])?;
            if owners >= PACKED_READER_SLOT_COUNT as u64 {
                return Err(WorkspaceError::Busy);
            }
            let generation = next_packed_root_generation(&values[3])?;
            let writes = vec![
                KvWrite::Put {
                    key: keys[0].clone(),
                    value: bytes.clone(),
                },
                KvWrite::Put {
                    key: keys[1].clone(),
                    value: bytes.clone(),
                },
                KvWrite::Put {
                    key: keys[2].clone(),
                    value: (owners + 1).to_le_bytes().to_vec(),
                },
                KvWrite::Put {
                    key: keys[3].clone(),
                    value: encode(&generation)?,
                },
            ];
            if self
                .backend
                .compare_and_swap_before(&checks_for(&keys, &values), &writes, live.expires_at_ns)
                .await?
            {
                return Ok(owner);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    pub async fn validate_packed_entity_lower(
        &self,
        owner: &PackedEntityLowerOwner,
    ) -> Result<(), WorkspaceError> {
        let _permit = self.reader_pin_admission(OPERATION_BYTES)?;
        let keys = vec![owner.key(false), owner.key(true)];
        let (values, _) = self.pin_values(&keys).await?;
        if owner.released
            || values[0].as_deref() != Some(owner.encode()?.as_slice())
            || values[1] != values[0]
        {
            return Err(WorkspaceError::Fenced);
        }
        if !self
            .backend
            .compare_and_swap(&checks_for(&keys, &values), &[])
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    /// Retrying after a lost release response is safe. The terminal row is
    /// retained permanently, so a delayed attach cannot recreate this owner.
    pub async fn release_packed_entity_lower(
        &self,
        owner: &PackedEntityLowerOwner,
        proof: &crate::workspace::entity::lifecycle::DeletedLowerOwnerProof,
    ) -> Result<(), WorkspaceError> {
        let lower = proof.binding();
        if owner.released
            || proof.volume_id() != owner.volume_id
            || proof.namespace_id() != owner.namespace_id
            || lower.manifest_key != owner.binding.binding.manifest.key.as_bytes()
            || lower.manifest_digest != owner.binding.binding.manifest.digest
            || lower.max_ino != owner.binding.highest_inode
            || lower.root_ino != 1
        {
            return Err(WorkspaceError::Fenced);
        }
        let _permit = self.reader_pin_admission(OPERATION_BYTES)?;
        let mut terminal = owner.clone();
        terminal.released = true;
        let active_bytes = owner.encode()?;
        let terminal_bytes = terminal.encode()?;
        let keys = vec![
            owner.key(false),
            owner.key(true),
            OWNER_COUNT_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
        ];
        for _ in 0..CAS_MAX_RETRIES {
            let (values, _) = self.pin_values(&keys).await?;
            let owners = count(&values[2])?;
            if values[0].as_deref() == Some(terminal_bytes.as_slice()) && values[1].is_none() {
                if self
                    .backend
                    .compare_and_swap(&checks_for(&keys, &values), &[])
                    .await?
                {
                    return Ok(());
                }
                continue;
            }
            if values[0].as_deref() != Some(active_bytes.as_slice())
                || values[1] != values[0]
                || owners == 0
            {
                return Err(WorkspaceError::Fenced);
            }
            let generation = next_packed_root_generation(&values[3])?;
            let writes = vec![
                KvWrite::Put {
                    key: keys[0].clone(),
                    value: terminal_bytes.clone(),
                },
                KvWrite::Delete {
                    key: keys[1].clone(),
                },
                KvWrite::Put {
                    key: keys[2].clone(),
                    value: (owners - 1).to_le_bytes().to_vec(),
                },
                KvWrite::Put {
                    key: keys[3].clone(),
                    value: encode(&generation)?,
                },
            ];
            if self
                .backend
                .compare_and_swap(&checks_for(&keys, &values), &writes)
                .await?
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    pub(super) async fn append_packed_entity_lower_roots(
        &self,
        roots: &mut PackedReaderPinRoots,
    ) -> Result<(), WorkspaceError> {
        let _probe = self.reader_pin_admission(OPERATION_BYTES)?;
        let keys = vec![
            OWNER_COUNT_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
        ];
        let (values, _) = self.pin_values(&keys).await?;
        if !merge_reader_authority_checks(&mut roots.checks, &checks_for(&keys, &values)) {
            return Err(WorkspaceError::Busy);
        }
        let owners = count(&values[0])?;
        if values[0].is_none() {
            return Ok(());
        }
        if roots._permit.is_none() {
            roots._permit = Some(self.reader_pin_admission(SCAN_BYTES)?);
        }
        let entries = self
            .backend
            .scan_prefix_with_byte_limits(
                ACTIVE_OWNER_PREFIX,
                KvReadLimits {
                    max_records: PACKED_READER_SLOT_COUNT + 1,
                    max_key_bytes: 256,
                    max_value_bytes: RECORD_BYTES,
                    max_total_bytes: 4 << 20,
                    max_response_bytes: 4 << 20,
                    max_data_requests: 1024,
                },
            )
            .await?;
        if entries.len() > PACKED_READER_SLOT_COUNT {
            return Err(pin_error("entity owner census bound"));
        }
        if entries.len() as u64 != owners {
            return Err(WorkspaceError::Busy);
        }
        for entry in entries {
            let owner = PackedEntityLowerOwner::decode(&entry.value)?;
            if owner.released || entry.key != owner.key(true) {
                return Err(pin_error("entity active identity"));
            }
            let pair = vec![owner.key(false), owner.key(true)];
            let (rows, _) = self.pin_values(&pair).await?;
            if rows[0].as_deref() != Some(entry.value.as_slice()) || rows[1] != rows[0] {
                return Err(WorkspaceError::Busy);
            }
            roots.checks.extend(checks_for(&pair, &rows));
            roots
                .native_roots
                .insert(owner.binding.base_revision.layer_id);
            roots.native_roots.insert(owner.binding.head_layer_id);
            roots.bindings.push(owner.binding);
        }
        Ok(())
    }
}
