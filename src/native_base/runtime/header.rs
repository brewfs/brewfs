use crc32c::crc32c;
use serde::Serialize;

use crate::native_base::wire::uvarint::{Reader, Writer};
use crate::native_base::write::keys::Keys;
use crate::native_base::write::store::{ControlStore, StoreError, Txn};

pub const NATIVE_VOLUME_FORMAT: &str = "workspace-native-v2";
pub const NATIVE_SCHEMA_VERSION: u32 = 2;
pub const NATIVE_CONTROL_VERSION: u32 = 2;
pub const NATIVE_WIRE_MAJOR: u16 = 3;
pub const NATIVE_WIRE_MINOR: u16 = 0;

/// A volume requires the P2 Frozen Metadata reader when this bit is set.
pub const FROZEN_METADATA_FEATURE: u64 = 1 << 0;
const KNOWN_REQUIRED_FEATURES: u64 = FROZEN_METADATA_FEATURE;
const HEADER_MAGIC: &[u8; 4] = b"BNVH";
const HEADER_ENCODING_VERSION: u16 = 1;
const MAX_NAMESPACE_LEN: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeRuntimeCapabilities {
    pub native_packed_base: bool,
    pub frozen_base_metadata: bool,
}

impl NativeRuntimeCapabilities {
    pub const fn compiled() -> Self {
        Self {
            native_packed_base: cfg!(feature = "native-packed-base"),
            frozen_base_metadata: cfg!(feature = "frozen-base-metadata"),
        }
    }

    fn supported_required_features(self) -> u64 {
        if self.frozen_base_metadata {
            FROZEN_METADATA_FEATURE
        } else {
            0
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NativeVolumeHeader {
    pub volume_format: String,
    pub schema_version: u32,
    pub native_control_version: u32,
    pub wire_major: u16,
    pub wire_minor: u16,
    pub required_features: u64,
    pub volume_id: [u8; 16],
    pub storage_namespace_id: [u8; 16],
}

impl NativeVolumeHeader {
    pub fn p1(volume_id: [u8; 16], storage_namespace_id: [u8; 16]) -> Self {
        Self {
            volume_format: NATIVE_VOLUME_FORMAT.into(),
            schema_version: NATIVE_SCHEMA_VERSION,
            native_control_version: NATIVE_CONTROL_VERSION,
            wire_major: NATIVE_WIRE_MAJOR,
            wire_minor: NATIVE_WIRE_MINOR,
            required_features: 0,
            volume_id,
            storage_namespace_id,
        }
    }

    pub fn validate(
        &self,
        capabilities: NativeRuntimeCapabilities,
    ) -> Result<(), RuntimeAdmissionError> {
        if !capabilities.native_packed_base {
            return Err(RuntimeAdmissionError::FeatureNotCompiled(
                "native-packed-base",
            ));
        }
        if self.volume_format != NATIVE_VOLUME_FORMAT {
            return Err(RuntimeAdmissionError::UnsupportedVolumeFormat(
                self.volume_format.clone(),
            ));
        }
        if self.schema_version != NATIVE_SCHEMA_VERSION {
            return Err(RuntimeAdmissionError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }
        if self.native_control_version != NATIVE_CONTROL_VERSION {
            return Err(RuntimeAdmissionError::UnsupportedControlVersion(
                self.native_control_version,
            ));
        }
        if self.wire_major != NATIVE_WIRE_MAJOR || self.wire_minor != NATIVE_WIRE_MINOR {
            return Err(RuntimeAdmissionError::UnsupportedWireVersion {
                major: self.wire_major,
                minor: self.wire_minor,
            });
        }
        if self.required_features & !KNOWN_REQUIRED_FEATURES != 0 {
            return Err(RuntimeAdmissionError::UnknownRequiredFeatures(
                self.required_features & !KNOWN_REQUIRED_FEATURES,
            ));
        }
        let unavailable = self.required_features & !capabilities.supported_required_features();
        if unavailable != 0 {
            return Err(RuntimeAdmissionError::UnavailableRequiredFeatures(
                unavailable,
            ));
        }
        if self.volume_id == [0; 16] || self.storage_namespace_id == [0; 16] {
            return Err(RuntimeAdmissionError::InvalidHeader(
                "volume and storage namespace IDs must be non-zero".into(),
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.put(HEADER_MAGIC);
        writer.u16(HEADER_ENCODING_VERSION);
        writer.u16(0);
        writer.bytes(self.volume_format.as_bytes());
        writer.u32(self.schema_version);
        writer.u32(self.native_control_version);
        writer.u16(self.wire_major);
        writer.u16(self.wire_minor);
        writer.u64(self.required_features);
        writer.put(&self.volume_id);
        writer.put(&self.storage_namespace_id);
        let mut encoded = writer.into_bytes();
        encoded.extend_from_slice(&crc32c(&encoded).to_le_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RuntimeAdmissionError> {
        if encoded.len() < 8 {
            return Err(RuntimeAdmissionError::InvalidHeader(
                "native volume header is truncated".into(),
            ));
        }
        let (payload, checksum) = encoded.split_at(encoded.len() - 4);
        let expected = u32::from_le_bytes(checksum.try_into().unwrap());
        if crc32c(payload) != expected {
            return Err(RuntimeAdmissionError::InvalidHeader(
                "native volume header checksum mismatch".into(),
            ));
        }
        let mut reader = Reader::new(payload);
        if reader.take(4, "native volume header")? != HEADER_MAGIC {
            return Err(RuntimeAdmissionError::InvalidHeader(
                "native volume header magic mismatch".into(),
            ));
        }
        let encoding_version = reader.u16("native volume header")?;
        if encoding_version != HEADER_ENCODING_VERSION {
            return Err(RuntimeAdmissionError::InvalidHeader(format!(
                "unsupported native volume header encoding {encoding_version}"
            )));
        }
        if reader.u16("native volume header")? != 0 {
            return Err(RuntimeAdmissionError::InvalidHeader(
                "native volume header reserved field is non-zero".into(),
            ));
        }
        let volume_format = std::str::from_utf8(reader.bytes("native volume header")?)
            .map_err(|_| RuntimeAdmissionError::InvalidHeader("volume format is not UTF-8".into()))?
            .to_owned();
        let header = Self {
            volume_format,
            schema_version: reader.u32("native volume header")?,
            native_control_version: reader.u32("native volume header")?,
            wire_major: reader.u16("native volume header")?,
            wire_minor: reader.u16("native volume header")?,
            required_features: reader.u64("native volume header")?,
            volume_id: reader.take(16, "native volume header")?.try_into().unwrap(),
            storage_namespace_id: reader.take(16, "native volume header")?.try_into().unwrap(),
        };
        if !reader.is_empty() {
            return Err(RuntimeAdmissionError::InvalidHeader(
                "native volume header has trailing bytes".into(),
            ));
        }
        Ok(header)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeAdmissionError {
    #[error("feature not compiled: {0}")]
    FeatureNotCompiled(&'static str),
    #[error("unsupported volume format {0}")]
    UnsupportedVolumeFormat(String),
    #[error("unsupported native schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("unsupported native control version {0}")]
    UnsupportedControlVersion(u32),
    #[error("unsupported native wire version {major}/{minor}")]
    UnsupportedWireVersion { major: u16, minor: u16 },
    #[error("unknown required native features 0x{0:016x}")]
    UnknownRequiredFeatures(u64),
    #[error("required native features are not compiled 0x{0:016x}")]
    UnavailableRequiredFeatures(u64),
    #[error("invalid native volume header: {0}")]
    InvalidHeader(String),
    #[error("native volume header is missing for namespace {0}")]
    MissingHeader(String),
    #[error("native volume namespace is invalid: {0}")]
    InvalidNamespace(String),
    #[error("native volume namespace already exists")]
    NamespaceExists,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Wire(#[from] crate::native_base::wire::error::WireError),
}

fn validate_namespace(namespace: &str) -> Result<(), RuntimeAdmissionError> {
    if namespace.is_empty() || namespace.len() > MAX_NAMESPACE_LEN {
        return Err(RuntimeAdmissionError::InvalidNamespace(
            "must contain 1..=128 bytes".into(),
        ));
    }
    if !namespace
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(RuntimeAdmissionError::InvalidNamespace(
            "only ASCII letters, digits, '.', '-' and '_' are allowed".into(),
        ));
    }
    Ok(())
}

pub async fn load_volume_header(
    store: &dyn ControlStore,
    namespace: &str,
    capabilities: NativeRuntimeCapabilities,
) -> Result<NativeVolumeHeader, RuntimeAdmissionError> {
    validate_namespace(namespace)?;
    let encoded = store
        .get(&Keys::volume_header(namespace))
        .await?
        .ok_or_else(|| RuntimeAdmissionError::MissingHeader(namespace.into()))?;
    let header = NativeVolumeHeader::decode(&encoded)?;
    header.validate(capabilities)?;
    Ok(header)
}

pub async fn initialize_volume(
    store: &dyn ControlStore,
    namespace: &str,
    header: &NativeVolumeHeader,
    capabilities: NativeRuntimeCapabilities,
) -> Result<(), RuntimeAdmissionError> {
    validate_namespace(namespace)?;
    header.validate(capabilities)?;
    let key = Keys::volume_header(namespace);
    store
        .run(
            Txn::new()
                .check_absent(key.clone())
                .put(key, header.encode()),
        )
        .await
        .map_err(|error| match error {
            StoreError::Conflict => RuntimeAdmissionError::NamespaceExists,
            other => RuntimeAdmissionError::Store(other),
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::native_base::write::memory::MemoryControlStore;

    fn all_capabilities() -> NativeRuntimeCapabilities {
        NativeRuntimeCapabilities {
            native_packed_base: true,
            frozen_base_metadata: true,
        }
    }

    #[test]
    fn volume_header_round_trips_and_checksum_is_fail_closed() {
        let header = NativeVolumeHeader::p1([1; 16], [2; 16]);
        let encoded = header.encode();
        assert_eq!(NativeVolumeHeader::decode(&encoded).unwrap(), header);

        let mut corrupt = encoded;
        corrupt[12] ^= 1;
        assert!(matches!(
            NativeVolumeHeader::decode(&corrupt),
            Err(RuntimeAdmissionError::InvalidHeader(_))
        ));
    }

    #[test]
    fn admission_rejects_control_wire_and_required_feature_mismatches() {
        let mut header = NativeVolumeHeader::p1([1; 16], [2; 16]);
        header.native_control_version = 1;
        assert!(matches!(
            header.validate(all_capabilities()),
            Err(RuntimeAdmissionError::UnsupportedControlVersion(1))
        ));

        header.native_control_version = NATIVE_CONTROL_VERSION;
        header.required_features = 1 << 63;
        assert!(matches!(
            header.validate(all_capabilities()),
            Err(RuntimeAdmissionError::UnknownRequiredFeatures(_))
        ));

        header.required_features = FROZEN_METADATA_FEATURE;
        assert!(matches!(
            header.validate(NativeRuntimeCapabilities {
                native_packed_base: true,
                frozen_base_metadata: false,
            }),
            Err(RuntimeAdmissionError::UnavailableRequiredFeatures(
                FROZEN_METADATA_FEATURE
            ))
        ));
    }

    #[tokio::test]
    async fn initialization_is_create_only_and_load_revalidates() {
        let store = Arc::new(MemoryControlStore::new());
        let header = NativeVolumeHeader::p1([1; 16], [2; 16]);
        initialize_volume(&*store, "new-volume", &header, all_capabilities())
            .await
            .unwrap();
        assert_eq!(
            load_volume_header(&*store, "new-volume", all_capabilities())
                .await
                .unwrap(),
            header
        );
        assert!(matches!(
            initialize_volume(&*store, "new-volume", &header, all_capabilities()).await,
            Err(RuntimeAdmissionError::NamespaceExists)
        ));
    }

    #[tokio::test]
    async fn namespace_cannot_escape_the_locator_prefix() {
        let store = MemoryControlStore::new();
        let err = load_volume_header(&store, "../other", all_capabilities())
            .await
            .unwrap_err();
        assert!(matches!(err, RuntimeAdmissionError::InvalidNamespace(_)));
    }
}
