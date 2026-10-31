//! Wire identities for the two supported immutable catalog-lower profiles.
//!
//! Legacy revision JSON remains byte-for-byte compatible for idempotency digests.
//! A native P1 revision also binds the separate native volume/namespace identity;
//! it is deliberately not a native PublishedRevision or a frozen-metadata snapshot.

use crate::workspace_overlay::{ids::LayerId, model::BaseRevision};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Revision {
    Native(NativeCatalogRevision),
    Workspace(BaseRevision),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeRevisionKind {
    #[serde(rename = "native_p1_catalog")]
    Catalog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCatalogRevision {
    pub kind: NativeRevisionKind,
    pub volume_id: Uuid,
    pub storage_namespace_id: [u8; 16],
    #[serde(deserialize_with = "deserialize_catalog_revision")]
    pub catalog_revision: BaseRevision,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogRevisionFields {
    layer_id: LayerId,
    sealed_version: u64,
    root_hash: [u8; 32],
}

impl From<CatalogRevisionFields> for BaseRevision {
    fn from(value: CatalogRevisionFields) -> Self {
        Self {
            layer_id: value.layer_id,
            sealed_version: value.sealed_version,
            root_hash: value.root_hash,
        }
    }
}

fn deserialize_catalog_revision<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BaseRevision, D::Error> {
    CatalogRevisionFields::deserialize(deserializer).map(Into::into)
}

impl<'de> Deserialize<'de> for Revision {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.get("kind").is_some() {
            serde_json::from_value(value)
                .map(Self::Native)
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::from_value::<CatalogRevisionFields>(value)
                .map(|value| Self::Workspace(value.into()))
                .map_err(serde::de::Error::custom)
        }
    }
}

impl From<BaseRevision> for Revision {
    fn from(value: BaseRevision) -> Self {
        Self::Workspace(value)
    }
}

impl std::ops::Deref for Revision {
    type Target = BaseRevision;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Workspace(value) => value,
            Self::Native(value) => &value.catalog_revision,
        }
    }
}

impl std::ops::DerefMut for Revision {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Workspace(value) => value,
            Self::Native(value) => &mut value.catalog_revision,
        }
    }
}

impl Revision {
    pub fn catalog_revision(&self) -> &BaseRevision {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> BaseRevision {
        BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 7,
            root_hash: [23; 32],
        }
    }

    #[test]
    fn legacy_wire_identity_is_unchanged() {
        let original = catalog();
        let legacy_json = serde_json::to_vec(&original).unwrap();
        let revision: Revision = serde_json::from_slice(&legacy_json).unwrap();
        assert_eq!(revision, Revision::Workspace(original));
        assert_eq!(serde_json::to_vec(&revision).unwrap(), legacy_json);
    }

    #[test]
    fn native_identity_is_explicit_and_cannot_fall_back_to_legacy() {
        let native = Revision::Native(NativeCatalogRevision {
            kind: NativeRevisionKind::Catalog,
            volume_id: Uuid::new_v4(),
            storage_namespace_id: [9; 16],
            catalog_revision: catalog(),
        });
        let json = serde_json::to_value(&native).unwrap();
        assert_eq!(
            serde_json::from_value::<Revision>(json.clone()).unwrap(),
            native
        );
        let mut unknown = json;
        unknown["kind"] = "native_published".into();
        assert!(serde_json::from_value::<Revision>(unknown).is_err());
        let mut legacy = serde_json::to_value(catalog()).unwrap();
        legacy["native_endpoint"] = "must-not-be-accepted".into();
        assert!(serde_json::from_value::<Revision>(legacy).is_err());
    }
}
