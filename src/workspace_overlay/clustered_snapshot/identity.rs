//! Snapshot-stable directory identity derived from raw POSIX names.

use std::fmt;

use super::name::{NameBytes, NameError};

const DIR_KEY_DOMAIN: &[u8] = b"BrewFS.DirKey.v2";

/// The authenticated 128-bit identity of a logical directory.
///
/// A typed wrapper prevents callers from accidentally using a hash or inode
/// number as a `DirKey`.  The wire representation is always exactly 16 bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DirKey(pub [u8; 16]);

impl DirKey {
    pub const ROOT_TAG: &'static [u8] = b"root";

    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl AsRef<[u8]> for DirKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirKeyError {
    InvalidName(NameError),
    Mismatch { expected: DirKey, actual: DirKey },
}

impl fmt::Display for DirKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName(error) => write!(f, "invalid directory name: {error}"),
            Self::Mismatch { expected, actual } => {
                write!(
                    f,
                    "derived directory key {actual:?} does not match {expected:?}"
                )
            }
        }
    }
}

impl std::error::Error for DirKeyError {}

impl From<NameError> for DirKeyError {
    fn from(error: NameError) -> Self {
        Self::InvalidName(error)
    }
}

/// Derive the root directory identity for one volume.
pub fn derive_root_dir_key(volume_id: [u8; 16]) -> DirKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DIR_KEY_DOMAIN);
    hasher.update(&volume_id);
    hasher.update(DirKey::ROOT_TAG);
    let digest = hasher.finalize();
    let mut key = [0u8; 16];
    key.copy_from_slice(&digest.as_bytes()[..16]);
    DirKey(key)
}

/// Derive a child identity without UTF-8 conversion or Unicode normalization.
/// The length prefix is little-endian and binds `a` and `a\0` to different
/// identities even when a caller constructs names from arbitrary bytes.
pub fn derive_child_dir_key(
    volume_id: [u8; 16],
    parent: DirKey,
    raw_name: &[u8],
) -> Result<DirKey, DirKeyError> {
    let name = NameBytes::new(raw_name.to_vec())?;
    let length = u32::try_from(name.as_bytes().len()).map_err(|_| {
        DirKeyError::InvalidName(NameError::TooLong {
            len: name.as_bytes().len(),
            max: NameBytes::MAX_LEN,
        })
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(DIR_KEY_DOMAIN);
    hasher.update(&volume_id);
    hasher.update(parent.as_ref());
    hasher.update(&length.to_le_bytes());
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut key = [0u8; 16];
    key.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(DirKey(key))
}

/// Validate the child identity embedded in a directory entry. Producers and
/// readers should call this before accepting a directory skeleton; comparing a
/// caller-provided hash without recomputing it could merge unrelated paths.
pub fn validate_child_dir_key(
    volume_id: [u8; 16],
    parent: DirKey,
    raw_name: &[u8],
    expected: DirKey,
) -> Result<(), DirKeyError> {
    let actual = derive_child_dir_key(volume_id, parent, raw_name)?;
    if actual != expected {
        return Err(DirKeyError::Mismatch { expected, actual });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_and_child_keys_are_deterministic_and_raw_byte_bound() {
        let volume = [7; 16];
        let root = derive_root_dir_key(volume);
        assert_eq!(root, derive_root_dir_key(volume));
        let ascii = derive_child_dir_key(volume, root, b"data").unwrap();
        let raw = derive_child_dir_key(volume, root, &[b'd', b'a', b't', 0x80]).unwrap();
        assert_ne!(ascii, raw);
        assert_eq!(ascii, derive_child_dir_key(volume, root, b"data").unwrap());
    }

    #[test]
    fn child_keys_bind_parent_and_reject_profile_violations() {
        let volume = [9; 16];
        let root = derive_root_dir_key(volume);
        let other = derive_root_dir_key([10; 16]);
        assert_ne!(
            derive_child_dir_key(volume, root, b"x").unwrap(),
            derive_child_dir_key(volume, other, b"x").unwrap()
        );
        assert!(matches!(
            derive_child_dir_key(volume, root, &[]),
            Err(DirKeyError::InvalidName(NameError::Empty))
        ));
        assert!(matches!(
            derive_child_dir_key(volume, root, &[b'x'; NameBytes::MAX_LEN + 1]),
            Err(DirKeyError::InvalidName(NameError::TooLong { .. }))
        ));
        let child = derive_child_dir_key(volume, root, b"child").unwrap();
        assert!(validate_child_dir_key(volume, root, b"child", child).is_ok());
        assert!(matches!(
            validate_child_dir_key(volume, root, b"child", root),
            Err(DirKeyError::Mismatch { .. })
        ));
    }
}
