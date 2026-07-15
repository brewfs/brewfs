//! Etcd backend-specific data structures

use crate::meta::Permission;
use crate::meta::entities::content_meta::EntryType;
use crate::meta::file_lock::PlockRecord;
use crate::meta::store::{FileAttr, FileType};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Etcd entry information (reverse index: inode -> file/directory attributes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdEntryInfo {
    pub is_file: bool,
    pub size: Option<i64>,
    pub version: Option<i32>,
    pub access_time: i64,
    pub modify_time: i64,
    pub create_time: i64,
    pub permission: Permission,
    pub nlink: u32,
    pub parent_inode: i64,
    pub entry_name: String,
    pub deleted: bool,
    #[serde(default)]
    pub entry_type: Option<EntryType>,
    #[serde(default)]
    pub rdev: u32,
    #[serde(default)]
    pub symlink_target: Option<String>,
}

/// Etcd forward index entry ((parent_id, name) -> inode)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    feature = "rkyv-serialization",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct EtcdForwardEntry {
    pub parent_inode: i64,
    pub name: String,
    pub inode: i64,
    pub is_file: bool,
    #[serde(default)]
    pub entry_type: Option<EntryType>,
}

impl EtcdEntryInfo {
    pub fn permission(&self) -> &Permission {
        &self.permission
    }

    #[allow(dead_code)]
    pub fn set_permission(&mut self, permission: Permission) {
        self.permission = permission;
    }

    #[allow(dead_code)]
    pub fn mode(&self) -> u32 {
        self.permission.mode
    }

    #[allow(dead_code)]
    pub fn uid(&self) -> u32 {
        self.permission.uid
    }

    #[allow(dead_code)]
    pub fn gid(&self) -> u32 {
        self.permission.gid
    }

    /// Converts EtcdEntryInfo to FileAttr for cache updates
    ///
    /// # Arguments
    ///
    /// * `ino` - The inode number (extracted from the r:{ino} key)
    ///
    /// # Returns
    ///
    /// FileAttr suitable for direct cache insertion
    pub fn to_file_attr(&self, ino: i64) -> FileAttr {
        let legacy_kind = if self.is_file {
            if self.symlink_target.is_some() {
                FileType::Symlink
            } else {
                FileType::File
            }
        } else {
            FileType::Dir
        };
        let kind = self
            .entry_type
            .clone()
            .map(FileType::from)
            .unwrap_or(legacy_kind);
        let size = if let Some(target) = &self.symlink_target {
            target.len() as u64
        } else if kind == FileType::Dir {
            4096
        } else {
            self.size.unwrap_or(0).max(0) as u64
        };

        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            kind,
            mode: self.permission.mode,
            rdev: self.rdev,
            uid: self.permission.uid,
            gid: self.permission.gid,
            atime: self.access_time,
            mtime: self.modify_time,
            ctime: self.create_time,
            nlink: self.nlink,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn special_node_json_round_trip_preserves_kind_and_rdev() {
        let entry = EtcdEntryInfo {
            is_file: true,
            size: Some(0),
            version: Some(0),
            access_time: 1,
            modify_time: 2,
            create_time: 3,
            permission: Permission::new(FileType::CharDevice.mode_type_bits() | 0o600, 1, 2),
            nlink: 1,
            parent_inode: 1,
            entry_name: "null".to_string(),
            deleted: false,
            entry_type: Some(EntryType::CharDevice),
            rdev: libc::makedev(1, 3) as u32,
            symlink_target: None,
        };

        let encoded = serde_json::to_vec(&entry).unwrap();
        let decoded: EtcdEntryInfo = serde_json::from_slice(&encoded).unwrap();
        let attr = decoded.to_file_attr(42);

        assert_eq!(attr.kind, FileType::CharDevice);
        assert_eq!(attr.mode, entry.permission.mode);
        assert_eq!(attr.rdev, entry.rdev);
    }

    #[test]
    fn legacy_file_json_defaults_to_regular_file_without_rdev() {
        let decoded: EtcdEntryInfo = serde_json::from_str(
            r#"{"is_file":true,"size":0,"version":0,"access_time":1,"modify_time":2,"create_time":3,"permission":{"mode":33188,"uid":0,"gid":0},"nlink":1,"parent_inode":1,"entry_name":"file","deleted":false}"#,
        )
        .unwrap();
        let attr = decoded.to_file_attr(43);

        assert_eq!(attr.kind, FileType::File);
        assert_eq!(attr.rdev, 0);
    }
}

impl EtcdForwardEntry {
    pub fn resolved_entry_type(&self) -> EntryType {
        if let Some(entry_type) = &self.entry_type {
            entry_type.clone()
        } else if self.is_file {
            EntryType::File
        } else {
            EntryType::Directory
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdPlock {
    pub sid: Uuid,
    pub owner: i64,
    pub records: Vec<PlockRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    feature = "rkyv-serialization",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct EtcdLinkParent {
    pub parent_inode: i64,
    pub entry_name: String,
}
