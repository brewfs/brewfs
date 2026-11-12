//! Inode-bound cold attributes for explicit queries and permission decisions.

use super::{SnapshotObjectKind, SnapshotObjectRef, encode_snapshot_object};
use crate::meta::store::AclRule;
use crate::workspace::packed::wire::{PackedResult, PackedWireError, Reader, Writer};

pub const SNAPSHOT_COLD_BODY_LIMIT: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotXattr {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotColdAttributes {
    pub inode: u64,
    pub symlink_target: Option<Vec<u8>>,
    pub xattrs: Vec<SnapshotXattr>,
    pub acl: Vec<AclRule>,
}

impl SnapshotColdAttributes {
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        self.validate()?;
        let mut w = Writer::default();
        w.bytes(b"CA05");
        w.u64(self.inode);
        w.u8(u8::from(self.symlink_target.is_some()));
        w.bytes(&[0; 3]);
        let target = self.symlink_target.as_deref().unwrap_or_default();
        w.u32(target.len() as u32);
        w.bytes(target);
        w.u32(self.xattrs.len() as u32);
        for xattr in &self.xattrs {
            w.u16(xattr.name.len() as u16);
            w.u32(xattr.value.len() as u32);
            w.bytes(&xattr.name);
            w.bytes(&xattr.value);
        }
        w.u32(self.acl.len() as u32);
        for rule in &self.acl {
            w.u8(rule.acl_type);
            w.u32(rule.qualifier);
            w.u32(rule.permissions);
        }
        encode_snapshot_object(
            SnapshotObjectKind::ColdAttributes,
            &w.finish(),
            SNAPSHOT_COLD_BODY_LIMIT,
        )
    }
    pub fn decode(
        reference: &SnapshotObjectRef,
        bytes: &[u8],
        expected_inode: u64,
    ) -> PackedResult<Self> {
        if reference.kind != SnapshotObjectKind::ColdAttributes {
            return Err(PackedWireError::Invalid(
                "wire 005 cold ref has wrong kind".into(),
            ));
        }
        let body = reference.verify(bytes, SNAPSHOT_COLD_BODY_LIMIT)?;
        let mut r = Reader::new(body);
        if r.take(4)? != b"CA05" {
            return Err(PackedWireError::UnsupportedFormat(
                "wire 005 cold payload mismatch".into(),
            ));
        }
        let inode = r.u64()?;
        if inode != expected_inode {
            return Err(PackedWireError::Invalid(
                "wire 005 cold object belongs to another inode".into(),
            ));
        }
        let tag = r.u8()?;
        if tag > 1 {
            return Err(PackedWireError::UnsupportedFormat(
                "wire 005 cold symlink tag is unknown".into(),
            ));
        }
        r.skip_zeroes(3)?;
        let target_len = r.u32()? as usize;
        if target_len > 4096 || (tag == 0 && target_len != 0) {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 symlink target exceeds exact budget".into(),
            ));
        }
        let target = r.take(target_len)?.to_vec();
        let symlink_target = (tag == 1).then_some(target);
        let count = r.u32()? as usize;
        if count > 1024 || count > body.len() / 7 {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 xattr count exceeds payload budget".into(),
            ));
        }
        let mut xattrs = Vec::with_capacity(count);
        for _ in 0..count {
            let name_len = r.u16()? as usize;
            let value_len = r.u32()? as usize;
            if name_len > 255 || value_len > 65536 {
                return Err(PackedWireError::LimitExceeded(
                    "wire 005 xattr bytes exceed limits".into(),
                ));
            }
            xattrs.push(SnapshotXattr {
                name: r.take(name_len)?.to_vec(),
                value: r.take(value_len)?.to_vec(),
            });
        }
        let count = r.u32()? as usize;
        if count > 1024 || count > body.len() / 9 {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 ACL count exceeds payload budget".into(),
            ));
        }
        let mut acl = Vec::with_capacity(count);
        for _ in 0..count {
            acl.push(AclRule {
                acl_type: r.u8()?,
                qualifier: r.u32()?,
                permissions: r.u32()?,
            });
        }
        if !r.is_empty() {
            return Err(PackedWireError::Invalid(
                "wire 005 cold object has trailing fields".into(),
            ));
        }
        let attrs = Self {
            inode,
            symlink_target,
            xattrs,
            acl,
        };
        attrs.validate()?;
        Ok(attrs)
    }
    fn validate(&self) -> PackedResult<()> {
        if self.inode == 0
            || self.inode > i64::MAX as u64
            || self.xattrs.len() > 1024
            || self.acl.len() > 1024
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 cold inode/count exceeds limits".into(),
            ));
        }
        let mut bytes = 28usize;
        if let Some(target) = &self.symlink_target {
            if target.is_empty() || target.len() > 4096 || target.contains(&0) {
                return Err(PackedWireError::Invalid(
                    "wire 005 symlink target is empty, overlong or contains NUL".into(),
                ));
            }
            bytes += target.len();
        }
        let mut last: Option<&[u8]> = None;
        for xattr in &self.xattrs {
            if xattr.name.is_empty()
                || xattr.name.len() > 255
                || xattr.name.contains(&0)
                || xattr.value.len() > 65536
                || last.is_some_and(|name| name >= xattr.name.as_slice())
            {
                return Err(PackedWireError::Invalid(
                    "wire 005 xattrs are not bounded/canonically ordered".into(),
                ));
            }
            bytes += 6 + xattr.name.len() + xattr.value.len();
            last = Some(&xattr.name);
            if xattr.name == crate::meta::posix_acl::ACCESS_XATTR
                || xattr.name == crate::meta::posix_acl::DEFAULT_XATTR
            {
                crate::meta::posix_acl::PosixAcl::decode(&xattr.value).map_err(|reason| {
                    PackedWireError::Invalid(format!("wire 005 POSIX ACL: {reason}"))
                })?;
            }
        }
        let mut previous = None;
        for rule in &self.acl {
            let key = (rule.acl_type, rule.qualifier);
            if rule.permissions > 7 || previous.is_some_and(|last| last >= key) {
                return Err(PackedWireError::Invalid(
                    "wire 005 ACL rules are not canonical rwx entries".into(),
                ));
            }
            previous = Some(key);
            bytes += 9;
        }
        if bytes > SNAPSHOT_COLD_BODY_LIMIT {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 cold attributes exceed page budget".into(),
            ));
        }
        Ok(())
    }

    /// Cold ACL bytes and hot permission bits describe the same inode.
    pub(super) fn validate_for_inode(&self, kind: u8, mode: u32) -> PackedResult<()> {
        self.validate()?;
        for xattr in &self.xattrs {
            if xattr.name == crate::meta::posix_acl::DEFAULT_XATTR && kind != 2 {
                return Err(PackedWireError::Invalid(
                    "default POSIX ACL requires a directory".into(),
                ));
            }
            if xattr.name == crate::meta::posix_acl::ACCESS_XATTR {
                let acl = crate::meta::posix_acl::PosixAcl::decode(&xattr.value)
                    .map_err(|reason| PackedWireError::Invalid(reason.into()))?;
                if kind == 3 || acl.mode_bits() != mode & 0o777 {
                    return Err(PackedWireError::Invalid(
                        "POSIX ACL disagrees with hot inode kind/mode".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cold_attributes_roundtrip_raw_symlink_xattrs_acl_and_identity() {
        let attrs = SnapshotColdAttributes {
            inode: 7,
            symlink_target: Some(b"../raw-\xff".to_vec()),
            xattrs: vec![SnapshotXattr {
                name: b"user.test".to_vec(),
                value: vec![0, 255, 1],
            }],
            acl: vec![AclRule {
                acl_type: 1,
                qualifier: 1000,
                permissions: 7,
            }],
        };
        let bytes = attrs.encode().unwrap();
        let reference = SnapshotObjectRef::from_bytes(
            "cold".into(),
            SnapshotObjectKind::ColdAttributes,
            &bytes,
        )
        .unwrap();
        assert_eq!(
            SnapshotColdAttributes::decode(&reference, &bytes, 7).unwrap(),
            attrs
        );
        assert!(SnapshotColdAttributes::decode(&reference, &bytes, 8).is_err());
        let mut changed = attrs.clone();
        changed.xattrs[0].value.push(2);
        assert!(matches!(
            SnapshotColdAttributes::decode(&reference, &changed.encode().unwrap(), 7),
            Err(PackedWireError::HashMismatch { .. }) | Err(PackedWireError::Invalid(_))
        ));
    }
    #[test]
    fn cold_attributes_reject_duplicates_invalid_targets_and_budgets() {
        let mut attrs = SnapshotColdAttributes {
            inode: 1,
            symlink_target: None,
            xattrs: vec![SnapshotXattr {
                name: b"user.test".to_vec(),
                value: vec![],
            }],
            acl: vec![],
        };
        attrs.xattrs.push(attrs.xattrs[0].clone());
        assert!(attrs.encode().is_err());
        attrs.xattrs.pop();
        attrs.symlink_target = Some(b"bad\0target".to_vec());
        assert!(attrs.encode().is_err());
        attrs.symlink_target = None;
        attrs.xattrs[0].value = vec![0; 65537];
        assert!(attrs.encode().is_err());
    }

    #[test]
    fn cold_codec_rejects_malformed_linux_acl_xattrs() {
        for name in [
            crate::meta::posix_acl::ACCESS_XATTR,
            crate::meta::posix_acl::DEFAULT_XATTR,
        ] {
            let attrs = SnapshotColdAttributes {
                inode: 1,
                symlink_target: None,
                xattrs: vec![SnapshotXattr {
                    name: name.to_vec(),
                    value: b"invalid-acl".to_vec(),
                }],
                acl: vec![],
            };
            assert!(
                attrs.encode().is_err(),
                "malformed ACL passed cold encoding"
            );
        }
    }

    #[test]
    fn cold_acl_context_rejects_default_on_files_and_hot_mode_disagreement() {
        let mut value = 2u32.to_le_bytes().to_vec();
        for (tag, permissions, id) in [
            (1u16, 6u16, u32::MAX),
            (2, 4, 1000),
            (4, 0, u32::MAX),
            (16, 4, u32::MAX),
            (32, 0, u32::MAX),
        ] {
            value.extend_from_slice(&tag.to_le_bytes());
            value.extend_from_slice(&permissions.to_le_bytes());
            value.extend_from_slice(&id.to_le_bytes());
        }
        let mut attrs = SnapshotColdAttributes {
            inode: 2,
            symlink_target: None,
            xattrs: vec![SnapshotXattr {
                name: crate::meta::posix_acl::ACCESS_XATTR.to_vec(),
                value,
            }],
            acl: vec![],
        };
        assert!(attrs.validate_for_inode(1, 0o100640).is_ok());
        assert!(attrs.validate_for_inode(1, 0o100644).is_err());
        assert!(attrs.validate_for_inode(3, 0o120640).is_err());
        attrs.xattrs[0].name = crate::meta::posix_acl::DEFAULT_XATTR.to_vec();
        assert!(attrs.validate_for_inode(1, 0o100640).is_err());
        assert!(attrs.validate_for_inode(2, 0o040755).is_ok());
    }
}
