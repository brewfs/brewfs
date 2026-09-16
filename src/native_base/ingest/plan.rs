//! The stable upload plan (spec 08 §3/§12).
//!
//! The plan is frozen before any byte leaves the machine and persisted
//! with a digest. On resume the digest and the algorithm version are
//! checked; a plan that changed underneath the session is a
//! [`IngestError::PlanMismatch`], never a silent re-partition, and old
//! upload receipts are never mixed into a differently-planned attempt.

use std::fs;
use std::io::Write;
use std::path::Path;

use sha2::{Digest, Sha256};

use super::backend::PartPlan;
use super::error::IngestError;
use super::source::ConsistencyPolicy;
use super::upload::{PlannedObject, PutMode, RemoteVerificationProfile};

/// Bumped whenever the plan encoding or the part-checksum algorithm
/// changes. A resume across versions is refused (spec 08 §12).
pub const PLAN_ALGORITHM_VERSION: u16 = 1;

/// SHA-256 over the algorithm version and the canonical plan encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadPlanDigest(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadPlan {
    pub volume_id: [u8; 16],
    pub consistency: ConsistencyPolicy,
    pub verification: RemoteVerificationProfile,
    /// Part size for large objects; 0 means a single create-only PUT.
    pub part_size: u64,
    pub objects: Vec<PlannedObject>,
}

impl RemoteVerificationProfile {
    pub fn as_u8(self) -> u8 {
        match self {
            RemoteVerificationProfile::ExactReadback => 1,
            RemoteVerificationProfile::ServiceValidatedChecksums => 2,
        }
    }

    pub fn from_u8(v: u8) -> Result<RemoteVerificationProfile, IngestError> {
        match v {
            1 => Ok(RemoteVerificationProfile::ExactReadback),
            2 => Ok(RemoteVerificationProfile::ServiceValidatedChecksums),
            other => Err(IngestError::PlanMismatch(format!(
                "unknown verification profile {other}"
            ))),
        }
    }
}

impl UploadPlan {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = crate::native_base::wire::uvarint::Writer::new();
        w.u16(PLAN_ALGORITHM_VERSION);
        w.put(&self.volume_id);
        w.u8(self.consistency.as_u8());
        w.u8(self.verification.as_u8());
        w.u64(self.part_size);
        w.u32(self.objects.len() as u32);
        for object in &self.objects {
            w.put(&object.object_id);
            w.bytes(&object.key);
            w.put(&object.full_hash);
            w.u64(object.len);
            w.u8(match object.mode {
                PutMode::CreateOnly => 1,
                PutMode::Multipart => 2,
            });
            w.u32(object.parts.len() as u32);
            for part in &object.parts {
                w.u32(part.part_number);
                w.u64(part.offset);
                w.u64(part.len);
                w.put(&part.sha256);
            }
        }
        w.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<UploadPlan, IngestError> {
        fn wire(e: crate::native_base::wire::error::WireError) -> IngestError {
            IngestError::PlanMismatch(format!("upload plan decode: {e}"))
        }
        let mut r = crate::native_base::wire::uvarint::Reader::new(bytes);
        let what = "upload plan";
        let version = r.u16(what).map_err(wire)?;
        if version != PLAN_ALGORITHM_VERSION {
            return Err(IngestError::PlanMismatch(format!(
                "plan algorithm version {version} is not the supported {PLAN_ALGORITHM_VERSION}"
            )));
        }
        let volume_id: [u8; 16] = r.take(16, what).map_err(wire)?.try_into().unwrap();
        let policy_byte = r.u8(what).map_err(wire)?;
        let consistency = ConsistencyPolicy::from_u8(policy_byte).ok_or_else(|| {
            IngestError::PlanMismatch(format!("unknown consistency policy {policy_byte}"))
        })?;
        let verification = RemoteVerificationProfile::from_u8(r.u8(what).map_err(wire)?)?;
        let part_size = r.u64(what).map_err(wire)?;
        let count = r.u32(what).map_err(wire)? as usize;
        if count > bytes.len() {
            return Err(IngestError::PlanMismatch(
                "object count exceeds input".into(),
            ));
        }
        let mut objects = Vec::with_capacity(count);
        for _ in 0..count {
            let object_id: [u8; 16] = r.take(16, what).map_err(wire)?.try_into().unwrap();
            let key = r.bytes(what).map_err(wire)?.to_vec();
            let full_hash: [u8; 32] = r.take(32, what).map_err(wire)?.try_into().unwrap();
            let len = r.u64(what).map_err(wire)?;
            let mode = match r.u8(what).map_err(wire)? {
                1 => PutMode::CreateOnly,
                2 => PutMode::Multipart,
                other => {
                    return Err(IngestError::PlanMismatch(format!(
                        "unknown put mode {other}"
                    )));
                }
            };
            let parts_count = r.u32(what).map_err(wire)? as usize;
            if parts_count > bytes.len() {
                return Err(IngestError::PlanMismatch("part count exceeds input".into()));
            }
            let mut parts = Vec::with_capacity(parts_count);
            for _ in 0..parts_count {
                let part_number = r.u32(what).map_err(wire)?;
                let offset = r.u64(what).map_err(wire)?;
                let part_len = r.u64(what).map_err(wire)?;
                let sha256: [u8; 32] = r.take(32, what).map_err(wire)?.try_into().unwrap();
                parts.push(PartPlan {
                    part_number,
                    offset,
                    len: part_len,
                    sha256,
                });
            }
            objects.push(PlannedObject {
                object_id,
                key,
                full_hash,
                len,
                mode,
                parts,
            });
        }
        Ok(UploadPlan {
            volume_id,
            consistency,
            verification,
            part_size,
            objects,
        })
    }

    pub fn digest(&self) -> UploadPlanDigest {
        let mut hasher = Sha256::new();
        hasher.update(PLAN_ALGORITHM_VERSION.to_le_bytes());
        hasher.update(self.encode());
        UploadPlanDigest(hasher.finalize().into())
    }

    /// Persist the frozen plan durably: temp file, fsync, rename,
    /// fsync(parent) — the same dance as the checkpoint (spec 08 §9).
    pub fn write_to(&self, path: &Path) -> Result<UploadPlanDigest, IngestError> {
        let digest = self.digest();
        let tmp = path.with_extension("plan.tmp");
        let mut file = fs::File::create(&tmp)
            .map_err(|e| IngestError::Backend(format!("create {}: {e}", tmp.display())))?;
        file.write_all(&self.encode())
            .map_err(|e| IngestError::Backend(format!("write {}: {e}", tmp.display())))?;
        file.sync_all()
            .map_err(|e| IngestError::Backend(format!("fsync {}: {e}", tmp.display())))?;
        drop(file);
        fs::rename(&tmp, path)
            .map_err(|e| IngestError::Backend(format!("rename to {}: {e}", path.display())))?;
        if let Some(parent) = path.parent()
            && let Ok(dir) = fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }
        Ok(digest)
    }

    pub fn read_from(path: &Path) -> Result<UploadPlan, IngestError> {
        let bytes = fs::read(path)
            .map_err(|e| IngestError::Backend(format!("read {}: {e}", path.display())))?;
        UploadPlan::decode(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> UploadPlan {
        let mut obj = PlannedObject::single_put(
            [7u8; 16],
            b"native-base/v3/k/pack/o/h.brfdp".to_vec(),
            [9u8; 32],
            100,
        );
        obj.parts[0].sha256 = [9u8; 32];
        UploadPlan {
            volume_id: [1u8; 16],
            consistency: ConsistencyPolicy::BestEffortDetected,
            verification: RemoteVerificationProfile::ExactReadback,
            part_size: 0,
            objects: vec![obj],
        }
    }

    #[test]
    fn plan_roundtrip_and_digest_stability() {
        let plan = sample();
        let decoded = UploadPlan::decode(&plan.encode()).unwrap();
        assert_eq!(decoded, plan);
        assert_eq!(decoded.digest(), plan.digest());
        // Deterministic: same content, same digest.
        assert_eq!(sample().digest(), plan.digest());
    }

    #[test]
    fn digest_changes_when_the_plan_changes() {
        let mut plan = sample();
        let before = plan.digest();
        plan.part_size = 4096;
        assert_ne!(plan.digest(), before);
    }

    #[test]
    fn unknown_algorithm_version_is_refused() {
        let mut bytes = sample().encode();
        // The version is the first uvarint field; rewrite it to 2.
        bytes[0] = 2;
        let err = UploadPlan::decode(&bytes).unwrap_err();
        assert!(matches!(err, IngestError::PlanMismatch(_)), "{err}");
    }

    #[test]
    fn plan_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload.plan");
        let plan = sample();
        let digest = plan.write_to(&path).unwrap();
        assert_eq!(UploadPlan::read_from(&path).unwrap().digest(), digest);
    }
}
