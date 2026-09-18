//! The upload executor: stable plans, receipts, and ambiguity resolution
//! (spec 08 §7–§8).
//!
//! The executor never re-allocates a logical identity after an ambiguous
//! result. Every uncertain outcome — a part timeout, a lost complete
//! response, a `NoSuchUpload` — is resolved by *querying* the same
//! upload/object identity and comparing against the plan's facts; only a
//! definitively failed upload restarts, and it restarts as a new attempt
//! over the same object key and hash.
//!
//! A 412 on a create-only PUT is an idempotent success only when the
//! stored object matches the same length and hash; anything else is a
//! permanent [`IngestError::PreconditionMismatch`]. A complete response
//! that reports failure — including a failure embedded in an HTTP 200
//! body — is [`IngestError::CompleteFailed`], never REMOTE_VERIFIED.

use sha2::{Digest, Sha256};

use crate::native_base::wire::refs::ObjectId;

use super::backend::{
    CompleteOutcome, MultipartSpec, MultipartStatus, PartPlan, PartReceipt, UploadBackend,
    UploadError,
};
use super::error::IngestError;

/// The two remote-verification evidence profiles (spec 08 §8). The two
/// are not interchangeable and the artifact records which one was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteVerificationProfile {
    /// Conservative default: the client streams the complete object back
    /// and its SHA-256 must equal the locally sealed `full_hash`.
    ExactReadback,
    /// Explicitly enabled: the upload was created with the object's
    /// SHA-256, every part was service-verified against a fixed
    /// range/length/checksum, and the composite, part count and total
    /// length were confirmed at complete time.
    ServiceValidatedChecksums,
}

/// How one planned object reaches the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutMode {
    /// One atomic create-only PUT (spec 08 §6, E02).
    CreateOnly,
    /// Multipart upload with a persisted part plan.
    Multipart,
}

/// One object in the stable upload plan (spec 08 §3/§12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedObject {
    pub object_id: ObjectId,
    pub key: Vec<u8>,
    pub full_hash: [u8; 32],
    pub len: u64,
    pub mode: PutMode,
    /// The exact part plan; for [`PutMode::CreateOnly`] a single
    /// synthetic entry covering the whole object.
    pub parts: Vec<PartPlan>,
}

impl PlannedObject {
    pub fn single_put(object_id: ObjectId, key: Vec<u8>, full_hash: [u8; 32], len: u64) -> Self {
        PlannedObject {
            object_id,
            key,
            full_hash,
            len,
            mode: PutMode::CreateOnly,
            parts: vec![PartPlan {
                part_number: 1,
                offset: 0,
                len,
                sha256: full_hash,
            }],
        }
    }

    pub fn multipart(
        object_id: ObjectId,
        key: Vec<u8>,
        full_hash: [u8; 32],
        len: u64,
        part_size: u64,
    ) -> Self {
        assert!(part_size > 0, "part size must be positive");
        let mut parts = Vec::new();
        let mut offset = 0u64;
        let mut number = 1u32;
        while offset < len {
            let take = part_size.min(len - offset);
            parts.push(PartPlan {
                part_number: number,
                offset,
                len: take,
                sha256: [0u8; 32],
            });
            offset += take;
            number += 1;
        }
        if parts.is_empty() {
            // Zero-length objects still complete as a single empty part.
            parts.push(PartPlan {
                part_number: 1,
                offset: 0,
                len: 0,
                sha256: [0u8; 32],
            });
        }
        PlannedObject {
            object_id,
            key,
            full_hash,
            len,
            mode: PutMode::Multipart,
            parts,
        }
    }

    /// Fill in the per-part checksums from the object bytes. Called once
    /// when the plan is frozen; the digests are then immutable.
    pub fn bind_part_checksums(&mut self, bytes: &[u8]) {
        assert_eq!(bytes.len() as u64, self.len, "plan/object length mismatch");
        for part in &mut self.parts {
            let start = part.offset as usize;
            let end = start + part.len as usize;
            part.sha256 = Sha256::digest(&bytes[start..end]).into();
        }
    }
}

/// The evidence that one object is REMOTE_VERIFIED.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationEvidence {
    /// A full readback matched the locally sealed hash.
    ExactReadback,
    /// The service verified the create checksum, every part, and the
    /// composite; the composite equals the local hash but is not the
    /// source of `ObjectRef.full_hash`.
    ServiceValidated { composite_sha256: [u8; 32] },
}

/// One fully uploaded and verified object.
#[derive(Debug, Clone)]
pub struct UploadedObject {
    pub planned: PlannedObject,
    pub receipts: Vec<PartReceipt>,
    pub evidence: VerificationEvidence,
}

/// Bounded retry counts; ambiguity resolution must terminate.
const MAX_PART_RETRIES: usize = 4;
const MAX_COMPLETE_RETRIES: usize = 4;
const MAX_UPLOAD_RESTARTS: usize = 3;

pub struct UploadExecutor<'a> {
    backend: &'a dyn UploadBackend,
    profile: RemoteVerificationProfile,
}

impl<'a> UploadExecutor<'a> {
    pub fn new(backend: &'a dyn UploadBackend, profile: RemoteVerificationProfile) -> Self {
        UploadExecutor { backend, profile }
    }

    /// Upload and verify one object. `bytes` must be the locally sealed
    /// object this plan was frozen for.
    pub async fn upload_object(
        &self,
        planned: &PlannedObject,
        bytes: &[u8],
    ) -> Result<UploadedObject, IngestError> {
        if bytes.len() as u64 != planned.len {
            return Err(IngestError::PlanMismatch(format!(
                "object {} bytes {} does not match the planned length {}",
                hex(&planned.object_id),
                bytes.len(),
                planned.len
            )));
        }
        let local_hash: [u8; 32] = Sha256::digest(bytes).into();
        if local_hash != planned.full_hash {
            return Err(IngestError::LocalVerificationFailed(format!(
                "object {} content does not match its planned hash",
                hex(&planned.object_id)
            )));
        }

        let receipts = match planned.mode {
            PutMode::CreateOnly => self.upload_create_only(planned, bytes).await?,
            PutMode::Multipart => self.upload_multipart(planned, bytes).await?,
        };

        let evidence = self.verify_remote(planned).await?;
        Ok(UploadedObject {
            planned: planned.clone(),
            receipts,
            evidence,
        })
    }

    async fn upload_create_only(
        &self,
        planned: &PlannedObject,
        bytes: &[u8],
    ) -> Result<Vec<PartReceipt>, IngestError> {
        match self.backend.create_only_put(&planned.key, bytes).await {
            Ok(()) => Ok(Vec::new()),
            Err(UploadError::PreconditionFailed) => {
                // 412: idempotent only for the same content (spec 08 §7).
                match self.backend.inspect(&planned.key).await? {
                    Some(stat) if stat.len == planned.len && stat.sha256 == planned.full_hash => {
                        Ok(Vec::new())
                    }
                    Some(stat) => Err(IngestError::PreconditionMismatch(format!(
                        "object {} exists with len {} hash {}, planned len {} hash {}",
                        hex(&planned.object_id),
                        stat.len,
                        hex(&stat.sha256),
                        planned.len,
                        hex(&planned.full_hash)
                    ))),
                    None => Err(IngestError::PreconditionMismatch(format!(
                        "412 on object {} but the object is not inspectable",
                        hex(&planned.object_id)
                    ))),
                }
            }
            Err(other) => Err(other.into()),
        }
    }

    async fn upload_multipart(
        &self,
        planned: &PlannedObject,
        bytes: &[u8],
    ) -> Result<Vec<PartReceipt>, IngestError> {
        let mut restarts = 0usize;
        loop {
            match self.attempt_multipart(planned, bytes).await {
                Ok(receipts) => return Ok(receipts),
                Err(IngestError::CompleteFailed(message)) => {
                    // A definitively failed complete never becomes verified
                    // by retrying the same parts (INGEST-006).
                    return Err(IngestError::CompleteFailed(message));
                }
                Err(err) if err.is_retryable() => {
                    restarts += 1;
                    if restarts > MAX_UPLOAD_RESTARTS {
                        return Err(IngestError::AmbiguousUpload(format!(
                            "multipart for {} did not converge: {err}",
                            hex(&planned.object_id)
                        )));
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn attempt_multipart(
        &self,
        planned: &PlannedObject,
        bytes: &[u8],
    ) -> Result<Vec<PartReceipt>, IngestError> {
        let spec = MultipartSpec {
            sha256: planned.full_hash,
            part_count: planned.parts.len() as u32,
            total_len: planned.len,
        };
        let upload = self.backend.multipart_create(&planned.key, &spec).await?;

        let mut receipts: Vec<PartReceipt> = Vec::new();
        for part in &planned.parts {
            let chunk = &bytes[part.offset as usize..part.offset as usize + part.len as usize];
            let mut retries = 0usize;
            let receipt = loop {
                match self
                    .backend
                    .multipart_upload_part(&upload, part, chunk)
                    .await
                {
                    Ok(receipt) => break receipt,
                    Err(UploadError::Timeout) => {
                        // Query the upload state; never blindly re-send
                        // (spec 08 §7).
                        retries += 1;
                        if retries > MAX_PART_RETRIES {
                            return Err(IngestError::AmbiguousUpload(format!(
                                "part {} of object {} kept timing out",
                                part.part_number,
                                hex(&planned.object_id)
                            )));
                        }
                        match self.backend.multipart_status(&upload).await? {
                            MultipartStatus::InProgress { received_parts } => {
                                if received_parts.contains(&part.part_number) {
                                    // The part landed despite the timeout;
                                    // recover its identity from the plan.
                                    break PartReceipt {
                                        part_number: part.part_number,
                                        etag: Vec::new(),
                                        sha256: part.sha256,
                                    };
                                }
                                // Not landed: retry the same part number.
                            }
                            MultipartStatus::Completed { .. } => {
                                // The whole upload completed out from
                                // under us; verify and finish.
                                return Ok(receipts_for(&planned.parts));
                            }
                            MultipartStatus::Aborted => {
                                return Err(IngestError::AmbiguousUpload(format!(
                                    "upload for {} was aborted",
                                    hex(&planned.object_id)
                                )));
                            }
                        }
                    }
                    Err(other) => return Err(other.into()),
                }
            };
            receipts.push(receipt);
        }
        receipts.sort_by_key(|receipt| receipt.part_number);

        // Complete with the exact part list; parse the full response
        // (spec 08 §7).
        let mut retries = 0usize;
        loop {
            match self.backend.multipart_complete(&upload, &receipts).await {
                Ok(CompleteOutcome::Completed { .. }) => return Ok(receipts.clone()),
                Ok(CompleteOutcome::Failed(message)) => {
                    return Err(IngestError::CompleteFailed(message));
                }
                Err(UploadError::Timeout) => {
                    retries += 1;
                    if retries > MAX_COMPLETE_RETRIES {
                        return Err(IngestError::AmbiguousUpload(format!(
                            "complete for {} kept timing out",
                            hex(&planned.object_id)
                        )));
                    }
                    // Query the same identity; a completed upload may have
                    // lost its response (INGEST-005).
                    match self.backend.multipart_status(&upload).await? {
                        MultipartStatus::Completed { .. } => return Ok(receipts.clone()),
                        MultipartStatus::InProgress { .. } => continue,
                        MultipartStatus::Aborted => {
                            return Err(IngestError::AmbiguousUpload(format!(
                                "upload for {} was aborted during complete",
                                hex(&planned.object_id)
                            )));
                        }
                    }
                }
                Err(UploadError::NoSuchUpload) => {
                    // NoSuchUpload may mean completed or deleted; check the
                    // final object (spec 08 §7).
                    match self.backend.inspect(&planned.key).await? {
                        Some(stat)
                            if stat.len == planned.len && stat.sha256 == planned.full_hash =>
                        {
                            return Ok(receipts.clone());
                        }
                        Some(stat) => {
                            return Err(IngestError::PreconditionMismatch(format!(
                                "object {} exists with len {} hash {} after NoSuchUpload",
                                hex(&planned.object_id),
                                stat.len,
                                hex(&stat.sha256)
                            )));
                        }
                        None => {
                            // Deleted (or never started): restart the
                            // multipart over the same identity.
                            return Err(IngestError::AmbiguousUpload(format!(
                                "upload for {} vanished; restarting",
                                hex(&planned.object_id)
                            )));
                        }
                    }
                }
                Err(other) => return Err(other.into()),
            }
        }
    }

    /// Produce the REMOTE_VERIFIED evidence for one uploaded object
    /// (spec 08 §8).
    async fn verify_remote(
        &self,
        planned: &PlannedObject,
    ) -> Result<VerificationEvidence, IngestError> {
        match self.profile {
            RemoteVerificationProfile::ExactReadback => {
                let readback = self.backend.read_full(&planned.key).await?;
                let digest: [u8; 32] = Sha256::digest(&readback).into();
                if readback.len() as u64 != planned.len || digest != planned.full_hash {
                    return Err(IngestError::RemoteVerificationFailed(format!(
                        "readback of object {} does not match (len {}, hash {})",
                        hex(&planned.object_id),
                        readback.len(),
                        hex(&digest)
                    )));
                }
                Ok(VerificationEvidence::ExactReadback)
            }
            RemoteVerificationProfile::ServiceValidatedChecksums => {
                // The service proved the create checksum, every part and
                // the composite during upload. Confirm the final object's
                // facts as declared by the service.
                match self.backend.inspect(&planned.key).await? {
                    Some(stat) if stat.len == planned.len && stat.sha256 == planned.full_hash => {
                        Ok(VerificationEvidence::ServiceValidated {
                            composite_sha256: stat.sha256,
                        })
                    }
                    Some(stat) => Err(IngestError::RemoteVerificationFailed(format!(
                        "service-validated object {} has len {} hash {}",
                        hex(&planned.object_id),
                        stat.len,
                        hex(&stat.sha256)
                    ))),
                    None => Err(IngestError::RemoteVerificationFailed(format!(
                        "service-validated object {} is missing",
                        hex(&planned.object_id)
                    ))),
                }
            }
        }
    }
}

fn receipts_for(parts: &[PartPlan]) -> Vec<PartReceipt> {
    parts
        .iter()
        .map(|part| PartReceipt {
            part_number: part.part_number,
            etag: Vec::new(),
            sha256: part.sha256,
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::super::backend::{
        InjectedCompleteFault, InjectedPartFailure, MemoryUploadBackend, ObjectStat, UploadId,
    };
    use super::*;

    /// A backend whose object facts disagree with the stored content: the
    /// reported composite is not the locally sealed full hash.
    struct MismatchingFactsBackend {
        inner: MemoryUploadBackend,
    }

    #[async_trait]
    impl UploadBackend for MismatchingFactsBackend {
        async fn create_only_put(&self, key: &[u8], bytes: &[u8]) -> Result<(), UploadError> {
            self.inner.create_only_put(key, bytes).await
        }

        async fn range_get_exact(
            &self,
            key: &[u8],
            offset: u64,
            buf: &mut [u8],
        ) -> Result<(), UploadError> {
            self.inner.range_get_exact(key, offset, buf).await
        }

        async fn read_full(&self, key: &[u8]) -> Result<Vec<u8>, UploadError> {
            self.inner.read_full(key).await
        }

        async fn inspect(&self, key: &[u8]) -> Result<Option<ObjectStat>, UploadError> {
            Ok(self.inner.inspect(key).await?.map(|mut stat| {
                stat.sha256[0] ^= 0xff;
                stat
            }))
        }

        async fn multipart_create(
            &self,
            key: &[u8],
            spec: &MultipartSpec,
        ) -> Result<UploadId, UploadError> {
            self.inner.multipart_create(key, spec).await
        }

        async fn multipart_upload_part(
            &self,
            upload: &UploadId,
            part: &PartPlan,
            bytes: &[u8],
        ) -> Result<PartReceipt, UploadError> {
            self.inner.multipart_upload_part(upload, part, bytes).await
        }

        async fn multipart_complete(
            &self,
            upload: &UploadId,
            parts: &[PartReceipt],
        ) -> Result<CompleteOutcome, UploadError> {
            self.inner.multipart_complete(upload, parts).await
        }

        async fn multipart_status(
            &self,
            upload: &UploadId,
        ) -> Result<MultipartStatus, UploadError> {
            self.inner.multipart_status(upload).await
        }
    }

    fn object(key: &[u8], bytes: &[u8]) -> (PlannedObject, Vec<u8>) {
        let mut planned = PlannedObject::single_put(
            [1u8; 16],
            key.to_vec(),
            Sha256::digest(bytes).into(),
            bytes.len() as u64,
        );
        planned.bind_part_checksums(bytes);
        (planned, bytes.to_vec())
    }

    fn multipart_object(key: &[u8], bytes: &[u8], part_size: u64) -> (PlannedObject, Vec<u8>) {
        let mut planned = PlannedObject::multipart(
            [2u8; 16],
            key.to_vec(),
            Sha256::digest(bytes).into(),
            bytes.len() as u64,
            part_size,
        );
        planned.bind_part_checksums(bytes);
        (planned, bytes.to_vec())
    }

    #[tokio::test]
    async fn create_only_upload_and_readback_verification() {
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, bytes) = object(b"obj", b"hello world");
        let uploaded = executor.upload_object(&planned, &bytes).await.unwrap();
        assert_eq!(uploaded.evidence, VerificationEvidence::ExactReadback);
    }

    #[tokio::test]
    async fn corrupted_payload_fails_exact_readback() {
        // INGEST-008: length is right, payload is not; only a full
        // readback catches it.
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, bytes) = object(b"obj", b"hello world");
        backend.create_only_put(&planned.key, &bytes).await.unwrap();
        backend.corrupt_payload(&planned.key);
        // The pre-existing object has a different hash now; the upload
        // path itself must refuse.
        let err = executor.upload_object(&planned, &bytes).await.unwrap_err();
        assert!(
            matches!(
                err,
                IngestError::PreconditionMismatch(_) | IngestError::RemoteVerificationFailed(_)
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn precondition_412_same_content_is_idempotent_and_different_is_rejected() {
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);

        // Same content: the executor accepts the 412 as idempotent.
        let (planned, bytes) = object(b"same", b"identical bytes");
        backend.create_only_put(b"same", &bytes).await.unwrap();
        executor.upload_object(&planned, &bytes).await.unwrap();

        // Different content: permanent rejection.
        let (other, other_bytes) = object(b"same", b"different bytes!");
        let err = executor
            .upload_object(&other, &other_bytes)
            .await
            .unwrap_err();
        assert!(matches!(err, IngestError::PreconditionMismatch(_)), "{err}");
    }

    #[tokio::test]
    async fn lost_part_response_is_resolved_by_query_not_resend() {
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, bytes) = multipart_object(b"mp", &[4u8; 6000], 2000); // 3 parts
        // Part 2 times out but lands server-side.
        backend.inject_part_failure(InjectedPartFailure {
            part_number: 2,
            landed: true,
        });
        let uploaded = executor.upload_object(&planned, &bytes).await.unwrap();
        assert_eq!(uploaded.receipts.len(), 3);
        assert_eq!(backend.read_full(b"mp").await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn part_timeout_without_landing_retries_the_same_part_number() {
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, bytes) = multipart_object(b"mp", &[6u8; 4000], 2000); // 2 parts
        // Part 1 fails twice without landing, then succeeds.
        backend.inject_part_failure(InjectedPartFailure {
            part_number: 1,
            landed: false,
        });
        backend.inject_part_failure(InjectedPartFailure {
            part_number: 1,
            landed: false,
        });
        executor.upload_object(&planned, &bytes).await.unwrap();
        assert_eq!(backend.read_full(b"mp").await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn lost_complete_response_resumes_by_status() {
        // INGEST-005: complete succeeds server-side, the response is lost.
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, bytes) = multipart_object(b"mp", &[8u8; 3000], 3000);
        backend.inject_complete_fault(InjectedCompleteFault::ResponseLost);
        let uploaded = executor.upload_object(&planned, &bytes).await.unwrap();
        assert_eq!(uploaded.evidence, VerificationEvidence::ExactReadback);
        // Exactly one object, assembled once.
        assert_eq!(backend.read_full(b"mp").await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn http200_embedded_error_never_becomes_verified() {
        // INGEST-006.
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, bytes) = multipart_object(b"mp", &[8u8; 3000], 3000);
        backend.inject_complete_fault(InjectedCompleteFault::Http200WithEmbeddedError(
            "InternalError".into(),
        ));
        let err = executor.upload_object(&planned, &bytes).await.unwrap_err();
        assert!(matches!(err, IngestError::CompleteFailed(_)), "{err}");
        assert_eq!(backend.inspect(b"mp").await.unwrap(), None);
    }

    #[tokio::test]
    async fn no_such_upload_after_completion_is_resolved_by_final_object() {
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, bytes) = multipart_object(b"mp", &[3u8; 3000], 3000);
        backend.inject_complete_fault(InjectedCompleteFault::NoSuchUploadAfterCompletion);
        let uploaded = executor.upload_object(&planned, &bytes).await.unwrap();
        assert_eq!(uploaded.evidence, VerificationEvidence::ExactReadback);
    }

    #[tokio::test]
    async fn service_validated_profile_uses_service_facts_not_readback() {
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(
            &backend,
            RemoteVerificationProfile::ServiceValidatedChecksums,
        );
        let (planned, bytes) = multipart_object(b"mp", &[3u8; 5000], 2500);
        let uploaded = executor.upload_object(&planned, &bytes).await.unwrap();
        match uploaded.evidence {
            VerificationEvidence::ServiceValidated { composite_sha256 } => {
                // The composite equals the local full hash, but the
                // ObjectRef hash stays the locally computed one.
                assert_eq!(composite_sha256, planned.full_hash);
            }
            other => panic!("expected service-validated evidence, got {other:?}"),
        }
    }

    /// VFY-001: the multipart composite is a *different* fact from the
    /// locally sealed full hash. When the two disagree the object is refused,
    /// so the service value can never be written into `ObjectRef.full_hash`.
    #[tokio::test]
    async fn service_composite_that_differs_from_the_local_hash_is_refused() {
        let backend = MismatchingFactsBackend {
            inner: MemoryUploadBackend::new(),
        };
        let executor = UploadExecutor::new(
            &backend,
            RemoteVerificationProfile::ServiceValidatedChecksums,
        );
        let (planned, bytes) = object(b"obj", b"hello world");
        let err = executor.upload_object(&planned, &bytes).await.unwrap_err();
        assert!(
            matches!(err, IngestError::RemoteVerificationFailed(_)),
            "{err}"
        );
        // The identity used by every later read is still the locally sealed
        // full hash; the mismatching service composite produced no evidence.
        let local_full_hash: [u8; 32] = Sha256::digest(&bytes).into();
        assert_eq!(planned.full_hash, local_full_hash);
    }

    #[tokio::test]
    async fn plan_length_or_hash_mismatch_is_refused_before_upload() {
        let backend = MemoryUploadBackend::new();
        let executor = UploadExecutor::new(&backend, RemoteVerificationProfile::ExactReadback);
        let (planned, _) = object(b"obj", b"hello world");
        // Wrong bytes for the plan.
        let err = executor
            .upload_object(&planned, b"hello worlx")
            .await
            .unwrap_err();
        assert!(
            matches!(err, IngestError::LocalVerificationFailed(_)),
            "{err}"
        );
        // Wrong length.
        let err = executor
            .upload_object(&planned, b"short")
            .await
            .unwrap_err();
        assert!(matches!(err, IngestError::PlanMismatch(_)), "{err}");
    }

    #[tokio::test]
    async fn multipart_part_layout_covers_the_object_exactly() {
        let bytes = vec![1u8; 5001];
        let planned = PlannedObject::multipart(
            [9u8; 16],
            b"k".to_vec(),
            Sha256::digest(&bytes).into(),
            5001,
            2000,
        );
        assert_eq!(planned.parts.len(), 3);
        assert_eq!(planned.parts[0].len, 2000);
        assert_eq!(planned.parts[1].len, 2000);
        assert_eq!(planned.parts[2].len, 1001);
        assert_eq!(planned.parts.iter().map(|p| p.len).sum::<u64>(), 5001);

        let empty =
            PlannedObject::multipart([9u8; 16], b"k".to_vec(), Sha256::digest([]).into(), 0, 2000);
        assert_eq!(empty.parts.len(), 1);
        assert_eq!(empty.parts[0].len, 0);
    }
}
