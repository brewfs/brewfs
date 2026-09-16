//! The object-backend capability interface for ingestion (spec 08 §6).
//!
//! The existing `ObjectBackend` trait cannot express multipart receipts
//! or conditional outcomes, and its range GET allows partial/missing to
//! read as zero — so ingestion defines its own, stricter contract here:
//!
//! - [`UploadBackend::create_only_put`] is an atomic create-only PUT.
//!   A backend that cannot provide it (and no verified publication
//!   alternative) must refuse to enable writable publication (E02) —
//!   HEAD-then-PUT emulation is not accepted.
//! - [`UploadBackend::range_get_exact`] fills the whole buffer or fails;
//!   short reads surface as errors, never as padded zeros.
//! - Multipart operations return *classified* results so the caller can
//!   resolve ambiguity by querying the same identity, never by
//!   re-allocating one (spec 08 §7).
//!
//! [`MemoryUploadBackend`] implements the contract in memory with fault
//! injection for every acceptance scenario: part failures at any index,
//! a complete response that is an HTTP 200 with an embedded error, a lost
//! complete response, `NoSuchUpload` after a real completion, and silent
//! payload corruption after storage.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use super::error::IngestError;

/// Opaque upload identity handed out by a backend.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UploadId(pub u64);

/// The SHA-256 the upload was created with, the exact part count and the
/// total length the backend must confirm at complete time
/// (service-validated profile, spec 08 §8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartSpec {
    pub sha256: [u8; 32],
    pub part_count: u32,
    pub total_len: u64,
}

/// One planned part: fixed byte range and its local SHA-256, persisted
/// *before* the part is uploaded (spec 08 §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartPlan {
    pub part_number: u32,
    pub offset: u64,
    pub len: u64,
    pub sha256: [u8; 32],
}

/// The receipt for one uploaded part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartReceipt {
    pub part_number: u32,
    pub etag: Vec<u8>,
    pub sha256: [u8; 32],
}

/// The object facts needed to resolve a 412 or a `NoSuchUpload`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStat {
    pub len: u64,
    pub sha256: [u8; 32],
}

/// The outcome of a multipart complete. A protocol-level failure — even
/// one embedded in an HTTP 200 body — is [`CompleteOutcome::Failed`]:
/// the object is NOT remote-verified (spec 08 §7, E03).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompleteOutcome {
    Completed {
        composite_sha256: [u8; 32],
        part_count: u32,
        total_len: u64,
    },
    Failed(String),
}

/// The state of an upload identity, used to resolve ambiguous results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartStatus {
    /// The part numbers the server has received so far.
    InProgress {
        received_parts: Vec<u32>,
    },
    Completed {
        sha256: [u8; 32],
        len: u64,
    },
    Aborted,
}

/// Classified upload errors (spec 08 §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadError {
    /// The key does not exist.
    NotFound,
    /// 412: the object already exists. Resolve by comparing content.
    PreconditionFailed,
    /// 409: never "already written" — re-verify and restart per the
    /// backend protocol.
    Conflict,
    /// NoSuchUpload: may be completed or deleted; check the final object.
    NoSuchUpload,
    /// Network timeout: the outcome is unknown; query, do not re-send
    /// blindly.
    Timeout,
    Backend(String),
}

impl From<UploadError> for IngestError {
    fn from(err: UploadError) -> IngestError {
        match err {
            UploadError::NotFound => IngestError::Backend("object not found".into()),
            UploadError::PreconditionFailed => {
                IngestError::PreconditionMismatch("create-only PUT hit an existing object".into())
            }
            UploadError::Conflict => IngestError::AmbiguousUpload("409 conflict".into()),
            UploadError::NoSuchUpload => IngestError::AmbiguousUpload("no such upload".into()),
            UploadError::Timeout => IngestError::AmbiguousUpload("timeout".into()),
            UploadError::Backend(message) => IngestError::Backend(message),
        }
    }
}

/// The ingestion object-backend contract (spec 08 §6).
#[async_trait]
pub trait UploadBackend: Send + Sync {
    /// Atomic create-only PUT. `Ok(())` means the object is stored, or it
    /// already existed with *identical* content (idempotent retry).
    /// [`UploadError::PreconditionFailed`] means it existed with
    /// different content.
    async fn create_only_put(&self, key: &[u8], bytes: &[u8]) -> Result<(), UploadError>;

    /// Fill `buf` completely from `offset`, or fail. No zero padding.
    async fn range_get_exact(
        &self,
        key: &[u8],
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), UploadError>;

    /// Read the whole object (bounded by the caller's budget).
    async fn read_full(&self, key: &[u8]) -> Result<Vec<u8>, UploadError>;

    /// The current facts of `key`, if it exists.
    async fn inspect(&self, key: &[u8]) -> Result<Option<ObjectStat>, UploadError>;

    /// Start a multipart upload whose full-object SHA-256, part count and
    /// total length the backend will verify at complete time.
    async fn multipart_create(
        &self,
        key: &[u8],
        spec: &MultipartSpec,
    ) -> Result<UploadId, UploadError>;

    /// Upload one part. The backend verifies the part's length and
    /// SHA-256 against the plan.
    async fn multipart_upload_part(
        &self,
        upload: &UploadId,
        part: &PartPlan,
        bytes: &[u8],
    ) -> Result<PartReceipt, UploadError>;

    /// Complete with the exact, ordered part list. The full response is
    /// parsed: a failure embedded in an HTTP 200 is still
    /// [`CompleteOutcome::Failed`].
    async fn multipart_complete(
        &self,
        upload: &UploadId,
        parts: &[PartReceipt],
    ) -> Result<CompleteOutcome, UploadError>;

    /// Query the state of an upload identity for ambiguity resolution.
    async fn multipart_status(&self, upload: &UploadId) -> Result<MultipartStatus, UploadError>;
}

/// A part failure to inject: which part number, and whether the part
/// actually landed server-side despite the timeout.
#[derive(Debug, Clone)]
pub struct InjectedPartFailure {
    pub part_number: u32,
    pub landed: bool,
}

/// What `multipart_complete` does when the fault fires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InjectedCompleteFault {
    /// The complete response is an HTTP 200 whose parsed body carries an
    /// error (E03 / INGEST-006).
    Http200WithEmbeddedError(String),
    /// The complete succeeds server-side but the response is lost: the
    /// client sees a timeout and must query status (INGEST-005).
    ResponseLost,
    /// Complete reports NoSuchUpload although the object was assembled:
    /// the caller must check the final object (spec 08 §7).
    NoSuchUploadAfterCompletion,
}

#[derive(Default)]
struct MemoryInner {
    objects: BTreeMap<Vec<u8>, Vec<u8>>,
    uploads: BTreeMap<u64, MemoryUpload>,
}

struct MemoryUpload {
    key: Vec<u8>,
    spec: MultipartSpec,
    parts: BTreeMap<u32, PartReceipt>,
    /// Part data by number, for assembly at complete time.
    part_data: BTreeMap<u32, Vec<u8>>,
    completed: Option<([u8; 32], u64)>,
    aborted: bool,
}

/// In-memory [`UploadBackend`] with fault injection.
pub struct MemoryUploadBackend {
    inner: Mutex<MemoryInner>,
    next_upload: AtomicU64,
    /// One-shot injected failure for `create_only_put`, armed to fire on
    /// a specific 1-based call count.
    fail_create_only: Mutex<Option<(u64, UploadError)>>,
    /// Number of `create_only_put` calls seen so far.
    create_only_calls: AtomicU64,
    /// Part failures by part number.
    fail_parts: Mutex<BTreeMap<u32, InjectedPartFailure>>,
    /// The complete fault, one-shot.
    complete_fault: Mutex<Option<InjectedCompleteFault>>,
    /// Keys whose stored payload is silently corrupted after storage
    /// (INGEST-008: HEAD/length still matches, content does not).
    corrupt_keys: Mutex<Vec<Vec<u8>>>,
    /// Whether create-only PUT is supported at all (E02).
    supports_create_only: bool,
}

impl Default for MemoryUploadBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryUploadBackend {
    pub fn new() -> MemoryUploadBackend {
        MemoryUploadBackend {
            inner: Mutex::new(MemoryInner::default()),
            next_upload: AtomicU64::new(1),
            fail_create_only: Mutex::new(None),
            create_only_calls: AtomicU64::new(0),
            fail_parts: Mutex::new(BTreeMap::new()),
            complete_fault: Mutex::new(None),
            corrupt_keys: Mutex::new(Vec::new()),
            supports_create_only: true,
        }
    }

    /// Disable create-only PUT: the backend cannot support atomic
    /// creation, so writable publication must be refused (E02).
    pub fn without_create_only(mut self) -> MemoryUploadBackend {
        self.supports_create_only = false;
        self
    }

    /// Fail the next `create_only_put` call with `err`.
    pub fn inject_create_only_failure(&self, err: UploadError) {
        self.inject_create_only_failure_on(0, err);
    }

    /// Fail the `nth` `create_only_put` call counting from now: 0 = the
    /// next call, 1 = the one after it, and so on.
    pub fn inject_create_only_failure_on(&self, nth: u64, err: UploadError) {
        let base = self.create_only_calls.load(Ordering::SeqCst);
        *self.fail_create_only.lock().unwrap() = Some((base + 1 + nth, err));
    }

    pub fn inject_part_failure(&self, failure: InjectedPartFailure) {
        self.fail_parts
            .lock()
            .unwrap()
            .insert(failure.part_number, failure);
    }

    pub fn inject_complete_fault(&self, fault: InjectedCompleteFault) {
        *self.complete_fault.lock().unwrap() = Some(fault);
    }

    /// Corrupt the stored payload of `key` in place (length preserved).
    pub fn corrupt_payload(&self, key: &[u8]) {
        self.corrupt_keys.lock().unwrap().push(key.to_vec());
        let mut inner = self.inner.lock().unwrap();
        if let Some(bytes) = inner.objects.get_mut(key) {
            // Flip a byte in the middle, keeping the length.
            if !bytes.is_empty() {
                let mid = bytes.len() / 2;
                bytes[mid] ^= 0xff;
            }
        }
    }

    pub fn object_keys(&self) -> Vec<Vec<u8>> {
        self.inner.lock().unwrap().objects.keys().cloned().collect()
    }

    pub fn stored_bytes(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.lock().unwrap().objects.get(key).cloned()
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[async_trait]
impl UploadBackend for MemoryUploadBackend {
    async fn create_only_put(&self, key: &[u8], bytes: &[u8]) -> Result<(), UploadError> {
        if !self.supports_create_only {
            return Err(UploadError::Backend(
                "backend does not support atomic create-only PUT".into(),
            ));
        }
        let call = self.create_only_calls.fetch_add(1, Ordering::SeqCst) + 1;
        // Take the injection out first so the guard is released before
        // any re-arm below (std Mutex is not reentrant).
        let pending = self.fail_create_only.lock().unwrap().take();
        if let Some((fire_on, injected)) = pending {
            if call == fire_on {
                return Err(injected);
            }
            // Not this call: re-arm for the targeted one.
            *self.fail_create_only.lock().unwrap() = Some((fire_on, injected));
        }
        let mut inner = self.inner.lock().unwrap();
        match inner.objects.get(key) {
            Some(existing) if existing == bytes => Ok(()), // idempotent retry
            Some(_) => Err(UploadError::PreconditionFailed),
            None => {
                inner.objects.insert(key.to_vec(), bytes.to_vec());
                Ok(())
            }
        }
    }

    async fn range_get_exact(
        &self,
        key: &[u8],
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), UploadError> {
        let inner = self.inner.lock().unwrap();
        let bytes = inner.objects.get(key).ok_or(UploadError::NotFound)?;
        let start = offset as usize;
        let end = start + buf.len();
        if end > bytes.len() {
            return Err(UploadError::Backend(
                "short read: requested range beyond the stored object".into(),
            ));
        }
        buf.copy_from_slice(&bytes[start..end]);
        Ok(())
    }

    async fn read_full(&self, key: &[u8]) -> Result<Vec<u8>, UploadError> {
        let inner = self.inner.lock().unwrap();
        inner.objects.get(key).cloned().ok_or(UploadError::NotFound)
    }

    async fn inspect(&self, key: &[u8]) -> Result<Option<ObjectStat>, UploadError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.objects.get(key).map(|bytes| ObjectStat {
            len: bytes.len() as u64,
            sha256: sha256(bytes),
        }))
    }

    async fn multipart_create(
        &self,
        key: &[u8],
        spec: &MultipartSpec,
    ) -> Result<UploadId, UploadError> {
        let id = self.next_upload.fetch_add(1, Ordering::SeqCst);
        self.inner.lock().unwrap().uploads.insert(
            id,
            MemoryUpload {
                key: key.to_vec(),
                spec: spec.clone(),
                parts: BTreeMap::new(),
                part_data: BTreeMap::new(),
                completed: None,
                aborted: false,
            },
        );
        Ok(UploadId(id))
    }

    async fn multipart_upload_part(
        &self,
        upload: &UploadId,
        part: &PartPlan,
        bytes: &[u8],
    ) -> Result<PartReceipt, UploadError> {
        // Service-side verification (spec 08 §8): the part must match the
        // plan's length and checksum.
        if bytes.len() as u64 != part.len {
            return Err(UploadError::Backend(format!(
                "part {} length {} does not match the plan {}",
                part.part_number,
                bytes.len(),
                part.len
            )));
        }
        let digest = sha256(bytes);
        if digest != part.sha256 {
            return Err(UploadError::Backend(format!(
                "part {} checksum mismatch",
                part.part_number
            )));
        }

        // Injected fault: the caller sees a timeout; the part may or may
        // not have landed.
        let upload_id = upload.0;
        if let Some(failure) = self.fail_parts.lock().unwrap().remove(&part.part_number) {
            if failure.landed {
                let mut inner = self.inner.lock().unwrap();
                if let Some(upload) = inner.uploads.get_mut(&upload_id) {
                    let receipt = PartReceipt {
                        part_number: part.part_number,
                        etag: format!("etag-{upload_id}-{}", part.part_number).into_bytes(),
                        sha256: digest,
                    };
                    upload.parts.insert(part.part_number, receipt.clone());
                    upload.part_data.insert(part.part_number, bytes.to_vec());
                }
            }
            return Err(UploadError::Timeout);
        }

        let mut inner = self.inner.lock().unwrap();
        let upload = inner
            .uploads
            .get_mut(&upload_id)
            .ok_or(UploadError::NoSuchUpload)?;
        if upload.completed.is_some() || upload.aborted {
            return Err(UploadError::NoSuchUpload);
        }
        let receipt = PartReceipt {
            part_number: part.part_number,
            etag: format!("etag-{upload_id}-{}", part.part_number).into_bytes(),
            sha256: digest,
        };
        upload.parts.insert(part.part_number, receipt.clone());
        upload.part_data.insert(part.part_number, bytes.to_vec());
        Ok(receipt)
    }

    async fn multipart_complete(
        &self,
        upload: &UploadId,
        parts: &[PartReceipt],
    ) -> Result<CompleteOutcome, UploadError> {
        let fault = self.complete_fault.lock().unwrap().take();
        let mut inner = self.inner.lock().unwrap();
        let (key, spec, received) = {
            let state = inner
                .uploads
                .get(&upload.0)
                .ok_or(UploadError::NoSuchUpload)?;
            if state.aborted {
                return Err(UploadError::NoSuchUpload);
            }
            if let Some((_, len)) = state.completed {
                let sha = inner
                    .objects
                    .get(&state.key)
                    .map(|bytes| sha256(bytes))
                    .unwrap_or([0u8; 32]);
                // A repeated complete of an already completed upload is
                // idempotent when the part list matches.
                return Ok(CompleteOutcome::Completed {
                    composite_sha256: sha,
                    part_count: parts.len() as u32,
                    total_len: len,
                });
            }
            let mut received: Vec<PartReceipt> = state.parts.values().cloned().collect();
            received.sort_by_key(|receipt| receipt.part_number);
            (state.key.clone(), state.spec.clone(), received)
        };

        // Assemble from the *server-side* part records; the caller's list
        // must match on part numbers and content checksums. The ETag is
        // server-assigned opaque data the caller cannot always recover
        // after a timeout (the executor reconstructs the receipt from the
        // plan + upload status), so it is not part of the match.
        let same_parts = received.len() == parts.len()
            && received.iter().zip(parts).all(|(server, listed)| {
                server.part_number == listed.part_number && server.sha256 == listed.sha256
            });
        if !same_parts {
            return Ok(CompleteOutcome::Failed(format!(
                "part list mismatch: server has {} parts, complete listed {}",
                received.len(),
                parts.len()
            )));
        }
        let mut assembled = Vec::new();
        {
            let state = inner.uploads.get(&upload.0).unwrap();
            for receipt in &received {
                assembled.extend_from_slice(&state.part_data[&receipt.part_number]);
            }
        }
        let total_len = assembled.len() as u64;
        if total_len != spec.total_len {
            return Ok(CompleteOutcome::Failed(format!(
                "total length {total_len} does not match the declared {}",
                spec.total_len
            )));
        }
        if received.len() as u32 != spec.part_count {
            return Ok(CompleteOutcome::Failed(format!(
                "part count {} does not match the declared {}",
                received.len(),
                spec.part_count
            )));
        }
        let composite = sha256(&assembled);
        if composite != spec.sha256 {
            return Ok(CompleteOutcome::Failed(
                "composite checksum mismatch".into(),
            ));
        }

        // The assembly succeeded server-side. Apply the injected fault.
        if let Some(fault) = fault {
            match fault {
                InjectedCompleteFault::Http200WithEmbeddedError(message) => {
                    // The object is NOT stored: the operation failed even
                    // though the transport said 200.
                    return Ok(CompleteOutcome::Failed(message));
                }
                InjectedCompleteFault::ResponseLost => {
                    inner.objects.insert(key.clone(), assembled);
                    let state = inner.uploads.get_mut(&upload.0).unwrap();
                    state.completed = Some((composite, total_len));
                    return Err(UploadError::Timeout);
                }
                InjectedCompleteFault::NoSuchUploadAfterCompletion => {
                    inner.objects.insert(key.clone(), assembled);
                    let state = inner.uploads.get_mut(&upload.0).unwrap();
                    state.completed = Some((composite, total_len));
                    return Err(UploadError::NoSuchUpload);
                }
            }
        }

        inner.objects.insert(key.clone(), assembled);
        let part_count = received.len() as u32;
        let state = inner.uploads.get_mut(&upload.0).unwrap();
        state.completed = Some((composite, total_len));
        Ok(CompleteOutcome::Completed {
            composite_sha256: composite,
            part_count,
            total_len,
        })
    }

    async fn multipart_status(&self, upload: &UploadId) -> Result<MultipartStatus, UploadError> {
        let inner = self.inner.lock().unwrap();
        let state = inner
            .uploads
            .get(&upload.0)
            .ok_or(UploadError::NoSuchUpload)?;
        if let Some((sha, len)) = state.completed {
            return Ok(MultipartStatus::Completed { sha256: sha, len });
        }
        if state.aborted {
            return Ok(MultipartStatus::Aborted);
        }
        Ok(MultipartStatus::InProgress {
            received_parts: state.parts.keys().copied().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_for(bytes: &[u8], part_count: u32) -> MultipartSpec {
        MultipartSpec {
            sha256: sha256(bytes),
            part_count,
            total_len: bytes.len() as u64,
        }
    }

    fn plan_parts(bytes: &[u8], chunk: usize) -> Vec<PartPlan> {
        bytes
            .chunks(chunk)
            .enumerate()
            .map(|(index, part)| PartPlan {
                part_number: index as u32 + 1,
                offset: (index * chunk) as u64,
                len: part.len() as u64,
                sha256: sha256(part),
            })
            .collect()
    }

    #[tokio::test]
    async fn create_only_put_is_idempotent_for_identical_content() {
        let backend = MemoryUploadBackend::new();
        backend.create_only_put(b"k", b"v").await.unwrap();
        backend.create_only_put(b"k", b"v").await.unwrap();
        assert_eq!(
            backend
                .create_only_put(b"k", b"different")
                .await
                .unwrap_err(),
            UploadError::PreconditionFailed
        );
    }

    #[tokio::test]
    async fn create_only_put_unsupported_is_refused() {
        let backend = MemoryUploadBackend::new().without_create_only();
        let err = backend.create_only_put(b"k", b"v").await.unwrap_err();
        assert!(matches!(err, UploadError::Backend(_)));
    }

    #[tokio::test]
    async fn range_get_exact_never_pads() {
        let backend = MemoryUploadBackend::new();
        backend.create_only_put(b"k", b"0123456789").await.unwrap();
        let mut buf = [0u8; 4];
        backend.range_get_exact(b"k", 2, &mut buf).await.unwrap();
        assert_eq!(&buf, b"2345");
        let mut beyond = [0u8; 4];
        assert!(backend.range_get_exact(b"k", 8, &mut beyond).await.is_err());
        assert!(
            backend
                .range_get_exact(b"missing", 0, &mut beyond)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn multipart_happy_path_verifies_composite() {
        let backend = MemoryUploadBackend::new();
        let bytes = vec![7u8; 5000];
        let parts = plan_parts(&bytes, 2000); // 3 parts
        let upload = backend
            .multipart_create(b"obj", &spec_for(&bytes, parts.len() as u32))
            .await
            .unwrap();
        let mut receipts = Vec::new();
        for part in &parts {
            let chunk = &bytes[part.offset as usize..part.offset as usize + part.len as usize];
            receipts.push(
                backend
                    .multipart_upload_part(&upload, part, chunk)
                    .await
                    .unwrap(),
            );
        }
        let outcome = backend
            .multipart_complete(&upload, &receipts)
            .await
            .unwrap();
        let CompleteOutcome::Completed {
            composite_sha256,
            part_count,
            total_len,
        } = outcome
        else {
            panic!("expected completion");
        };
        assert_eq!(part_count, 3);
        assert_eq!(total_len, 5000);
        assert_eq!(composite_sha256, sha256(&bytes));
        assert_eq!(backend.read_full(b"obj").await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn part_checksum_mismatch_is_refused_server_side() {
        let backend = MemoryUploadBackend::new();
        let bytes = vec![1u8; 100];
        let mut parts = plan_parts(&bytes, 100);
        parts[0].sha256 = [0u8; 32]; // wrong plan checksum
        let upload = backend
            .multipart_create(b"obj", &spec_for(&bytes, 1))
            .await
            .unwrap();
        let err = backend
            .multipart_upload_part(&upload, &parts[0], &bytes)
            .await
            .unwrap_err();
        assert!(matches!(err, UploadError::Backend(_)));
    }

    #[tokio::test]
    async fn http200_with_embedded_error_is_a_failed_complete() {
        let backend = MemoryUploadBackend::new();
        let bytes = vec![3u8; 100];
        let parts = plan_parts(&bytes, 100);
        let upload = backend
            .multipart_create(b"obj", &spec_for(&bytes, 1))
            .await
            .unwrap();
        let receipt = backend
            .multipart_upload_part(&upload, &parts[0], &bytes)
            .await
            .unwrap();
        backend.inject_complete_fault(InjectedCompleteFault::Http200WithEmbeddedError(
            "InvalidPartOrder".into(),
        ));
        let outcome = backend
            .multipart_complete(&upload, &[receipt])
            .await
            .unwrap();
        assert!(matches!(outcome, CompleteOutcome::Failed(_)));
        // The object was not stored.
        assert_eq!(backend.inspect(b"obj").await.unwrap(), None);
    }

    #[tokio::test]
    async fn lost_complete_response_is_resolvable_by_status() {
        let backend = MemoryUploadBackend::new();
        let bytes = vec![5u8; 100];
        let parts = plan_parts(&bytes, 100);
        let upload = backend
            .multipart_create(b"obj", &spec_for(&bytes, 1))
            .await
            .unwrap();
        let receipt = backend
            .multipart_upload_part(&upload, &parts[0], &bytes)
            .await
            .unwrap();
        backend.inject_complete_fault(InjectedCompleteFault::ResponseLost);
        let err = backend
            .multipart_complete(&upload, &[receipt.clone()])
            .await
            .unwrap_err();
        assert_eq!(err, UploadError::Timeout);
        // The status query shows the completion.
        assert_eq!(
            backend.multipart_status(&upload).await.unwrap(),
            MultipartStatus::Completed {
                sha256: sha256(&bytes),
                len: 100
            }
        );
        // A repeated complete with the same part list is idempotent.
        let outcome = backend
            .multipart_complete(&upload, &[receipt])
            .await
            .unwrap();
        assert!(matches!(outcome, CompleteOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn no_such_upload_after_completion_is_resolvable_by_object() {
        let backend = MemoryUploadBackend::new();
        let bytes = vec![9u8; 100];
        let parts = plan_parts(&bytes, 100);
        let upload = backend
            .multipart_create(b"obj", &spec_for(&bytes, 1))
            .await
            .unwrap();
        let receipt = backend
            .multipart_upload_part(&upload, &parts[0], &bytes)
            .await
            .unwrap();
        backend.inject_complete_fault(InjectedCompleteFault::NoSuchUploadAfterCompletion);
        let err = backend
            .multipart_complete(&upload, &[receipt])
            .await
            .unwrap_err();
        assert_eq!(err, UploadError::NoSuchUpload);
        // NoSuchUpload may mean completed: the final object is there.
        assert_eq!(
            backend.inspect(b"obj").await.unwrap().unwrap(),
            ObjectStat {
                len: 100,
                sha256: sha256(&bytes)
            }
        );
    }
}
