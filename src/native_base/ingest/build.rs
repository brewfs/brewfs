//! The phased ingest builder (spec 08 §5–§8).
//!
//! One builder owns one session and drives it through the phases:
//!
//! 1. **inventory** — walk the source, persist `inventory.bin`, journal
//!    `INVENTORIED`;
//! 2. **freeze** — revalidate the source, read the file extents (hardlink
//!    groups read once), seal PlainBytes frames into DataPack objects
//!    under the session's `objects/` directory, persist `upload.plan`
//!    with its digest, journal `PLAN_FROZEN` and `METADATA_STAGED`;
//! 3. **upload** — revalidate the source again (publication boundary),
//!    check the plan digest, upload each sealed object through the
//!    [`UploadExecutor`] (skipping objects already recorded as verified),
//!    journal `DATA_VERIFIED`;
//! 4. **stop** — journal `BUILT_UNPUBLISHED`. The P1 default: the build
//!    is durable but nothing is visible until the manifest layer
//!    publishes it.
//!
//! Every failure leaves the session on disk in its last durable state,
//! diagnosable and resumable. A source that changed after inventory stops
//! publication with [`IngestError::SourceChanged`].

use std::collections::HashSet;
use std::fs;

use sha2::{Digest, Sha256};

use crate::native_base::wire::datapack::{PackBuilder, PackFrame, full_object_hash};

use super::backend::UploadBackend;
use super::error::IngestError;
use super::plan::UploadPlan;
use super::session::{Session, SessionState};
use super::source::{EntryKind, IngestSource, SourceEntry, decode_inventory, encode_inventory};
use super::upload::{PlannedObject, RemoteVerificationProfile, UploadExecutor};

/// Tuning for one build. The plan it produces is what gets frozen, so a
/// resume with different tuning is caught by the plan-digest check, not
/// silently accepted.
#[derive(Debug, Clone)]
pub struct IngestBuildConfig {
    pub volume_id: [u8; 16],
    /// Objects larger than this use multipart uploads; 0 disables
    /// multipart (single create-only PUT for everything).
    pub part_size: u64,
    /// Maximum raw file content sealed into one DataPack.
    pub pack_content_budget: u64,
    /// Bound on total sealed bytes staged in the session (spec 08 §3:
    /// bounded spool). Exceeding it stops the build before the disk does.
    pub disk_budget: u64,
    pub verification: RemoteVerificationProfile,
}

impl Default for IngestBuildConfig {
    fn default() -> Self {
        IngestBuildConfig {
            volume_id: [0u8; 16],
            part_size: 0,
            pack_content_budget: 8 * 1024 * 1024,
            disk_budget: 512 * 1024 * 1024,
            verification: RemoteVerificationProfile::ExactReadback,
        }
    }
}

/// Where one source file's content landed: which planned object and which
/// frame ordinal inside its pack. Consumed by the manifest layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameOrigin {
    pub path: Vec<u8>,
    pub object_index: u32,
    pub frame_ordinal: u64,
}

/// What a completed (or stopped) build produced.
#[derive(Debug, Clone)]
pub struct BuildOutcome {
    pub session_id: String,
    pub state: SessionState,
    pub objects: usize,
    pub verified: usize,
    pub frames: Vec<FrameOrigin>,
}

pub struct IngestBuilder<S: IngestSource> {
    source: S,
    session: Session,
    config: IngestBuildConfig,
    entries: Vec<SourceEntry>,
    frames: Vec<FrameOrigin>,
}

impl<S: IngestSource> IngestBuilder<S> {
    pub fn new(session: Session, source: S, config: IngestBuildConfig) -> Self {
        IngestBuilder {
            source,
            session,
            config,
            entries: Vec::new(),
            frames: Vec::new(),
        }
    }

    /// Give the session back (e.g. to close it cleanly).
    pub fn into_session(self) -> Session {
        self.session
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn outcome(&self) -> BuildOutcome {
        BuildOutcome {
            session_id: self.session.id().to_string(),
            state: self.session.state(),
            objects: self.plan_from_disk().map(|p| p.objects.len()).unwrap_or(0),
            verified: self.session.verified_objects().len(),
            frames: self.frames.clone(),
        }
    }

    /// Phase 1: inventory the source and persist it.
    pub fn run_inventory(&mut self) -> Result<(), IngestError> {
        match self.session.state() {
            SessionState::New => {}
            SessionState::Inventoried => return Ok(()), // idempotent resume
            other => {
                return Err(IngestError::InvalidTransition(format!(
                    "inventory requires NEW, session is {}",
                    other.name()
                )));
            }
        }
        let mut entries = self.source.inventory()?;
        // Deterministic order regardless of walker order.
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        let image = encode_inventory(self.source.policy(), &entries);
        fs::write(self.session.inventory_path(), &image)
            .map_err(|e| IngestError::Backend(format!("write inventory: {e}")))?;
        self.session.transition(SessionState::Inventoried)?;
        self.entries = entries;
        Ok(())
    }

    fn load_entries(&mut self) -> Result<(), IngestError> {
        if !self.entries.is_empty() {
            return Ok(());
        }
        let bytes = fs::read(self.session.inventory_path())
            .map_err(|e| IngestError::Backend(format!("read inventory: {e}")))?;
        let (_policy, entries) =
            decode_inventory(&bytes).map_err(|e| IngestError::Backend(format!("{e}")))?;
        self.entries = entries;
        Ok(())
    }

    /// Phase 2: revalidate, seal packs, freeze and persist the plan.
    pub fn freeze_plan(&mut self) -> Result<(), IngestError> {
        match self.session.state() {
            SessionState::Inventoried => {}
            SessionState::MetadataStaged => return Ok(()), // idempotent resume
            other => {
                return Err(IngestError::InvalidTransition(format!(
                    "freeze requires INVENTORIED, session is {}",
                    other.name()
                )));
            }
        }
        self.load_entries()?;
        // Source consistency at freeze time (spec 08 §1).
        self.source.revalidate(&self.entries)?;

        let mut planned_objects: Vec<PlannedObject> = Vec::new();
        let mut sealed_total: u64 = 0;
        let mut pack_index: u32 = 0;
        let mut packer = PackBuilder::new();
        let mut pack_content: u64 = 0;

        let mut seen_hardlink_groups: HashSet<u64> = HashSet::new();
        for entry in self.entries.clone() {
            let content: Vec<u8> = match &entry.kind {
                EntryKind::Directory => continue,
                EntryKind::Symlink { target } => target.clone(),
                EntryKind::File { .. } => match entry.hardlink_group {
                    Some(group) if !seen_hardlink_groups.insert(group) => {
                        // Content already sealed for this group; the
                        // inventory keeps every member's identity.
                        continue;
                    }
                    _ => read_whole(&self.source, &entry)?,
                },
            };
            if packer.frame_count() > 0
                && pack_content + content.len() as u64 > self.config.pack_content_budget
            {
                flush_pack(
                    &self.config,
                    &self.session,
                    pack_index,
                    &mut packer,
                    &mut planned_objects,
                    &mut sealed_total,
                )?;
                pack_index += 1;
                pack_content = 0;
                packer = PackBuilder::new();
            }
            let frame = PackFrame::plain_bytes(&content)
                .map_err(|e| IngestError::Backend(format!("{e}")))?;
            let ordinal = packer.frame_count() as u64;
            packer.push(frame);
            pack_content += content.len() as u64;
            self.frames.push(FrameOrigin {
                path: entry.path.clone(),
                object_index: pack_index,
                frame_ordinal: ordinal,
            });
        }
        flush_pack(
            &self.config,
            &self.session,
            pack_index,
            &mut packer,
            &mut planned_objects,
            &mut sealed_total,
        )?;

        let plan = UploadPlan {
            volume_id: self.config.volume_id,
            consistency: self.source.policy(),
            verification: self.config.verification,
            part_size: self.config.part_size,
            objects: planned_objects,
        };
        let digest = plan.write_to(&self.session.plan_path())?;
        self.session.transition(SessionState::PlanFrozen)?;
        self.session.record_plan_digest(digest)?;
        self.session.transition(SessionState::MetadataStaged)?;
        Ok(())
    }

    /// Phase 3: upload every sealed object, skipping ones already
    /// verified, and journal `DATA_VERIFIED`.
    pub async fn upload(&mut self, backend: &dyn UploadBackend) -> Result<(), IngestError> {
        match self.session.state() {
            SessionState::MetadataStaged => self.session.transition(SessionState::DataUploading)?,
            SessionState::DataUploading | SessionState::DataVerified => {}
            other => {
                return Err(IngestError::InvalidTransition(format!(
                    "upload requires METADATA_STAGED, session is {}",
                    other.name()
                )));
            }
        }
        if self.session.state() == SessionState::DataVerified {
            return Ok(()); // idempotent resume
        }
        self.load_entries()?;
        // Source consistency at the publication boundary (spec 08 §1).
        self.source.revalidate(&self.entries)?;

        let plan = self.plan_from_disk()?;
        // The plan on disk must be exactly the frozen one (spec 08 §12).
        let recorded = self
            .session
            .plan_digest()
            .ok_or_else(|| IngestError::CorruptJournal("no plan digest in journal".into()))?;
        if plan.digest() != recorded {
            return Err(IngestError::PlanMismatch(format!(
                "upload plan digest {} does not match the journaled {}",
                hex(&plan.digest().0),
                hex(&recorded.0)
            )));
        }

        let executor = UploadExecutor::new(backend, plan.verification);
        let already: HashSet<u32> = self
            .session
            .verified_objects()
            .iter()
            .map(|(i, _)| *i)
            .collect();
        for (index, planned) in plan.objects.iter().enumerate() {
            let index = index as u32;
            if already.contains(&index) {
                continue; // receipt already journaled; do not re-upload
            }
            let sealed_path = sealed_object_path(&self.session, index);
            let bytes = fs::read(&sealed_path)
                .map_err(|e| IngestError::Backend(format!("read sealed object: {e}")))?;
            executor.upload_object(planned, &bytes).await?;
            self.session
                .record_object_verified(index, planned.full_hash)?;
        }
        self.session.transition(SessionState::DataVerified)?;
        Ok(())
    }

    /// Phase 4 (P1 default): stop built-but-unpublished.
    pub fn stop_built_unpublished(&mut self) -> Result<(), IngestError> {
        match self.session.state() {
            SessionState::DataVerified => {}
            SessionState::BuiltUnpublished => return Ok(()),
            other => {
                return Err(IngestError::InvalidTransition(format!(
                    "stopping requires DATA_VERIFIED, session is {}",
                    other.name()
                )));
            }
        }
        self.session.transition(SessionState::BuiltUnpublished)
    }

    /// Run the remaining phases to the P1 default stop. Resumable: a
    /// session in any intermediate state continues from where its journal
    /// says it stopped.
    pub async fn build_to_unpublished(
        &mut self,
        backend: &dyn UploadBackend,
    ) -> Result<BuildOutcome, IngestError> {
        match self.session.state() {
            SessionState::New => {
                self.run_inventory()?;
                self.freeze_plan()?;
            }
            SessionState::Inventoried => self.freeze_plan()?,
            SessionState::MetadataStaged => {}
            SessionState::DataUploading | SessionState::DataVerified => {}
            SessionState::BuiltUnpublished => return Ok(self.outcome()),
            other => {
                return Err(IngestError::InvalidTransition(format!(
                    "cannot resume a session in state {}",
                    other.name()
                )));
            }
        }
        self.upload(backend).await?;
        self.stop_built_unpublished()?;
        Ok(self.outcome())
    }

    fn plan_from_disk(&self) -> Result<UploadPlan, IngestError> {
        UploadPlan::read_from(&self.session.plan_path())
    }
}

/// Seal the current pack into the session's `objects/` directory and add
/// it to the plan.
fn flush_pack(
    config: &IngestBuildConfig,
    session: &Session,
    pack_index: u32,
    packer: &mut PackBuilder,
    planned_objects: &mut Vec<PlannedObject>,
    sealed_total: &mut u64,
) -> Result<(), IngestError> {
    if packer.frame_count() == 0 {
        return Ok(());
    }
    let bytes = packer
        .build()
        .map_err(|e| IngestError::Backend(format!("{e}")))?;
    if *sealed_total + bytes.len() as u64 > config.disk_budget {
        return Err(IngestError::InsufficientDisk {
            needed: *sealed_total + bytes.len() as u64,
            available: config.disk_budget,
        });
    }
    let full_hash = full_object_hash(&bytes);
    let object_id = derive_object_id(&config.volume_id, pack_index);
    let key = crate::native_base::write::receipts::object_key(
        &config.volume_id,
        "pack",
        &object_id,
        &full_hash,
    );
    let path = sealed_object_path(session, pack_index);
    fs::write(&path, &bytes)
        .map_err(|e| IngestError::Backend(format!("write {}: {e}", path.display())))?;
    let mut planned = if config.part_size > 0 && bytes.len() as u64 > config.part_size {
        PlannedObject::multipart(
            object_id,
            key,
            full_hash,
            bytes.len() as u64,
            config.part_size,
        )
    } else {
        PlannedObject::single_put(object_id, key, full_hash, bytes.len() as u64)
    };
    planned.bind_part_checksums(&bytes);
    planned_objects.push(planned);
    *sealed_total += bytes.len() as u64;
    Ok(())
}

fn sealed_object_path(session: &Session, index: u32) -> std::path::PathBuf {
    session.objects_dir().join(format!("{index:08}.brfdp"))
}

/// Deterministic object id for pack `index` of `volume`: first 16 bytes
/// of SHA-256(volume || index). Stable across resumes.
fn derive_object_id(volume: &[u8; 16], index: u32) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(volume);
    hasher.update(index.to_le_bytes());
    let digest = hasher.finalize();
    digest[..16].try_into().unwrap()
}

/// Read one file's full logical extent through the source's exact
/// read_range (holes read as zeros; nothing is synthesized from a 404).
fn read_whole<S: IngestSource>(source: &S, entry: &SourceEntry) -> Result<Vec<u8>, IngestError> {
    let size = match entry.kind {
        EntryKind::File { size } => size,
        _ => {
            return Err(IngestError::Backend(
                "read_whole on a non-file entry".into(),
            ));
        }
    };
    let mut buf = vec![0u8; size as usize];
    if size > 0 {
        source.read_range(&entry.token, 0, &mut buf)?;
    }
    Ok(buf)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::super::backend::{MemoryUploadBackend, UploadError};
    use super::super::source::{ConsistencyPolicy, LocalDirSource};
    use super::*;
    use std::io::Write as _;
    use std::path::Path;

    fn config() -> IngestBuildConfig {
        IngestBuildConfig {
            volume_id: [3u8; 16],
            part_size: 0,
            pack_content_budget: 1024,
            disk_budget: 1024 * 1024,
            verification: RemoteVerificationProfile::ExactReadback,
        }
    }

    fn write_source(dir: &Path, files: &[(&str, &[u8])]) {
        fs::create_dir_all(dir).unwrap();
        for (name, content) in files {
            let path = dir.join(name);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            let mut f = fs::File::create(&path).unwrap();
            f.write_all(content).unwrap();
        }
    }

    #[tokio::test]
    async fn build_to_unpublished_stops_unpublished_and_uploads_every_object() {
        let root = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        write_source(src.path(), &[("a.txt", b"alpha"), ("sub/b.txt", b"beta")]);

        let session = Session::create(root.path(), "s1").unwrap();
        let mut builder = IngestBuilder::new(
            session,
            LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
            config(),
        );
        let backend = MemoryUploadBackend::new();
        let outcome = builder.build_to_unpublished(&backend).await.unwrap();
        assert_eq!(outcome.state, SessionState::BuiltUnpublished);
        assert_eq!(outcome.verified, outcome.objects);
        assert!(outcome.objects >= 1);
        assert_eq!(outcome.frames.len(), 2);
        // Unpublished: no artifact view.
        let err = builder.session().artifact_view().unwrap_err();
        assert!(matches!(err, IngestError::NotPublished(_)), "{err}");
    }

    #[tokio::test]
    async fn source_change_after_inventory_stops_publication() {
        let root = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        write_source(src.path(), &[("a.txt", b"alpha")]);

        let session = Session::create(root.path(), "s1").unwrap();
        let mut builder = IngestBuilder::new(
            session,
            LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
            config(),
        );
        builder.run_inventory().unwrap();
        // Mutate the source after the inventory was taken.
        fs::write(src.path().join("a.txt"), b"ALPHA-MUTATED").unwrap();
        let backend = MemoryUploadBackend::new();
        let err = builder.build_to_unpublished(&backend).await.unwrap_err();
        assert!(matches!(err, IngestError::SourceChanged(_)), "{err}");
        // The session stays on disk, diagnosable, in INVENTORIED.
        assert_eq!(builder.session().state(), SessionState::Inventoried);
        assert!(builder.session().inventory_path().is_file());
        // Nothing was uploaded.
        assert_eq!(builder.session().verified_objects().len(), 0);
    }

    #[tokio::test]
    async fn disk_budget_stops_the_build_before_anything_is_uploaded() {
        let root = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        write_source(src.path(), &[("big.bin", &[7u8; 4096])]);

        let session = Session::create(root.path(), "s1").unwrap();
        let mut builder = IngestBuilder::new(
            session,
            LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
            IngestBuildConfig {
                disk_budget: 1024,
                ..config()
            },
        );
        let backend = MemoryUploadBackend::new();
        let err = builder.build_to_unpublished(&backend).await.unwrap_err();
        assert!(matches!(err, IngestError::InsufficientDisk { .. }), "{err}");
        assert_eq!(builder.session().verified_objects().len(), 0);
        assert!(builder.session().artifact_view().is_err());
    }

    #[tokio::test]
    async fn resume_after_clean_close_finishes_the_upload() {
        let root = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        write_source(src.path(), &[("a.txt", b"alpha"), ("b.txt", b"beta")]);

        let backend = MemoryUploadBackend::new();
        // First run: inventory + freeze, then stop (an interrupted build
        // before any upload).
        {
            let session = Session::create(root.path(), "s1").unwrap();
            let mut builder = IngestBuilder::new(
                session,
                LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
                config(),
            );
            builder.run_inventory().unwrap();
            builder.freeze_plan().unwrap();
            assert_eq!(builder.session().state(), SessionState::MetadataStaged);
            builder.into_session().close();
        }
        // Resume: a fresh builder over the same session.
        {
            let session = Session::open(root.path(), "s1").unwrap();
            let mut builder = IngestBuilder::new(
                session,
                LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
                config(),
            );
            let outcome = builder.build_to_unpublished(&backend).await.unwrap();
            assert_eq!(outcome.state, SessionState::BuiltUnpublished);
            assert_eq!(outcome.verified, outcome.objects);
        }
    }

    #[tokio::test]
    async fn resume_mid_upload_skips_the_verified_object() {
        let root = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        // Two files with a tiny pack budget → two packs/objects.
        write_source(
            src.path(),
            &[("a.txt", &[1u8; 512]), ("b.txt", &[2u8; 512])],
        );
        let mut cfg = config();
        cfg.pack_content_budget = 512;

        let backend = MemoryUploadBackend::new();
        // Run 1: the first object uploads, the second fails (transient
        // backend error), leaving the session mid-upload with object 0
        // verified.
        {
            let session = Session::create(root.path(), "s1").unwrap();
            let mut builder = IngestBuilder::new(
                session,
                LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
                cfg.clone(),
            );
            builder.run_inventory().unwrap();
            builder.freeze_plan().unwrap();
            // Fail the second object's create-only PUT (the first call
            // succeeds, the next one fails).
            backend.inject_create_only_failure_on(1, UploadError::Backend("transient".into()));
            let err = builder.upload(&backend).await.unwrap_err();
            assert!(matches!(err, IngestError::Backend(_)), "{err}");
            assert_eq!(builder.session().state(), SessionState::DataUploading);
            assert_eq!(builder.session().verified_objects().len(), 1);
            builder.into_session().close();
        }
        // Run 2: resume — object 0 is skipped (not re-uploaded), object 1
        // uploads, the session reaches BUILT_UNPUBLISHED.
        {
            let session = Session::open(root.path(), "s1").unwrap();
            let mut builder = IngestBuilder::new(
                session,
                LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
                cfg,
            );
            let outcome = builder.build_to_unpublished(&backend).await.unwrap();
            assert_eq!(outcome.state, SessionState::BuiltUnpublished);
            assert_eq!(outcome.verified, outcome.objects);
            assert_eq!(outcome.objects, 2);
        }
    }

    #[tokio::test]
    async fn changed_plan_on_resume_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        write_source(src.path(), &[("a.txt", b"alpha")]);

        let backend = MemoryUploadBackend::new();
        {
            let session = Session::create(root.path(), "s1").unwrap();
            let mut builder = IngestBuilder::new(
                session,
                LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
                config(),
            );
            builder.run_inventory().unwrap();
            builder.freeze_plan().unwrap();
            builder.into_session().close();
        }
        // Tamper with the frozen plan on disk.
        let plan_path = root.path().join("s1/upload.plan");
        let mut bytes = fs::read(&plan_path).unwrap();
        // Flip a byte inside the first object's key region (after the
        // volume id + policy + profile + part-size fields).
        let idx = 1 + 16 + 1 + 1 + 8 + 16;
        if idx < bytes.len() {
            bytes[idx] ^= 0xff;
        }
        fs::write(&plan_path, &bytes).unwrap();
        {
            let session = Session::open(root.path(), "s1").unwrap();
            let mut builder = IngestBuilder::new(
                session,
                LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
                config(),
            );
            let err = builder.build_to_unpublished(&backend).await.unwrap_err();
            // Either the plan no longer decodes or its digest moved; both
            // are a refusal, never a silent re-plan.
            assert!(
                matches!(err, IngestError::PlanMismatch(_) | IngestError::Backend(_)),
                "{err}"
            );
        }
    }

    #[tokio::test]
    async fn without_create_only_the_build_is_refused_not_simulated() {
        // E02: no atomic create-only PUT → refusal, never HEAD+PUT.
        let root = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        write_source(src.path(), &[("a.txt", b"alpha")]);

        let session = Session::create(root.path(), "s1").unwrap();
        let mut builder = IngestBuilder::new(
            session,
            LocalDirSource::new(src.path(), ConsistencyPolicy::SnapshotBacked),
            config(),
        );
        let backend = MemoryUploadBackend::new().without_create_only();
        let err = builder.build_to_unpublished(&backend).await.unwrap_err();
        assert!(matches!(err, IngestError::Backend(_)), "{err}");
        assert_eq!(builder.session().verified_objects().len(), 0);
    }
}
