//! Local session layout and the builder state machine (spec 08 §3/§4/§9).
//!
//! Layout under `<sessions-root>/<id>/`:
//!
//! ```text
//! session.json    human-readable session record (informational)
//! inventory.bin   the frozen source inventory
//! upload.plan     the frozen upload plan + digest
//! journal.wal     the BNWL write-ahead journal (authoritative)
//! checkpoint.bin  compaction point: state + verified-object set
//! objects/        locally sealed object bytes before upload
//! spool/          scratch space (pack assembly)
//! owner.lock      exclusive builder ownership
//! ```
//!
//! The journal is authoritative: every state transition is appended to
//! `journal.wal` before it takes effect, and `session.json` /
//! `checkpoint.bin` are refreshed afterwards. Unpublished session output
//! is not visible through any published path: [`Session::artifact_view`]
//! refuses until the state is `Published`.

use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::error::IngestError;
use super::plan::UploadPlanDigest;
use super::wal::{self, Checkpoint, ReplayOutcome, WalWriter, kind as wal_kind};

/// Journal record kinds used by the session layer (spec 08 §9). The
/// generation-begin kind lives in [`wal::kind`].
pub mod record {
    /// Payload: the new session state as one byte.
    pub const STATE_TRANSITION: u16 = 1;
    /// Payload: the 32-byte digest of the frozen upload plan.
    pub const PLAN_DIGEST: u16 = 2;
    /// Payload: object index (u32 LE) + the object's 32-byte full hash.
    pub const OBJECT_VERIFIED: u16 = 3;
}

/// Builder states (spec 08 §4). The default successful stop for a P1
/// build is `BuiltUnpublished`; reaching `Published` requires the
/// manifest layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    New,
    Inventoried,
    PlanFrozen,
    MetadataStaged,
    DataUploading,
    DataVerified,
    SealVerified,
    ManifestVerified,
    Published,
    /// Built, durable, but deliberately not published. The session stays
    /// on disk; the artifacts are not visible.
    BuiltUnpublished,
}

impl SessionState {
    pub fn as_u8(self) -> u8 {
        match self {
            SessionState::New => 1,
            SessionState::Inventoried => 2,
            SessionState::PlanFrozen => 3,
            SessionState::MetadataStaged => 4,
            SessionState::DataUploading => 5,
            SessionState::DataVerified => 6,
            SessionState::SealVerified => 7,
            SessionState::ManifestVerified => 8,
            SessionState::Published => 9,
            SessionState::BuiltUnpublished => 10,
        }
    }

    pub fn from_u8(v: u8) -> Result<SessionState, IngestError> {
        match v {
            1 => Ok(SessionState::New),
            2 => Ok(SessionState::Inventoried),
            3 => Ok(SessionState::PlanFrozen),
            4 => Ok(SessionState::MetadataStaged),
            5 => Ok(SessionState::DataUploading),
            6 => Ok(SessionState::DataVerified),
            7 => Ok(SessionState::SealVerified),
            8 => Ok(SessionState::ManifestVerified),
            9 => Ok(SessionState::Published),
            10 => Ok(SessionState::BuiltUnpublished),
            other => Err(IngestError::CorruptJournal(format!(
                "unknown session state {other}"
            ))),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SessionState::New => "NEW",
            SessionState::Inventoried => "INVENTORIED",
            SessionState::PlanFrozen => "PLAN_FROZEN",
            SessionState::MetadataStaged => "METADATA_STAGED",
            SessionState::DataUploading => "DATA_UPLOADING",
            SessionState::DataVerified => "DATA_VERIFIED",
            SessionState::SealVerified => "SEAL_VERIFIED",
            SessionState::ManifestVerified => "MANIFEST_VERIFIED",
            SessionState::Published => "PUBLISHED",
            SessionState::BuiltUnpublished => "BUILT_UNPUBLISHED",
        }
    }

    /// The valid successors (spec 08 §4). `BuiltUnpublished` is an
    /// explicit stop from any built state; the pipeline states advance
    /// strictly forward.
    pub fn can_transition_to(self, next: SessionState) -> bool {
        use SessionState::*;
        match self {
            New => next == Inventoried,
            Inventoried => next == PlanFrozen,
            PlanFrozen => next == MetadataStaged,
            MetadataStaged => next == DataUploading || next == BuiltUnpublished,
            DataUploading => next == DataVerified,
            DataVerified => next == SealVerified || next == BuiltUnpublished,
            SealVerified => next == ManifestVerified || next == BuiltUnpublished,
            ManifestVerified => next == Published || next == BuiltUnpublished,
            Published | BuiltUnpublished => false,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SessionJson {
    id: String,
    state: String,
    generation: u64,
}

/// An owned builder session over one session directory.
pub struct Session {
    root: PathBuf,
    id: String,
    state: SessionState,
    generation: u64,
    wal: Option<WalWriter>,
    plan_digest: Option<UploadPlanDigest>,
    verified: Vec<(u32, [u8; 32])>,
    #[cfg(unix)]
    lock_file: Option<fs::File>,
}

impl Session {
    /// Create a fresh session. Fails with
    /// [`IngestError::SessionLocked`] if the directory already has an
    /// owner.
    pub fn create(root: &Path, id: &str) -> Result<Session, IngestError> {
        let dir = root.join(id);
        fs::create_dir_all(&dir)
            .map_err(|e| IngestError::Backend(format!("create {}: {e}", dir.display())))?;
        fs::create_dir_all(dir.join("objects"))
            .map_err(|e| IngestError::Backend(format!("create objects/: {e}")))?;
        fs::create_dir_all(dir.join("spool"))
            .map_err(|e| IngestError::Backend(format!("create spool/: {e}")))?;
        let mut session = Session {
            root: dir,
            id: id.to_string(),
            state: SessionState::New,
            generation: 1,
            wal: None,
            plan_digest: None,
            verified: Vec::new(),
            #[cfg(unix)]
            lock_file: None,
        };
        session.acquire_lock()?;
        session.write_session_json()?;
        let mut wal = WalWriter::open(&session.journal_path())?;
        wal.append(wal_kind::GENERATION_BEGIN, &1u64.to_le_bytes())?;
        session.wal = Some(wal);
        session.write_checkpoint()?;
        Ok(session)
    }

    /// Reopen a session after a clean close (or a crash — the OS releases
    /// the advisory lock when the owner dies). The journal is replayed to
    /// derive the authoritative state; a damaged record in the middle of
    /// the journal stops the resume, while a truncated or CRC-bad final
    /// record is dropped with bounded effect.
    pub fn open(root: &Path, id: &str) -> Result<Session, IngestError> {
        let dir = root.join(id);
        if !dir.is_dir() {
            return Err(IngestError::Backend(format!(
                "session directory {} does not exist",
                dir.display()
            )));
        }
        let mut session = Session {
            root: dir.clone(),
            id: id.to_string(),
            state: SessionState::New,
            generation: 1,
            wal: None,
            plan_digest: None,
            verified: Vec::new(),
            #[cfg(unix)]
            lock_file: None,
        };
        session.acquire_lock()?;

        // The journal is authoritative.
        let replay = wal::replay(&session.journal_path())?;
        let mut state = SessionState::New;
        for record in &replay.records {
            match record.kind {
                wal_kind::GENERATION_BEGIN if record.payload.len() == 8 => {
                    session.generation =
                        u64::from_le_bytes(record.payload[..8].try_into().unwrap());
                }
                record::STATE_TRANSITION if record.payload.len() == 1 => {
                    state = SessionState::from_u8(record.payload[0])?;
                }
                record::PLAN_DIGEST if record.payload.len() == 32 => {
                    session.plan_digest =
                        Some(UploadPlanDigest(record.payload[..32].try_into().unwrap()));
                }
                record::OBJECT_VERIFIED if record.payload.len() == 36 => {
                    let index = u32::from_le_bytes(record.payload[..4].try_into().unwrap());
                    let hash: [u8; 32] = record.payload[4..36].try_into().unwrap();
                    session.record_verified(index, hash);
                }
                _ => {
                    return Err(IngestError::CorruptJournal(format!(
                        "unknown journal record kind {} (len {})",
                        record.kind,
                        record.payload.len()
                    )));
                }
            }
        }
        if let ReplayOutcome::DroppedTail {
            dropped_sequence: Some(seq),
        } = replay.outcome
        {
            // Bounded recovery: the final partial record is gone; the
            // state machine continues from the last durable transition.
            eprintln!("ingest session {id}: dropped damaged journal tail record (sequence {seq})");
        }
        session.state = state;

        // Merge any checkpointed verified objects (compaction may be
        // ahead of a journal that was just tail-truncated).
        if let Some(checkpoint) = wal::read_checkpoint(&session.checkpoint_path())?
            && checkpoint.generation == session.generation
        {
            session.merge_checkpoint_state(&checkpoint)?;
        }

        // Re-open the writer; it repairs a damaged tail in place.
        session.wal = Some(WalWriter::open(&session.journal_path())?);
        session.write_session_json()?;
        Ok(session)
    }

    fn record_verified(&mut self, index: u32, hash: [u8; 32]) {
        if !self.verified.iter().any(|(i, _)| *i == index) {
            self.verified.push((index, hash));
        }
    }

    fn merge_checkpoint_state(&mut self, checkpoint: &Checkpoint) -> Result<(), IngestError> {
        let bytes = &checkpoint.state;
        if bytes.is_empty() {
            return Ok(());
        }
        let mut pos = 0usize;
        let state = SessionState::from_u8(bytes[pos])?;
        pos += 1;
        let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + count * 36 != bytes.len() {
            return Err(IngestError::CorruptJournal(
                "checkpoint verified-object section has the wrong length".into(),
            ));
        }
        for _ in 0..count {
            let index = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
            let hash: [u8; 32] = bytes[pos + 4..pos + 36].try_into().unwrap();
            pos += 36;
            self.record_verified(index, hash);
        }
        // The journal is authoritative for the state itself; the
        // checkpoint only adds verified objects it may have compacted.
        let _ = state;
        Ok(())
    }

    fn acquire_lock(&mut self) -> Result<(), IngestError> {
        let lock_path = self.lock_path();
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&lock_path)
                .map_err(|e| {
                    IngestError::SessionLocked(format!("cannot open {}: {e}", lock_path.display()))
                })?;
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc != 0 {
                return Err(IngestError::SessionLocked(format!(
                    "session {} is owned by another builder (flock on {})",
                    self.id,
                    lock_path.display()
                )));
            }
            self.lock_file = Some(file);
        }
        #[cfg(not(unix))]
        {
            // Fallback: create-new lock file. A crashed owner leaves the
            // file behind; recovery requires removing it deliberately.
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
                .map_err(|e| {
                    IngestError::SessionLocked(format!(
                        "session {} is owned by another builder ({} exists: {e})",
                        self.id,
                        lock_path.display()
                    ))
                })?;
            let _ = file;
        }
        Ok(())
    }

    fn release_lock(&mut self) {
        #[cfg(unix)]
        {
            self.lock_file = None; // dropping closes + releases the flock
        }
        #[cfg(not(unix))]
        {
            let _ = fs::remove_file(self.lock_path());
        }
    }

    fn write_session_json(&self) -> Result<(), IngestError> {
        let json = SessionJson {
            id: self.id.clone(),
            state: self.state.name().to_string(),
            generation: self.generation,
        };
        let bytes = serde_json::to_vec_pretty(&json)
            .map_err(|e| IngestError::Backend(format!("encode session.json: {e}")))?;
        let path = self.root.join("session.json");
        let tmp = self.root.join("session.json.tmp");
        fs::write(&tmp, &bytes)
            .map_err(|e| IngestError::Backend(format!("write {}: {e}", tmp.display())))?;
        fs::rename(&tmp, &path)
            .map_err(|e| IngestError::Backend(format!("rename session.json: {e}")))?;
        Ok(())
    }

    /// Append a state transition to the journal first, then refresh
    /// session.json (spec 08 §4: every transition is journaled).
    pub fn transition(&mut self, next: SessionState) -> Result<(), IngestError> {
        if !self.state.can_transition_to(next) {
            return Err(IngestError::InvalidTransition(format!(
                "{} -> {}",
                self.state.name(),
                next.name()
            )));
        }
        let wal = self
            .wal
            .as_mut()
            .ok_or_else(|| IngestError::Backend("journal writer is closed".into()))?;
        wal.append(record::STATE_TRANSITION, &[next.as_u8()])?;
        self.state = next;
        self.write_session_json()?;
        self.write_checkpoint()?;
        Ok(())
    }

    /// Record the frozen plan digest (spec 08 §12: tamper detection on
    /// resume).
    pub fn record_plan_digest(&mut self, digest: UploadPlanDigest) -> Result<(), IngestError> {
        let wal = self
            .wal
            .as_mut()
            .ok_or_else(|| IngestError::Backend("journal writer is closed".into()))?;
        wal.append(record::PLAN_DIGEST, &digest.0)?;
        self.plan_digest = Some(digest);
        Ok(())
    }

    /// Record that one planned object is uploaded and REMOTE_VERIFIED.
    pub fn record_object_verified(
        &mut self,
        index: u32,
        hash: [u8; 32],
    ) -> Result<(), IngestError> {
        let mut payload = Vec::with_capacity(36);
        payload.extend_from_slice(&index.to_le_bytes());
        payload.extend_from_slice(&hash);
        let wal = self
            .wal
            .as_mut()
            .ok_or_else(|| IngestError::Backend("journal writer is closed".into()))?;
        wal.append(record::OBJECT_VERIFIED, &payload)?;
        self.record_verified(index, hash);
        Ok(())
    }

    /// Compact the current state + verified set into checkpoint.bin
    /// (temp → fsync → rename → fsync parent).
    pub fn write_checkpoint(&self) -> Result<(), IngestError> {
        let mut state = Vec::with_capacity(5 + self.verified.len() * 36);
        state.push(self.state.as_u8());
        state.extend_from_slice(&(self.verified.len() as u32).to_le_bytes());
        for (index, hash) in &self.verified {
            state.extend_from_slice(&index.to_le_bytes());
            state.extend_from_slice(hash);
        }
        wal::write_checkpoint(
            &self.checkpoint_path(),
            &Checkpoint {
                generation: self.generation,
                state,
            },
        )
    }

    /// The published artifact view. Unpublished session output is not
    /// visible (spec 08 §4/§13).
    pub fn artifact_view(&self) -> Result<ArtifactView<'_>, IngestError> {
        match self.state {
            SessionState::Published => Ok(ArtifactView { session: self }),
            other => Err(IngestError::NotPublished(format!(
                "session {} is in state {}",
                self.id,
                other.name()
            ))),
        }
    }

    pub fn state(&self) -> SessionState {
        self.state
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn plan_digest(&self) -> Option<UploadPlanDigest> {
        self.plan_digest
    }

    /// Objects already recorded as uploaded + verified, as `(index,
    /// full_hash)` pairs in journal order.
    pub fn verified_objects(&self) -> &[(u32, [u8; 32])] {
        &self.verified
    }

    pub fn inventory_path(&self) -> PathBuf {
        self.root.join("inventory.bin")
    }

    pub fn plan_path(&self) -> PathBuf {
        self.root.join("upload.plan")
    }

    pub fn journal_path(&self) -> PathBuf {
        self.root.join("journal.wal")
    }

    pub fn checkpoint_path(&self) -> PathBuf {
        self.root.join("checkpoint.bin")
    }

    pub fn objects_dir(&self) -> PathBuf {
        self.root.join("objects")
    }

    pub fn spool_dir(&self) -> PathBuf {
        self.root.join("spool")
    }

    fn lock_path(&self) -> PathBuf {
        self.root.join("owner.lock")
    }

    /// Close the session cleanly, releasing ownership. The session
    /// directory (inventory, plan, journal, checkpoint, objects) stays on
    /// disk for resume or diagnosis.
    pub fn close(mut self) {
        self.release_lock();
        self.wal = None;
    }
}

/// The published view of a session. Only constructible when the state is
/// `Published`.
pub struct ArtifactView<'a> {
    session: &'a Session,
}

impl std::fmt::Debug for ArtifactView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArtifactView")
            .field("session", &self.session.id)
            .finish()
    }
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("state", &self.state.name())
            .field("generation", &self.generation)
            .finish()
    }
}

impl<'a> ArtifactView<'a> {
    /// The session root — the artifact set a publisher exposes.
    pub fn session_root(&self) -> &Path {
        self.session.root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn fresh_session_starts_new_with_journal_generation() {
        let root = temp_root();
        let session = Session::create(root.path(), "s1").unwrap();
        assert_eq!(session.state(), SessionState::New);
        assert_eq!(session.generation, 1);
        assert!(session.journal_path().is_file());
        assert!(session.inventory_path().parent().unwrap().is_dir());
        session.close();
    }

    #[test]
    fn second_builder_is_refused_while_the_first_holds_the_lock() {
        let root = temp_root();
        let first = Session::create(root.path(), "s1").unwrap();
        let err = Session::open(root.path(), "s1").unwrap_err();
        assert!(matches!(err, IngestError::SessionLocked(_)), "{err}");
        first.close();
        // After a clean close the session can be reopened.
        let second = Session::open(root.path(), "s1").unwrap();
        assert_eq!(second.state(), SessionState::New);
        second.close();
    }

    #[test]
    fn transitions_are_journaled_and_replayed() {
        let root = temp_root();
        {
            let mut session = Session::create(root.path(), "s1").unwrap();
            session.transition(SessionState::Inventoried).unwrap();
            session.transition(SessionState::PlanFrozen).unwrap();
            session
                .record_plan_digest(UploadPlanDigest([7u8; 32]))
                .unwrap();
            session.record_object_verified(0, [1u8; 32]).unwrap();
            session.close();
        }
        let resumed = Session::open(root.path(), "s1").unwrap();
        assert_eq!(resumed.state(), SessionState::PlanFrozen);
        assert_eq!(resumed.plan_digest(), Some(UploadPlanDigest([7u8; 32])));
        assert_eq!(resumed.verified_objects(), &[(0, [1u8; 32])]);
        resumed.close();
    }

    #[test]
    fn invalid_transitions_are_refused() {
        let root = temp_root();
        let mut session = Session::create(root.path(), "s1").unwrap();
        assert!(matches!(
            session.transition(SessionState::DataVerified),
            Err(IngestError::InvalidTransition(_))
        ));
        session.transition(SessionState::Inventoried).unwrap();
        session.transition(SessionState::PlanFrozen).unwrap();
        session.transition(SessionState::MetadataStaged).unwrap();
        session.transition(SessionState::BuiltUnpublished).unwrap();
        // Terminal.
        assert!(matches!(
            session.transition(SessionState::DataUploading),
            Err(IngestError::InvalidTransition(_))
        ));
        session.close();
    }

    #[test]
    fn truncated_journal_tail_recovers_to_the_last_durable_state() {
        let root = temp_root();
        {
            let mut session = Session::create(root.path(), "s1").unwrap();
            session.transition(SessionState::Inventoried).unwrap();
            session.transition(SessionState::PlanFrozen).unwrap();
            session.close();
        }
        // Truncate the journal mid-final-record.
        let journal = root.path().join("s1/journal.wal");
        let bytes = fs::read(&journal).unwrap();
        fs::write(&journal, &bytes[..bytes.len() - 10]).unwrap();
        let resumed = Session::open(root.path(), "s1").unwrap();
        // The final (partial) transition is dropped; the state falls back
        // to the last fully durable one.
        assert_eq!(resumed.state(), SessionState::Inventoried);
        resumed.close();
    }

    #[test]
    fn damaged_middle_record_stops_the_resume() {
        let root = temp_root();
        {
            let mut session = Session::create(root.path(), "s1").unwrap();
            session.transition(SessionState::Inventoried).unwrap();
            session.transition(SessionState::PlanFrozen).unwrap();
            session.transition(SessionState::MetadataStaged).unwrap();
            session.close();
        }
        // Corrupt the first state-transition record's CRC region (a
        // middle record, not the tail).
        let journal = root.path().join("s1/journal.wal");
        let mut bytes = fs::read(&journal).unwrap();
        // Layout: GEN_BEGIN record (24B) then STATE records (24B each
        // with empty payload). Flip a byte inside the third record.
        bytes[24 + 24 + 5] ^= 0xff;
        fs::write(&journal, &bytes).unwrap();
        let err = Session::open(root.path(), "s1").unwrap_err();
        assert!(matches!(err, IngestError::CorruptJournal(_)), "{err}");
    }

    #[test]
    fn unpublished_sessions_have_no_artifact_view() {
        let root = temp_root();
        let mut session = Session::create(root.path(), "s1").unwrap();
        session.transition(SessionState::Inventoried).unwrap();
        session.transition(SessionState::PlanFrozen).unwrap();
        session.transition(SessionState::MetadataStaged).unwrap();
        session.transition(SessionState::BuiltUnpublished).unwrap();
        let err = session.artifact_view().unwrap_err();
        assert!(matches!(err, IngestError::NotPublished(_)), "{err}");
        session.close();
    }
}
