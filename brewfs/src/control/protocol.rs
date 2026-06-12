use crate::control::job::{JobInfo, JobOutcome, JobState};
use crate::meta::store::{FileAttr, FileType, MetaStoreCapabilities};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ControlRequest {
    Ping,
    GetInfo,
    RunGc {
        dry_run: bool,
    },
    GetJob {
        job_id: String,
    },
    ListDirectory {
        path: String,
    },
    StatPath {
        path: String,
    },
    ReadLink {
        path: String,
    },
    GetAcl {
        path: String,
    },
    PutAcl {
        path: String,
        entries: Vec<ControlAclEntry>,
    },
    DeleteAcl {
        path: String,
    },
    ListTrash,
    RestoreTrashEntry {
        entry_id: String,
    },
    DeleteTrashEntry {
        entry_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ControlResponse {
    Pong,
    Info {
        pid: u32,
        mount_point: String,
        started_at: i64,
        version: String,
        meta_backend: String,
        capabilities: MetaStoreCapabilities,
    },
    Accepted {
        job_id: String,
    },
    JobStatus {
        job_id: String,
        state: JobState,
        detail: Option<String>,
        outcome: Option<JobOutcome>,
    },
    DirectoryListing {
        path: String,
        entries: Vec<ControlDirectoryEntry>,
    },
    PathMetadata {
        path: String,
        metadata: ControlPathMetadata,
    },
    SymlinkTarget {
        path: String,
        target: String,
    },
    Acl {
        path: String,
        entries: Vec<ControlAclEntry>,
    },
    AclDeleted {
        path: String,
    },
    Trash {
        entries: Vec<ControlTrashEntry>,
    },
    TrashRestored {
        entry_id: String,
    },
    TrashDeleted {
        entry_id: String,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ControlDirectoryEntry {
    pub name: String,
    pub inode: i64,
    pub kind: ControlFileKind,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_ns: i64,
    pub has_acl: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ControlPathMetadata {
    pub inode: i64,
    pub kind: ControlFileKind,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ControlAclEntry {
    pub scope: String,
    pub tag: String,
    pub id: Option<u32>,
    pub perm: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ControlTrashEntry {
    pub id: String,
    pub original_path: String,
    pub size: Option<u64>,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlFileKind {
    File,
    Directory,
    Symlink,
}

impl From<FileType> for ControlFileKind {
    fn from(kind: FileType) -> Self {
        match kind {
            FileType::File => Self::File,
            FileType::Dir => Self::Directory,
            FileType::Symlink => Self::Symlink,
        }
    }
}

impl From<FileAttr> for ControlPathMetadata {
    fn from(attr: FileAttr) -> Self {
        Self {
            inode: attr.ino,
            kind: ControlFileKind::from(attr.kind),
            size: attr.size,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            mtime_ns: attr.mtime,
        }
    }
}

impl From<JobInfo> for ControlResponse {
    fn from(job: JobInfo) -> Self {
        Self::JobStatus {
            job_id: job.job_id,
            state: job.state,
            detail: job.detail,
            outcome: job.outcome,
        }
    }
}
