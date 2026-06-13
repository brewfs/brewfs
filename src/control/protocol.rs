use crate::control::job::{JobInfo, JobOutcome, JobState};
use crate::meta::store::{FileAttr, FileType, MetaStoreCapabilities};

pub const CONTROL_ACL_XATTR_NAME: &str = "system.brewfs.acl";

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

pub fn validate_acl_entries(entries: &[ControlAclEntry]) -> Result<(), String> {
    for (index, entry) in entries.iter().enumerate() {
        validate_acl_entry(entry, index)?;
    }
    validate_acl_entry_set(entries)?;
    Ok(())
}

fn validate_acl_entry_set(entries: &[ControlAclEntry]) -> Result<(), String> {
    let has_access_entries = entries.iter().any(|entry| entry.scope == "access");
    if !has_access_entries {
        return Ok(());
    }

    let has_user_obj = has_acl_entry(entries, "access", "user_obj");
    let has_group_obj = has_acl_entry(entries, "access", "group_obj");
    let has_other = has_acl_entry(entries, "access", "other");
    if has_user_obj && has_group_obj && has_other {
        Ok(())
    } else {
        Err("access ACL must include user_obj, group_obj, and other entries".to_string())
    }
}

fn has_acl_entry(entries: &[ControlAclEntry], scope: &str, tag: &str) -> bool {
    entries
        .iter()
        .any(|entry| entry.scope == scope && entry.tag == tag)
}

fn validate_acl_entry(entry: &ControlAclEntry, index: usize) -> Result<(), String> {
    let entry_number = index + 1;
    if entry.scope != "access" && entry.scope != "default" {
        return Err(format!(
            "ACL entry {entry_number} scope must be access or default"
        ));
    }

    if !matches!(
        entry.tag.as_str(),
        "user_obj" | "user" | "group_obj" | "group" | "mask" | "other"
    ) {
        return Err(format!("ACL entry {entry_number} tag is not supported"));
    }

    if !valid_acl_perm(&entry.perm) {
        return Err(format!(
            "ACL entry {entry_number} perm must use rwx characters like rw- or r-x"
        ));
    }

    if matches!(entry.tag.as_str(), "user" | "group") && entry.id.is_none() {
        return Err(format!(
            "ACL entry {entry_number} tag {} requires id",
            entry.tag
        ));
    }

    if !matches!(entry.tag.as_str(), "user" | "group") && entry.id.is_some() {
        return Err(format!(
            "ACL entry {entry_number} tag {} must not include id",
            entry.tag
        ));
    }

    Ok(())
}

fn valid_acl_perm(perm: &str) -> bool {
    let mut chars = perm.chars();
    matches!(chars.next(), Some('r' | '-'))
        && matches!(chars.next(), Some('w' | '-'))
        && matches!(chars.next(), Some('x' | '-'))
        && chars.next().is_none()
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
