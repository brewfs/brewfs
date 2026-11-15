//! Offline Native Overlay merge requests and durable commit results.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::workspace::ids::{LayerId, WorkspaceId};

/// Reuse the same operation ID when retrying an interrupted invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MergeRequest {
    pub operation: Uuid,
    pub child: WorkspaceId,
    pub target: WorkspaceId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MergeConflict {
    pub ino: i64,
    pub name: Option<Vec<u8>>,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MergeResult {
    pub request: MergeRequest,
    pub committed: bool,
    pub target_head: LayerId,
    pub target_sequence: u64,
    pub scanned_records: usize,
    pub applied_records: usize,
    pub conflicts: Vec<MergeConflict>,
}
