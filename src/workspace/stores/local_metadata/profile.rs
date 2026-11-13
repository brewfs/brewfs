//! Opt-in aggregate wall timings. Nested/concurrent stages are NOT additive.
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Clone, Copy)]
pub(super) enum Stage {
    Update,
    AdmissionWait,
    UpdateStateWait,
    Merge,
    ValidateRemote,
    UpdateInstall,
    ReadStateWait,
    ReadRemote,
    AuthStateWait,
    AuthRemote,
    FlushWait,
    FreezeStateWait,
    FreezeInstall,
    ReceiptRead,
    RemoteCommit,
    AckStateWait,
    AckInstall,
    BlockingQueue,
    JournalEncode,
    JournalWrite,
    JournalFileSync,
    JournalRenameSync,
}

const NAMES: &[&str] = &[
    "update",
    "admission_wait",
    "update_state_wait",
    "merge",
    "validate_remote",
    "update_install",
    "read_state_wait",
    "read_remote",
    "auth_state_wait",
    "auth_remote",
    "flush_wait",
    "freeze_state_wait",
    "freeze_install",
    "receipt_read",
    "remote_commit",
    "ack_state_wait",
    "ack_install",
    "blocking_queue",
    "journal_encode",
    "journal_write",
    "journal_file_sync",
    "journal_rename_sync",
];

#[derive(Default)]
struct Counter {
    calls: AtomicU64,
    ns: AtomicU64,
}

pub(super) struct Profile {
    enabled: bool,
    counters: [Counter; NAMES.len()],
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            enabled: std::env::var("BREWFS_METADATA_PROFILE").as_deref() == Ok("1"),
            counters: std::array::from_fn(|_| Counter::default()),
        }
    }
}

impl Profile {
    pub(super) fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }
    pub(super) fn record(&self, stage: Stage, start: Option<Instant>) {
        if let Some(start) = start {
            let counter = &self.counters[stage as usize];
            counter
                .ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            counter.calls.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(super) fn span(&self, stage: Stage) -> Span<'_> {
        Span {
            profile: self,
            stage,
            start: self.start(),
        }
    }
    pub(super) fn snapshot(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();
        for (name, counter) in NAMES.iter().zip(&self.counters) {
            result.insert(
                (*name).into(),
                serde_json::json!({
                    "calls": counter.calls.load(Ordering::Relaxed),
                    "total_ns": counter.ns.load(Ordering::Relaxed),
                }),
            );
        }
        serde_json::Value::Object(result)
    }
    pub(super) fn emit(&self) {
        if self.enabled {
            eprintln!("BREWFS_METADATA_PROFILE {}", self.snapshot());
        }
    }
}

pub(super) struct Span<'a> {
    profile: &'a Profile,
    stage: Stage,
    start: Option<Instant>,
}
impl Drop for Span<'_> {
    fn drop(&mut self) {
        self.profile.record(self.stage, self.start);
    }
}
