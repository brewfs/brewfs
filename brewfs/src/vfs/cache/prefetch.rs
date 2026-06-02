use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use tokio::sync::{Semaphore, mpsc};

/// Priority level for prefetch tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PrefetchPriority {
    /// Synchronous demand read — highest priority.
    Demand,
    /// Sequential readahead — medium priority.
    Sequential,
    /// Background warmup — lowest priority.
    Background,
}

/// A prefetch task submitted to the global prefetcher.
#[derive(Debug, Clone)]
pub struct PrefetchTask {
    pub ino: i64,
    pub start: u64,
    pub len: u64,
    pub priority: PrefetchPriority,
    pub owner_fh: u64,
}

/// Trait for a global prefetch scheduler.
///
/// The prefetcher accepts tasks from FileReader sessions and schedules them
/// against the ReadCache, respecting concurrency limits and priorities.
#[async_trait::async_trait]
pub trait Prefetcher: Send + Sync {
    /// Submit a prefetch task. May be dropped if the queue is full.
    async fn submit(&self, task: PrefetchTask);

    /// Cancel all pending prefetch tasks for a specific file handle.
    async fn cancel_for_handle(&self, ino: i64, fh: u64);
}

/// Unique key for deduplicating in-flight prefetch ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RangeKey {
    ino: i64,
    start: u64,
    end: u64,
}

/// Global prefetch scheduler that coordinates readahead across all handles.
///
/// Design:
/// - Bounded task queue (mpsc channel) prevents unbounded memory growth
/// - Semaphore limits concurrent prefetch I/O to avoid saturating the backend
/// - In-flight DashSet deduplicates overlapping ranges without mutex contention
/// - Each drained batch runs demand reads before sequential and background work
pub struct GlobalPrefetcher {
    tx: mpsc::Sender<PrefetchTask>,
    cancelled: Arc<DashSet<(i64, u64)>>,
    pending_by_handle: Arc<DashMap<(i64, u64), usize>>,
}

impl GlobalPrefetcher {
    /// Create a new prefetcher with the given concurrency limit and queue depth.
    pub fn new<F, Fut>(concurrency: usize, queue_depth: usize, fetch_fn: F) -> Self
    where
        F: Fn(i64, u64, u64) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(queue_depth);
        let in_flight = Arc::new(DashSet::new());
        let cancelled = Arc::new(DashSet::new());
        let pending_by_handle = Arc::new(DashMap::new());

        let worker_in_flight = in_flight.clone();
        let worker_cancelled = cancelled.clone();
        let worker_pending = pending_by_handle.clone();
        let sem = Arc::new(Semaphore::new(concurrency));
        let fetch_fn = Arc::new(fetch_fn);

        tokio::spawn(Self::worker_loop(
            rx,
            sem,
            worker_in_flight,
            worker_cancelled,
            worker_pending,
            fetch_fn,
        ));

        Self {
            tx,
            cancelled,
            pending_by_handle,
        }
    }

    fn increment_pending(pending_by_handle: &DashMap<(i64, u64), usize>, key: (i64, u64)) {
        pending_by_handle
            .entry(key)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    fn decrement_pending(pending_by_handle: &DashMap<(i64, u64), usize>, key: (i64, u64)) {
        let mut remove = false;
        if let Some(mut entry) = pending_by_handle.get_mut(&key) {
            if *entry <= 1 {
                remove = true;
            } else {
                *entry -= 1;
            }
        }
        if remove {
            pending_by_handle.remove(&key);
        }
    }

    fn cleanup_cancelled_if_drained(
        cancelled: &DashSet<(i64, u64)>,
        pending_by_handle: &DashMap<(i64, u64), usize>,
        key: (i64, u64),
    ) {
        if !pending_by_handle.contains_key(&key) {
            cancelled.remove(&key);
        }
    }

    fn finish_pending(
        pending_by_handle: &DashMap<(i64, u64), usize>,
        cancelled: &DashSet<(i64, u64)>,
        key: (i64, u64),
    ) {
        Self::decrement_pending(pending_by_handle, key);
        Self::cleanup_cancelled_if_drained(cancelled, pending_by_handle, key);
    }

    async fn worker_loop<F, Fut>(
        mut rx: mpsc::Receiver<PrefetchTask>,
        sem: Arc<Semaphore>,
        in_flight: Arc<DashSet<RangeKey>>,
        cancelled: Arc<DashSet<(i64, u64)>>,
        pending_by_handle: Arc<DashMap<(i64, u64), usize>>,
        fetch_fn: Arc<F>,
    ) where
        F: Fn(i64, u64, u64) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        // Batch buffer: drain up to 64 tasks per wake-up to amortize the
        // async recv overhead across multiple items.
        let mut batch = Vec::with_capacity(64);

        loop {
            batch.clear();

            // Block on first task; then opportunistically drain pending.
            let first = rx.recv().await;
            let Some(first) = first else { break };
            batch.push(first);
            // recv_many fills up to capacity without blocking
            let _ = rx.recv_many(&mut batch, 63).await;
            batch.sort_by_key(|task| task.priority);

            for task in batch.drain(..) {
                let owner = (task.ino, task.owner_fh);
                // Skip if handle was cancelled (lock-free DashSet lookup).
                if cancelled.contains(&owner) {
                    Self::finish_pending(&pending_by_handle, &cancelled, owner);
                    continue;
                }

                let key = RangeKey {
                    ino: task.ino,
                    start: task.start,
                    end: task.start + task.len,
                };

                // Deduplicate: insert returns false if key already existed.
                if !in_flight.insert(key) {
                    Self::finish_pending(&pending_by_handle, &cancelled, owner);
                    continue;
                }

                let permit = match sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        Self::finish_pending(&pending_by_handle, &cancelled, owner);
                        break;
                    }
                };
                Self::finish_pending(&pending_by_handle, &cancelled, owner);

                let in_flight_done = in_flight.clone();
                let fetch = fetch_fn.clone();
                tokio::spawn(async move {
                    fetch(task.ino, task.start, task.len).await;
                    in_flight_done.remove(&key);
                    drop(permit);
                });
            }
        }
    }
}

#[async_trait::async_trait]
impl Prefetcher for GlobalPrefetcher {
    async fn submit(&self, task: PrefetchTask) {
        let owner = (task.ino, task.owner_fh);
        Self::increment_pending(&self.pending_by_handle, owner);
        if self.tx.try_send(task).is_err() {
            Self::decrement_pending(&self.pending_by_handle, owner);
        }
    }

    async fn cancel_for_handle(&self, ino: i64, fh: u64) {
        let owner = (ino, fh);
        if self.pending_by_handle.contains_key(&owner) {
            self.cancelled.insert(owner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn cancelled_handle_skips_queued_task_behind_other_prefetches() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (task_tx, task_rx) = mpsc::channel(2048);
        let in_flight = Arc::new(DashSet::new());
        let cancelled = Arc::new(DashSet::new());
        let pending_by_handle = Arc::new(DashMap::new());
        let sem = Arc::new(Semaphore::new(1024));

        let cancelled_ino = 9000;
        let cancelled_fh = 77;
        for fh in 0..129 {
            cancelled.insert((cancelled_ino, fh));
        }

        for i in 0..640 {
            GlobalPrefetcher::increment_pending(&pending_by_handle, (1, 1));
            task_tx
                .send(PrefetchTask {
                    ino: 1,
                    start: i * 4096,
                    len: 4096,
                    priority: PrefetchPriority::Sequential,
                    owner_fh: 1,
                })
                .await
                .unwrap();
        }

        GlobalPrefetcher::increment_pending(&pending_by_handle, (cancelled_ino, cancelled_fh));
        task_tx
            .send(PrefetchTask {
                ino: cancelled_ino,
                start: 0,
                len: 4096,
                priority: PrefetchPriority::Sequential,
                owner_fh: cancelled_fh,
            })
            .await
            .unwrap();
        drop(task_tx);

        let worker = tokio::spawn(GlobalPrefetcher::worker_loop(
            task_rx,
            sem,
            in_flight,
            cancelled,
            pending_by_handle,
            Arc::new(move |ino, start, _len| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send((ino, start));
                }
            }),
        ));

        let mut control_seen = 0usize;
        let mut cancelled_ran = false;
        let deadline = tokio::time::sleep(Duration::from_secs(2));
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => break,
                event = rx.recv() => {
                    let Some((ino, _start)) = event else { break };
                    if ino == cancelled_ino {
                        cancelled_ran = true;
                        break;
                    }
                    control_seen += 1;
                    if control_seen >= 640 {
                        break;
                    }
                }
            }
        }

        worker.await.unwrap();

        assert_eq!(control_seen, 640);
        assert!(!cancelled_ran, "cancelled handle prefetch task ran");

        if let Ok(Some((ino, _))) = timeout(Duration::from_millis(100), rx.recv()).await {
            assert_ne!(ino, cancelled_ino, "cancelled task should not emit");
        }
    }

    #[tokio::test]
    async fn demand_priority_runs_before_background_tasks_in_same_batch() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (task_tx, task_rx) = mpsc::channel(128);
        let in_flight = Arc::new(DashSet::new());
        let cancelled = Arc::new(DashSet::new());
        let pending_by_handle = Arc::new(DashMap::new());
        let sem = Arc::new(Semaphore::new(1));

        for i in 0..8 {
            GlobalPrefetcher::increment_pending(&pending_by_handle, (1, 1));
            task_tx
                .send(PrefetchTask {
                    ino: 1,
                    start: i * 4096,
                    len: 4096,
                    priority: PrefetchPriority::Background,
                    owner_fh: 1,
                })
                .await
                .unwrap();
        }

        GlobalPrefetcher::increment_pending(&pending_by_handle, (1, 1));
        task_tx
            .send(PrefetchTask {
                ino: 1,
                start: 999 * 4096,
                len: 4096,
                priority: PrefetchPriority::Demand,
                owner_fh: 1,
            })
            .await
            .unwrap();
        drop(task_tx);

        GlobalPrefetcher::worker_loop(
            task_rx,
            sem,
            in_flight,
            cancelled,
            pending_by_handle,
            Arc::new(move |_ino, start, _len| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(start);
                }
            }),
        )
        .await;

        assert_eq!(
            timeout(Duration::from_secs(1), rx.recv()).await.unwrap(),
            Some(999 * 4096),
            "demand prefetch should be scheduled ahead of background prefetch"
        );
    }

    #[tokio::test]
    async fn deduplicates_identical_in_flight_ranges() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let hold = Arc::new(tokio::sync::Notify::new());
        let prefetcher = GlobalPrefetcher::new(8, 32, {
            let hold = hold.clone();
            move |ino, start, _len| {
                let tx = tx.clone();
                let hold = hold.clone();
                async move {
                    let _ = tx.send((ino, start));
                    hold.notified().await;
                }
            }
        });

        for _ in 0..8 {
            prefetcher
                .submit(PrefetchTask {
                    ino: 10,
                    start: 4096,
                    len: 4096,
                    priority: PrefetchPriority::Sequential,
                    owner_fh: 1,
                })
                .await;
        }

        assert_eq!(
            timeout(Duration::from_secs(1), rx.recv()).await.unwrap(),
            Some((10, 4096))
        );
        assert!(
            timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err(),
            "duplicate in-flight ranges should piggyback on the first task"
        );
        hold.notify_waiters();
    }
}
