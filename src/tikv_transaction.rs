//! Cancellation-safe ownership for TiKV transactions.
//!
//! `tikv-client` deliberately reports an active transaction from `Drop`, but
//! Rust future cancellation can drop a transaction before an enclosing
//! operation gets a chance to run its error path.  This guard keeps the
//! transaction alive in a best-effort rollback task in that case.  Callers
//! still explicitly commit or roll back on every ordinary path; the guard is
//! the cancellation boundary, not a replacement for those paths.

use std::{
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
};

use tikv_client::Transaction;

pub(crate) type RollbackFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;

pub(crate) trait RollbackAction<T>: Send + 'static {
    fn rollback(self, transaction: T) -> RollbackFuture;
}

impl<T, F> RollbackAction<T> for F
where
    F: FnOnce(T) -> RollbackFuture + Send + 'static,
{
    fn rollback(self, transaction: T) -> RollbackFuture {
        self(transaction)
    }
}

/// Owns a transaction until it reaches a terminal state.
pub(crate) struct TxnGuard<T, R>
where
    R: RollbackAction<T>,
{
    transaction: Option<T>,
    rollback: Option<R>,
    operation: &'static str,
}

impl<T, R> TxnGuard<T, R>
where
    R: RollbackAction<T>,
{
    fn with_rollback(transaction: T, rollback: R, operation: &'static str) -> Self {
        Self {
            transaction: Some(transaction),
            rollback: Some(rollback),
            operation,
        }
    }

    /// Return the guarded transaction for normal operations.
    pub(crate) fn transaction_mut(&mut self) -> &mut T {
        self.transaction
            .as_mut()
            .expect("transaction guard already finished")
    }

    /// Mark a transaction that the caller has completed as terminal.
    ///
    /// The transaction must already have committed or rolled back.  This only
    /// disarms the cancellation fallback; it does not send an RPC.
    pub(crate) fn finish(&mut self) {
        self.transaction.take();
        self.rollback.take();
    }
}

impl<T, R> Deref for TxnGuard<T, R>
where
    R: RollbackAction<T>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.transaction
            .as_ref()
            .expect("transaction guard already finished")
    }
}

impl<T, R> DerefMut for TxnGuard<T, R>
where
    R: RollbackAction<T>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.transaction_mut()
    }
}

impl<T, R> Drop for TxnGuard<T, R>
where
    R: RollbackAction<T>,
{
    fn drop(&mut self) {
        let Some(transaction) = self.transaction.take() else {
            return;
        };
        let Some(rollback) = self.rollback.take() else {
            return;
        };
        let operation = self.operation;
        let rollback = rollback.rollback(transaction);
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            log::error!(
                "cannot schedule TiKV transaction rollback after cancellation (operation={operation})"
            );
            return;
        };
        handle.spawn(async move {
            if let Err(error) = rollback.await {
                log::debug!(
                    "TiKV transaction rollback after cancellation failed (operation={operation}): {error}"
                );
            }
        });
    }
}

/// Guard for the vendored TiKV client's default transaction type.
pub(crate) type TiKvTxnGuard = TxnGuard<Transaction, fn(Transaction) -> RollbackFuture>;

fn rollback_tikv(transaction: Transaction) -> RollbackFuture {
    Box::pin(async move {
        let mut transaction = transaction;
        transaction
            .rollback()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    })
}

impl TxnGuard<Transaction, fn(Transaction) -> RollbackFuture> {
    pub(crate) fn new_tikv(transaction: Transaction, operation: &'static str) -> Self {
        Self::with_rollback(transaction, rollback_tikv, operation)
    }

    // Preserve the vendored client's error type so existing backend error
    // mapping keeps the original TiKV details. The type is intentionally
    // large, so boxing it here would change every caller's error path.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn commit(
        &mut self,
    ) -> Result<Option<tikv_client::Timestamp>, tikv_client::Error> {
        let result = self.transaction_mut().commit().await;
        if result.is_ok() {
            self.finish();
        }
        result
    }

    #[allow(clippy::result_large_err)]
    pub(crate) async fn rollback(&mut self) -> Result<(), tikv_client::Error> {
        let result = self.transaction_mut().rollback().await;
        if result.is_ok() {
            self.finish();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::{RollbackFuture, TxnGuard};

    struct FakeTransaction {
        key: usize,
        locks: Arc<tokio::sync::Mutex<HashSet<usize>>>,
        active_drop_warnings: Arc<AtomicUsize>,
        active: bool,
    }

    impl FakeTransaction {
        async fn lock(&self) {
            loop {
                if self.locks.lock().await.insert(self.key) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }

        async fn rollback(mut self) -> Result<(), String> {
            self.locks.lock().await.remove(&self.key);
            self.active = false;
            Ok(())
        }

        async fn commit(&mut self) {
            self.locks.lock().await.remove(&self.key);
            self.active = false;
        }
    }

    impl Drop for FakeTransaction {
        fn drop(&mut self) {
            if self.active {
                self.active_drop_warnings.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn rollback_fake(transaction: FakeTransaction) -> RollbackFuture {
        Box::pin(transaction.rollback())
    }

    #[tokio::test]
    async fn cancelled_guard_rolls_back_without_active_drop_warning() {
        let locks = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let warnings = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(tokio::sync::Notify::new());
        let notified = ready.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let task = tokio::spawn({
            let locks = locks.clone();
            let warnings = warnings.clone();
            let ready = ready.clone();
            async move {
                let transaction = FakeTransaction {
                    key: 7,
                    locks,
                    active_drop_warnings: warnings,
                    active: true,
                };
                let guard = TxnGuard::with_rollback(transaction, rollback_fake, "guard-test");
                guard.lock().await;
                ready.notify_one();
                std::future::pending::<()>().await;
            }
        });
        notified.await;
        task.abort();
        task.await
            .expect_err("the operation future should be cancelled");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if locks.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("rollback task should release the fake lock");
        assert_eq!(warnings.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn five_thousand_imports_survive_four_workers_and_failures() {
        const FILES: usize = 5_000;
        const WORKERS: usize = 4;
        let locks = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let imported = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let warnings = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(WORKERS);

        for worker in 0..WORKERS {
            let locks = locks.clone();
            let imported = imported.clone();
            let warnings = warnings.clone();
            let failures = failures.clone();
            let completed = completed.clone();
            workers.push(tokio::spawn(async move {
                for file in (worker..FILES).step_by(WORKERS) {
                    loop {
                        let transaction = FakeTransaction {
                            key: file,
                            locks: locks.clone(),
                            active_drop_warnings: warnings.clone(),
                            active: true,
                        };
                        let mut guard =
                            TxnGuard::with_rollback(transaction, rollback_fake, "5k-import");
                        guard.lock().await;
                        // Deterministic pseudo-random failure injection: the
                        // first attempt of roughly one in seven files is
                        // cancelled while its lock is held.
                        let inject_failure =
                            (file.wrapping_mul(1103515245).wrapping_add(12345) % 7) == 0
                                && failures.fetch_add(1, Ordering::Relaxed) < FILES / 7;
                        if inject_failure {
                            drop(guard);
                            tokio::task::yield_now().await;
                            continue;
                        }
                        imported.lock().await.insert(file);
                        guard.transaction_mut().commit().await;
                        guard.finish();
                        completed.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }));
        }
        for worker in workers {
            worker.await.expect("import worker should finish");
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if locks.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all cancelled transactions should roll back");
        assert_eq!(imported.lock().await.len(), FILES);
        assert_eq!(completed.load(Ordering::Relaxed), FILES);
        assert_eq!(warnings.load(Ordering::Relaxed), 0);
    }
}
