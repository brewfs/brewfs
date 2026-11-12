//! The actual bounded Get-batch driver: one transaction, private output,
//! shared attempt budget and fixed deadline after the original TSO is acquired.

use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use super::{BOUNDED_READ_MAX_POINT_KEYS, backend};
use crate::workspace::error::WorkspaceError;
use crate::workspace::stores::kv_backend::KvReadLimits;

const LOCK_CONTINUATIONS: u8 = 2;
const LOCK_BACKOFF: Duration = Duration::from_millis(20);

pub(super) struct BoundedPointReadContext {
    deadline: Instant,
    remaining_attempts: usize,
    remaining_originals: usize,
    remaining_lock_continuations: u8,
    remaining_key_lock_retries: u8,
    remaining_region_retries: u8,
}

impl BoundedPointReadContext {
    /// Call once after client/TS acquisition fixes this read-only timestamp.
    /// Pass the same timeout used by client_config; this bounds only the data
    /// batch, not client startup/initial TSO or the whole workspace API call.
    pub(super) fn new(
        max_data_requests: usize,
        original_keys: usize,
        rpc_timeout: Duration,
    ) -> Result<Self, WorkspaceError> {
        Self::new_at(
            max_data_requests,
            original_keys,
            rpc_timeout,
            Instant::now(),
        )
    }

    fn new_at(
        max_data_requests: usize,
        original_keys: usize,
        rpc_timeout: Duration,
        now: Instant,
    ) -> Result<Self, WorkspaceError> {
        Self::new_at_with_cap(
            max_data_requests,
            original_keys,
            rpc_timeout,
            now,
            BOUNDED_READ_MAX_POINT_KEYS,
        )
    }

    fn new_at_with_cap(
        max_data_requests: usize,
        original_keys: usize,
        rpc_timeout: Duration,
        now: Instant,
        hard_cap: usize,
    ) -> Result<Self, WorkspaceError> {
        let attempts = max_data_requests.min(hard_cap);
        if attempts == 0 || original_keys > attempts || rpc_timeout.is_zero() {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid bounded TiKV point-read deadline/request plan".into(),
            ));
        }
        let whole_timeout = rpc_timeout.checked_mul(attempts as u32).ok_or_else(|| {
            WorkspaceError::InvalidReadPlan("bounded TiKV read deadline overflow".into())
        })?;
        let deadline = now.checked_add(whole_timeout).ok_or_else(|| {
            WorkspaceError::InvalidReadPlan("bounded TiKV read deadline overflow".into())
        })?;
        Ok(Self {
            deadline,
            remaining_attempts: attempts,
            remaining_originals: original_keys,
            remaining_lock_continuations: LOCK_CONTINUATIONS,
            remaining_key_lock_retries: LOCK_CONTINUATIONS,
            remaining_region_retries: LOCK_CONTINUATIONS,
        })
    }

    pub(super) fn deadline(&self) -> Instant {
        self.deadline
    }

    fn ensure_before_deadline(&self) -> Result<(), WorkspaceError> {
        if Instant::now() >= self.deadline {
            return Err(backend("bounded read data-batch deadline exhausted"));
        }
        Ok(())
    }

    /// Reserve before each SDK Get invocation, including failed invocations.
    /// This is conservative if topology lookup fails before data dispatch:
    /// actual data requests can never exceed these reserved attempt slots.
    pub(super) fn admit_attempt(&mut self) -> Result<Instant, WorkspaceError> {
        self.admit_attempt_at(Instant::now())
    }

    fn admit_attempt_at(&mut self, now: Instant) -> Result<Instant, WorkspaceError> {
        if now >= self.deadline {
            return Err(WorkspaceError::Backend(
                "TiKV bounded point-read data-batch deadline exhausted".into(),
            ));
        }
        if self.remaining_attempts == 0 || self.remaining_originals == 0 {
            return Err(WorkspaceError::InvalidReadPlan(
                "TiKV bounded point-read data-attempt budget exhausted".into(),
            ));
        }
        self.remaining_attempts -= 1;
        Ok(self.deadline)
    }

    /// Mark success only after the hard decoder bound and semantic value/total
    /// checks have passed. Values remain private until every original succeeds.
    pub(super) fn record_success(&mut self) -> Result<(), WorkspaceError> {
        self.remaining_originals = self.remaining_originals.checked_sub(1).ok_or_else(|| {
            WorkspaceError::InvalidReadPlan("bounded TiKV read success count overflow".into())
        })?;
        Ok(())
    }

    /// Only after a definite precommit authentication conflict and successful
    /// rollback. Every check must be read again within the original budget.
    /// The caller separately limits this to one full authentication restart.
    pub(super) fn reset_authentication_originals(&mut self, originals: usize) -> bool {
        self.reset_authentication_originals_at(originals, Instant::now())
    }

    fn reset_authentication_originals_at(&mut self, originals: usize, now: Instant) -> bool {
        if originals == 0 || self.remaining_attempts < originals || now >= self.deadline {
            return false;
        }
        self.remaining_originals = originals;
        true
    }

    /// Call only for SDK Error::is_bounded_read_lock_conflict() == true.
    /// None means return that original error immediately, preserving its cause.
    /// Original successes still need one data attempt each: a strict keys.len()
    /// budget therefore never authorizes even its first continuation.
    pub(super) fn reserve_lock_continuation(&mut self) -> Option<Instant> {
        self.reserve_lock_continuation_at(Instant::now())
    }

    /// Spend a separate, bounded continuation for a key-level lock diagnostic
    /// that was structurally classified by the TiKV client. This does not
    /// resolve the lock or replenish the original read-attempt budget.
    pub(super) fn reserve_key_lock_retry(&mut self) -> Option<Instant> {
        self.reserve_key_lock_retry_at(Instant::now())
    }

    fn reserve_key_lock_retry_at(&mut self, now: Instant) -> Option<Instant> {
        if self.remaining_originals == 0
            || self.remaining_attempts < self.remaining_originals
            || self.remaining_key_lock_retries == 0
            || now >= self.deadline
        {
            return None;
        }
        let wake = now.checked_add(LOCK_BACKOFF)?;
        if wake >= self.deadline {
            return None;
        }
        self.remaining_key_lock_retries -= 1;
        Some(wake)
    }

    /// Spend a separate, bounded continuation for an EpochNotMatch region
    /// diagnostic. The client refreshes routing on the next dispatch; this
    /// never resolves a lock or replays a mutation.
    pub(super) fn reserve_region_retry(&mut self) -> Option<Instant> {
        let now = Instant::now();
        if self.remaining_originals == 0
            || self.remaining_attempts < self.remaining_originals
            || self.remaining_region_retries == 0
            || now >= self.deadline
        {
            return None;
        }
        let wake = now.checked_add(LOCK_BACKOFF)?;
        if wake >= self.deadline {
            return None;
        }
        self.remaining_region_retries -= 1;
        Some(wake)
    }

    fn reserve_lock_continuation_at(&mut self, now: Instant) -> Option<Instant> {
        if self.remaining_originals == 0
            || self.remaining_attempts < self.remaining_originals
            || self.remaining_lock_continuations == 0
            || now >= self.deadline
        {
            return None;
        }
        let wake = now.checked_add(LOCK_BACKOFF)?;
        if wake >= self.deadline {
            return None;
        }
        self.remaining_lock_continuations -= 1;
        Some(wake)
    }

    /// The SDK must recalculate this immediately before the real data dispatch,
    /// after topology lookup, and set grpc_timeout to this remaining duration.
    /// This helper is illustrative until the SDK accepts the absolute deadline.
    #[cfg(test)]
    fn dispatch_timeout_at(&self, now: Instant, configured_timeout: Duration) -> Option<Duration> {
        (now < self.deadline).then(|| configured_timeout.min(self.deadline - now))
    }
}

struct PointReadFailure {
    cause: WorkspaceError,
    verified_local_lock: bool,
    retryable_key_lock: bool,
    retryable_region: bool,
}

/// An existing read-only transaction, not a factory for fresh transactions.
#[async_trait]
trait BoundedPointReader: Send {
    async fn get_until(
        &mut self,
        key: Vec<u8>,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, PointReadFailure>;
}

#[async_trait]
impl BoundedPointReader for tikv_client::Transaction {
    async fn get_until(
        &mut self,
        key: Vec<u8>,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, PointReadFailure> {
        self.get_uncached_single_region_until(key, deadline)
            .await
            .map_err(|error| {
                // Classification precedes Display mapping; remote diagnostics,
                // malformed/unknown/mixed errors never mint the local marker.
                let verified_local_lock = error.is_bounded_read_lock_conflict();
                let retryable_key_lock = error.is_bounded_read_retryable_key_lock();
                let retryable_region = error.is_bounded_read_retryable_region();
                PointReadFailure {
                    cause: backend(error),
                    verified_local_lock,
                    retryable_key_lock,
                    retryable_region,
                }
            })
    }
}

pub(super) async fn read_batch(
    transaction: &mut tikv_client::Transaction,
    namespace_prefix: &[u8],
    keys: &[Vec<u8>],
    limits: KvReadLimits,
    rpc_timeout: Duration,
) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
    read_batch_with_reader(transaction, namespace_prefix, keys, limits, rpc_timeout).await
}

pub(super) async fn read_publication_packet(
    transaction: &mut tikv_client::Transaction,
    namespace_prefix: &[u8],
    keys: &[Vec<u8>],
    limits: KvReadLimits,
    rpc_timeout: Duration,
) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
    read_publication_packet_with_reader(transaction, namespace_prefix, keys, limits, rpc_timeout)
        .await
}

async fn read_publication_packet_with_reader(
    reader: &mut impl BoundedPointReader,
    namespace_prefix: &[u8],
    keys: &[Vec<u8>],
    limits: KvReadLimits,
    rpc_timeout: Duration,
) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
    crate::workspace::stores::kv_backend::validate_publication_packet_limits(keys, limits)?;
    let mut context = BoundedPointReadContext::new_at_with_cap(
        limits.max_data_requests,
        keys.len(),
        rpc_timeout,
        Instant::now(),
        64,
    )?;
    let mut values = Vec::with_capacity(keys.len());
    let mut total = 0;
    for window in keys.chunks(BOUNDED_READ_MAX_POINT_KEYS) {
        read_window_with_reader(
            reader,
            namespace_prefix,
            window,
            limits,
            &mut context,
            &mut total,
            &mut values,
        )
        .await?;
    }
    context.ensure_before_deadline()?;
    Ok(values)
}

async fn read_batch_with_reader(
    reader: &mut impl BoundedPointReader,
    namespace_prefix: &[u8],
    keys: &[Vec<u8>],
    limits: KvReadLimits,
    rpc_timeout: Duration,
) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
    limits.validate_keys(keys)?;
    let mut context =
        BoundedPointReadContext::new(limits.max_data_requests, keys.len(), rpc_timeout)?;
    let mut values = Vec::with_capacity(keys.len());
    let mut total = 0usize;
    read_window_with_reader(
        reader,
        namespace_prefix,
        keys,
        limits,
        &mut context,
        &mut total,
        &mut values,
    )
    .await?;
    context.ensure_before_deadline()?;
    Ok(values)
}

async fn read_window_with_reader(
    reader: &mut impl BoundedPointReader,
    namespace_prefix: &[u8],
    keys: &[Vec<u8>],
    limits: KvReadLimits,
    context: &mut BoundedPointReadContext,
    total: &mut usize,
    values: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), WorkspaceError> {
    for key in keys {
        let mut scoped = Vec::with_capacity(namespace_prefix.len() + key.len());
        scoped.extend_from_slice(namespace_prefix);
        scoped.extend_from_slice(key);
        let value = loop {
            context.admit_attempt()?;
            match reader.get_until(scoped.clone(), context.deadline()).await {
                Ok(value) => break value,
                Err(failure) => {
                    #[cfg(test)]
                    if std::env::var_os("BREWFS_TEST_NATIVE_CAS_DIAGNOSTICS").is_some() {
                        eprintln!(
                            "native-point-get error verified_lock={} retryable_key_lock={} remaining_attempts={} remaining_originals={} remaining_continuations={} remaining_key_retries={}",
                            failure.verified_local_lock,
                            failure.retryable_key_lock,
                            context.remaining_attempts,
                            context.remaining_originals,
                            context.remaining_lock_continuations,
                            context.remaining_key_lock_retries,
                        );
                    }
                    let wake = if failure.verified_local_lock {
                        context.reserve_lock_continuation()
                    } else if failure.retryable_key_lock {
                        context.reserve_key_lock_retry()
                    } else if failure.retryable_region {
                        context.reserve_region_retry()
                    } else {
                        None
                    };
                    let Some(wake) = wake else {
                        // A fixed key-lock diagnostic is a pre-submit read
                        // conflict. If this batch has no spare data-attempt
                        // slot, return Busy so the enclosing topology/CAS
                        // operation can rebuild its read transaction. Keep
                        // unrelated and locally certified lock errors intact.
                        return if failure.retryable_key_lock || failure.retryable_region {
                            Err(WorkspaceError::Busy)
                        } else {
                            Err(failure.cause)
                        };
                    };
                    tokio::time::sleep_until(wake).await;
                }
            }
        };
        let bytes = value.as_ref().map_or(0, Vec::len);
        *total = total
            .checked_add(key.len())
            .and_then(|sum| sum.checked_add(bytes))
            .ok_or_else(|| backend("bounded point byte count overflow"))?;
        if bytes > limits.max_value_bytes || *total > limits.max_total_bytes {
            return Err(backend("bounded point decoded value bytes exceeded"));
        }
        context.ensure_before_deadline()?;
        context.record_success()?;
        values.push(value);
    }
    // An immediately-ready RPC/decoder or semantic check may cross a timeout
    // boundary while its future is being polled. No late successful batch.
    context.ensure_before_deadline()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(maximum: usize, originals: usize) -> (BoundedPointReadContext, Instant) {
        let now = Instant::now();
        (
            BoundedPointReadContext::new_at(maximum, originals, Duration::from_secs(2), now)
                .unwrap(),
            now,
        )
    }

    #[test]
    fn strict_original_budget_returns_first_conflict_without_continuation() {
        let (mut context, now) = plan(12, 12);
        context.admit_attempt_at(now).unwrap();
        assert_eq!(context.remaining_attempts, 11);
        assert!(context.reserve_lock_continuation_at(now).is_none());
        assert_eq!(context.remaining_lock_continuations, 2);
    }

    #[test]
    fn authentication_restart_rechecks_successes_without_refilling_attempts_or_deadline() {
        let (mut context, now) = plan(7, 3);
        let deadline = context.deadline();
        context.admit_attempt_at(now).unwrap();
        context.record_success().unwrap();
        context.admit_attempt_at(now).unwrap(); // conflict on second check
        assert_eq!(context.remaining_attempts, 5);
        assert!(context.reset_authentication_originals_at(3, now));
        assert_eq!(context.remaining_originals, 3);
        assert_eq!(context.remaining_attempts, 5);
        assert_eq!(context.deadline(), deadline);
        for _ in 0..3 {
            context.admit_attempt_at(now).unwrap();
            context.record_success().unwrap();
        }
        assert_eq!(context.remaining_attempts, 2);
        assert!(!context.reset_authentication_originals_at(3, now));
        assert!(!context.reset_authentication_originals_at(1, deadline));
    }

    #[test]
    fn exact_authentication_request_budget_cannot_rebuild_even_after_first_conflict() {
        let (mut context, now) = plan(32, 32);
        context.admit_attempt_at(now).unwrap();
        assert!(!context.reset_authentication_originals_at(32, now));
        assert_eq!(context.remaining_originals, 32);
        assert_eq!(context.remaining_attempts, 31);
    }

    #[test]
    fn continuations_share_batch_budget_and_preserve_all_unread_keys() {
        let (mut context, now) = plan(4, 3);
        context.admit_attempt_at(now).unwrap(); // first key: verified lock
        assert!(context.reserve_lock_continuation_at(now).is_some());
        context.admit_attempt_at(now).unwrap(); // same key: success
        context.record_success().unwrap();
        context.admit_attempt_at(now).unwrap(); // second key: verified lock
        assert_eq!(context.remaining_attempts, 1);
        assert_eq!(context.remaining_originals, 2);
        assert!(context.reserve_lock_continuation_at(now).is_none());
    }

    #[test]
    fn persistent_lock_uses_at_most_two_continuations_even_with_large_spare_budget() {
        let (mut context, now) = plan(32, 1);
        for expected_remaining in [31, 30] {
            context.admit_attempt_at(now).unwrap();
            assert_eq!(context.remaining_attempts, expected_remaining);
            assert!(context.reserve_lock_continuation_at(now).is_some());
        }
        context.admit_attempt_at(now).unwrap();
        assert!(context.reserve_lock_continuation_at(now).is_none());
        assert_eq!(context.remaining_attempts, 29);
    }

    #[test]
    fn fixed_deadline_includes_backoff_and_never_resets_at_new_attempt() {
        let (mut context, now) = plan(2, 1);
        let deadline = context.deadline();
        assert_eq!(deadline - now, Duration::from_secs(4));
        context.admit_attempt_at(now).unwrap();
        let near_end = deadline - Duration::from_millis(10);
        assert!(context.reserve_lock_continuation_at(near_end).is_none());
        assert_eq!(context.deadline(), deadline);
        assert_eq!(
            context.dispatch_timeout_at(near_end, Duration::from_secs(2)),
            Some(Duration::from_millis(10))
        );
        assert!(context.admit_attempt_at(deadline).is_err());
        assert_eq!(context.remaining_attempts, 1);
        assert!(
            context
                .dispatch_timeout_at(deadline, Duration::from_secs(2))
                .is_none()
        );
    }

    #[test]
    fn original_successes_do_not_replenish_attempt_budget_or_continuations() {
        let (mut context, now) = plan(3, 2);
        context.admit_attempt_at(now).unwrap();
        context.record_success().unwrap();
        assert_eq!(context.remaining_attempts, 2);
        assert_eq!(context.remaining_originals, 1);
        context.admit_attempt_at(now).unwrap();
        assert!(context.reserve_lock_continuation_at(now).is_some());
        context.admit_attempt_at(now).unwrap();
        context.record_success().unwrap();
        assert_eq!(context.remaining_attempts, 0);
        assert!(context.admit_attempt_at(now).is_err());
        assert!(context.reserve_lock_continuation_at(now).is_none());
    }

    #[test]
    fn caller_budget_is_clamped_to_original_point_hard_cap() {
        let (mut context, now) = plan(4096, 32);
        assert_eq!(context.remaining_attempts, 32);
        assert_eq!(context.deadline() - now, Duration::from_secs(64));
        context.admit_attempt_at(now).unwrap();
        assert!(context.reserve_lock_continuation_at(now).is_none());
        assert!(BoundedPointReadContext::new_at(32, 33, Duration::from_secs(2), now).is_err());
    }

    #[test]
    fn zero_timeout_and_overflow_fail_before_data_admission() {
        let now = Instant::now();
        assert!(BoundedPointReadContext::new_at(0, 0, Duration::from_secs(2), now).is_err());
        assert!(BoundedPointReadContext::new_at(1, 1, Duration::ZERO, now).is_err());
        assert!(BoundedPointReadContext::new_at(32, 1, Duration::MAX, now).is_err());
    }
}

#[cfg(test)]
mod batch_tests {
    use super::super::Operations;
    use super::*;
    use futures::FutureExt;
    use std::collections::VecDeque;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    enum Answer {
        Value(Option<Vec<u8>>),
        VerifiedLock,
        RetryableKeyLock,
        UnverifiedDiagnostic,
        Other,
        Held,
        ReadyAfterDeadline,
        AfterSharedDeadline,
    }

    #[derive(Clone)]
    struct Observation {
        key: Vec<u8>,
        transaction_version: u64,
        deadline: Instant,
    }

    struct DropReceipt(Arc<AtomicUsize>);
    impl Drop for DropReceipt {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Reader {
        version: u64,
        answers: VecDeque<Answer>,
        calls: Vec<Observation>,
        dropped: Arc<AtomicUsize>,
    }

    impl Reader {
        fn new(answers: impl IntoIterator<Item = Answer>) -> Self {
            Self {
                version: 123,
                answers: answers.into_iter().collect(),
                calls: Vec::new(),
                dropped: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl BoundedPointReader for Reader {
        async fn get_until(
            &mut self,
            key: Vec<u8>,
            deadline: Instant,
        ) -> Result<Option<Vec<u8>>, PointReadFailure> {
            self.calls.push(Observation {
                key,
                deadline,
                transaction_version: self.version,
            });
            match self
                .answers
                .pop_front()
                .expect("unexpected extra data request")
            {
                Answer::Value(value) => Ok(value),
                Answer::VerifiedLock => Err(PointReadFailure {
                    cause: WorkspaceError::Backend("test local verified lock".into()),
                    verified_local_lock: true,
                    retryable_key_lock: false,
                    retryable_region: false,
                }),
                Answer::RetryableKeyLock => Err(PointReadFailure {
                    cause: WorkspaceError::Backend("test retryable key lock".into()),
                    verified_local_lock: false,
                    retryable_key_lock: true,
                    retryable_region: false,
                }),
                Answer::UnverifiedDiagnostic => Err(PointReadFailure {
                    cause: WorkspaceError::Backend("test local verified lock".into()),
                    verified_local_lock: false,
                    retryable_key_lock: false,
                    retryable_region: false,
                }),
                Answer::Other => Err(PointReadFailure {
                    cause: WorkspaceError::Backend("original unrelated error".into()),
                    verified_local_lock: false,
                    retryable_key_lock: false,
                    retryable_region: false,
                }),
                Answer::Held => {
                    let _receipt = DropReceipt(self.dropped.clone());
                    futures::future::pending().await
                }
                Answer::AfterSharedDeadline => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    std::thread::sleep(remaining + Duration::from_millis(1));
                    Ok(Some(vec![1]))
                }
                Answer::ReadyAfterDeadline => {
                    std::thread::sleep(Duration::from_millis(20));
                    Ok(Some(vec![1]))
                }
            }
        }
    }

    fn publication_limits(count: usize, attempts: usize) -> KvReadLimits {
        KvReadLimits {
            max_records: count,
            max_data_requests: attempts,
            ..limits(attempts)
        }
    }

    fn publication_keys(count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|index| format!("k{index:03}").into_bytes())
            .collect()
    }

    #[tokio::test]
    async fn publication_packet_crosses_32_with_one_timestamp_and_deadline() {
        let keys = publication_keys(40);
        let mut reader = Reader::new((0..40).map(|index| Answer::Value(Some(vec![index]))));
        let values = read_publication_packet_with_reader(
            &mut reader,
            b"scope/",
            &keys,
            publication_limits(40, 42),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(
            values,
            (0..40).map(|index| Some(vec![index])).collect::<Vec<_>>()
        );
        assert_eq!(reader.calls.len(), 40);
        assert!(reader.calls.iter().all(
            |call| call.transaction_version == 123 && call.deadline == reader.calls[0].deadline
        ));
        for (call, key) in reader.calls.iter().zip(&keys) {
            assert_eq!(call.key, [b"scope/".as_slice(), key].concat());
        }
    }

    #[tokio::test]
    async fn publication_packet_does_not_enlarge_ordinary_32_key_driver() {
        let mut reader = Reader::new([]);
        assert!(
            read_batch_with_reader(
                &mut reader,
                b"scope/",
                &publication_keys(33),
                publication_limits(33, 35),
                Duration::from_secs(2)
            )
            .await
            .is_err()
        );
        assert!(reader.calls.is_empty());
        assert!(BoundedPointReadContext::new(64, 33, Duration::from_secs(2)).is_err());
    }

    #[tokio::test]
    async fn publication_packet_shares_two_lock_continuations_across_windows() {
        let keys = publication_keys(62);
        let mut answers = Vec::new();
        for index in 0..62 {
            if index == 0 || index == 32 {
                answers.push(Answer::VerifiedLock);
            }
            answers.push(Answer::Value(Some(vec![index])));
        }
        let mut reader = Reader::new(answers);
        let values = read_publication_packet_with_reader(
            &mut reader,
            b"scope/",
            &keys,
            publication_limits(62, 64),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(values.len(), 62);
        assert_eq!(reader.calls.len(), 64);
        assert_eq!(reader.calls[0].key, reader.calls[1].key);
        assert_eq!(reader.calls[33].key, reader.calls[34].key);
        assert!(reader.calls.iter().all(
            |call| call.deadline == reader.calls[0].deadline && call.transaction_version == 123
        ));
    }

    #[tokio::test]
    async fn publication_packet_64_originals_have_zero_lock_spare_in_second_window() {
        let mut answers: Vec<_> = (0..32).map(|_| Answer::Value(None)).collect();
        answers.push(Answer::VerifiedLock);
        let mut reader = Reader::new(answers);
        let error = read_publication_packet_with_reader(
            &mut reader,
            b"scope/",
            &publication_keys(64),
            publication_limits(64, 64),
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("test local verified lock"));
        assert_eq!(reader.calls.len(), 33);
    }

    #[tokio::test]
    async fn publication_packet_keeps_aggregate_bytes_across_second_window() {
        let mut reader = Reader::new((0..33).map(|_| Answer::Value(Some(vec![0; 10]))));
        let mut limits = publication_limits(33, 35);
        limits.max_total_bytes = 460; // 32 * (4 byte key + 10 byte value) fits; 33 does not.
        let error = read_publication_packet_with_reader(
            &mut reader,
            b"scope/",
            &publication_keys(33),
            limits,
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("decoded value bytes exceeded"));
        assert_eq!(reader.calls.len(), 33);
    }

    #[tokio::test]
    async fn publication_packet_later_unknown_error_returns_no_partial_or_retry() {
        let mut answers: Vec<_> = (0..32).map(|_| Answer::Value(None)).collect();
        answers.push(Answer::Other);
        let mut reader = Reader::new(answers);
        let error = read_publication_packet_with_reader(
            &mut reader,
            b"scope/",
            &publication_keys(40),
            publication_limits(40, 42),
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("original unrelated error"));
        assert_eq!(reader.calls.len(), 33);
    }

    #[tokio::test]
    async fn publication_packet_late_second_window_result_is_not_success() {
        let mut answers: Vec<_> = (0..32).map(|_| Answer::Value(None)).collect();
        answers.push(Answer::AfterSharedDeadline);
        let mut reader = Reader::new(answers);
        let error = read_publication_packet_with_reader(
            &mut reader,
            b"scope/",
            &publication_keys(33),
            publication_limits(33, 33),
            Duration::from_millis(2),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("deadline exhausted"));
        assert_eq!(reader.calls.len(), 33);
    }

    #[tokio::test]
    async fn publication_packet_rejects_65_keys_or_attempts_before_dispatch() {
        for (keys, limits) in [
            (publication_keys(65), publication_limits(65, 64)),
            (publication_keys(33), publication_limits(33, 65)),
        ] {
            let mut reader = Reader::new([]);
            assert!(
                read_publication_packet_with_reader(
                    &mut reader,
                    b"scope/",
                    &keys,
                    limits,
                    Duration::from_secs(2)
                )
                .await
                .is_err()
            );
            assert!(reader.calls.is_empty());
        }
    }

    #[tokio::test]
    async fn publication_packet_cancellation_in_second_window_releases_inline_owners() {
        let operations = Arc::<Operations>::default();
        let mut answers: Vec<_> = (0..32).map(|_| Answer::Value(None)).collect();
        answers.push(Answer::Held);
        let mut reader = Reader::new(answers);
        let dropped = reader.dropped.clone();
        let keys = publication_keys(40);
        let mut future = Box::pin(async {
            let _operation = operations.enter()?;
            read_publication_packet_with_reader(
                &mut reader,
                b"scope/",
                &keys,
                publication_limits(40, 42),
                Duration::from_secs(2),
            )
            .await
        });
        assert!(future.as_mut().now_or_never().is_none());
        assert_eq!(operations.state.lock().unwrap().active, 1);
        drop(future);
        assert_eq!(reader.calls.len(), 33);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(operations.state.lock().unwrap().active, 0);
        operations.close_and_drain().await;
        assert!(operations.enter().is_err());
    }

    fn limits(maximum: usize) -> KvReadLimits {
        KvReadLimits {
            max_records: 32,
            max_key_bytes: 1024,
            max_value_bytes: 1024,
            max_total_bytes: 64 << 10,
            max_response_bytes: 16 << 10,
            max_data_requests: maximum,
        }
    }

    fn keys() -> Vec<Vec<u8>> {
        vec![b"a".to_vec(), b"b".to_vec()]
    }

    async fn run(
        reader: &mut Reader,
        keys: &[Vec<u8>],
        maximum: usize,
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        read_batch_with_reader(
            reader,
            b"scope/",
            keys,
            limits(maximum),
            Duration::from_secs(2),
        )
        .await
    }

    #[tokio::test]
    async fn verified_lock_continues_same_key_timestamp_and_fixed_batch_deadline() {
        let mut reader = Reader::new([
            Answer::VerifiedLock,
            Answer::Value(Some(vec![1])),
            Answer::Value(None),
        ]);
        assert_eq!(
            run(&mut reader, &keys(), 3).await.unwrap(),
            vec![Some(vec![1]), None]
        );
        assert_eq!(
            reader
                .calls
                .iter()
                .map(|call| call.key.as_slice())
                .collect::<Vec<_>>(),
            vec![
                b"scope/a".as_slice(),
                b"scope/a".as_slice(),
                b"scope/b".as_slice()
            ]
        );
        assert!(
            reader
                .calls
                .iter()
                .all(|call| call.transaction_version == 123)
        );
        assert!(
            reader
                .calls
                .iter()
                .all(|call| call.deadline == reader.calls[0].deadline)
        );
    }

    #[tokio::test]
    async fn retryable_key_lock_retries_at_the_same_timestamp_with_spare_budget() {
        let mut reader = Reader::new([
            Answer::RetryableKeyLock,
            Answer::Value(Some(vec![1])),
            Answer::Value(None),
        ]);
        assert_eq!(
            run(&mut reader, &keys(), 3).await.unwrap(),
            vec![Some(vec![1]), None]
        );
        assert_eq!(
            reader
                .calls
                .iter()
                .map(|call| call.key.as_slice())
                .collect::<Vec<_>>(),
            vec![
                b"scope/a".as_slice(),
                b"scope/a".as_slice(),
                b"scope/b".as_slice()
            ]
        );
    }

    #[tokio::test]
    async fn retryable_key_lock_without_spare_budget_returns_busy_for_outer_retry() {
        let mut reader = Reader::new([Answer::RetryableKeyLock]);
        let error = run(&mut reader, &keys()[..1], 1).await.unwrap_err();
        assert!(matches!(error, WorkspaceError::Busy));
        assert_eq!(reader.calls.len(), 1);
    }

    #[tokio::test]
    async fn strict_key_count_budget_does_not_retry_its_first_verified_lock() {
        let mut reader = Reader::new([Answer::VerifiedLock]);
        let error = run(&mut reader, &keys(), 2).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "workspace backend error: test local verified lock"
        );
        assert_eq!(reader.calls.len(), 1);
    }

    #[tokio::test]
    async fn unverified_matching_diagnostic_does_not_authorize_continuation() {
        let mut reader = Reader::new([Answer::UnverifiedDiagnostic]);
        assert!(run(&mut reader, &keys(), 32).await.is_err());
        assert_eq!(reader.calls.len(), 1);
    }

    #[tokio::test]
    async fn spare_is_shared_between_keys_and_later_failure_returns_no_partial_batch() {
        let mut reader = Reader::new([
            Answer::VerifiedLock,
            Answer::Value(Some(vec![1])),
            Answer::VerifiedLock,
        ]);
        let error = run(&mut reader, &keys(), 3).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "workspace backend error: test local verified lock"
        );
        assert_eq!(reader.calls.len(), 3);
        assert_eq!(reader.calls[2].key, b"scope/b");
    }

    #[tokio::test]
    async fn unrelated_later_error_discards_prior_success_without_retry() {
        let mut reader = Reader::new([Answer::Value(Some(vec![1])), Answer::Other]);
        let error = run(&mut reader, &keys(), 32).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "workspace backend error: original unrelated error"
        );
        assert_eq!(reader.calls.len(), 2);
    }

    #[tokio::test]
    async fn persistent_lock_stops_after_three_requests_even_with_large_spare_budget() {
        let mut reader = Reader::new([
            Answer::VerifiedLock,
            Answer::VerifiedLock,
            Answer::VerifiedLock,
        ]);
        assert!(run(&mut reader, &keys()[..1], 32).await.is_err());
        assert_eq!(reader.calls.len(), 3);
    }

    #[tokio::test]
    async fn logical_value_and_aggregate_limits_remain_hard_after_continuation() {
        let mut reader = Reader::new([Answer::VerifiedLock, Answer::Value(Some(vec![0; 1025]))]);
        let error = run(&mut reader, &keys()[..1], 3).await.unwrap_err();
        assert!(error.to_string().contains("decoded value bytes exceeded"));
        assert_eq!(reader.calls.len(), 2);
        let mut reader = Reader::new([
            Answer::Value(Some(vec![0; 1024])),
            Answer::Value(Some(vec![0; 1024])),
        ]);
        let mut small = limits(2);
        small.max_total_bytes = 1026;
        assert!(
            read_batch_with_reader(
                &mut reader,
                b"scope/",
                &keys(),
                small,
                Duration::from_secs(2)
            )
            .await
            .is_err()
        );
        assert_eq!(reader.calls.len(), 2);
    }

    #[tokio::test]
    async fn ready_result_after_deadline_cannot_return_a_successful_batch() {
        let mut reader = Reader::new([Answer::ReadyAfterDeadline]);
        let error = read_batch_with_reader(
            &mut reader,
            b"scope/",
            &keys()[..1],
            limits(1),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("data-batch deadline exhausted"));
        assert_eq!(reader.calls.len(), 1);
    }

    #[tokio::test]
    async fn held_data_read_cancellation_releases_inline_data_and_operation_owners() {
        let operations = Arc::<Operations>::default();
        let mut reader = Reader::new([Answer::Held]);
        let dropped = reader.dropped.clone();
        let keys = keys();
        let mut future = Box::pin(async {
            let _operation = operations.enter()?;
            run(&mut reader, &keys[..1], 1).await
        });
        assert!(future.as_mut().now_or_never().is_none());
        assert_eq!(operations.state.lock().unwrap().active, 1);
        drop(future);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(operations.state.lock().unwrap().active, 0);
        operations.close_and_drain().await;
        assert!(operations.enter().is_err());
    }
}
