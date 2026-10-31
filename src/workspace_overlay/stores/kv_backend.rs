//! Transactional key/value substrate shared by remote workspace catalogs.

use async_trait::async_trait;

use crate::workspace_overlay::error::WorkspaceError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvCheck {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvWrite {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Limits for a read admitted by the caller before awaiting the backend.
/// `max_response_bytes` is the hard per-response protocol bound; decoded
/// key/value/total limits are semantic limits, and may be checked after a
/// response protected by the hard bound. Arbitrarily large decoded responses
/// followed by Vec len checks do not implement this contract. Caller admission
/// must cover transport/decoder expansion and retained output, not just wire
/// bytes. Backends may enforce a smaller fixed hard response bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KvReadLimits {
    pub max_records: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    /// Sum of logical key and stored-value bytes in the complete result.
    pub max_total_bytes: usize,
    pub max_response_bytes: usize,
    /// Maximum application data requests/pages. Backend topology/clock RPCs
    /// have their own finite retry/envelope limits, and are not counted here.
    pub max_data_requests: usize,
}

/// Validate bounded exact-check inputs without cloning their values.
/// Authentication is a fixed catalog operation, limited to 32 authority keys.
pub(crate) fn validate_bounded_authentication_checks(
    checks: &[KvCheck],
    limits: KvReadLimits,
) -> Result<(), WorkspaceError> {
    limits.validate()?;
    if checks.is_empty()
        || checks.len() > 32
        || checks.len() > limits.max_records
        || checks.len() > limits.max_data_requests
    {
        return Err(WorkspaceError::InvalidReadPlan(
            "bounded authentication authority count exceeded".into(),
        ));
    }
    let mut total = 0usize;
    for (index, check) in checks.iter().enumerate() {
        let bytes = check.expected.as_ref().map_or(0, Vec::len);
        if check.key.len() > limits.max_key_bytes
            || bytes > limits.max_value_bytes
            || checks[..index].iter().any(|prior| prior.key == check.key)
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid bounded authentication key/value plan".into(),
            ));
        }
        total = total
            .checked_add(check.key.len())
            .and_then(|sum| sum.checked_add(bytes))
            .ok_or_else(|| {
                WorkspaceError::InvalidReadPlan("bounded authentication byte overflow".into())
            })?;
        if total > limits.max_total_bytes {
            return Err(WorkspaceError::InvalidReadPlan(
                "bounded authentication expected bytes exceeded".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_publication_packet_limits(
    keys: &[Vec<u8>],
    limits: KvReadLimits,
) -> Result<(), WorkspaceError> {
    limits.validate_keys(keys)?;
    if keys.is_empty()
        || keys.len() > 64
        || limits.max_records > 64
        || limits.max_key_bytes > 1024
        || limits.max_value_bytes > 48 << 10
        || limits.max_total_bytes > 2 << 20
        || limits.max_response_bytes > 2 << 20
        || limits.max_data_requests > 64
        || keys.len() > limits.max_data_requests
    {
        return Err(WorkspaceError::InvalidReadPlan(
            "native publication proof packet exceeds its fixed limits".into(),
        ));
    }
    Ok(())
}

impl KvReadLimits {
    pub fn validate(self) -> Result<(), WorkspaceError> {
        // Redis Lua integers must remain exact. This also bounds request plans.
        const MAX_EXACT_LUA_INTEGER: usize = 1 << 30;
        if self.max_records == 0
            || self.max_records > 1024
            || self.max_key_bytes == 0
            || self.max_key_bytes > MAX_EXACT_LUA_INTEGER
            || self.max_value_bytes == 0
            || self.max_value_bytes > MAX_EXACT_LUA_INTEGER
            || self.max_total_bytes == 0
            || self.max_total_bytes > MAX_EXACT_LUA_INTEGER
            || self.max_response_bytes < 256
            || self.max_response_bytes > MAX_EXACT_LUA_INTEGER
            || self.max_data_requests == 0
            || self.max_data_requests > 4096
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid KV response byte limits".into(),
            ));
        }
        Ok(())
    }

    pub fn validate_keys(self, keys: &[Vec<u8>]) -> Result<(), WorkspaceError> {
        self.validate()?;
        if keys.len() > self.max_records
            || keys.iter().any(|key| key.len() > self.max_key_bytes)
            || keys
                .iter()
                .try_fold(0usize, |sum, key| sum.checked_add(key.len()))
                .is_none_or(|bytes| bytes > self.max_total_bytes)
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "KV request exceeds response byte limits".into(),
            ));
        }
        Ok(())
    }

    pub fn validate_scan_page(
        self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
    ) -> Result<(), WorkspaceError> {
        self.validate()?;
        if self.max_key_bytes > 1024
            || prefix.len() > self.max_key_bytes
            || after_key_exclusive
                .is_some_and(|key| key.len() > self.max_key_bytes || !key.starts_with(prefix))
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "bounded KV page requires a prefix/cursor within the 1024-byte key schema".into(),
            ));
        }
        Ok(())
    }
}

/// The temporal CAS deliberately has no unguarded form. Keep validation common
/// to remote implementations so an invalid interval never reaches a backend.
pub(crate) fn validate_cas_time_window(
    not_before_ns: Option<i64>,
    before_ns: Option<i64>,
) -> Result<(), WorkspaceError> {
    if (not_before_ns.is_none() && before_ns.is_none())
        || not_before_ns.is_some_and(|value| value <= 0)
        || before_ns.is_some_and(|value| value <= 0)
        || matches!((not_before_ns, before_ns), (Some(lower), Some(upper)) if lower >= upper)
    {
        return Err(WorkspaceError::InvalidReadPlan(
            "temporal CAS requires a nonempty, positive backend-clock interval".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod scan_page_contract {
    use super::*;
    use futures::FutureExt;
    use std::time::Duration;

    async fn page_after_seed(
        backend: &impl WorkspaceKvBackend,
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        // Ordinary TiKV commits may return after committing only the primary.
        // Give their real secondary-commit tasks a finite chance to finish;
        // bounded reads themselves do not enable the unbounded lock resolver.
        for attempt in 0..20 {
            match backend
                .scan_prefix_page_with_byte_limits(b"paged/", after, limits)
                .await
            {
                Err(error)
                    if error
                        .to_string()
                        .contains("bounded read returned a key or region error")
                        && attempt < 19 =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                result => return result,
            }
        }
        unreachable!()
    }

    pub(crate) async fn assert_real_pages(backend: &impl WorkspaceKvBackend) {
        // More than one complete-prefix schema, with byte successors at page
        // boundaries. Retain only one result page while checking no omission.
        let mut keys = (0..1057)
            .map(|index| format!("paged/{index:08}").into_bytes())
            .collect::<Vec<_>>();
        keys.extend([
            b"paged/zz\0".to_vec(),
            b"paged/zz\0\xff".to_vec(),
            b"paged/zz\xff".to_vec(),
        ]);
        let outside = b"outside/paged".to_vec();
        let poison = b"paged/\xff".to_vec();
        let limits = KvReadLimits {
            max_records: 7,
            max_key_bytes: 1024,
            max_value_bytes: 4,
            max_total_bytes: 7 * (1024 + 4),
            max_response_bytes: 16 << 10,
            // TiKV returns short pages after three single-row RPCs. Redis
            // consumes one Lua request and may return all seven rows.
            max_data_requests: 3,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            std::panic::AssertUnwindSafe(async {
                for (chunk_index, chunk) in keys.chunks(32).enumerate() {
                    let writes = chunk
                        .iter()
                        .enumerate()
                        .map(|(index, key)| KvWrite::Put {
                            key: key.clone(),
                            value: ((32 * chunk_index + index) as u32).to_be_bytes().to_vec(),
                        })
                        .collect::<Vec<_>>();
                    assert!(backend.compare_and_swap(&[], &writes).await?);
                }
                assert!(
                    backend
                        .compare_and_swap(
                            &[],
                            &[KvWrite::Put {
                                key: outside.clone(),
                                value: vec![0; 4],
                            }],
                        )
                        .await?
                );
                let mut after: Option<Vec<u8>> = None;
                let mut next_index = 0usize;
                let mut saw_short_page = false;
                loop {
                    let page = page_after_seed(backend, after.as_deref(), limits).await?;
                    assert!(page.len() <= limits.max_records);
                    if page.is_empty() {
                        break;
                    }
                    saw_short_page |= page.len() < limits.max_records;
                    for row in &page {
                        assert_eq!(row.key, keys[next_index]);
                        assert_eq!(row.value, (next_index as u32).to_be_bytes());
                        assert!(after.as_ref().is_none_or(|last| &row.key > last));
                        after = Some(row.key.clone());
                        next_index += 1;
                    }
                    assert!(next_index <= keys.len());
                }
                assert_eq!(next_index, keys.len());
                assert!(saw_short_page);
                assert!(
                    backend
                        .scan_prefix_page_with_byte_limits(b"paged/", Some(&outside), limits)
                        .await
                        .is_err()
                );
                let aggregate_too_small = KvReadLimits {
                    max_total_bytes: 1,
                    ..limits
                };
                assert!(
                    page_after_seed(backend, None, aggregate_too_small)
                        .await
                        .is_err()
                );
                assert!(
                    backend
                        .compare_and_swap(
                            &[],
                            &[KvWrite::Put {
                                key: poison.clone(),
                                value: vec![0; 2 << 20],
                            }],
                        )
                        .await?
                );
                assert!(
                    page_after_seed(backend, after.as_deref(), limits)
                        .await
                        .is_err(),
                    "cursor must not bypass oversized next persisted value"
                );
                Ok::<(), WorkspaceError>(())
            })
            .catch_unwind(),
        )
        .await;
        keys.extend([outside, poison]);
        // Exact test keys only. Cleanup uses the native transaction client so
        // any known committed secondary locks can be resolved before deletion.
        for chunk in keys.chunks(32) {
            let deletes = chunk
                .iter()
                .map(|key| KvWrite::Delete { key: key.clone() })
                .collect::<Vec<_>>();
            backend.compare_and_swap(&[], &deletes).await.unwrap();
        }
        assert!(
            matches!(result, Ok(Ok(Ok(())))),
            "real bounded page case failed: {result:?}"
        );
    }
}

#[async_trait]
pub trait WorkspaceKvBackend: Send + Sync + 'static {
    /// Authenticate a distinct operator connection created by the trusted
    /// administrator Secret factory. This application boundary is inside the
    /// SPEC sidecar/operator TCB; it does not promise native per-RPC/transition
    /// ACLs. A runtime connection or caller-provided role flag is insufficient.
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "independent authenticated operator credentials",
        ))
    }

    fn supports_consistent_reads(&self) -> bool {
        false
    }

    /// True only when every `scan_prefix_page_with_byte_limits` page is one
    /// atomic range read whose sole stop condition is `max_records`, so a
    /// page shorter than that proves no further key exists in the range.
    /// Default false: callers must continue until an empty page.
    fn short_scan_page_is_end(&self) -> bool {
        false
    }
    fn name(&self) -> &'static str;

    /// Private native-GC adapter opt-in. A positive quota selects durable,
    /// bounded metadata-family finalization. It does not certify a complete
    /// topology/root/shared-extent proof or enable scoped topology authority.
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        None
    }

    /// Stop admission, drain calls and join retained backend tasks. Cancellation
    /// leaves the join obligation available for a later shutdown caller.
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError>;

    /// Return exact values in the same order as `keys`.
    ///
    /// Remote backends override this to collapse a fixed two-layer lookup into
    /// one network round trip. The default keeps lightweight test backends
    /// simple without changing their semantics.
    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(key).await?);
        }
        Ok(values)
    }

    /// All returned keys must come from a single commit version. Sequential
    /// point reads cannot implement this permission/authentication contract.
    async fn get_many_consistent(
        &self,
        _keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "consistent multi-key read",
        ))
    }

    /// A single consistent read version and clock in the backend lease domain.
    /// Separate TIME/get calls or an interleavable pipeline are insufficient.
    async fn get_many_consistent_with_time(
        &self,
        _keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "consistent timed multi-key read",
        ))
    }

    /// One consistent read version/time, with a hard per-response transport
    /// bound before protobuf/RESP materialization and bounded RPC count.
    /// Unsupported is deliberate: unbounded reads followed by len checks are
    /// not a fallback for corrupt oversized persistent metadata.
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        Err(WorkspaceError::UnsupportedCapability(
            "bounded consistent timed KV values",
        ))
    }

    /// Complete native publication proof, including original guards and exact
    /// native hold sidecars. Backends must use one consistent read version/time
    /// across all bounded point windows; separate snapshot calls are invalid.
    /// The dedicated packet cap does not enlarge ordinary point-read admission.
    async fn get_publication_packet_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        validate_publication_packet_limits(keys, limits)?;
        self.get_many_consistent_with_time_bounded(keys, limits)
            .await
    }

    /// Return exact values and a backend-authoritative timestamp. Remote
    /// backends override this to share the transaction/pipeline used by the
    /// read instead of paying a second round trip for lease validation time.
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let now = self.server_time_ns().await?;
        Ok((self.get_many(keys).await?, now))
    }

    /// Return logical key/value pairs whose keys start with `prefix`.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError>;

    /// Return at most max_records key/value pairs for prefix. Callers that
    /// need overflow detection request one sentinel row beyond their budget.
    /// A backend must apply the limit before materializing rows. An unbounded
    /// scan followed by truncation does not implement this contract.
    async fn scan_prefix_bounded(
        &self,
        _prefix: &[u8],
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if max_records == 0 {
            return Err(WorkspaceError::InvalidReadPlan(
                "bounded prefix scan requires a positive row limit".into(),
            ));
        }
        Err(WorkspaceError::UnsupportedCapability("bounded prefix scan"))
    }

    /// Like scan_prefix_bounded, with hard per-response transport/RPC limits
    /// and semantic per-record and complete-result decoded byte limits.
    async fn scan_prefix_with_byte_limits(
        &self,
        _prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate()?;
        Err(WorkspaceError::UnsupportedCapability(
            "bounded prefix scan value bytes",
        ))
    }

    /// One bounded keyset page in strict ascending logical-key order. Every
    /// key starts with `prefix` and is greater than `after_key_exclusive`.
    /// Returns at most max_records, with the same pre-materialization envelope
    /// contract as the complete-prefix API. A short page is not an end marker:
    /// continue from its last key until an empty page proves no keys remain.
    /// A page that exhausts its RPC limit without visiting any row fails closed
    /// rather than returning a false end marker across empty TiKV regions.
    /// Each page has its own consistent read version; frozen-layer callers must
    /// validate their exact immutable fence before and after each page.
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after_key_exclusive)?;
        Err(WorkspaceError::UnsupportedCapability(
            "bounded prefix keyset page",
        ))
    }

    /// Atomically verify every exact-value condition and apply all writes.
    /// Returns `false` when any condition changed and the caller should retry.
    /// An `Err(WorkspaceError::Busy)` result is reserved for a pre-submit
    /// conflict where the backend proves that no write was submitted; an
    /// ambiguous or post-submit transport result must use `Backend` so callers
    /// never replay an uncertain mutation.
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError>;

    /// Strict Manager bootstrap. At the same serialized validation point as
    /// exact checks, clock expiry and writes, reject every existing logical
    /// namespace key except the exact checked Manager controller record.
    /// Backend-owned index bookkeeping is not a user record. A separate scan
    /// followed by ordinary CAS does not implement this contract.
    async fn compare_and_swap_in_empty_namespace(
        &self,
        _checks: &[KvCheck],
        _writes: &[KvWrite],
        before_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        validate_cas_time_window(None, Some(before_ns))?;
        Err(WorkspaceError::UnsupportedCapability(
            "atomic empty namespace initialization",
        ))
    }

    /// Authenticate exact checks with a backend-clock deadline and no writes.
    /// The caller owns retained checks and response/decoder expansion. Bounds
    /// apply before authority value materialization; max_data_requests bounds
    /// locking authority reads. TiKV commit/cleanup use the same capped client
    /// with finite, separately bounded protocol steps and no resolver retries.
    /// A snapshot or an unbounded CAS is not an implementation fallback.
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        validate_bounded_authentication_checks(checks, limits)?;
        validate_cas_time_window(None, Some(expires_at_ns))?;
        Err(WorkspaceError::UnsupportedCapability(
            "bounded backend-clock exact-check authentication",
        ))
    }

    /// Exact-key CAS plus a backend-clock lease predicate at the locked
    /// validation point. False means value conflict; expiry returns Fenced.
    /// Redis evaluates TIME inside Lua. TiKV samples PD after locking all
    /// checked keys and immediately before commit; this is not a guarantee
    /// about wall-clock time at the eventual commit timestamp.
    async fn compare_and_swap_before(
        &self,
        _checks: &[KvCheck],
        _writes: &[KvWrite],
        _expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "backend-clock guarded CAS",
        ))
    }

    /// Exact-key CAS within a backend-clock interval at the locked validation
    /// point: `now >= not_before_ns`, when present, and `now < before_ns`, when
    /// present. A lower bound that has not been reached returns Busy; a reached
    /// upper bound returns Fenced. False means only an exact-value conflict.
    /// Both bounds absent, nonpositive bounds, and lower >= upper are invalid.
    /// Redis samples TIME in the same Lua script as conditions and writes.
    /// TiKV samples fresh PD time after locking all checked keys and immediately
    /// before commit; this does not guarantee wall-clock time at the eventual
    /// commit timestamp. An implementation must not compose separate TIME/CAS
    /// calls or fall back to an unguarded CAS.
    async fn compare_and_swap_in_time_window(
        &self,
        _checks: &[KvCheck],
        _writes: &[KvWrite],
        not_before_ns: Option<i64>,
        before_ns: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        validate_cas_time_window(not_before_ns, before_ns)?;
        Err(WorkspaceError::UnsupportedCapability(
            "backend-clock windowed CAS",
        ))
    }

    /// Backend-authoritative wall-clock time for lease expiry.
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError>;
}

#[cfg(test)]
mod bounded_read_tests {
    use super::*;

    #[test]
    fn kv_byte_limit_request_boundaries_are_exact() {
        let limits = KvReadLimits {
            max_records: 2,
            max_key_bytes: 3,
            max_value_bytes: 4,
            max_total_bytes: 5,
            max_response_bytes: 512,
            max_data_requests: 1,
        };
        assert!(
            limits
                .validate_keys(&[b"abc".to_vec(), b"de".to_vec()])
                .is_ok()
        );
        assert!(limits.validate_keys(&[b"abcd".to_vec()]).is_err());
        assert!(
            limits
                .validate_keys(&[b"abc".to_vec(), b"def".to_vec()])
                .is_err()
        );
        assert!(limits.validate_keys(&[vec![], vec![], vec![]]).is_err());
        assert!(
            KvReadLimits {
                max_records: 0,
                ..limits
            }
            .validate()
            .is_err()
        );
        assert!(
            KvReadLimits {
                max_total_bytes: usize::MAX,
                ..limits
            }
            .validate()
            .is_err()
        );
    }
}

#[cfg(test)]
pub(super) mod time_window_contract {
    use std::panic::AssertUnwindSafe;

    use futures_util::FutureExt;

    use super::*;

    struct NoWindowBackend;

    #[async_trait]
    impl WorkspaceKvBackend for NoWindowBackend {
        fn name(&self) -> &'static str {
            "no-time-window"
        }

        async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
            panic!("window default must not perform a read")
        }

        async fn scan_prefix(&self, _: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
            panic!("window default must not perform a scan")
        }

        async fn compare_and_swap(
            &self,
            _: &[KvCheck],
            _: &[KvWrite],
        ) -> Result<bool, WorkspaceError> {
            panic!("window default must not fall back to an unguarded CAS")
        }

        async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
            panic!("window default must not compose TIME with CAS")
        }
    }

    #[tokio::test]
    async fn window_default_rejects_invalid_intervals_and_never_falls_back() {
        for (lower, upper) in [
            (None, None),
            (Some(0), None),
            (Some(-1), None),
            (None, Some(0)),
            (None, Some(-1)),
            (Some(10), Some(10)),
            (Some(11), Some(10)),
        ] {
            assert!(matches!(
                NoWindowBackend
                    .compare_and_swap_in_time_window(&[], &[], lower, upper)
                    .await,
                Err(WorkspaceError::InvalidReadPlan(_))
            ));
        }
        for (lower, upper) in [
            (Some(1), None),
            (None, Some(i64::MAX)),
            (Some(1), Some(i64::MAX)),
        ] {
            assert!(matches!(
                NoWindowBackend
                    .compare_and_swap_in_time_window(&[], &[], lower, upper)
                    .await,
                Err(WorkspaceError::UnsupportedCapability(
                    "backend-clock windowed CAS"
                ))
            ));
        }
    }

    /// Called only in a unique real-backend test namespace. Cleanup deletes
    /// these two explicit test rows, including after assertion failure.
    pub(in crate::workspace_overlay::stores) async fn assert_real_window<B: WorkspaceKvBackend>(
        backend: &B,
    ) {
        let pin = b"time-window/pin".to_vec();
        let generation = b"time-window/root-generation".to_vec();
        let keys = [pin.clone(), generation.clone()];
        let absent = keys
            .iter()
            .map(|key| KvCheck {
                key: key.clone(),
                expected: None,
            })
            .collect::<Vec<_>>();
        let initial = [Some(b"pin-live".to_vec()), Some(b"generation-1".to_vec())];
        let outcome = AssertUnwindSafe(async {
            let writes = keys
                .iter()
                .zip(&initial)
                .map(|(key, value)| KvWrite::Put {
                    key: key.clone(),
                    value: value.clone().unwrap(),
                })
                .collect::<Vec<_>>();
            assert!(backend.compare_and_swap(&absent, &writes).await.unwrap());
            let exact = keys
                .iter()
                .zip(&initial)
                .map(|(key, expected)| KvCheck {
                    key: key.clone(),
                    expected: expected.clone(),
                })
                .collect::<Vec<_>>();
            let reap = [
                KvWrite::Delete { key: pin.clone() },
                KvWrite::Put {
                    key: generation.clone(),
                    value: b"generation-2".to_vec(),
                },
            ];
            let now = backend.server_time_ns().await.unwrap();
            let future = now.checked_add(60_000_000_000).unwrap();
            assert!(matches!(
                backend
                    .compare_and_swap_in_time_window(&exact, &reap, Some(future), None)
                    .await,
                Err(WorkspaceError::Busy)
            ));
            assert_eq!(backend.get_many_consistent(&keys).await.unwrap(), initial);
            assert!(matches!(
                backend
                    .compare_and_swap_in_time_window(&exact, &reap, None, Some(now - 1))
                    .await,
                Err(WorkspaceError::Fenced)
            ));
            assert_eq!(backend.get_many_consistent(&keys).await.unwrap(), initial);
            assert!(matches!(
                backend
                    .compare_and_swap_in_time_window(&exact, &reap, None, None)
                    .await,
                Err(WorkspaceError::InvalidReadPlan(_))
            ));
            assert!(matches!(
                backend
                    .compare_and_swap_in_time_window(&exact, &reap, Some(future), Some(future))
                    .await,
                Err(WorkspaceError::InvalidReadPlan(_))
            ));
            assert_eq!(backend.get_many_consistent(&keys).await.unwrap(), initial);
            assert!(
                !backend
                    .compare_and_swap_in_time_window(&absent, &reap, Some(now - 1), Some(future))
                    .await
                    .unwrap()
            );
            assert_eq!(backend.get_many_consistent(&keys).await.unwrap(), initial);
            assert!(
                backend
                    .compare_and_swap_in_time_window(&exact, &[], None, Some(future))
                    .await
                    .unwrap()
            );
            assert!(
                backend
                    .compare_and_swap_in_time_window(&exact, &[], Some(now - 1), Some(future))
                    .await
                    .unwrap()
            );
            assert!(
                backend
                    .compare_and_swap_in_time_window(&exact, &reap, Some(now - 1), None)
                    .await
                    .unwrap()
            );
            assert_eq!(
                backend.get_many_consistent(&keys).await.unwrap(),
                [None, Some(b"generation-2".to_vec())]
            );
            assert!(
                !backend
                    .compare_and_swap_in_time_window(&exact, &reap, Some(now - 1), None)
                    .await
                    .unwrap()
            );
            assert_eq!(
                backend.get_many_consistent(&keys).await.unwrap(),
                [None, Some(b"generation-2".to_vec())]
            );
        })
        .catch_unwind()
        .await;
        let cleanup = backend
            .compare_and_swap(
                &[],
                &keys
                    .into_iter()
                    .map(|key| KvWrite::Delete { key })
                    .collect::<Vec<_>>(),
            )
            .await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        assert!(cleanup.unwrap());
    }
}
