//! Conservative binding adoption and single-use packed object collection.
//! Every existing binding/history graph remains retained in this first route.
//! Native snapshot/fork/lease ambiguity cannot make a packed graph disappear.

use super::history_retirement::HistoryDeleteContext;
use super::*;

pub(super) const GATE_KEY: &[u8] = b"packed/registry/gate";
const GATE_LIMIT: usize = 128;
type AbortedRootRead = (Vec<Vec<u8>>, Vec<Option<Vec<u8>>>, RootRow);

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryGate {
    epoch: u64,
    run_id: Uuid,
    active: bool,
    catalog_rows: u64,
    audited_objects: u64,
}
impl RegistryGate {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.epoch == 0 || self.run_id.is_nil() {
            return Err(journal_error("invalid registry activation gate"));
        }
        let mut out = row_header(b"PRG3");
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(self.run_id.as_bytes());
        out.push(u8::from(self.active));
        out.extend_from_slice(&self.catalog_rows.to_le_bytes());
        out.extend_from_slice(&self.audited_objects.to_le_bytes());
        finish_record(out, GATE_LIMIT)
    }
    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PRG3", GATE_LIMIT)?;
        let result = Self {
            epoch: c.u64()?,
            run_id: Uuid::from_bytes(c.take()?),
            active: boolean(c.take::<1>()?[0])?,
            catalog_rows: c.u64()?,
            audited_objects: c.u64()?,
        };
        c.end()?;
        result.encode()?;
        Ok(result)
    }
}

struct MigrationFence {
    gate: Vec<u8>,
    generation: Option<Vec<u8>>,
}
impl MigrationFence {
    fn checks(&self) -> Vec<KvCheck> {
        vec![
            KvCheck {
                key: GATE_KEY.to_vec(),
                expected: Some(self.gate.clone()),
            },
            KvCheck {
                key: PACKED_ROOT_GENERATION_KEY.to_vec(),
                expected: self.generation.clone(),
            },
        ]
    }
}

pub(crate) struct PackedRegistryMigrationOptions<'a> {
    pub scratch: &'a std::path::Path,
    pub graph_limits: SnapshotIndexAuditLimits,
    pub max_catalog_rows: u64,
    pub cancel: CancellationToken,
}
pub(crate) struct PackedObjectCollectionOptions {
    pub max_objects: u64,
    pub cancel: CancellationToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PackedRegistryMigrationReport {
    pub epoch: u64,
    pub catalog_rows: u64,
    pub audited_objects: u64,
}

/// No Clone or public constructor. Unknown reserve/dispatch/DELETE outcomes
/// leave a permanent DeletePending row and never issue a replacement guard.
pub(crate) struct PackedDeleteGuard {
    reference: SnapshotObjectRef,
    delete_id: Uuid,
    revision: u64,
    dispatched: bool,
    _permit: SnapshotOwnedPermit,
}

fn live(budget: &SnapshotMountBudget, cancel: &CancellationToken) -> Result<(), WorkspaceError> {
    if budget.state().closed || cancel.is_cancelled() {
        return Err(WorkspaceError::Busy);
    }
    Ok(())
}
fn page_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: 32,
        max_key_bytes: 1024,
        max_value_bytes: REGISTRY_RECORD_LIMIT,
        max_total_bytes: 512 << 10,
        max_response_bytes: 512 << 10,
        max_data_requests: 1024,
    }
}
fn exact_checks(keys: &[Vec<u8>], values: &[Option<Vec<u8>>]) -> Vec<KvCheck> {
    keys.iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect()
}
fn append_checks(checks: &mut Vec<KvCheck>, added: Vec<KvCheck>) -> Result<(), WorkspaceError> {
    for added in added {
        if let Some(existing) = checks.iter().find(|old| old.key == added.key) {
            if existing.expected != added.expected {
                return Err(WorkspaceError::Busy);
            }
        } else {
            checks.push(added);
        }
    }
    Ok(())
}
fn root_bytes(raw: &Option<Vec<u8>>) -> Result<&[u8], WorkspaceError> {
    raw.as_deref()
        .ok_or_else(|| journal_error("retained registry root missing"))
}
fn active_gate(raw: &Option<Vec<u8>>) -> Result<RegistryGate, WorkspaceError> {
    let gate = RegistryGate::decode(root_bytes(raw)?)?;
    if !gate.active {
        return Err(WorkspaceError::Busy);
    }
    Ok(gate)
}

pub(super) fn require_active_retirement_gate(raw: &Option<Vec<u8>>) -> Result<(), WorkspaceError> {
    active_gate(raw)?;
    Ok(())
}

struct BindingAdopter<'a, B> {
    store: &'a KvWorkspaceStore<B>,
    frozen: Vec<KvCheck>,
    binding: PackedLowerBindingRecord,
    incarnation: Uuid,
    journal_id: JournalId,
    budget: &'a Arc<SnapshotMountBudget>,
    cancel: CancellationToken,
}
#[async_trait]
impl<B: WorkspaceKvBackend> SnapshotStagedObjectVerifier for BindingAdopter<'_, B> {
    async fn verify_reference(
        &self,
        reference: &SnapshotObjectRef,
    ) -> crate::workspace::packed::PackedResult<()> {
        self.adopt(reference)
            .await
            .map_err(|error| crate::workspace::packed::PackedWireError::Backend(error.to_string()))
    }
}
impl<B: WorkspaceKvBackend> BindingAdopter<'_, B> {
    async fn adopt(&self, reference: &SnapshotObjectRef) -> Result<(), WorkspaceError> {
        live(self.budget, &self.cancel)?;
        let keys = vec![
            registry_root_key(self.incarnation),
            registry_history_root_key(&self.binding),
            registry_object_key(reference),
            registry_member_key(reference, self.incarnation),
        ];
        let values = self.store.packed_journal_values(&keys).await?;
        if values.len() != keys.len() || values[0] != values[1] {
            return Err(journal_error("adopting root/mapping mismatch"));
        }
        let mut root = RootRow::decode(root_bytes(&values[0])?)?;
        if root.state != RootState::AdoptingBinding
            || root.binding.as_ref() != Some(&self.binding)
            || root.incarnation != self.incarnation
            || root.journal_id != self.journal_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut object = values[2]
            .as_deref()
            .map(ObjectRow::decode)
            .transpose()?
            .unwrap_or(ObjectRow {
                reference: reference.clone(),
                revision: 0,
                state: ObjectState::Live,
                memberships: 0,
                pending_puts: 0,
                delete_id: Uuid::nil(),
                delete_dispatched: false,
            });
        if object.reference != *reference || object.state != ObjectState::Live {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = self.frozen.clone();
        append_checks(&mut checks, exact_checks(&keys, &values))?;
        if let Some(raw) = &values[3] {
            let member = MemberRow::decode(raw)?;
            if member.reference != *reference
                || member.incarnation != self.incarnation
                || member.journal_id != self.journal_id
                || !member.adopted
                || !member.retained
            {
                return Err(WorkspaceError::Fenced);
            }
            let key = registry_reverse_key(self.incarnation, member.ordinal);
            let reverse = self
                .store
                .packed_journal_values(std::slice::from_ref(&key))
                .await?;
            if reverse.len() != 1
                || reverse[0].as_deref()
                    != Some(reference.encode_value().map_err(journal_error)?.as_slice())
            {
                return Err(WorkspaceError::Fenced);
            }
            checks.push(KvCheck {
                key,
                expected: reverse[0].clone(),
            });
            return self.store.registry_cas(&checks, &[]).await;
        }
        if root.members >= MAX_OBJECTS {
            return Err(WorkspaceError::Busy);
        }
        let ordinal = root.members;
        let member = MemberRow {
            reference: reference.clone(),
            journal_id: self.journal_id,
            incarnation: self.incarnation,
            ordinal,
            put_id: Uuid::nil(),
            adopted: true,
            pending_put: false,
            dispatched: false,
            retained: true,
        };
        object.revision = increment(object.revision)?;
        object.memberships = increment(object.memberships)?;
        root.revision = increment(root.revision)?;
        root.members = increment(root.members)?;
        let reverse_key = registry_reverse_key(self.incarnation, ordinal);
        checks.push(KvCheck {
            key: reverse_key.clone(),
            expected: None,
        });
        let writes = vec![
            KvWrite::Put {
                key: keys[0].clone(),
                value: root.encode()?,
            },
            KvWrite::Put {
                key: keys[1].clone(),
                value: root.encode()?,
            },
            KvWrite::Put {
                key: keys[2].clone(),
                value: object.encode()?,
            },
            KvWrite::Put {
                key: keys[3].clone(),
                value: member.encode()?,
            },
            KvWrite::Put {
                key: reverse_key,
                value: reference.encode_value().map_err(journal_error)?,
            },
        ];
        live(self.budget, &self.cancel)?;
        self.store.registry_cas(&checks, &writes).await
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    fn registry_budget(&self, budget: &Arc<SnapshotMountBudget>) -> Result<(), WorkspaceError> {
        let canonical =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "canonical registry budget",
                ))?;
        if !Arc::ptr_eq(canonical, budget) {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
    async fn registry_cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<(), WorkspaceError> {
        if !self.backend.compare_and_swap(checks, writes).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    /// Gate bytes join the caller's same publication CAS even before activation.
    /// After activation, standalone writers must already own the exact retained
    /// mapping; new graph publication uses the PPJ/root/mapping atomic route.
    pub(crate) async fn packed_registry_publication_checks(
        &self,
        binding: &PackedLowerBindingRecord,
    ) -> Result<(Option<SnapshotOwnedPermit>, Vec<KvCheck>), WorkspaceError> {
        // Mount owners reserve before bounded registry reads. A pre-registry
        // initial install can precede budget configuration; Active gates
        // always require the canonical owner below.
        let owner = self
            .packed_reader_pin_budget
            .get()
            .map(|budget| {
                budget
                    .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
                    .map_err(journal_budget_error)
            })
            .transpose()?;
        let keys = [
            GATE_KEY.to_vec(),
            registry_history_root_key(binding),
            super::borrowed_alias::borrowed_history_key(binding),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != keys.len() {
            return Err(journal_error("short publication registry gate read"));
        }
        let mut checks = exact_checks(&keys, &values);
        let gate = values[0].as_deref().map(RegistryGate::decode).transpose()?;
        if let Some(raw) = &values[2] {
            if owner.is_none() || gate.is_none_or(|gate| !gate.active) || values[1].is_some() {
                return Err(WorkspaceError::Fenced);
            }
            let alias = super::borrowed_alias::BorrowedHistoryAlias::decode(raw)?;
            super::super::native_publication::append_exact_checks(
                &mut checks,
                self.borrowed_history_source_checks(binding, &alias).await?,
            )?;
            return Ok((owner, checks));
        }
        if gate.is_none_or(|gate| !gate.active) {
            return Ok((owner, checks));
        }
        if owner.is_none() {
            return Err(WorkspaceError::UnsupportedCapability(
                "canonical registry budget",
            ));
        }
        let root = RootRow::decode(root_bytes(&values[1])?)?;
        if root.state != RootState::BindingHistory
            || root.binding.as_ref() != Some(binding)
            || root.pending_puts != 0
            || root.members == 0
        {
            return Err(WorkspaceError::Fenced);
        }
        let key = registry_root_key(root.incarnation);
        let actual = self
            .packed_journal_values(std::slice::from_ref(&key))
            .await?;
        if actual.len() != 1 || actual[0] != values[1] {
            return Err(WorkspaceError::Fenced);
        }
        checks.push(KvCheck {
            key,
            expected: actual[0].clone(),
        });
        Ok((owner, checks))
    }

    /// Enumerate and authenticate every existing current/history graph. The
    /// activation CAS is reachable only after every bounded keyset scan returns
    /// an empty page under the same root-generation and migration incarnation.
    pub(crate) async fn migrate_packed_object_registry<O: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: PackedRegistryMigrationOptions<'_>,
    ) -> Result<PackedRegistryMigrationReport, WorkspaceError> {
        self.registry_budget(budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if options.max_catalog_rows == 0 || options.max_catalog_rows > MAX_OBJECTS {
            return Err(WorkspaceError::Busy);
        }
        live(budget, &options.cancel)?;
        let keys = [GATE_KEY.to_vec(), PACKED_ROOT_GENERATION_KEY.to_vec()];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 2 {
            return Err(journal_error("short registry migration read"));
        }
        next_packed_root_generation(&values[1])?;
        let old_gate = values[0].as_deref().map(RegistryGate::decode).transpose()?;
        if let Some(gate) = &old_gate
            && gate.active
        {
            self.registry_cas(&exact_checks(&keys, &values), &[])
                .await?;
            return Ok(PackedRegistryMigrationReport {
                epoch: gate.epoch,
                catalog_rows: gate.catalog_rows,
                audited_objects: gate.audited_objects,
            });
        }
        let mut gate = RegistryGate {
            epoch: increment(old_gate.map_or(0, |gate| gate.epoch))?,
            run_id: Uuid::new_v4(),
            active: false,
            catalog_rows: 0,
            audited_objects: 0,
        };
        let fence = MigrationFence {
            gate: gate.encode()?,
            generation: values[1].clone(),
        };
        self.registry_cas(
            &exact_checks(&keys, &values),
            &[KvWrite::Put {
                key: GATE_KEY.to_vec(),
                value: fence.gate.clone(),
            }],
        )
        .await?;
        for prefix in [b"packed/current/".as_slice(), b"packed/history/"] {
            let mut after: Option<Vec<u8>> = None;
            loop {
                live(budget, &options.cancel)?;
                self.registry_cas(&fence.checks(), &[]).await?;
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(prefix, after.as_deref(), page_limits())
                    .await?;
                self.registry_cas(&fence.checks(), &[]).await?;
                if page.is_empty() {
                    break;
                }
                for entry in page {
                    if !entry.key.starts_with(prefix)
                        || after.as_ref().is_some_and(|last| &entry.key <= last)
                    {
                        return Err(journal_error("registry census cursor/key mismatch"));
                    }
                    gate.catalog_rows = increment(gate.catalog_rows)?;
                    if gate.catalog_rows > options.max_catalog_rows {
                        return Err(WorkspaceError::Busy);
                    }
                    let binding = PackedLowerBindingRecord::decode(&entry.value)?;
                    let expected_key = if prefix == b"packed/current/" {
                        packed_current_key(binding.workspace_id)
                    } else {
                        packed_history_key(binding.workspace_id, binding.binding.binding_version)
                    };
                    if entry.key != expected_key {
                        return Err(journal_error("census PWB key/record mismatch"));
                    }
                    let audited = self
                        .adopt_existing_packed_binding(
                            &fence, &entry, binding, client, budget, &options,
                        )
                        .await?;
                    gate.audited_objects = gate
                        .audited_objects
                        .checked_add(audited)
                        .ok_or_else(|| journal_error("migration object counter overflow"))?;
                    after = Some(entry.key);
                }
            }
        }
        live(budget, &options.cancel)?;
        gate.active = true;
        self.registry_cas(
            &fence.checks(),
            &[KvWrite::Put {
                key: GATE_KEY.to_vec(),
                value: gate.encode()?,
            }],
        )
        .await?;
        live(budget, &options.cancel)?;
        Ok(PackedRegistryMigrationReport {
            epoch: gate.epoch,
            catalog_rows: gate.catalog_rows,
            audited_objects: gate.audited_objects,
        })
    }

    async fn adopt_existing_packed_binding<O: ObjectBackend + Clone>(
        &self,
        fence: &MigrationFence,
        entry: &KvEntry,
        binding: PackedLowerBindingRecord,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: &PackedRegistryMigrationOptions<'_>,
    ) -> Result<u64, WorkspaceError> {
        let history_key = packed_history_key(binding.workspace_id, binding.binding.binding_version);
        let mapping_key = registry_history_root_key(&binding);
        let mut frozen = fence.checks();
        frozen.push(KvCheck {
            key: entry.key.clone(),
            expected: Some(entry.value.clone()),
        });
        append_checks(
            &mut frozen,
            vec![KvCheck {
                key: history_key,
                expected: Some(binding.encode()?),
            }],
        )?;
        let mapping = self
            .packed_journal_values(std::slice::from_ref(&mapping_key))
            .await?;
        if mapping.len() != 1 {
            return Err(journal_error("short adoption mapping read"));
        }
        let root = if let Some(raw) = &mapping[0] {
            let root = RootRow::decode(raw)?;
            if root.binding.as_ref() != Some(&binding)
                || root.pending_puts != 0
                || !matches!(
                    root.state,
                    RootState::AdoptingBinding | RootState::BindingHistory
                )
            {
                return Err(WorkspaceError::Fenced);
            }
            let root_key = registry_root_key(root.incarnation);
            let actual = self
                .packed_journal_values(std::slice::from_ref(&root_key))
                .await?;
            if actual.len() != 1 || actual[0] != mapping[0] {
                return Err(WorkspaceError::Fenced);
            }
            let mut checks = frozen.clone();
            checks.push(KvCheck {
                key: mapping_key.clone(),
                expected: mapping[0].clone(),
            });
            checks.push(KvCheck {
                key: root_key,
                expected: actual[0].clone(),
            });
            self.registry_cas(&checks, &[]).await?;
            if root.state == RootState::BindingHistory {
                return Ok(0);
            }
            root
        } else {
            let root = RootRow {
                journal_id: JournalId::new(),
                incarnation: Uuid::new_v4(),
                revision: 1,
                state: RootState::AdoptingBinding,
                members: 0,
                pending_puts: 0,
                binding: Some(binding.clone()),
            };
            let mut checks = frozen.clone();
            checks.extend([
                KvCheck {
                    key: mapping_key.clone(),
                    expected: None,
                },
                KvCheck {
                    key: registry_root_key(root.incarnation),
                    expected: None,
                },
            ]);
            self.registry_cas(
                &checks,
                &[
                    KvWrite::Put {
                        key: mapping_key.clone(),
                        value: root.encode()?,
                    },
                    KvWrite::Put {
                        key: registry_root_key(root.incarnation),
                        value: root.encode()?,
                    },
                ],
            )
            .await?;
            root
        };
        let adopter = BindingAdopter {
            store: self,
            frozen: frozen.clone(),
            binding: binding.clone(),
            incarnation: root.incarnation,
            journal_id: root.journal_id,
            budget,
            cancel: options.cancel.clone(),
        };
        let graph = audit_snapshot_staged_index_contexts(
            client,
            &binding.binding.manifest,
            options.scratch,
            budget.clone(),
            options.graph_limits,
            options.cancel.clone(),
            &adopter,
        )
        .await
        .map_err(journal_budget_error)?;
        let keys = [registry_root_key(root.incarnation), mapping_key];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 2 || values[0] != values[1] {
            return Err(journal_error("adoption final root/mapping mismatch"));
        }
        let mut final_root = RootRow::decode(root_bytes(&values[0])?)?;
        let (_, root_inode, highest_inode) = graph.publication_facts();
        if final_root.state != RootState::AdoptingBinding
            || final_root.incarnation != root.incarnation
            || final_root.journal_id != root.journal_id
            || final_root.binding.as_ref() != Some(&binding)
            || final_root.members != graph.counts().objects
            || final_root.pending_puts != 0
            || root_inode != 1
            || highest_inode >= i64::MAX as u64 - 1
            || binding.highest_inode < highest_inode as i64
        {
            return Err(WorkspaceError::Fenced);
        }
        final_root.state = RootState::BindingHistory;
        final_root.revision = increment(final_root.revision)?;
        append_checks(&mut frozen, exact_checks(&keys, &values))?;
        live(budget, &options.cancel)?;
        self.registry_cas(
            &frozen,
            &[
                KvWrite::Put {
                    key: keys[0].clone(),
                    value: final_root.encode()?,
                },
                KvWrite::Put {
                    key: keys[1].clone(),
                    value: final_root.encode()?,
                },
            ],
        )
        .await?;
        Ok(graph.counts().objects)
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Exercise a process restart between a real membership release and its
    /// DELETE reservation, without manufacturing durable completion flags.
    #[cfg(test)]
    pub(super) async fn release_one_aborted_member_for_test(
        &self,
        expected: &PackedJournalRecord,
    ) -> Result<SnapshotObjectRef, WorkspaceError> {
        self.begin_aborted_root_retirement(expected).await?;
        let prefix = format!(
            "{REGISTRY_REVERSE_PREFIX}{}/",
            expected.source.staging_id.simple()
        )
        .into_bytes();
        self.check_aborted_root_retirement(expected).await?;
        let page = self
            .backend
            .scan_prefix_page_with_byte_limits(&prefix, None, page_limits())
            .await?;
        self.check_aborted_root_retirement(expected).await?;
        let entry = page.first().ok_or(WorkspaceError::Fenced)?;
        let reference = SnapshotObjectRef::decode_value(&entry.value).map_err(journal_error)?;
        if !self
            .release_aborted_graph_member(expected, entry, &reference)
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(reference)
    }

    /// A runnable conservative collector: all BindingHistory/AdoptingBinding
    /// roots are permanently retained; only a durably Aborted PPJ whose PUTs
    /// all completed may release its own exact ordinal memberships.
    pub(crate) async fn collect_aborted_packed_graph<O: ObjectBackend + Clone>(
        &self,
        expected: &PackedJournalRecord,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        max_members: u64,
        cancel: CancellationToken,
    ) -> Result<u64, WorkspaceError> {
        self.registry_budget(budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        live(budget, &cancel)?;
        if expected.phase != PackedJournalPhase::Aborted || max_members == 0 {
            return Err(WorkspaceError::Fenced);
        }
        self.begin_aborted_root_retirement(expected).await?;
        let prefix = format!(
            "{REGISTRY_REVERSE_PREFIX}{}/",
            expected.source.staging_id.simple()
        )
        .into_bytes();
        let mut after: Option<Vec<u8>> = None;
        let mut deleted = 0u64;
        let mut released = 0u64;
        loop {
            live(budget, &cancel)?;
            // Reverse rows are removed atomically with member release. A later
            // tick resumes at the remaining rows while the root stays Retiring.
            if released == max_members {
                return Ok(deleted);
            }
            // Every page has its own read version. Validate terminal ownership
            // on both sides; each release then consumes exact page/row checks.
            self.check_aborted_root_retirement(expected).await?;
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(&prefix, after.as_deref(), page_limits())
                .await?;
            self.check_aborted_root_retirement(expected).await?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                live(budget, &cancel)?;
                if released == max_members {
                    return Ok(deleted);
                }
                if !entry.key.starts_with(&prefix)
                    || after.as_ref().is_some_and(|last| &entry.key <= last)
                {
                    return Err(journal_error("root retirement cursor/key mismatch"));
                }
                let reference =
                    SnapshotObjectRef::decode_value(&entry.value).map_err(journal_error)?;
                if self
                    .release_aborted_graph_member(expected, &entry, &reference)
                    .await?
                {
                    self.delete_registered_packed_object(client, reference, budget, &cancel)
                        .await?;
                    deleted = increment(deleted)?;
                }
                // Shared objects cost a membership release too. Yield only
                // after any single-use DELETE has reached its durable outcome.
                released = increment(released)?;
                after = Some(entry.key);
            }
        }
        live(budget, &cancel)?;
        let (keys, values, mut root) = self.aborted_root_values(expected).await?;
        if root.members != 0 || root.pending_puts != 0 {
            return Err(journal_error("empty root index with retained members"));
        }
        if root.state == RootState::Retiring {
            root.state = RootState::Retired;
            root.revision = increment(root.revision)?;
            self.registry_cas(
                &exact_checks(&keys, &values),
                &[
                    KvWrite::Put {
                        key: keys[2].clone(),
                        value: root.encode()?,
                    },
                    put(keys[1].clone(), &next_packed_root_generation(&values[1])?)?,
                ],
            )
            .await?;
        } else if root.state != RootState::Retired {
            return Err(WorkspaceError::Fenced);
        }
        live(budget, &cancel)?;
        Ok(deleted)
    }

    async fn aborted_root_values(
        &self,
        expected: &PackedJournalRecord,
    ) -> Result<AbortedRootRead, WorkspaceError> {
        let keys = vec![
            GATE_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            registry_root_key(expected.source.staging_id),
            journal_key(expected.journal_id),
            active_key(expected.journal_id),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != keys.len() {
            return Err(journal_error("short terminal root read"));
        }
        active_gate(&values[0])?;
        next_packed_root_generation(&values[1])?;
        if values[3].as_deref() != Some(expected.encode()?.as_slice()) || values[4].is_some() {
            return Err(WorkspaceError::Fenced);
        }
        let root = RootRow::decode(root_bytes(&values[2])?)?;
        if root.journal_id != expected.journal_id
            || root.incarnation != expected.source.staging_id
            || root.binding.is_some()
            || root.pending_puts != 0
            || root.members > expected.object_count
            || !matches!(
                root.state,
                RootState::AbortedRetained | RootState::Retiring | RootState::Retired
            )
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok((keys, values, root))
    }
    async fn begin_aborted_root_retirement(
        &self,
        expected: &PackedJournalRecord,
    ) -> Result<(), WorkspaceError> {
        let (keys, values, mut root) = self.aborted_root_values(expected).await?;
        let writes = if root.state == RootState::AbortedRetained {
            if root.members != expected.object_count {
                return Err(journal_error("aborted root cardinality mismatch"));
            }
            root.state = RootState::Retiring;
            root.revision = increment(root.revision)?;
            vec![
                KvWrite::Put {
                    key: keys[2].clone(),
                    value: root.encode()?,
                },
                put(keys[1].clone(), &next_packed_root_generation(&values[1])?)?,
            ]
        } else {
            Vec::new()
        };
        self.registry_cas(&exact_checks(&keys, &values), &writes)
            .await
    }
    async fn check_aborted_root_retirement(
        &self,
        expected: &PackedJournalRecord,
    ) -> Result<(), WorkspaceError> {
        let (keys, values, root) = self.aborted_root_values(expected).await?;
        if !matches!(root.state, RootState::Retiring | RootState::Retired) {
            return Err(WorkspaceError::Fenced);
        }
        self.registry_cas(&exact_checks(&keys, &values), &[]).await
    }
    async fn release_aborted_graph_member(
        &self,
        expected: &PackedJournalRecord,
        entry: &KvEntry,
        reference: &SnapshotObjectRef,
    ) -> Result<bool, WorkspaceError> {
        let (mut keys, mut values, mut root) = self.aborted_root_values(expected).await?;
        if root.state != RootState::Retiring {
            return Err(WorkspaceError::Fenced);
        }
        let member_keys = [
            registry_object_key(reference),
            registry_member_key(reference, root.incarnation),
            entry.key.clone(),
        ];
        let member_values = self.packed_journal_values(&member_keys).await?;
        if member_values.len() != 3 || member_values[2].as_deref() != Some(entry.value.as_slice()) {
            return Err(WorkspaceError::Busy);
        }
        let mut object = ObjectRow::decode(root_bytes(&member_values[0])?)?;
        let mut member = MemberRow::decode(root_bytes(&member_values[1])?)?;
        if object.reference != *reference
            || object.state != ObjectState::Live
            || member.reference != *reference
            || member.incarnation != root.incarnation
            || member.journal_id != expected.journal_id
            || member.adopted
            || !member.retained
            || member.pending_put
            || !member.dispatched
            || entry.key != registry_reverse_key(root.incarnation, member.ordinal)
        {
            return Err(WorkspaceError::Fenced);
        }
        let occurrence = self
            .packed_journal_values(&[object_key(expected.journal_id, member.ordinal)])
            .await?;
        if occurrence.len() != 1 {
            return Err(journal_error("short retiring occurrence read"));
        }
        let occurrence_row = PackedJournalObject::decode(root_bytes(&occurrence[0])?)?;
        if occurrence_row.reference != *reference || !occurrence_row.uploaded {
            return Err(WorkspaceError::Fenced);
        }
        let occurrence_key = object_key(expected.journal_id, member.ordinal);
        let occurrence_value = occurrence[0].clone();
        keys.extend(member_keys.iter().cloned());
        values.extend(member_values.iter().cloned());
        let mut checks = exact_checks(&keys, &values);
        checks.push(KvCheck {
            key: occurrence_key,
            expected: occurrence_value,
        });
        member.retained = false;
        object.memberships = decrement(object.memberships)?;
        object.revision = increment(object.revision)?;
        root.members = decrement(root.members)?;
        root.revision = increment(root.revision)?;
        let retiring = object.memberships == 0;
        if retiring {
            if object.pending_puts != 0 {
                return Err(WorkspaceError::Fenced);
            }
            object.state = ObjectState::Retiring;
        }
        let writes = [
            KvWrite::Put {
                key: keys[2].clone(),
                value: root.encode()?,
            },
            put(keys[1].clone(), &next_packed_root_generation(&values[1])?)?,
            KvWrite::Put {
                key: member_keys[0].clone(),
                value: object.encode()?,
            },
            KvWrite::Put {
                key: member_keys[1].clone(),
                value: member.encode()?,
            },
            KvWrite::Delete {
                key: member_keys[2].clone(),
            },
        ];
        self.registry_cas(&checks, &writes).await?;
        Ok(retiring)
    }

    async fn reserve_packed_delete(
        &self,
        reference: SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
        history: Option<HistoryDeleteContext>,
    ) -> Result<PackedDeleteGuard, WorkspaceError> {
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])
            .map_err(journal_budget_error)?;
        let keys = [
            GATE_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            registry_object_key(&reference),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 3 {
            return Err(journal_error("short DELETE reserve read"));
        }
        active_gate(&values[0])?;
        let mut object = ObjectRow::decode(root_bytes(&values[2])?)?;
        if object.reference != reference
            || object.state != ObjectState::Retiring
            || object.memberships != 0
            || object.pending_puts != 0
        {
            return Err(WorkspaceError::Fenced);
        }
        object.state = ObjectState::DeletePending;
        object.delete_id = Uuid::new_v4();
        object.delete_dispatched = false;
        object.revision = increment(object.revision)?;
        self.packed_delete_transition_cas(
            &reference,
            history,
            &exact_checks(&keys, &values),
            &[
                KvWrite::Put {
                    key: keys[2].clone(),
                    value: object.encode()?,
                },
                put(keys[1].clone(), &next_packed_root_generation(&values[1])?)?,
            ],
        )
        .await?;
        Ok(PackedDeleteGuard {
            reference,
            delete_id: object.delete_id,
            revision: object.revision,
            dispatched: false,
            _permit: permit,
        })
    }
    async fn dispatch_packed_delete(
        &self,
        mut guard: PackedDeleteGuard,
        history: Option<HistoryDeleteContext>,
    ) -> Result<PackedDeleteGuard, WorkspaceError> {
        let keys = [
            GATE_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            registry_object_key(&guard.reference),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 3 {
            return Err(journal_error("short DELETE dispatch read"));
        }
        active_gate(&values[0])?;
        let mut object = ObjectRow::decode(root_bytes(&values[2])?)?;
        if guard.dispatched
            || object.reference != guard.reference
            || object.state != ObjectState::DeletePending
            || object.delete_id != guard.delete_id
            || object.revision != guard.revision
            || object.delete_dispatched
        {
            return Err(WorkspaceError::Fenced);
        }
        object.delete_dispatched = true;
        object.revision = increment(object.revision)?;
        self.packed_delete_transition_cas(
            &guard.reference,
            history,
            &exact_checks(&keys, &values),
            &[
                KvWrite::Put {
                    key: keys[2].clone(),
                    value: object.encode()?,
                },
                put(keys[1].clone(), &next_packed_root_generation(&values[1])?)?,
            ],
        )
        .await?;
        guard.dispatched = true;
        guard.revision = object.revision;
        Ok(guard)
    }
    async fn finish_packed_delete(
        &self,
        guard: PackedDeleteGuard,
        history: Option<HistoryDeleteContext>,
    ) -> Result<(), WorkspaceError> {
        let keys = [
            GATE_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            registry_object_key(&guard.reference),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 3 {
            return Err(journal_error("short DELETE completion read"));
        }
        active_gate(&values[0])?;
        let mut object = ObjectRow::decode(root_bytes(&values[2])?)?;
        if !guard.dispatched
            || object.reference != guard.reference
            || object.state != ObjectState::DeletePending
            || object.delete_id != guard.delete_id
            || object.revision != guard.revision
            || !object.delete_dispatched
        {
            return Err(WorkspaceError::Fenced);
        }
        object.state = ObjectState::Deleted;
        object.revision = increment(object.revision)?;
        self.packed_delete_transition_cas(
            &guard.reference,
            history,
            &exact_checks(&keys, &values),
            &[
                KvWrite::Put {
                    key: keys[2].clone(),
                    value: object.encode()?,
                },
                put(keys[1].clone(), &next_packed_root_generation(&values[1])?)?,
            ],
        )
        .await
    }
    /// The only production completion path consumes this guard after this
    /// exact physical DELETE succeeds. Cancellation/error leaves quarantine.
    pub(crate) async fn delete_registered_packed_object<O: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<O>,
        reference: SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
        cancel: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        self.delete_registered_packed_object_owned(client, reference, budget, cancel, None)
            .await
    }

    pub(super) async fn delete_registered_packed_object_for_history<O: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<O>,
        reference: SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
        cancel: &CancellationToken,
        history: HistoryDeleteContext,
    ) -> Result<(), WorkspaceError> {
        self.delete_registered_packed_object_owned(client, reference, budget, cancel, Some(history))
            .await
    }

    async fn packed_delete_transition_cas(
        &self,
        reference: &SnapshotObjectRef,
        history: Option<HistoryDeleteContext>,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<(), WorkspaceError> {
        if let Some(context) = history {
            let mut complete = self
                .history_retirement_delete_checks(context, reference)
                .await?;
            append_checks(&mut complete, checks.to_vec())?;
            self.history_clock_cas(&complete, writes, Some(context.lower))
                .await
        } else {
            self.registry_cas(checks, writes).await
        }
    }

    async fn delete_registered_packed_object_owned<O: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<O>,
        reference: SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
        cancel: &CancellationToken,
        history: Option<HistoryDeleteContext>,
    ) -> Result<(), WorkspaceError> {
        self.registry_budget(budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        live(budget, cancel)?;
        let guard = self
            .reserve_packed_delete(reference, budget, history)
            .await?;
        live(budget, cancel)?;
        let guard = self.dispatch_packed_delete(guard, history).await?;
        live(budget, cancel)?;
        tokio::select! {
            _ = cancel.cancelled() => return Err(WorkspaceError::Busy),
            outcome = client.delete_object(&guard.reference.key) => outcome.map_err(|error| WorkspaceError::Backend(error.to_string()))?,
        }
        live(budget, cancel)?;
        self.finish_packed_delete(guard, history).await
    }

    /// Durable restart pickup for objects whose last membership was released
    /// before the earlier collector could send DELETE. Unknown pending rows
    /// are skipped permanently; absence/HEAD is never consulted.
    pub(crate) async fn collect_retiring_packed_objects<O: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: PackedObjectCollectionOptions,
    ) -> Result<u64, WorkspaceError> {
        self.registry_budget(budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if options.max_objects == 0 || options.max_objects > MAX_OBJECTS {
            return Err(WorkspaceError::Busy);
        }
        let mut after: Option<Vec<u8>> = None;
        let mut visited = 0u64;
        let mut deleted = 0u64;
        loop {
            live(budget, &options.cancel)?;
            let gate_keys = [GATE_KEY.to_vec()];
            let gate = self.packed_journal_values(&gate_keys).await?;
            if gate.len() != 1 {
                return Err(journal_error("short collector gate read"));
            }
            active_gate(&gate[0])?;
            self.registry_cas(&exact_checks(&gate_keys, &gate), &[])
                .await?;
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    REGISTRY_OBJECT_PREFIX.as_bytes(),
                    after.as_deref(),
                    page_limits(),
                )
                .await?;
            self.registry_cas(&exact_checks(&gate_keys, &gate), &[])
                .await?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                if !entry.key.starts_with(REGISTRY_OBJECT_PREFIX.as_bytes())
                    || after.as_ref().is_some_and(|last| &entry.key <= last)
                {
                    return Err(journal_error("object collector cursor/key mismatch"));
                }
                visited = increment(visited)?;
                if visited > options.max_objects {
                    return Err(WorkspaceError::Busy);
                }
                let object = ObjectRow::decode(&entry.value)?;
                if entry.key != registry_object_key(&object.reference) {
                    return Err(journal_error("global object key/record mismatch"));
                }
                if object.state == ObjectState::Retiring {
                    live(budget, &options.cancel)?;
                    self.delete_registered_packed_object(
                        client,
                        object.reference,
                        budget,
                        &options.cancel,
                    )
                    .await?;
                    deleted = increment(deleted)?;
                }
                after = Some(entry.key);
            }
        }
        live(budget, &options.cancel)?;
        Ok(deleted)
    }
}
