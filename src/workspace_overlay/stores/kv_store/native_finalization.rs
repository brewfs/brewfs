//! Durable metadata cleanup after the collector's physical prerequisites.
//! This is the bounded metadata phase of the existing small-catalog collector;
//! it does not authenticate large CONTROL or replace its complete mark proof.

use super::super::kv_backend::KvReadLimits;
use super::*;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const STATE_PREFIX: &[u8] = b"packed/v3/native-finalization/";
const STATE_BYTES: usize = 4096;
pub(super) const MAX_TARGETS: usize = 4;
const MAX_QUOTA: usize = 32;
const FAMILIES: usize = 7;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Basis {
    run: Uuid,
    inventory: Option<Vec<u8>>,
    volume: Vec<u8>,
    targets: Vec<(LayerId, Option<Vec<u8>>)>,
    incarnations: Vec<Option<Vec<u8>>>,
    carrier_digest: [u8; 32],
    family: usize,
}

pub(super) fn is_state_key(key: &[u8]) -> bool {
    key.strip_prefix(STATE_PREFIX).is_some_and(|suffix| {
        suffix.len() == 64 && suffix.iter().all(|byte| b"0123456789abcdef".contains(byte))
    })
}

pub(super) fn is_reverse_state_key(key: &[u8]) -> bool {
    key.strip_prefix(b"packed/v3/native-reverse/state/")
        .is_some_and(|suffix| {
            std::str::from_utf8(suffix)
                .ok()
                .and_then(|value| value.parse::<LayerId>().ok())
                .is_some_and(|id| native_reverse::state_key(id) == key)
        })
}

fn point_limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: STATE_BYTES,
        max_total_bytes: 112 << 10,
        max_response_bytes: 128 << 10,
        max_data_requests: 32,
    }
}

fn family_prefix(layer: LayerId, family: usize) -> Vec<u8> {
    match family {
        0 => dentry_layer_prefix(layer),
        1 => inode_layer_prefix(layer),
        2 => xattr_layer_prefix(layer),
        3 => acl_layer_prefix(layer),
        4 => extent_layer_prefix(layer),
        5 => native_reverse::layer_prefix(layer),
        6 => extent_owner_layer_prefix(layer),
        _ => unreachable!("validated durable family phase"),
    }
}

fn state_key(targets: &[LayerId]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"packed-v3-native-metadata-finalization");
    for target in targets {
        digest.update(target.as_bytes());
    }
    [STATE_PREFIX, hex::encode(digest.finalize()).as_bytes()].concat()
}

fn digest_carrier(checks: &[KvCheck], writes: &[KvWrite]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"packed-v3-native-finalization-carrier-basis");
    digest.update((checks.len() as u64).to_be_bytes());
    for check in checks {
        digest.update((check.key.len() as u64).to_be_bytes());
        digest.update(&check.key);
        digest.update([u8::from(check.expected.is_some())]);
        if let Some(value) = &check.expected {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
    }
    digest.update((writes.len() as u64).to_be_bytes());
    for write in writes {
        match write {
            KvWrite::Put { key, value } => {
                digest.update([1]);
                digest.update((key.len() as u64).to_be_bytes());
                digest.update(key);
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value);
            }
            KvWrite::Delete { key } => {
                digest.update([0]);
                digest.update((key.len() as u64).to_be_bytes());
                digest.update(key);
            }
        }
    }
    digest.finalize().into()
}

impl Basis {
    fn validate(&self, layers: &[LayerId]) -> Result<(), WorkspaceError> {
        if self.run.is_nil()
            || self.targets.len() != layers.len()
            || self.targets.len() > MAX_TARGETS
            || self.incarnations.len() != self.targets.len()
            || self.family > self.targets.len() * FAMILIES
            || !self.targets.iter().map(|(id, _)| id).eq(layers.iter())
        {
            return Err(WorkspaceError::Fenced);
        }
        layer_inventory_generation(&self.inventory)?;
        let header: VolumeHeader = decode_open_value(&self.volume, STATE_BYTES)?;
        if header.schema_version != WORKSPACE_SCHEMA_VERSION
            || header.volume_format != VOLUME_FORMAT
            || header.volume_id.is_nil()
            || header.created_at_ns <= 0
        {
            return Err(WorkspaceError::Fenced);
        }
        for ((id, raw), incarnation) in self.targets.iter().zip(&self.incarnations) {
            if let Some(raw) = raw {
                let layer: LayerRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
                if layer.layer_id != *id
                    || layer.schema_version != WORKSPACE_SCHEMA_VERSION
                    || layer.state != LayerState::Deleting
                    || layer.next_sequence == 0
                    || !(1..=32).contains(&layer.depth)
                {
                    return Err(WorkspaceError::Fenced);
                }
                native_reverse::gc_incarnation(
                    incarnation.as_deref().ok_or(WorkspaceError::Fenced)?,
                    *id,
                )?;
            } else if incarnation.is_some() {
                return Err(WorkspaceError::Fenced);
            }
        }
        Ok(())
    }

    fn authorities(&self, current_inventory: &Option<Vec<u8>>) -> Vec<KvCheck> {
        let mut checks = vec![
            KvCheck {
                key: LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                expected: current_inventory.clone(),
            },
            KvCheck {
                key: VOLUME_HEADER_KEY.to_vec(),
                expected: Some(self.volume.clone()),
            },
        ];
        checks.extend(self.targets.iter().map(|(id, expected)| KvCheck {
            key: hot_layer_key(*id),
            expected: expected.clone(),
        }));
        checks.extend(
            self.targets
                .iter()
                .zip(&self.incarnations)
                .map(|((id, _), expected)| KvCheck {
                    key: native_reverse::state_key(*id),
                    expected: expected.clone(),
                }),
        );
        checks
    }

    fn authenticate_control(
        &self,
        state: &ControlState,
        packed_roots: &BTreeSet<LayerId>,
    ) -> Result<(), WorkspaceError> {
        if state.schema_version != WORKSPACE_SCHEMA_VERSION
            || state.header.as_ref().map(encode).transpose()?.as_ref() != Some(&self.volume)
        {
            return Err(WorkspaceError::Fenced);
        }
        // The previous mark authenticated grace-cutoff roots. Retaining every
        // unreleased native lease here is conservative while its separate grace
        // reaper catches up; no expired/unfinished owner becomes an empty proof.
        let mut reachable = reachable_layers(state, i64::MIN);
        reachable.extend(reachable_from_roots(state, packed_roots.iter().copied()));
        for (id, raw) in &self.targets {
            if reachable.contains(id) {
                return Err(WorkspaceError::Busy);
            }
            if state.layers.get(id).map(encode).transpose()? != *raw {
                return Err(WorkspaceError::Busy);
            }
        }
        Ok(())
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn finalize_native_metadata_pages(
        &self,
        mut layers: Vec<LayerId>,
        quota: usize,
    ) -> Result<(), WorkspaceError> {
        if quota == 0 || quota > MAX_QUOTA || layers.len() > MAX_TARGETS {
            return Err(WorkspaceError::InvalidReadPlan(
                "native metadata finalization quota/target limit".into(),
            ));
        }
        layers.sort_unstable();
        layers.dedup();
        if layers.is_empty() || layers.iter().any(|id| id.as_uuid().is_nil()) {
            return Err(WorkspaceError::Fenced);
        }
        let key = state_key(&layers);
        let mut keys = vec![
            key.clone(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            VOLUME_HEADER_KEY.to_vec(),
        ];
        keys.extend(layers.iter().copied().map(hot_layer_key));
        let reverse_keys = layers
            .iter()
            .copied()
            .map(native_reverse::state_key)
            .collect::<Vec<_>>();
        keys.extend(reverse_keys.iter().cloned());
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, point_limits(keys.len()))
            .await?;
        if values.len() != keys.len() {
            return Err(WorkspaceError::Backend(
                "short native finalization basis".into(),
            ));
        }
        let mut raw_state = values[0].clone();
        let target_values = &values[3..3 + layers.len()];
        let incarnation_values = &values[3 + layers.len()..];
        layer_inventory_generation(&values[1])?;
        if raw_state.is_none() {
            for (target, incarnation) in target_values.iter().zip(incarnation_values) {
                if target.is_some() && incarnation.is_none() {
                    // A distinct, explicit admin action must authenticate
                    // and install a legacy Deleting incarnation. Finalization
                    // never turns absence into a new deletion authority.
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        let mut basis: Basis = if let Some(raw) = &raw_state {
            decode_open_value(raw, STATE_BYTES)?
        } else {
            Basis {
                run: Uuid::new_v4(),
                inventory: values[1].clone(),
                volume: values[2].clone().ok_or(WorkspaceError::Fenced)?,
                targets: layers
                    .iter()
                    .copied()
                    .zip(target_values.iter().cloned())
                    .collect(),
                incarnations: incarnation_values.to_vec(),
                carrier_digest: [0; 32],
                family: 0,
            }
        };
        basis.validate(&layers)?;
        if Some(&basis.volume) != values[2].as_ref()
            || basis
                .targets
                .iter()
                .map(|(_, raw)| raw)
                .ne(target_values.iter())
            || basis.incarnations.as_slice() != incarnation_values
        {
            // Never refresh a retained target or incarnation after partial
            // deletion. A new proof protocol must explicitly restart it.
            return Err(WorkspaceError::Busy);
        }
        // The saved inventory is first-admission evidence. Reauthenticate the
        // current complete roots/topology each CAS; exact stable Deleting
        // target identities prevent ABA while unrelated layer activity can
        // advance the inventory without permanently stalling this run.
        let mut authorities = basis.authorities(&values[1]);
        let carrier = self
            .prepare_carrier_metadata_deletion(&layers, &authorities)
            .await?;
        let carrier_digest = digest_carrier(&carrier.checks, &carrier.writes);
        if raw_state.is_some() && basis.carrier_digest != carrier_digest {
            return Err(WorkspaceError::Busy);
        }
        basis.carrier_digest = carrier_digest;
        self.merge_borrowed_checks(&mut authorities, carrier.checks.clone())?;
        if raw_state.is_none() {
            let encoded = encode(&basis)?;
            if encoded.len() > STATE_BYTES {
                return Err(WorkspaceError::Busy);
            }
            let mut checks = authorities.clone();
            checks.push(KvCheck {
                key: key.clone(),
                expected: None,
            });
            self.update_control_with_packed_roots_and_checks(
                true,
                &checks,
                |state, packed_roots, writes| {
                    basis.authenticate_control(state, packed_roots)?;
                    writes.push(KvWrite::Put {
                        key: key.clone(),
                        value: encoded.clone(),
                    });
                    Ok(())
                },
            )
            .await?;
            raw_state = Some(encoded);
        }
        let mut remaining = quota;
        while remaining > 0 && basis.family < layers.len() * FAMILIES {
            let layer_index = basis.family / FAMILIES;
            let family_index = basis.family % FAMILIES;
            let prefix = family_prefix(layers[layer_index], family_index);
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    &prefix,
                    None,
                    KvReadLimits {
                        max_records: remaining.min(8),
                        max_key_bytes: 1024,
                        max_value_bytes: 96 << 10,
                        max_total_bytes: 112 << 10,
                        max_response_bytes: 128 << 10,
                        max_data_requests: 32,
                    },
                )
                .await?;
            let mut previous: Option<&[u8]> = None;
            for row in &page {
                if !row.key.starts_with(&prefix)
                    || previous.is_some_and(|last| row.key.as_slice() <= last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                previous = Some(&row.key);
            }
            if page.len() > remaining.min(8)
                || (basis.targets[layer_index].1.is_none() && !page.is_empty())
            {
                return Err(WorkspaceError::Fenced);
            }
            let cost = page.len().max(1);
            let mut next = basis.clone();
            if page.is_empty() {
                next.family += 1;
            }
            let encoded = encode(&next)?;
            if encoded.len() > STATE_BYTES {
                return Err(WorkspaceError::Busy);
            }
            let mut checks = authorities.clone();
            checks.push(KvCheck {
                key: key.clone(),
                expected: raw_state.clone(),
            });
            self.update_control_with_packed_roots_and_checks(
                true,
                &checks,
                |state, packed_roots, writes| {
                    basis.authenticate_control(state, packed_roots)?;
                    writes.extend(page.iter().map(|row| KvWrite::Delete {
                        key: row.key.clone(),
                    }));
                    writes.push(KvWrite::Put {
                        key: key.clone(),
                        value: encoded.clone(),
                    });
                    Ok(())
                },
            )
            .await?;
            basis = next;
            raw_state = Some(encoded);
            remaining -= cost;
        }
        if basis.family < layers.len() * FAMILIES {
            return Err(WorkspaceError::Busy);
        }
        // A persisted cursor is progress, not an authority to assume emptiness.
        // Reobserve every terminal prefix before the final exact topology CAS.
        // This also rejects a corrupt/skipped durable family phase.
        for (index, layer) in layers.iter().enumerate() {
            for family in 0..FAMILIES {
                let prefix = family_prefix(*layer, family);
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &prefix,
                        None,
                        KvReadLimits {
                            max_records: 1,
                            max_key_bytes: 1024,
                            max_value_bytes: 96 << 10,
                            max_total_bytes: 112 << 10,
                            max_response_bytes: 128 << 10,
                            max_data_requests: 32,
                        },
                    )
                    .await?;
                if page.len() > 1 || page.iter().any(|row| !row.key.starts_with(&prefix)) {
                    return Err(WorkspaceError::Fenced);
                }
                if !page.is_empty() {
                    let mut next = basis.clone();
                    next.family = index * FAMILIES + family;
                    let encoded = encode(&next)?;
                    let mut checks = authorities.clone();
                    checks.push(KvCheck {
                        key: key.clone(),
                        expected: raw_state.clone(),
                    });
                    self.update_control_with_packed_roots_and_checks(
                        true,
                        &checks,
                        |state, packed_roots, writes| {
                            basis.authenticate_control(state, packed_roots)?;
                            writes.push(KvWrite::Put {
                                key: key.clone(),
                                value: encoded.clone(),
                            });
                            Ok(())
                        },
                    )
                    .await?;
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        // Reverse completeness state outlives all its build rows and every
        // forward-family empty observation; delete it only with topology.
        let mut checks = authorities;
        checks.push(KvCheck {
            key: key.clone(),
            expected: raw_state,
        });
        self.update_control_with_packed_roots_and_checks(
            true,
            &checks,
            |state, packed_roots, writes| {
                basis.authenticate_control(state, packed_roots)?;
                for layer in &layers {
                    state.layers.remove(layer);
                }
                writes.extend(carrier.writes.clone());
                writes.extend(
                    reverse_keys
                        .iter()
                        .cloned()
                        .map(|key| KvWrite::Delete { key }),
                );
                writes.push(KvWrite::Delete { key: key.clone() });
                Ok(())
            },
        )
        .await
    }
}

#[cfg(test)]
#[path = "native_finalization_tests.rs"]
mod tests;
