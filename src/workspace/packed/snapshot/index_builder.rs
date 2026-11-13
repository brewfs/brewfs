//! Streaming immutable index construction with one bounded buffer per level.

use super::{
    SnapshotIndexPage, SnapshotIndexRecord, SnapshotIndexValue, SnapshotObjectKind,
    SnapshotObjectRef,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace::packed::wire::{PackedResult, PackedWireError};

pub struct SnapshotIndexBuilder<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    kind: SnapshotObjectKind,
    prefix: String,
    record_limit: usize,
    body_limit: usize,
    levels: Vec<Vec<SnapshotIndexRecord>>,
    last_key: Option<Vec<u8>>,
    pages_written: u64,
    peak_buffered_bytes: usize,
    poisoned: bool,
}

impl<B: ObjectBackend + Clone + 'static> SnapshotIndexBuilder<B> {
    pub fn new(
        client: ObjectClient<B>,
        kind: SnapshotObjectKind,
        prefix: String,
        record_limit: usize,
        body_limit: usize,
    ) -> PackedResult<Self> {
        super::validate_key(&prefix)?;
        if !(2..=1024).contains(&record_limit)
            || !(16 * 1024..=super::index::SNAPSHOT_INDEX_BODY_LIMIT).contains(&body_limit)
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 index builder limits are invalid".into(),
            ));
        }
        SnapshotIndexPage {
            kind,
            height: 0,
            records: vec![],
        }
        .encode()?;
        Ok(Self {
            client,
            kind,
            prefix,
            record_limit,
            body_limit,
            levels: vec![Vec::new()],
            last_key: None,
            pages_written: 0,
            peak_buffered_bytes: 0,
            poisoned: false,
        })
    }

    pub async fn push(&mut self, record: SnapshotIndexRecord) -> PackedResult<()> {
        if self.poisoned {
            return Err(PackedWireError::Invalid(
                "wire 005 index builder was interrupted".into(),
            ));
        }
        if !matches!(record.value, SnapshotIndexValue::Leaf(_))
            || self
                .last_key
                .as_deref()
                .is_some_and(|key| key >= record.first_key.as_slice())
        {
            return Err(PackedWireError::Invalid(
                "wire 005 index input must be strictly ordered leaves".into(),
            ));
        }
        let encoded = SnapshotIndexPage {
            kind: self.kind,
            height: 0,
            records: vec![record.clone()],
        }
        .encode()?;
        if encoded.len() - super::SNAPSHOT_HEADER_LEN - super::SNAPSHOT_FOOTER_LEN > self.body_limit
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 input exceeds configured page budget".into(),
            ));
        }
        let last_key = record.last_key.clone();
        self.poisoned = true;
        if let Err(error) = self.append(0, record).await {
            self.poisoned = true;
            return Err(error);
        }
        self.poisoned = false;
        self.last_key = Some(last_key);
        Ok(())
    }

    async fn append(
        &mut self,
        mut level: usize,
        mut record: SnapshotIndexRecord,
    ) -> PackedResult<()> {
        loop {
            if level > 16 {
                return Err(PackedWireError::LimitExceeded(
                    "wire 005 index builder exceeds maximum height".into(),
                ));
            }
            while self.levels.len() <= level {
                self.levels.push(Vec::new());
            }
            let current_bytes = 12 + self.levels[level].iter().map(record_bytes).sum::<usize>();
            if !self.levels[level].is_empty()
                && (self.levels[level].len() >= self.record_limit
                    || current_bytes + record_bytes(&record) > self.body_limit)
            {
                let (reference, first, last, subtree_weight) = self.flush(level).await?;
                self.levels[level].push(record);
                self.track_buffers();
                record = SnapshotIndexRecord {
                    first_key: first,
                    last_key: last,
                    value: SnapshotIndexValue::Child {
                        reference,
                        subtree_weight,
                    },
                };
                level += 1;
            } else {
                self.levels[level].push(record);
                self.track_buffers();
                return Ok(());
            }
        }
    }

    async fn flush(
        &mut self,
        level: usize,
    ) -> PackedResult<(SnapshotObjectRef, Vec<u8>, Vec<u8>, u64)> {
        let records = std::mem::take(&mut self.levels[level]);
        let first = records
            .first()
            .map(|r| r.first_key.clone())
            .unwrap_or_default();
        let last = records
            .last()
            .map(|r| r.last_key.clone())
            .unwrap_or_default();
        let page = SnapshotIndexPage {
            kind: self.kind,
            height: level as u8,
            records,
        };
        let bytes = page.encode()?;
        let subtree_weight = page.total_weight()?;
        if bytes.len() - super::SNAPSHOT_HEADER_LEN - super::SNAPSHOT_FOOTER_LEN > self.body_limit {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 builder page exceeds configured budget".into(),
            ));
        }
        use sha2::{Digest, Sha256};
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let key = format!(
            "{}/{}/{}",
            self.prefix,
            self.kind as u8,
            hex::encode(digest)
        );
        let reference = SnapshotObjectRef::from_bytes(key, self.kind, &bytes)?;
        self.client
            .put_object_create_only(&reference.key, &bytes)
            .await
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        let observed = super::read_snapshot_page(&self.client, &reference, self.body_limit).await?;
        reference.verify(&observed, self.body_limit)?;
        self.pages_written += 1;
        Ok((reference, first, last, subtree_weight))
    }

    fn track_buffers(&mut self) {
        self.peak_buffered_bytes = self.peak_buffered_bytes.max(
            self.levels
                .iter()
                .map(|level| 12 + level.iter().map(record_bytes).sum::<usize>())
                .sum(),
        );
    }

    pub async fn finish(mut self) -> PackedResult<SnapshotObjectRef> {
        if self.poisoned {
            return Err(PackedWireError::Invalid(
                "wire 005 index builder was interrupted".into(),
            ));
        }
        loop {
            let occupied: Vec<usize> = self
                .levels
                .iter()
                .enumerate()
                .filter_map(|(i, records)| (!records.is_empty()).then_some(i))
                .collect();
            if occupied.is_empty() {
                return Ok(self.flush(0).await?.0);
            }
            let lowest = occupied[0];
            if occupied.len() == 1 {
                if self.levels[lowest].len() == 1
                    && let SnapshotIndexValue::Child { reference, .. } =
                        &self.levels[lowest][0].value
                {
                    return Ok(reference.clone());
                }
                return Ok(self.flush(lowest).await?.0);
            }
            let (reference, first, last, subtree_weight) = self.flush(lowest).await?;
            self.append(
                lowest + 1,
                SnapshotIndexRecord {
                    first_key: first,
                    last_key: last,
                    value: SnapshotIndexValue::Child {
                        reference,
                        subtree_weight,
                    },
                },
            )
            .await?;
        }
    }

    pub fn peak_buffered_bytes(&self) -> usize {
        self.peak_buffered_bytes
    }
    pub fn pages_written(&self) -> u64 {
        self.pages_written
    }
}

fn record_bytes(record: &SnapshotIndexRecord) -> usize {
    4 + record.first_key.len()
        + record.last_key.len()
        + match &record.value {
            SnapshotIndexValue::Leaf(value) => 4 + value.len(),
            SnapshotIndexValue::Child { reference, .. } => 52 + reference.key.len(),
        }
}

#[cfg(test)]
mod tests {
    use super::super::SnapshotIndexReader;
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;

    fn record(i: u32) -> SnapshotIndexRecord {
        SnapshotIndexRecord {
            first_key: i.to_be_bytes().to_vec(),
            last_key: i.to_be_bytes().to_vec(),
            value: SnapshotIndexValue::Leaf(i.to_le_bytes().to_vec()),
        }
    }

    #[tokio::test]
    async fn streaming_index_handles_full_partial_and_empty_levels() {
        for count in [0u32, 1, 2, 3, 8, 9, 65] {
            let dir = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(dir.path()));
            let mut builder = SnapshotIndexBuilder::new(
                client.clone(),
                SnapshotObjectKind::InodeIndex,
                "indexes".into(),
                2,
                16 * 1024,
            )
            .unwrap();
            for i in 1..=count {
                builder.push(record(i)).await.unwrap();
            }
            assert!(builder.peak_buffered_bytes() < 17 * 16 * 1024);
            let reference = builder.finish().await.unwrap();
            let reader = SnapshotIndexReader::new(client, 0);
            for i in 1..=count {
                assert_eq!(
                    reader
                        .lookup(&reference, &i.to_be_bytes())
                        .await
                        .unwrap()
                        .as_ref()
                        .map(|value| value.as_ref()),
                    Some(&i.to_le_bytes()[..])
                );
            }
            assert_eq!(
                reader
                    .lookup(&reference, &(count + 1).to_be_bytes())
                    .await
                    .unwrap(),
                None
            );
        }
    }

    #[tokio::test]
    async fn streaming_index_rejects_unsorted_duplicate_or_branch_input() {
        let dir = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(dir.path()));
        let mut builder = SnapshotIndexBuilder::new(
            client,
            SnapshotObjectKind::InodeIndex,
            "indexes".into(),
            16,
            16 * 1024,
        )
        .unwrap();
        builder.push(record(2)).await.unwrap();
        assert!(builder.push(record(2)).await.is_err());
        assert!(builder.push(record(1)).await.is_err());
        assert_eq!(builder.pages_written(), 0);
    }
}
