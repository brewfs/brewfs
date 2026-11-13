//! Binding-local immutable metadata memos. No payload bytes or errors are cached.

use std::future::Future;
use std::sync::Arc;

use super::{EntityDentry, EntityInode, Result};
use crate::workspace::entity::model::ChunkMap;
use crate::workspace::packed::snapshot::SnapshotColdAttributes;

/// Shared byte budget across all entity-facing memo classes, including keys
/// and a conservative entry overhead. This follows r31's 64 MiB lower memos.
pub(super) const LOWER_MEMO_BYTES: u64 = 64 << 20;
pub(super) const MAX_DIRECTORY_BYTES: usize = 4 << 20;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) enum Key {
    Inode(i64),
    Cold(i64),
    Dentry(i64, Vec<u8>),
    Chunk(i64, u64, u64),
    Directory(i64),
}

pub(super) enum Value {
    Inode(Option<Arc<EntityInode>>),
    Cold(Option<Arc<SnapshotColdAttributes>>),
    Dentry(Option<EntityDentry>),
    Chunk(Arc<ChunkMap>),
    Directory(Arc<Directory>),
}

/// A complete immutable directory retains its published enumeration order and
/// a separate sorted index of entry positions for name lookup.
pub(super) struct Directory {
    pub(super) entries: Vec<(Vec<u8>, EntityDentry)>,
    name_index: Vec<usize>,
    #[cfg(test)]
    comparisons: std::sync::atomic::AtomicUsize,
}

impl Directory {
    pub(super) fn from_entries(entries: Vec<(Vec<u8>, EntityDentry)>) -> Self {
        let mut name_index: Vec<_> = (0..entries.len()).collect();
        name_index.sort_unstable_by(|left, right| entries[*left].0.cmp(&entries[*right].0));
        Self {
            entries,
            name_index,
            #[cfg(test)]
            comparisons: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(super) fn lookup(&self, name: &[u8]) -> Option<&EntityDentry> {
        let index = self
            .name_index
            .binary_search_by(|index| {
                #[cfg(test)]
                self.comparisons
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.entries[*index].0.as_slice().cmp(name)
            })
            .ok()?;
        Some(&self.entries[self.name_index[index]].1)
    }
}

fn weight(key: &Key, value: &Arc<Value>) -> u32 {
    let key_bytes = match key {
        Key::Dentry(_, name) => name.len(),
        _ => 0,
    };
    let payload = match value.as_ref() {
        Value::Inode(Some(inode)) => {
            std::mem::size_of::<EntityInode>() + inode.symlink_target.as_ref().map_or(0, Vec::len)
        }
        Value::Cold(Some(cold)) => {
            std::mem::size_of::<SnapshotColdAttributes>()
                + cold.symlink_target.as_ref().map_or(0, Vec::len)
                + cold.acl.capacity() * std::mem::size_of::<crate::meta::store::AclRule>()
                + cold.xattrs.capacity()
                    * std::mem::size_of::<crate::workspace::packed::snapshot::SnapshotXattr>()
                + cold
                    .xattrs
                    .iter()
                    .map(|attr| attr.name.capacity() + attr.value.capacity())
                    .sum::<usize>()
        }
        Value::Chunk(map) => {
            std::mem::size_of::<ChunkMap>()
                + map.spans.capacity()
                    * std::mem::size_of::<crate::workspace::entity::model::ChunkSpan>()
        }
        Value::Directory(entries) => {
            std::mem::size_of::<Directory>()
                + entries.entries.capacity() * std::mem::size_of::<(Vec<u8>, EntityDentry)>()
                + entries.name_index.capacity() * std::mem::size_of::<usize>()
                + entries
                    .entries
                    .iter()
                    .map(|(name, _)| name.capacity())
                    .sum::<usize>()
        }
        Value::Inode(None) | Value::Cold(None) | Value::Dentry(_) => 0,
    };
    u32::try_from(key_bytes.saturating_add(payload))
        .unwrap_or(u32::MAX)
        .saturating_add(256)
}

pub(super) struct Memo {
    entries: moka::future::Cache<Key, Arc<Value>>,
    #[cfg(test)]
    fills: std::sync::atomic::AtomicU64,
}

impl Memo {
    pub(super) fn new() -> Self {
        Self::with_capacity(LOWER_MEMO_BYTES)
    }

    pub(super) fn with_capacity(capacity: u64) -> Self {
        Self {
            entries: moka::future::Cache::builder()
                .max_capacity(capacity)
                .weigher(weight)
                .eviction_policy(moka::policy::EvictionPolicy::lru())
                .build(),
            #[cfg(test)]
            fills: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(super) async fn peek(&self, key: &Key) -> Option<Arc<Value>> {
        self.entries.get(key).await
    }

    /// Async single-flight fill; successful immutable absence is cached, while
    /// all transport/authentication/permission errors remain errors and retryable.
    pub(super) async fn get<L, F>(&self, key: Key, load: L) -> Result<Arc<Value>>
    where
        L: FnOnce() -> F,
        F: Future<Output = Result<Value>>,
    {
        if let Some(value) = self.entries.get(&key).await {
            return Ok(value);
        }
        // Nested inode/cold/directory loaders have large future states. Keep
        // those states out of both this future and Moka's inline initializer,
        // and construct a loader only if Moka selects this caller to fill.
        self.entries
            .try_get_with(
                key,
                Box::pin(async move {
                    #[cfg(test)]
                    self.fills
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    load().await.map(Arc::new)
                }),
            )
            .await
            .map_err(|error| error.as_ref().clone())
    }

    #[cfg(test)]
    pub(super) async fn clear(&self) {
        self.entries.invalidate_all();
        self.entries.run_pending_tasks().await;
    }

    #[cfg(test)]
    pub(super) fn fills(&self) -> u64 {
        self.fills.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) async fn retained_bytes(&self) -> u64 {
        self.entries.run_pending_tasks().await;
        self.entries.weighted_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::entity::model::EntityKind;
    use crate::workspace::entity::resolver::MAX_DIRECTORY_ENTRIES;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn directory_lookup_is_logarithmic_and_preserves_enumeration_order() {
        let entries: Vec<_> = (0..MAX_DIRECTORY_ENTRIES)
            .rev()
            .map(|index| {
                (
                    format!("entry-{index:04}").into_bytes(),
                    EntityDentry::Present {
                        ino: index as i64 + 1,
                        kind: EntityKind::File,
                    },
                )
            })
            .collect();
        let directory = Directory::from_entries(entries.clone());
        // Allow log2(n) plus two comparisons per distinct query. The previous
        // scan needs n comparisons for each miss, regardless of memo warmth.
        let comparison_limit = MAX_DIRECTORY_ENTRIES.ilog2() as usize + 2;
        for index in 0..MAX_DIRECTORY_ENTRIES {
            let missing = format!("entry-{index:04}-missing");
            assert!(directory.lookup(missing.as_bytes()).is_none());
            let comparisons = directory.comparisons.swap(0, Ordering::Relaxed);
            assert!(comparisons > 0 && comparisons <= comparison_limit);
        }
        for (name, entry) in &entries {
            assert_eq!(directory.lookup(name), Some(entry));
            let comparisons = directory.comparisons.swap(0, Ordering::Relaxed);
            assert!(comparisons > 0 && comparisons <= comparison_limit);
        }
        assert_eq!(directory.entries, entries);
    }

    #[test]
    fn memo_future_does_not_embed_loader_state() {
        let memo = Memo::new();
        let load = || async {
            let bytes = std::hint::black_box([0u8; 1 << 20]);
            std::future::pending::<()>().await;
            std::hint::black_box(bytes);
            Ok(Value::Cold(None))
        };
        fn loader_bytes<F: Future>(_: &impl FnOnce() -> F) -> usize {
            std::mem::size_of::<F>()
        }
        assert!(loader_bytes(&load) >= 1 << 20);
        let future = memo.get(Key::Cold(1), load);
        assert!(std::mem::size_of_val(&future) < 64 << 10);
    }

    #[tokio::test]
    async fn memo_hit_does_not_construct_loader() {
        let memo = Memo::new();
        let constructions = AtomicU64::new(0);
        let load = || {
            constructions.fetch_add(1, Ordering::Relaxed);
            async { Ok(Value::Cold(None)) }
        };
        for _ in 0..2 {
            let value = memo.get(Key::Cold(1), load).await.unwrap();
            assert!(matches!(value.as_ref(), Value::Cold(None)));
        }
        assert_eq!(constructions.load(Ordering::Relaxed), 1);
        assert_eq!(memo.fills(), 1);
    }
}
