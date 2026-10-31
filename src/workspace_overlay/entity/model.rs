//! Current entity state for the independently versioned workspace layout.
//!
//! These records intentionally contain no layer, shared head, mutation sequence
//! or writable-layer digest. Immutable lower digests retain their integrity role.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::EntityError;

pub const ENTITY_FORMAT: &str = "workspace-entity-v1";
/// Older entity clients must reject namespaces whose acknowledged data can be local.
pub const ENTITY_WRITEBACK_FORMAT: &str = "workspace-entity-v1-local-writeback";
pub const MAX_CHUNK_SPANS: usize = 4096;
pub const MAX_NAME_BYTES: usize = 255;
pub const MAX_XATTR_BYTES: usize = 64 << 10;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LowerBinding {
    pub manifest_key: Vec<u8>,
    pub manifest_digest: [u8; 32],
    pub root_ino: i64,
    pub max_ino: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntityKind {
    File,
    Directory,
    Symlink,
    Fifo,
    Socket,
    CharDevice,
    BlockDevice,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntityInode {
    pub ino: i64,
    pub kind: EntityKind,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub symlink_target: Option<Vec<u8>>,
    pub parent_hint: Option<i64>,
    /// Monotonic lower-file visibility boundary. Extension never increases it.
    pub lower_visible_size: u64,
    /// Inode-local cache/read coherence only; never an ordering token for peers.
    pub data_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum UpperInode {
    Present(EntityInode),
    Deleted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntityDentry {
    Present { ino: i64, kind: EntityKind },
    Whiteout,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntityValue {
    Present(Vec<u8>),
    Whiteout,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChunkSource {
    Data {
        slice_id: u64,
        slice_offset: u64,
    },
    LowerRange {
        binding_digest: [u8; 32],
        ino: i64,
        file_offset: u64,
    },
    Hole,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChunkSpan {
    /// Offset relative to the beginning of this chunk.
    pub offset: u64,
    pub length: u64,
    pub source: ChunkSource,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChunkMap {
    pub chunk_size: u64,
    /// Complete normalized coverage of [0, chunk_size); holes are explicit.
    pub spans: Vec<ChunkSpan>,
}

fn corrupt(message: &str) -> EntityError {
    EntityError::Corrupt(message.into())
}

pub fn validate_name(name: &[u8]) -> Result<(), EntityError> {
    if name.is_empty()
        || name.len() > MAX_NAME_BYTES
        || name == b"."
        || name == b".."
        || name.contains(&0)
        || name.contains(&b'/')
    {
        return Err(EntityError::Invalid("invalid entity name".into()));
    }
    Ok(())
}

pub fn validate_xattr_name(name: &[u8]) -> Result<(), EntityError> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.contains(&0) {
        return Err(EntityError::Invalid("invalid entity xattr name".into()));
    }
    Ok(())
}

impl LowerBinding {
    pub fn validate(&self) -> Result<(), EntityError> {
        if self.manifest_key.is_empty()
            || self.manifest_key.len() > 1024
            || self.root_ino <= 0
            || self.max_ino < self.root_ino
        {
            return Err(corrupt("invalid immutable lower identity"));
        }
        Ok(())
    }
}

impl EntityInode {
    pub fn validate(&self) -> Result<(), EntityError> {
        if self.ino <= 0
            || self.mode & !0o7777 != 0
            || self.lower_visible_size > self.size
            || self.parent_hint.is_some_and(|parent| parent <= 0)
            || (self.kind == EntityKind::Symlink) != self.symlink_target.is_some()
            || self.symlink_target.as_ref().is_some_and(|target| {
                target.is_empty() || target.len() > 4096 || target.contains(&0)
            })
            || (self.kind != EntityKind::File && self.lower_visible_size != 0)
        {
            return Err(corrupt("invalid current inode record"));
        }
        Ok(())
    }
}

impl EntityDentry {
    pub fn validate(&self) -> Result<(), EntityError> {
        if matches!(self, Self::Present { ino, .. } if *ino <= 0) {
            return Err(corrupt("invalid dentry inode"));
        }
        Ok(())
    }
}

impl EntityValue {
    pub fn validate(&self) -> Result<(), EntityError> {
        if matches!(self, Self::Present(value) if value.len() > MAX_XATTR_BYTES) {
            return Err(corrupt("entity xattr exceeds its byte bound"));
        }
        Ok(())
    }
}

impl ChunkSource {
    fn validate_length(&self, length: u64) -> Result<(), EntityError> {
        match self {
            Self::Data {
                slice_id,
                slice_offset,
            } => {
                if *slice_id == 0 || *slice_id > i64::MAX as u64 {
                    return Err(corrupt("slice ID exceeds the positive allocator range"));
                }
                slice_offset
                    .checked_add(length)
                    .ok_or(EntityError::Overflow)?;
            }
            Self::LowerRange {
                ino, file_offset, ..
            } => {
                if *ino <= 0 {
                    return Err(corrupt("invalid lower-range inode"));
                }
                file_offset
                    .checked_add(length)
                    .ok_or(EntityError::Overflow)?;
            }
            Self::Hole => {}
        }
        Ok(())
    }

    fn shifted(&self, distance: u64) -> Result<Self, EntityError> {
        Ok(match self {
            Self::Data {
                slice_id,
                slice_offset,
            } => Self::Data {
                slice_id: *slice_id,
                slice_offset: slice_offset
                    .checked_add(distance)
                    .ok_or(EntityError::Overflow)?,
            },
            Self::LowerRange {
                binding_digest,
                ino,
                file_offset,
            } => Self::LowerRange {
                binding_digest: *binding_digest,
                ino: *ino,
                file_offset: file_offset
                    .checked_add(distance)
                    .ok_or(EntityError::Overflow)?,
            },
            Self::Hole => Self::Hole,
        })
    }

    fn continues(&self, length: u64, next: &Self) -> bool {
        self.shifted(length).is_ok_and(|shifted| &shifted == next)
    }
}

impl ChunkSpan {
    pub fn end(&self) -> Result<u64, EntityError> {
        self.offset
            .checked_add(self.length)
            .ok_or(EntityError::Overflow)
    }
}

impl ChunkMap {
    pub fn new(chunk_size: u64, spans: Vec<ChunkSpan>) -> Result<Self, EntityError> {
        let map = Self { chunk_size, spans };
        map.validate_shape(false)?;
        let result = Self {
            chunk_size,
            spans: Self::normalize(map.spans)?,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn holes(chunk_size: u64) -> Result<Self, EntityError> {
        Self::new(
            chunk_size,
            vec![ChunkSpan {
                offset: 0,
                length: chunk_size,
                source: ChunkSource::Hole,
            }],
        )
    }

    /// Materialize fallback metadata, never payload. The immutable adapter may
    /// refine LowerRange into its own sparse read plan when that range is read.
    pub fn inherited(
        chunk_size: u64,
        binding: &LowerBinding,
        ino: i64,
        chunk_start: u64,
        lower_visible_size: u64,
    ) -> Result<Self, EntityError> {
        binding.validate()?;
        if ino <= 0 || ino > binding.max_ino {
            return Err(corrupt("inherited inode outside the immutable lower"));
        }
        chunk_start
            .checked_add(chunk_size)
            .ok_or(EntityError::Overflow)?;
        let inherited = lower_visible_size
            .saturating_sub(chunk_start)
            .min(chunk_size);
        let mut spans = Vec::with_capacity(2);
        if inherited != 0 {
            spans.push(ChunkSpan {
                offset: 0,
                length: inherited,
                source: ChunkSource::LowerRange {
                    binding_digest: binding.manifest_digest,
                    ino,
                    file_offset: chunk_start,
                },
            });
        }
        if inherited < chunk_size {
            spans.push(ChunkSpan {
                offset: inherited,
                length: chunk_size - inherited,
                source: ChunkSource::Hole,
            });
        }
        Self::new(chunk_size, spans)
    }

    pub fn validate(&self) -> Result<(), EntityError> {
        self.validate_shape(true)
    }

    fn validate_shape(&self, normalized: bool) -> Result<(), EntityError> {
        if self.chunk_size == 0 || self.spans.is_empty() || self.spans.len() > MAX_CHUNK_SPANS {
            return Err(corrupt("invalid chunk size or span count"));
        }
        let mut end = 0;
        let mut previous: Option<&ChunkSpan> = None;
        for span in &self.spans {
            if span.offset != end || span.length == 0 {
                return Err(corrupt("chunk map contains a gap, overlap or empty span"));
            }
            span.source.validate_length(span.length)?;
            end = span.end()?;
            if end > self.chunk_size {
                return Err(corrupt("chunk span exceeds its chunk"));
            }
            if normalized
                && previous.is_some_and(|last| last.source.continues(last.length, &span.source))
            {
                return Err(corrupt("chunk map is not normalized"));
            }
            previous = Some(span);
        }
        if end != self.chunk_size {
            return Err(corrupt("chunk map is not complete"));
        }
        Ok(())
    }

    pub fn validate_binding(&self, binding: &LowerBinding) -> Result<(), EntityError> {
        binding.validate()?;
        self.validate()?;
        for span in &self.spans {
            if let ChunkSource::LowerRange {
                binding_digest,
                ino,
                ..
            } = &span.source
                && (*binding_digest != binding.manifest_digest || *ino > binding.max_ino)
            {
                return Err(corrupt("chunk map refers to another immutable lower"));
            }
        }
        Ok(())
    }

    fn normalize(spans: Vec<ChunkSpan>) -> Result<Vec<ChunkSpan>, EntityError> {
        let mut normalized: Vec<ChunkSpan> = Vec::with_capacity(spans.len());
        for span in spans {
            if let Some(last) = normalized.last_mut()
                && last.end()? == span.offset
                && last.source.continues(last.length, &span.source)
            {
                last.length = last
                    .length
                    .checked_add(span.length)
                    .ok_or(EntityError::Overflow)?;
            } else {
                normalized.push(span);
            }
        }
        Ok(normalized)
    }

    /// Replace a spatial interval, discarding the overwritten history. The
    /// original map remains intact on every validation/fragmentation error.
    pub fn replace(
        &self,
        offset: u64,
        length: u64,
        source: ChunkSource,
    ) -> Result<Self, EntityError> {
        self.validate()?;
        let end = offset.checked_add(length).ok_or(EntityError::Overflow)?;
        if length == 0 || end > self.chunk_size {
            return Err(EntityError::Invalid(
                "replacement is outside the chunk".into(),
            ));
        }
        source.validate_length(length)?;
        let mut spans = self.clip(0, offset)?;
        spans.push(ChunkSpan {
            offset,
            length,
            source,
        });
        spans.extend(self.clip(end, self.chunk_size - end)?);
        let next = Self {
            chunk_size: self.chunk_size,
            spans: Self::normalize(spans)?,
        };
        next.validate()?;
        Ok(next)
    }

    /// Return only requested spans, retaining chunk-relative logical offsets.
    /// Source offsets advance with clipping; empty reads have no spans.
    pub fn clip(&self, offset: u64, length: u64) -> Result<Vec<ChunkSpan>, EntityError> {
        self.validate()?;
        let end = offset.checked_add(length).ok_or(EntityError::Overflow)?;
        if end > self.chunk_size {
            return Err(EntityError::Invalid(
                "read range is outside the chunk".into(),
            ));
        }
        let mut spans = Vec::new();
        for span in &self.spans {
            let start = offset.max(span.offset);
            let stop = end.min(span.end()?);
            if start < stop {
                spans.push(ChunkSpan {
                    offset: start,
                    length: stop - start,
                    source: span.source.shifted(start - span.offset)?,
                });
            }
        }
        Ok(spans)
    }

    /// One ownership edge per distinct (chunk, slice), independent of spans
    /// and inode hardlinks. LowerRange ownership belongs to the lower pin.
    pub fn upper_slices(&self) -> Result<BTreeSet<u64>, EntityError> {
        self.validate()?;
        Ok(self
            .spans
            .iter()
            .filter_map(|span| match span.source {
                ChunkSource::Data { slice_id, .. } => Some(slice_id),
                _ => None,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> LowerBinding {
        LowerBinding {
            manifest_key: b"packed/manifest".to_vec(),
            manifest_digest: [7; 32],
            root_ino: 1,
            max_ino: 10,
        }
    }

    fn data(slice_id: u64, slice_offset: u64) -> ChunkSource {
        ChunkSource::Data {
            slice_id,
            slice_offset,
        }
    }

    #[test]
    fn chunk_overwrite_keeps_current_ranges_and_adjusts_source_offsets() {
        let map = ChunkMap::holes(16)
            .unwrap()
            .replace(0, 16, data(10, 20))
            .unwrap()
            .replace(4, 8, data(11, 0))
            .unwrap()
            .replace(6, 2, ChunkSource::Hole)
            .unwrap();
        assert_eq!(
            map.spans,
            vec![
                ChunkSpan {
                    offset: 0,
                    length: 4,
                    source: data(10, 20)
                },
                ChunkSpan {
                    offset: 4,
                    length: 2,
                    source: data(11, 0)
                },
                ChunkSpan {
                    offset: 6,
                    length: 2,
                    source: ChunkSource::Hole
                },
                ChunkSpan {
                    offset: 8,
                    length: 4,
                    source: data(11, 4)
                },
                ChunkSpan {
                    offset: 12,
                    length: 4,
                    source: data(10, 32)
                },
            ]
        );
        assert_eq!(map.upper_slices().unwrap(), BTreeSet::from([10, 11]));
        assert_eq!(
            map.clip(3, 10).unwrap(),
            vec![
                ChunkSpan {
                    offset: 3,
                    length: 1,
                    source: data(10, 23)
                },
                ChunkSpan {
                    offset: 4,
                    length: 2,
                    source: data(11, 0)
                },
                ChunkSpan {
                    offset: 6,
                    length: 2,
                    source: ChunkSource::Hole
                },
                ChunkSpan {
                    offset: 8,
                    length: 4,
                    source: data(11, 4)
                },
                ChunkSpan {
                    offset: 12,
                    length: 1,
                    source: data(10, 32)
                },
            ]
        );
    }

    #[test]
    fn compatible_neighbors_coalesce_but_same_slice_noncontiguous_offsets_do_not() {
        let map = ChunkMap::holes(16)
            .unwrap()
            .replace(0, 4, data(9, 0))
            .unwrap()
            .replace(4, 4, data(9, 4))
            .unwrap()
            .replace(8, 4, data(9, 20))
            .unwrap();
        assert_eq!(map.spans.len(), 3);
        assert_eq!(
            map.spans[0],
            ChunkSpan {
                offset: 0,
                length: 8,
                source: data(9, 0)
            }
        );
        assert_eq!(map.upper_slices().unwrap(), BTreeSet::from([9]));
        let cleared = map.replace(0, 12, ChunkSource::Hole).unwrap();
        assert_eq!(cleared, ChunkMap::holes(16).unwrap());
        assert!(cleared.upper_slices().unwrap().is_empty());
    }

    #[test]
    fn inherited_map_materializes_lower_boundary_and_zero_tail() {
        let lower = binding();
        let map = ChunkMap::inherited(16, &lower, 2, 16, 23).unwrap();
        assert_eq!(
            map.spans,
            vec![
                ChunkSpan {
                    offset: 0,
                    length: 7,
                    source: ChunkSource::LowerRange {
                        binding_digest: lower.manifest_digest,
                        ino: 2,
                        file_offset: 16,
                    }
                },
                ChunkSpan {
                    offset: 7,
                    length: 9,
                    source: ChunkSource::Hole
                },
            ]
        );
        assert_eq!(
            ChunkMap::inherited(16, &lower, 2, 32, 23).unwrap(),
            ChunkMap::holes(16).unwrap()
        );
        let changed = map.replace(3, 2, data(20, 0)).unwrap();
        assert_eq!(
            changed.spans[2],
            ChunkSpan {
                offset: 5,
                length: 2,
                source: ChunkSource::LowerRange {
                    binding_digest: lower.manifest_digest,
                    ino: 2,
                    file_offset: 21,
                }
            }
        );
        let mut other = lower.clone();
        other.manifest_digest = [8; 32];
        assert!(changed.validate_binding(&other).is_err());
    }

    #[test]
    fn malformed_maps_never_fall_through_or_silently_drop_ranges() {
        for spans in [
            vec![],
            vec![ChunkSpan {
                offset: 1,
                length: 15,
                source: ChunkSource::Hole,
            }],
            vec![ChunkSpan {
                offset: 0,
                length: 15,
                source: ChunkSource::Hole,
            }],
            vec![ChunkSpan {
                offset: 0,
                length: 17,
                source: ChunkSource::Hole,
            }],
            vec![ChunkSpan {
                offset: 0,
                length: 0,
                source: ChunkSource::Hole,
            }],
            vec![ChunkSpan {
                offset: 0,
                length: 16,
                source: data(0, 0),
            }],
            vec![ChunkSpan {
                offset: 0,
                length: 16,
                source: data(1, u64::MAX - 1),
            }],
            vec![
                ChunkSpan {
                    offset: 0,
                    length: 8,
                    source: data(1, 0),
                },
                ChunkSpan {
                    offset: 7,
                    length: 9,
                    source: data(2, 0),
                },
            ],
        ] {
            assert!(ChunkMap::new(16, spans).is_err());
        }
        let map = ChunkMap::holes(16).unwrap();
        assert!(map.replace(15, 2, data(1, 0)).is_err());
        assert!(map.replace(u64::MAX, 2, data(1, 0)).is_err());
        assert!(map.replace(0, 0, data(1, 0)).is_err());
        assert_eq!(map, ChunkMap::holes(16).unwrap());
    }

    #[test]
    fn fragmentation_limit_rejects_without_changing_the_previous_map() {
        let spans = (0..MAX_CHUNK_SPANS)
            .map(|index| ChunkSpan {
                offset: index as u64 * 2,
                length: 2,
                source: data(index as u64 + 1, 0),
            })
            .collect();
        let map = ChunkMap::new(MAX_CHUNK_SPANS as u64 * 2, spans).unwrap();
        assert!(map.replace(1, 1, ChunkSource::Hole).is_err());
        assert_eq!(map.spans.len(), MAX_CHUNK_SPANS);
        map.validate().unwrap();
    }

    #[test]
    fn raw_names_and_allocator_range_are_checked() {
        for name in [b"".as_slice(), b".", b"..", b"a/b", b"a\0b"] {
            assert!(validate_name(name).is_err());
        }
        assert!(validate_name(&[0xff, 0xfe]).is_ok());
        assert!(validate_name(&[b'a'; MAX_NAME_BYTES + 1]).is_err());
        assert!(
            ChunkMap::holes(1)
                .unwrap()
                .replace(0, 1, data(i64::MAX as u64, 0))
                .is_ok()
        );
        assert!(
            ChunkMap::holes(1)
                .unwrap()
                .replace(0, 1, data(i64::MAX as u64 + 1, 0))
                .is_err()
        );
    }
}
