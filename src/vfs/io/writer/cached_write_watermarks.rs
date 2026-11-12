//! Per-byte cached-write ordering. Retained across flush: a late FUSE cache
//! write must not replace a newer page that has already been committed.
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Watermark {
    end: u64,
    unique: u64,
}

/// Disjoint, nonempty ranges keyed by start. Adjacent equal watermarks coalesce.
/// Updating K intersecting ranges costs O((K + 1) log N); disjoint small writes
/// do not copy or scan the accumulated history.
#[derive(Default)]
pub(super) struct CachedWriteWatermarks {
    ranges: BTreeMap<u64, Watermark>,
    #[cfg(test)]
    examined: usize,
}

impl CachedWriteWatermarks {
    pub(super) fn record_and_allow(
        &mut self,
        start: u64,
        end: u64,
        unique: u64,
    ) -> Vec<(u64, u64)> {
        if start >= end {
            return Vec::new();
        }
        if unique == 0 {
            return vec![(start, end)];
        }

        // Only the predecessor can overlap start from the left. All other
        // intersecting ranges start inside [start, end).
        let predecessor = self.ranges.range(..=start).next_back();
        #[cfg(test)]
        {
            self.examined += usize::from(predecessor.is_some());
        }
        let first = predecessor
            .filter(|(_, range)| range.end > start)
            .map_or(start, |(&offset, _)| offset);
        let overlapping: Vec<_> = self
            .ranges
            .range(first..end)
            .map(|(&offset, &range)| (offset, range))
            .collect();
        #[cfg(test)]
        {
            self.examined += overlapping.len();
        }

        let mut allowed = Vec::new();
        let mut replacement = Vec::new();
        let mut cursor = start;
        for &(offset, old) in &overlapping {
            if offset < start {
                push_watermark(&mut replacement, offset, start, old.unique);
            }
            let overlap_start = offset.max(start);
            let overlap_end = old.end.min(end);
            if cursor < overlap_start {
                push_allowed(&mut allowed, cursor, overlap_start);
                push_watermark(&mut replacement, cursor, overlap_start, unique);
            }
            if old.unique <= unique {
                push_allowed(&mut allowed, overlap_start, overlap_end);
            }
            push_watermark(
                &mut replacement,
                overlap_start,
                overlap_end,
                unique.max(old.unique),
            );
            if old.end > end {
                push_watermark(&mut replacement, end, old.end, old.unique);
            }
            cursor = overlap_end;
        }
        if cursor < end {
            push_allowed(&mut allowed, cursor, end);
            push_watermark(&mut replacement, cursor, end, unique);
        }

        for (offset, _) in overlapping {
            self.ranges.remove(&offset);
        }
        for (offset, range) in replacement {
            self.insert_coalesced(offset, range);
        }
        allowed
    }

    fn insert_coalesced(&mut self, mut start: u64, mut range: Watermark) {
        let left = self.ranges.range(..start).next_back();
        #[cfg(test)]
        {
            self.examined += usize::from(left.is_some());
        }
        if let Some((&offset, &old)) = left {
            debug_assert!(old.end <= start);
            if old.end == start && old.unique == range.unique {
                self.ranges.remove(&offset);
                start = offset;
            }
        }
        let right = self.ranges.get(&range.end).copied();
        #[cfg(test)]
        {
            self.examined += usize::from(right.is_some());
        }
        if let Some(old) = right
            && old.unique == range.unique
        {
            self.ranges.remove(&range.end);
            range.end = old.end;
        }
        self.ranges.insert(start, range);
    }

    #[cfg(test)]
    pub(super) fn examined_ranges(&self) -> usize {
        self.examined
    }
}

fn push_allowed(allowed: &mut Vec<(u64, u64)>, start: u64, end: u64) {
    if let Some(last) = allowed.last_mut()
        && last.1 == start
    {
        last.1 = end;
    } else {
        allowed.push((start, end));
    }
}

fn push_watermark(ranges: &mut Vec<(u64, Watermark)>, start: u64, end: u64, unique: u64) {
    if let Some((_, last)) = ranges.last_mut()
        && last.end == start
        && last.unique == unique
    {
        last.end = end;
    } else {
        ranges.push((start, Watermark { end, unique }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_oracle(
        index: &mut CachedWriteWatermarks,
        bytes: &mut [u64],
        start: usize,
        end: usize,
        unique: u64,
    ) {
        let actual = index.record_and_allow(start as u64, end as u64, unique);
        let mut expected = Vec::new();
        for (offset, old) in bytes.iter_mut().enumerate().take(end).skip(start) {
            if unique == 0 || unique >= *old {
                push_allowed(&mut expected, offset as u64, offset as u64 + 1);
            }
            if unique != 0 {
                *old = (*old).max(unique);
            }
        }
        assert_eq!(actual, expected);
        let mut observed = vec![0; bytes.len()];
        let mut previous: Option<(u64, Watermark)> = None;
        for (&offset, &range) in &index.ranges {
            assert!(offset < range.end);
            if let Some((_, old)) = previous {
                assert!(old.end <= offset);
                assert!(old.end != offset || old.unique != range.unique);
            }
            observed[offset as usize..range.end as usize].fill(range.unique);
            previous = Some((offset, range));
        }
        assert_eq!(observed, bytes);
    }

    #[test]
    fn cached_watermarks_match_byte_oracle_for_overlaps_gaps_and_zero_unique() {
        let mut index = CachedWriteWatermarks::default();
        let mut bytes = [0; 128];
        for (start, end, unique) in [
            (16, 32, 4),
            (48, 64, 8),
            (0, 80, 6),
            (20, 60, 2),
            (24, 56, 9),
            (0, 128, 0),
            (0, 128, 9),
            (8, 100, 10),
            (100, 128, 10),
            (0, 8, 10),
            (32, 32, 99),
        ] {
            check_oracle(&mut index, &mut bytes, start, end, unique);
        }
        assert_eq!(index.ranges.len(), 1);
        let mut seed = 0x7e579aa7d1u64;
        for step in 0..5000u64 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let start = (seed % 128) as usize;
            let len = ((seed >> 16) % (129 - start) as u64) as usize;
            let unique = match seed & 3 {
                0 => 0,
                1 => (seed >> 32) % (step + 3),
                _ => step + 1,
            };
            check_oracle(&mut index, &mut bytes, start, start + len, unique);
        }
    }

    #[test]
    fn cached_watermarks_disjoint_writes_do_not_rescan_history() {
        for count in [128, 1024, 4096] {
            for reverse in [false, true] {
                let mut index = CachedWriteWatermarks::default();
                for step in 0..count {
                    let i = if reverse { count - step - 1 } else { step };
                    let start = i as u64 * 4096;
                    assert_eq!(
                        index.record_and_allow(start, start + 4096, step as u64 + 1),
                        vec![(start, start + 4096)]
                    );
                }
                assert_eq!(index.ranges.len(), count);
                assert!(
                    index.examined_ranges() <= 3 * count,
                    "{} visits for {count} writes",
                    index.examined_ranges()
                );
            }
        }
    }

    #[test]
    fn cached_watermarks_tiny_overwrite_only_visits_local_history() {
        let mut index = CachedWriteWatermarks::default();
        for i in 0..4096 {
            index.record_and_allow(i * 4096, (i + 1) * 4096, i + 1);
        }
        let before = index.examined_ranges();
        assert!(
            index
                .record_and_allow(2048 * 4096 + 1, 2048 * 4096 + 2, 1)
                .is_empty()
        );
        assert!(index.examined_ranges() - before <= 6);
        let before = index.examined_ranges();
        assert_eq!(
            index.record_and_allow(2048 * 4096 + 1, 2048 * 4096 + 2, 5000),
            vec![(2048 * 4096 + 1, 2048 * 4096 + 2)]
        );
        assert!(index.examined_ranges() - before <= 10);
    }
}
