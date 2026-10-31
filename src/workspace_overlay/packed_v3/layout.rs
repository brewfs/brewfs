use thiserror::Error;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

pub const DEFAULT_MIN_FRAME_RAW_BYTES: u64 = 256 * KIB;
pub const DEFAULT_MAX_RANDOM_FRAME_RAW_BYTES: u64 = 256 * KIB;
pub const DEFAULT_MAX_SEQUENTIAL_FRAME_RAW_BYTES: u64 = 8 * MIB;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum AccessProfile {
    RandomSmallFile = 0,
    SequentialSmallFile = 1,
    Mixed = 2,
}

impl AccessProfile {
    pub fn from_u8(value: u8) -> Result<Self, LayoutError> {
        match value {
            0 => Ok(Self::RandomSmallFile),
            1 => Ok(Self::SequentialSmallFile),
            2 => Ok(Self::Mixed),
            other => Err(LayoutError::InvalidProfile(other)),
        }
    }

    fn max_frame_raw_bytes(self, table: &SizeClassTable) -> u64 {
        match self {
            Self::RandomSmallFile | Self::Mixed => table.max_random_frame_raw_bytes,
            Self::SequentialSmallFile => table.max_sequential_frame_raw_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum SizeClass {
    Tiny = 0,
    Small = 1,
    Medium = 2,
    Large = 3,
}

impl SizeClass {
    pub fn from_u8(value: u8) -> Result<Self, LayoutError> {
        match value {
            0 => Ok(Self::Tiny),
            1 => Ok(Self::Small),
            2 => Ok(Self::Medium),
            3 => Ok(Self::Large),
            other => Err(LayoutError::InvalidSizeClass(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SizeClassTable {
    pub min_frame_raw_bytes: u64,
    pub max_random_frame_raw_bytes: u64,
    pub max_sequential_frame_raw_bytes: u64,
}

impl Default for SizeClassTable {
    fn default() -> Self {
        Self {
            min_frame_raw_bytes: DEFAULT_MIN_FRAME_RAW_BYTES,
            max_random_frame_raw_bytes: DEFAULT_MAX_RANDOM_FRAME_RAW_BYTES,
            max_sequential_frame_raw_bytes: DEFAULT_MAX_SEQUENTIAL_FRAME_RAW_BYTES,
        }
    }
}

impl SizeClassTable {
    pub fn validate(self) -> Result<Self, LayoutError> {
        if self.min_frame_raw_bytes == 0
            || self.max_random_frame_raw_bytes < self.min_frame_raw_bytes
            || self.max_sequential_frame_raw_bytes < self.min_frame_raw_bytes
            || !self.min_frame_raw_bytes.is_power_of_two()
            || !self.max_random_frame_raw_bytes.is_power_of_two()
            || !self.max_sequential_frame_raw_bytes.is_power_of_two()
        {
            return Err(LayoutError::InvalidSizeClassTable);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLayoutDecision {
    pub profile: AccessProfile,
    pub size_class: SizeClass,
    pub frame_raw_bytes: u64,
    pub frame_count: u64,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LayoutError {
    #[error("invalid access profile {0}")]
    InvalidProfile(u8),
    #[error("invalid size class {0}")]
    InvalidSizeClass(u8),
    #[error("invalid size class table")]
    InvalidSizeClassTable,
    #[error("file size is too large")]
    FileSizeOverflow,
}

pub fn choose_frame_layout(
    file_size: u64,
    p90_requested_range: Option<u64>,
    profile: AccessProfile,
    table: SizeClassTable,
) -> Result<FrameLayoutDecision, LayoutError> {
    let table = table.validate()?;
    let size_class = if file_size <= 256 * KIB {
        SizeClass::Tiny
    } else if file_size <= MIB {
        SizeClass::Small
    } else if file_size <= 16 * MIB {
        SizeClass::Medium
    } else {
        SizeClass::Large
    };

    if file_size == 0 {
        return Ok(FrameLayoutDecision {
            profile,
            size_class,
            frame_raw_bytes: 0,
            frame_count: 0,
        });
    }

    let access_target = p90_requested_range
        .unwrap_or(file_size)
        .max(table.min_frame_raw_bytes)
        .min(file_size);
    let max_frame = profile.max_frame_raw_bytes(&table);
    let frame_raw_bytes = round_up_pow2(access_target, table.min_frame_raw_bytes, max_frame)?;
    let frame_count = file_size
        .checked_add(frame_raw_bytes - 1)
        .ok_or(LayoutError::FileSizeOverflow)?
        / frame_raw_bytes;
    Ok(FrameLayoutDecision {
        profile,
        size_class,
        frame_raw_bytes,
        frame_count,
    })
}

fn round_up_pow2(value: u64, min_value: u64, max_value: u64) -> Result<u64, LayoutError> {
    if value == 0 || min_value == 0 || max_value < min_value {
        return Err(LayoutError::InvalidSizeClassTable);
    }
    let mut result = min_value;
    while result < value {
        result = result.checked_mul(2).ok_or(LayoutError::FileSizeOverflow)?;
        if result >= max_value {
            return Ok(max_value);
        }
    }
    Ok(result.min(max_value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_only_selection_matches_v3_profiles() {
        let table = SizeClassTable {
            max_random_frame_raw_bytes: 4 * MIB,
            ..SizeClassTable::default()
        };
        let tiny =
            choose_frame_layout(200 * KIB, None, AccessProfile::RandomSmallFile, table).unwrap();
        assert_eq!(tiny.size_class, SizeClass::Tiny);
        assert_eq!(tiny.frame_raw_bytes, 256 * KIB);
        assert_eq!(tiny.frame_count, 1);

        let medium =
            choose_frame_layout(10 * MIB, None, AccessProfile::RandomSmallFile, table).unwrap();
        assert_eq!(medium.size_class, SizeClass::Medium);
        assert_eq!(medium.frame_raw_bytes, 4 * MIB);
        assert_eq!(medium.frame_count, 3);

        let sequential =
            choose_frame_layout(10 * MIB, None, AccessProfile::SequentialSmallFile, table).unwrap();
        assert_eq!(sequential.frame_raw_bytes, 8 * MIB);
        assert_eq!(sequential.frame_count, 2);
    }

    #[test]
    fn default_random_and_mixed_frames_are_256_kib() {
        let table = SizeClassTable::default();
        assert_eq!(table.min_frame_raw_bytes, 256 * KIB);
        assert_eq!(table.max_random_frame_raw_bytes, 256 * KIB);
        assert_eq!(table.max_sequential_frame_raw_bytes, 8 * MIB);
        for profile in [AccessProfile::RandomSmallFile, AccessProfile::Mixed] {
            let decision = choose_frame_layout(10 * MIB, None, profile, table).unwrap();
            assert_eq!(decision.frame_raw_bytes, 256 * KIB);
            assert_eq!(decision.frame_count, 40);
        }
    }

    #[test]
    fn access_histogram_can_keep_large_file_frames_small() {
        let decision = choose_frame_layout(
            10 * MIB,
            Some(200 * KIB),
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
        )
        .unwrap();
        assert_eq!(decision.size_class, SizeClass::Medium);
        assert_eq!(decision.frame_raw_bytes, 256 * KIB);
        assert_eq!(decision.frame_count, 40);
    }

    #[test]
    fn zero_length_files_have_no_physical_frame() {
        let decision =
            choose_frame_layout(0, None, AccessProfile::Mixed, SizeClassTable::default()).unwrap();
        assert_eq!(decision.frame_raw_bytes, 0);
        assert_eq!(decision.frame_count, 0);
    }
}
