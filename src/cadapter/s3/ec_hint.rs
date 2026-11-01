//! Deterministic policies for finalized S3 bytes, never logical file/part size.

use anyhow::{Result, anyhow};

const BLOCK_SIZES: [usize; 4] = [65_536, 262_144, 1_048_576, 4_194_304];
const POLICY_ERROR: &str = "RustFS EC block size hint must be off, auto, tiered, tiered:MAX=off,MAX=B,MAX=B,MAX=B,*=B, or 65536, 262144, 1048576, 4194304 bytes";

/// An exhaustive five-row table. Finite upper bounds are inclusive; the last
/// row covers every remaining length. Private fields keep SDK callers from
/// bypassing validation. Repeated B values support explicit sparse-read tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RustfsEcBlockSizeTiers {
    upper_bounds: [usize; 4],
    block_sizes: [usize; 4],
}

impl Default for RustfsEcBlockSizeTiers {
    fn default() -> Self {
        Self {
            upper_bounds: [262_144, 1_048_576, 4_194_304, 16_777_216],
            block_sizes: BLOCK_SIZES,
        }
    }
}

impl RustfsEcBlockSizeTiers {
    fn parse(value: &str) -> Result<Self, String> {
        let rows: Vec<_> = value.split(',').collect();
        if rows.len() != 5 {
            return Err("tiered requires five rows ending in *=B (complete size coverage)".into());
        }
        let mut table = Self::default();
        for (index, row) in rows.iter().enumerate() {
            let (bound, block) = row.split_once('=').ok_or(POLICY_ERROR)?;
            if index == 4 {
                if bound != "*" {
                    return Err(
                        "tiered final upper bound must be * (complete size coverage)".into(),
                    );
                }
            } else {
                table.upper_bounds[index] = decimal(bound)?;
                if table.upper_bounds[index] == usize::MAX
                    || (index > 0 && table.upper_bounds[index] <= table.upper_bounds[index - 1])
                {
                    return Err("tiered finite upper bounds must be strictly increasing and leave a final interval".into());
                }
            }
            if index == 0 {
                if block != "off" || table.upper_bounds[0] < 262_144 {
                    return Err("tiered first row must preserve at least 262144=off for default small-object admission".into());
                }
            } else {
                let size = decimal(block)?;
                if !BLOCK_SIZES.contains(&size) {
                    return Err(POLICY_ERROR.into());
                }
                table.block_sizes[index - 1] = size;
            }
        }
        Ok(table)
    }

    #[cfg(feature = "rustfs-ec-block-size-hint")]
    fn for_object(self, object_bytes: usize) -> Option<usize> {
        let row = self
            .upper_bounds
            .partition_point(|bound| object_bytes > *bound);
        row.checked_sub(1).map(|index| self.block_sizes[index])
    }
}

fn decimal(value: &str) -> Result<usize, String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(POLICY_ERROR.into());
    }
    value.parse().map_err(|_| POLICY_ERROR.into())
}

/// Optional per-object layout policy, evaluated after compression and framing.
/// `Auto` retains the historical cutoff; `Tiered` is an opt-in candidate, not a
/// performance guarantee. See `doc/testing/rustfs-tiered-object-hint.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RustfsEcBlockSizeHint {
    Off,
    Auto,
    Fixed(usize),
    Tiered(RustfsEcBlockSizeTiers),
}

impl std::str::FromStr for RustfsEcBlockSizeHint {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "auto" => Ok(Self::Auto),
            "tiered" => Ok(Self::Tiered(RustfsEcBlockSizeTiers::default())),
            _ if value.starts_with("tiered:") => {
                RustfsEcBlockSizeTiers::parse(&value[7..]).map(Self::Tiered)
            }
            _ => decimal(value).and_then(|size| {
                if BLOCK_SIZES.contains(&size) {
                    Ok(Self::Fixed(size))
                } else {
                    Err(POLICY_ERROR.into())
                }
            }),
        }
    }
}

impl std::fmt::Display for RustfsEcBlockSizeHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Off => f.write_str("off"),
            Self::Auto => f.write_str("auto"),
            Self::Fixed(size) => write!(f, "{size}"),
            Self::Tiered(table) => write!(
                f,
                "tiered:{}=off,{}={},{}={},{}={},*={}",
                table.upper_bounds[0],
                table.upper_bounds[1],
                table.block_sizes[0],
                table.upper_bounds[2],
                table.block_sizes[1],
                table.upper_bounds[3],
                table.block_sizes[2],
                table.block_sizes[3]
            ),
        }
    }
}

impl<'de> serde::Deserialize<'de> for RustfsEcBlockSizeHint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Input {
            Bytes(usize),
            Text(String),
        }
        match <Input as serde::Deserialize>::deserialize(deserializer)? {
            Input::Bytes(size) => Ok(Self::Fixed(size)),
            Input::Text(value) => value.parse().map_err(serde::de::Error::custom),
        }
    }
}

impl RustfsEcBlockSizeHint {
    pub(crate) fn enabled(self) -> bool {
        self != Self::Off
    }

    #[cfg(feature = "rustfs-ec-block-size-hint")]
    pub(super) fn for_object(self, object_bytes: usize) -> Option<usize> {
        match self {
            Self::Off => None,
            Self::Fixed(size) => Some(size),
            Self::Auto if object_bytes > 1_048_576 => Some(1_048_576),
            Self::Auto => None,
            Self::Tiered(table) => table.for_object(object_bytes),
        }
    }

    #[cfg(feature = "rustfs-ec-block-size-hint")]
    pub(super) fn version(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Auto => "auto-v1",
            Self::Fixed(_) => "fixed",
            Self::Tiered(_) => "tiered-v1",
        }
    }
}

pub(crate) fn validate_rustfs_ec_block_size_hint(
    hint: Option<RustfsEcBlockSizeHint>,
) -> Result<()> {
    #[cfg(not(feature = "rustfs-ec-block-size-hint"))]
    if hint.is_some_and(RustfsEcBlockSizeHint::enabled) {
        anyhow::bail!("feature not compiled: rustfs-ec-block-size-hint");
    }
    if let Some(RustfsEcBlockSizeHint::Fixed(size)) = hint
        && !BLOCK_SIZES.contains(&size)
    {
        return Err(anyhow!(POLICY_ERROR));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiered_configuration_rejects_gaps_bad_order_and_unsupported_blocks() {
        for value in [
            "tiered:",
            "tiered:262144=off,1048576=65536,4194304=262144,16777216=1048576",
            "tiered:262144=off,1048576=65536,4194304=262144,16777216=1048576,33554432=4194304",
            "tiered:262144=off,262144=65536,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262144=off,262143=65536,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262143=off,1048576=65536,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262144=65536,1048576=65536,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262144=off,*=65536,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262144=off,1048576=131072,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262144=off,1048576=off,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262144=off,1048576=65536,4194304=262144,16777216=1048576,*=4194304,*=65536",
            "tiered:+262144=off,1048576=65536,4194304=262144,16777216=1048576,*=4194304",
            "tiered:262144=off,1048576=65536,4194304=262144,18446744073709551616=1048576,*=4194304",
        ] {
            assert!(value.parse::<RustfsEcBlockSizeHint>().is_err(), "{value}");
        }
    }

    #[test]
    fn tiered_and_custom_tables_roundtrip_without_losing_parameters() {
        for value in [
            "tiered",
            "auto",
            "off",
            "65536",
            "tiered:524288=off,2097152=65536,8388608=65536,33554432=65536,*=65536",
        ] {
            let policy: RustfsEcBlockSizeHint = value.parse().unwrap();
            assert_eq!(policy, policy.to_string().parse().unwrap());
        }
        assert!(validate_rustfs_ec_block_size_hint(Some(RustfsEcBlockSizeHint::Off)).is_ok());
    }

    #[cfg(feature = "rustfs-ec-block-size-hint")]
    #[test]
    fn custom_sparse_table_covers_boundaries_and_large_objects() {
        let policy: RustfsEcBlockSizeHint =
            "tiered:524288=off,2097152=65536,8388608=65536,33554432=65536,*=65536"
                .parse()
                .unwrap();
        for length in [0, 524_287, 524_288] {
            assert_eq!(policy.for_object(length), None);
        }
        for boundary in [524_289, 2_097_152, 8_388_608, 33_554_432, usize::MAX - 1] {
            for length in [boundary, boundary + 1] {
                assert_eq!(policy.for_object(length), Some(65_536));
            }
        }
    }
}
