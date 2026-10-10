//! Build-time layout policy and observed distributions authenticated by PM11.
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError, Reader, Writer};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, FrameLayoutDecision, PackedCodec, PackedFrameDescriptor, SizeClassTable,
    choose_frame_layout,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
#[repr(u8)]
pub enum V3FramePolicy {
    #[default]
    SizeOnly = 0,
    #[serde(rename = "static-256kib")]
    Static256Kib = 1,
    #[serde(rename = "static-1mib")]
    Static1Mib = 2,
    #[serde(rename = "static-4mib")]
    Static4Mib = 3,
    /// Select frame targets from an authenticated offline request histogram.
    #[serde(rename = "p90-training")]
    P90Training = 4,
}

impl V3FramePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SizeOnly => "size-only",
            Self::Static256Kib => "static-256kib",
            Self::Static1Mib => "static-1mib",
            Self::Static4Mib => "static-4mib",
            Self::P90Training => "p90-training",
        }
    }
    pub(super) fn target(self) -> Option<u64> {
        match self {
            Self::SizeOnly => None,
            Self::Static256Kib => Some(256 * 1024),
            Self::Static1Mib => Some(1024 * 1024),
            Self::Static4Mib => Some(4 * 1024 * 1024),
            Self::P90Training => None,
        }
    }
    pub(super) fn from_u8(value: u8) -> PackedResult<Self> {
        match value {
            0 => Ok(Self::SizeOnly),
            1 => Ok(Self::Static256Kib),
            2 => Ok(Self::Static1Mib),
            3 => Ok(Self::Static4Mib),
            4 => Ok(Self::P90Training),
            _ => Err(invalid("unknown frame policy")),
        }
    }
}

/// Authenticated inputs used by the p90 frame selector. The complete
/// training artifact remains in the runner manifest; these digests and the
/// selected rank are carried in BP12 so a reader cannot silently substitute a
/// hint at mount time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct V3P90Policy {
    pub trace_digest: [u8; 32],
    pub policy_digest: [u8; 32],
    pub histogram_digest: [u8; 32],
    pub sample_count: u64,
    pub requested_range: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct V3BuildPolicy {
    pub frames: V3FramePolicy,
    pub inline_data: bool,
    pub p90: Option<V3P90Policy>,
}

impl Default for V3BuildPolicy {
    fn default() -> Self {
        Self {
            frames: V3FramePolicy::SizeOnly,
            inline_data: true,
            p90: None,
        }
    }
}

impl V3BuildPolicy {
    pub fn select(
        self,
        size: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
    ) -> PackedResult<FrameLayoutDecision> {
        self.validate()?;
        let p90 = self.p90.map(|policy| policy.requested_range);
        let mut decision = choose_frame_layout(size, p90, profile, classes)
            .map_err(|error| invalid(&error.to_string()))?;
        if let Some(target) = self.frames.target() {
            let cap = match profile {
                AccessProfile::RandomSmallFile | AccessProfile::Mixed => {
                    classes.max_random_frame_raw_bytes
                }
                AccessProfile::SequentialSmallFile => classes.max_sequential_frame_raw_bytes,
            };
            if target > cap {
                return Err(invalid("static target exceeds published profile cap"));
            }
            if size != 0 {
                decision.frame_raw_bytes = target;
                decision.frame_count = size.div_ceil(target);
            }
        }
        Ok(decision)
    }

    fn validate(self) -> PackedResult<()> {
        match (self.frames, self.p90) {
            (V3FramePolicy::P90Training, Some(policy)) => {
                if policy.requested_range == 0 || policy.sample_count == 0 {
                    return Err(invalid("p90 policy has an empty sample/range"));
                }
                if policy.trace_digest == [0; 32]
                    || policy.policy_digest == [0; 32]
                    || policy.histogram_digest == [0; 32]
                {
                    return Err(invalid("p90 policy digest is empty"));
                }
            }
            (V3FramePolicy::P90Training, None) => {
                return Err(invalid(
                    "p90 frame policy requires authenticated provenance",
                ));
            }
            (_, Some(_)) => return Err(invalid("p90 provenance requires p90 frame policy")),
            (_, None) => {}
        }
        Ok(())
    }
}

fn invalid(message: &str) -> PackedWireError {
    PackedWireError::Invalid(format!("PM11 build policy {message}"))
}
fn add(target: &mut u64, amount: u64) -> PackedResult<()> {
    *target = target
        .checked_add(amount)
        .ok_or_else(|| invalid("counter overflows"))?;
    Ok(())
}
fn sum(counts: &[u64]) -> PackedResult<u64> {
    counts.iter().try_fold(0u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| invalid("total overflows"))
    })
}

/// Frame lengths describe actual raw bytes, including sparse-run/file tails,
/// never padded targets. Inline totals count dentry copies in GroupMeta; they
/// are distinct from unique inode/logical bytes and independent frame bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct V3BuildProvenance {
    pub policy: V3BuildPolicy,
    /// GroupMeta block codec; index/control/cold objects have their own wire.
    pub requested_metadata_codec: u8,
    pub requested_data_codec: u8,
    pub frame_count: u64,
    pub external_frame_count: u64,
    pub frame_raw_bytes: u64,
    pub frame_stored_bytes: u64,
    /// <=256 KiB, (256 KiB,1 MiB], (1 MiB,4 MiB], (4 MiB,8 MiB].
    pub frame_raw_size_counts: [u64; 4],
    /// Actual size_class labels; file class remains independent of raw length.
    pub frame_class_counts: [u64; 4],
    /// Actual descriptor codecs: [raw,zstd], including expansion fallback.
    pub frame_codec_counts: [u64; 2],
    pub metadata_codec_counts: [u64; 2],
    pub inline_dentries: u64,
    pub inline_payload_bytes: u64,
    /// Authenticated p90 provenance, when the p90 selector was used.
    pub p90: Option<V3P90Policy>,
}

impl V3BuildProvenance {
    pub(super) fn observe_frame(
        &mut self,
        frame: &PackedFrameDescriptor,
        external: bool,
    ) -> PackedResult<()> {
        let raw = u64::from(frame.raw_len);
        if raw == 0
            || raw > 8 * 1024 * 1024
            || self
                .policy
                .frames
                .target()
                .is_some_and(|target| raw > target)
        {
            return Err(invalid("actual frame exceeds selected target"));
        }
        let codec = PackedCodec::from_u8(frame.codec)? as usize;
        if self.requested_data_codec == PackedCodec::Raw as u8 && codec != 0 {
            return Err(invalid("raw policy produced compressed frame"));
        }
        let bucket = if raw <= 256 * 1024 {
            0
        } else if raw <= 1024 * 1024 {
            1
        } else if raw <= 4 * 1024 * 1024 {
            2
        } else {
            3
        };
        add(&mut self.frame_count, 1)?;
        if external {
            add(&mut self.external_frame_count, 1)?;
        }
        add(&mut self.frame_raw_bytes, raw)?;
        add(&mut self.frame_stored_bytes, u64::from(frame.stored_len))?;
        add(&mut self.frame_raw_size_counts[bucket], 1)?;
        add(&mut self.frame_class_counts[frame.size_class as usize], 1)?;
        add(&mut self.frame_codec_counts[codec], 1)
    }
    pub(super) fn observe_group(
        &mut self,
        group: &super::V3GroupRef,
        metadata: &crate::workspace_overlay::packed_v3::GroupMeta,
    ) -> PackedResult<()> {
        if self.requested_metadata_codec == PackedCodec::Raw as u8
            && group.meta_codec != PackedCodec::Raw
        {
            return Err(invalid("raw policy produced compressed metadata"));
        }
        add(
            &mut self.metadata_codec_counts[group.meta_codec as usize],
            1,
        )?;
        for entry in metadata.entries() {
            if !entry.inline_data.is_empty() {
                if !self.policy.inline_data {
                    return Err(invalid("inline-off input contains payload"));
                }
                add(&mut self.inline_dentries, 1)?;
                add(
                    &mut self.inline_payload_bytes,
                    entry.inline_data.len() as u64,
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn validate(&self) -> PackedResult<()> {
        self.policy.validate()?;
        if self.p90 != self.policy.p90 {
            return Err(invalid("p90 provenance disagrees with build policy"));
        }
        PackedCodec::from_u8(self.requested_metadata_codec)?;
        PackedCodec::from_u8(self.requested_data_codec)?;
        sum(&self.metadata_codec_counts)?;
        let bounds = [256 * 1024u64, 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024];
        let lower = [1u64, 256 * 1024 + 1, 1024 * 1024 + 1, 4 * 1024 * 1024 + 1];
        let mut minimum_raw = 0u64;
        let mut maximum_raw = 0u64;
        for ((count, minimum), maximum) in self.frame_raw_size_counts.iter().zip(lower).zip(bounds)
        {
            if self
                .policy
                .frames
                .target()
                .is_some_and(|target| target < minimum)
                && *count != 0
            {
                return Err(invalid("distribution exceeds fixed target"));
            }
            add(
                &mut minimum_raw,
                count
                    .checked_mul(minimum)
                    .ok_or_else(|| invalid("distribution bound overflows"))?,
            )?;
            add(
                &mut maximum_raw,
                count
                    .checked_mul(maximum)
                    .ok_or_else(|| invalid("distribution bound overflows"))?,
            )?;
        }
        if sum(&self.frame_raw_size_counts)? != self.frame_count
            || sum(&self.frame_class_counts)? != self.frame_count
            || sum(&self.frame_codec_counts)? != self.frame_count
            || self.external_frame_count > self.frame_count
            || (self.frame_count == 0) != (self.frame_raw_bytes == 0)
            || (self.frame_count == 0) != (self.frame_stored_bytes == 0)
            || (self.inline_dentries == 0) != (self.inline_payload_bytes == 0)
            || (!self.policy.inline_data
                && (self.inline_dentries != 0 || self.inline_payload_bytes != 0))
            || (self.requested_data_codec == 0 && self.frame_codec_counts[1] != 0)
            || (self.requested_metadata_codec == 0 && self.metadata_codec_counts[1] != 0)
            || self.frame_raw_bytes < minimum_raw
            || self.frame_raw_bytes > maximum_raw
            || self.frame_stored_bytes > self.frame_raw_bytes
            || self.inline_payload_bytes < self.inline_dentries
        {
            return Err(invalid("distribution totals disagree"));
        }
        Ok(())
    }
    pub(super) fn encode(&self, writer: &mut Writer) -> PackedResult<()> {
        self.validate()?;
        // Keep the fixed legacy BP11 payload byte-for-byte compatible.  A
        // p90 build opts into BP12, whose explicit extension authenticates
        // the frozen histogram provenance before any frame is consumed.
        writer.bytes(if self.p90.is_some() { b"BP12" } else { b"BP11" });
        writer.u8(self.policy.frames as u8);
        writer.u8(u8::from(self.policy.inline_data));
        writer.u8(self.requested_metadata_codec);
        writer.u8(self.requested_data_codec);
        for value in [
            self.frame_count,
            self.external_frame_count,
            self.frame_raw_bytes,
            self.frame_stored_bytes,
            self.inline_dentries,
            self.inline_payload_bytes,
        ] {
            writer.u64(value);
        }
        if self.p90.is_some() {
            writer.u8(1);
            if let Some(policy) = self.p90 {
                writer.bytes(&policy.trace_digest);
                writer.bytes(&policy.policy_digest);
                writer.bytes(&policy.histogram_digest);
                writer.u64(policy.sample_count);
                writer.u64(policy.requested_range);
            }
        }
        for values in [
            &self.frame_raw_size_counts[..],
            &self.frame_class_counts[..],
            &self.frame_codec_counts[..],
            &self.metadata_codec_counts[..],
        ] {
            for value in values {
                writer.u64(*value);
            }
        }
        Ok(())
    }
    pub(super) fn decode(reader: &mut Reader<'_>) -> PackedResult<Self> {
        let magic = reader.take(4)?;
        let has_p90_extension = match magic {
            b"BP11" => false,
            b"BP12" => true,
            _ => return Err(invalid("provenance version mismatch")),
        };
        let frames = V3FramePolicy::from_u8(reader.u8()?)?;
        let inline_data = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(invalid("inline policy is invalid")),
        };
        let requested_metadata_codec = reader.u8()?;
        let requested_data_codec = reader.u8()?;
        let mut result = Self {
            policy: V3BuildPolicy {
                frames,
                inline_data,
                p90: None,
            },
            requested_metadata_codec,
            requested_data_codec,
            frame_count: reader.u64()?,
            external_frame_count: reader.u64()?,
            frame_raw_bytes: reader.u64()?,
            frame_stored_bytes: reader.u64()?,
            inline_dentries: reader.u64()?,
            inline_payload_bytes: reader.u64()?,
            p90: None,
            ..Self::default()
        };
        let has_p90 = if has_p90_extension {
            match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(invalid("p90 provenance marker is invalid")),
            }
        } else {
            false
        };
        if has_p90 {
            result.p90 = Some(V3P90Policy {
                trace_digest: reader.array()?,
                policy_digest: reader.array()?,
                histogram_digest: reader.array()?,
                sample_count: reader.u64()?,
                requested_range: reader.u64()?,
            });
        }
        result.policy.p90 = result.p90;
        for values in [
            &mut result.frame_raw_size_counts[..],
            &mut result.frame_class_counts[..],
            &mut result.frame_codec_counts[..],
            &mut result.metadata_codec_counts[..],
        ] {
            for value in values {
                *value = reader.u64()?;
            }
        }
        result.validate()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn g15_static_target_preserves_file_class_and_actual_tail() {
        let policy = V3BuildPolicy {
            frames: V3FramePolicy::Static1Mib,
            inline_data: false,
            p90: None,
        };
        let classes = SizeClassTable::default();
        let selected = policy
            .select(2560 * 1024, AccessProfile::RandomSmallFile, classes)
            .unwrap();
        assert_eq!(selected.frame_raw_bytes, 1024 * 1024);
        assert_eq!(selected.frame_count, 3);
        assert_eq!(
            selected.size_class,
            crate::workspace_overlay::packed_v3::SizeClass::Medium
        );
        assert_eq!(
            V3BuildPolicy::default()
                .select(2560 * 1024, AccessProfile::RandomSmallFile, classes)
                .unwrap()
                .frame_raw_bytes,
            4 * 1024 * 1024
        );
        assert_eq!(
            policy
                .select(0, AccessProfile::RandomSmallFile, classes)
                .unwrap()
                .frame_count,
            0
        );
    }

    #[test]
    fn bp11_roundtrip_keeps_the_legacy_provenance_layout() {
        let provenance = V3BuildProvenance {
            policy: V3BuildPolicy::default(),
            requested_metadata_codec: PackedCodec::Raw as u8,
            requested_data_codec: PackedCodec::Raw as u8,
            ..Default::default()
        };
        let mut writer = Writer::default();
        provenance.encode(&mut writer).unwrap();
        let bytes = writer.finish();
        assert_eq!(&bytes[..4], b"BP11");
        let decoded = V3BuildProvenance::decode(&mut Reader::new(&bytes)).unwrap();
        assert_eq!(decoded, provenance);
    }

    #[test]
    fn bp12_roundtrip_authenticates_p90_provenance_and_selector() {
        let p90 = V3P90Policy {
            trace_digest: [1; 32],
            policy_digest: [2; 32],
            histogram_digest: [3; 32],
            sample_count: 100,
            requested_range: 200 * 1024,
        };
        let provenance = V3BuildProvenance {
            policy: V3BuildPolicy {
                frames: V3FramePolicy::P90Training,
                inline_data: false,
                p90: Some(p90),
            },
            requested_metadata_codec: PackedCodec::Raw as u8,
            requested_data_codec: PackedCodec::Raw as u8,
            p90: Some(p90),
            ..Default::default()
        };
        let selected = provenance
            .policy
            .select(
                10 * 1024 * 1024,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
            )
            .unwrap();
        assert_eq!(selected.frame_raw_bytes, 256 * 1024);
        let mut writer = Writer::default();
        provenance.encode(&mut writer).unwrap();
        let bytes = writer.finish();
        assert_eq!(&bytes[..4], b"BP12");
        let decoded = V3BuildProvenance::decode(&mut Reader::new(&bytes)).unwrap();
        assert_eq!(decoded, provenance);
    }
}
