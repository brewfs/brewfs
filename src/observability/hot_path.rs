//! Opt-in, bounded stage timings for the CLI's single mount. Nested stages
//! overlap; these are elapsed times (including waits), not additive CPU costs.
use super::*;

#[derive(Clone, Copy)]
#[allow(dead_code)] // Workspace stages are absent in non-Workspace builds.
pub(crate) enum Stage {
    OpenAdmission,
    OpenStat,
    OpenRecord,
    OpenHandle,
    PermissionGate,
    PermissionSnapshot,
    PermissionDecode,
    PermissionMiss,
    ExtentQuery,
    DirectoryFence,
    DirectoryQuery,
}
const NAMES: [&str; 11] = [
    "open_admission",
    "open_stat",
    "open_record",
    "open_handle",
    "permission_gate",
    "permission_snapshot",
    "permission_decode",
    "permission_miss",
    "extent_query",
    "directory_fence",
    "directory_query",
];

pub(super) struct Profile {
    enabled: bool,
    counts: [AtomicU64; NAMES.len()],
    nanos: [AtomicU64; NAMES.len()],
}
impl Profile {
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            nanos: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
    fn timer(&self, stage: Stage) -> Option<Timer<'_>> {
        self.enabled.then(|| Timer {
            profile: self,
            stage: stage as usize,
            start: Instant::now(),
        })
    }
    pub(super) fn render(&self, out: &mut String) {
        sample(
            out,
            "brewfs_hot_path_profile_enabled",
            "",
            u8::from(self.enabled),
        );
        if !self.enabled {
            return;
        }
        for (index, name) in NAMES.iter().enumerate() {
            let labels = format!("stage=\"{name}\"");
            sample(
                out,
                "brewfs_hot_path_calls_total",
                &labels,
                self.counts[index].load(Relaxed),
            );
            sample(
                out,
                "brewfs_hot_path_nanoseconds_total",
                &labels,
                self.nanos[index].load(Relaxed),
            );
        }
    }
}
pub(crate) struct Timer<'a> {
    profile: &'a Profile,
    stage: usize,
    start: Instant,
}
impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.profile.nanos[self.stage].fetch_add(
            self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Relaxed,
        );
        self.profile.counts[self.stage].fetch_add(1, Relaxed);
    }
}
#[inline]
pub(crate) fn timer(stage: Stage) -> Option<Timer<'static>> {
    ACTIVE
        .get()
        .and_then(|metrics| metrics.hot_path.timer(stage))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hot_path_profile_is_opt_in_and_has_only_fixed_labels() {
        let disabled = Profile::new(false);
        assert!(disabled.timer(Stage::OpenRecord).is_none());
        let mut text = String::new();
        disabled.render(&mut text);
        assert_eq!(text, "brewfs_hot_path_profile_enabled 0\n");
        let enabled = Profile::new(true);
        drop(enabled.timer(Stage::OpenRecord));
        drop(enabled.timer(Stage::OpenRecord));
        text.clear();
        enabled.render(&mut text);
        assert!(text.contains("brewfs_hot_path_calls_total{stage=\"open_record\"} 2\n"));
        assert_eq!(text.lines().count(), 1 + NAMES.len() * 2);
    }
}
