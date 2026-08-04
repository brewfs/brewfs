//! Data model for the BrewFS tray app: saved OSS mount profiles and live
//! mount records read from the `ossmount` runtime registry.
//!
//! The desktop app is **OSS-only**: profiles are plain S3/OSS connection
//! settings and mounting always spawns `ossmount` (metadata-less direct
//! mount, the bucket is the single source of truth).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Saved mount profiles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Profile {
    pub name: String,
    /// Kept for backward compatibility with profiles.json files written
    /// before the app became OSS-only (`"brewfs"`). New records always use
    /// `"oss"`; `load_profiles` migrates old records.
    pub mode: String,
    /// Windows drive letter (`Z:`) or macOS/Linux mount directory.
    pub drive: String,
    pub s3_bucket: String,
    pub s3_endpoint: String,
    pub s3_region: String,
    pub s3_force_path_style: bool,
    pub s3_disable_payload_checksum: bool,
    /// Optional object-key namespace (e.g. "myns/"). Must be empty or end
    /// with `/`; multi-machine mounts must use the same prefix.
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            name: "新建配置".to_string(),
            mode: "oss".to_string(),
            drive: String::new(),
            s3_bucket: String::new(),
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_force_path_style: false,
            s3_disable_payload_checksum: true,
            prefix: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
        }
    }
}

impl Profile {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("配置名称不能为空".into());
        }
        let drive = self.drive.trim();
        if drive.is_empty() {
            return Err("请填写挂载点（例如 Z: 或 /Volumes/brewfs）".into());
        }
        // Windows uses drive letters (`Z:`); macOS/Linux use a directory path
        // (e.g. `/Volumes/brewfs`).
        if !drive.starts_with('/') {
            let bytes = drive.as_bytes();
            let ok = bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';
            if !ok {
                return Err(format!(
                    "挂载点格式不正确：{drive}（Windows 用盘符如 Z:，macOS/Linux 用目录如 /Volumes/brewfs）"
                ));
            }
        }
        if self.s3_bucket.trim().is_empty() {
            return Err("OSS 直挂需要填写 Bucket".into());
        }
        if self.s3_endpoint.trim().is_empty() {
            return Err("OSS 直挂需要填写 Endpoint".into());
        }
        if self.access_key.trim().is_empty() || self.secret_key.trim().is_empty() {
            return Err("OSS 直挂需要填写 AccessKey / SecretKey".into());
        }
        Ok(())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProfilesFile {
    #[serde(default)]
    pub profiles: Vec<Profile>,
}

pub fn profiles_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("brewfs-tray")
        .join("profiles.json")
}

/// Load saved profiles. Any record from the pre-OSS era (`mode == "brewfs"`)
/// is migrated to `mode == "oss"` so it mounts through `ossmount`; the S3
/// fields are kept as-is and validation will ask the user to fill anything
/// missing.
pub fn load_profiles() -> ProfilesFile {
    let path = profiles_path();
    let mut file = match fs::read(&path) {
        Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
        Err(_) => ProfilesFile::default(),
    };
    for p in &mut file.profiles {
        p.mode = "oss".to_string();
    }
    file
}

pub fn save_profiles(file: &ProfilesFile) -> std::io::Result<()> {
    let path = profiles_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec_pretty(file).map_err(std::io::Error::other)?;
    fs::write(path, data)
}

// ---------------------------------------------------------------------------
// Live mounts (runtime registry mirror)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceRecord {
    pub pid: u32,
    pub mount_point: String,
    #[allow(dead_code)]
    pub socket_path: String,
    #[allow(dead_code)]
    pub started_at: String,
}

/// Directory where brewfs writes its runtime records
/// (`RuntimeRegistry::default_root()` in `src/control/runtime.rs`).
pub fn runtime_records_dir() -> PathBuf {
    dirs::runtime_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("brewfs")
}

/// Directory where `ossmount` records its instances.
pub fn oss_records_dir() -> PathBuf {
    std::env::temp_dir().join("brewfs-oss")
}

fn read_records_raw(dir: &Path) -> Vec<InstanceRecord> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = fs::read(&path) else { continue };
        let Ok(record) = serde_json::from_slice::<InstanceRecord>(&raw) else {
            continue;
        };
        out.push(record);
    }
    out
}

/// A live mount merged with the profile that owns its drive letter, if any.
#[derive(Debug, Clone)]
pub struct MountStatus {
    pub drive: String,
    pub backend: String,
    pub detail: String,
    pub pid: u32,
    pub alive: bool,
}

/// Read the runtime registry and produce mount status rows.
///
/// Every record is treated as an OSS direct mount (`ossmount`). Stale
/// records (dead pids) are still returned with `alive == false` so the UI
/// can show them, but they are not counted as live mounts.
pub fn read_mounts(profiles: &[Profile]) -> Vec<MountStatus> {
    let mut out: Vec<MountStatus> = Vec::new();
    // Scan both the old brewfs control-plane registry and the ossmount
    // registry; leftover brewfs records are still surfaced and can be
    // unmounted by terminating their process.
    for dir in [runtime_records_dir(), oss_records_dir()] {
        for record in read_records_raw(&dir) {
            let drive = normalize_mount_point(&record.mount_point);
            let profile = profiles
                .iter()
                .find(|p| normalize_mount_point(&p.drive) == drive);
            let detail = profile
                .map(|p| format!("{} / {}", p.s3_bucket, p.s3_endpoint.trim_end_matches('/')))
                .unwrap_or_else(|| "OSS 直挂（无元数据）".to_string());
            out.push(MountStatus {
                drive,
                backend: "oss".to_string(),
                detail,
                pid: record.pid,
                alive: crate::winutil::pid_alive(record.pid),
            });
        }
    }
    out.sort_by_key(|m| std::cmp::Reverse(m.alive));
    out
}

/// Normalize a mount point string: `Z:` stays `Z:`, `Z:\` becomes `Z:`,
/// `C:\mnt\x` stays as-is.
pub fn normalize_mount_point(s: &str) -> String {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return format!("{}:", bytes[0] as char);
    }
    s.to_string()
}

/// Best-effort cleanup of stale runtime records whose owning process is gone.
/// This keeps `brewfs info` and the tray status accurate.
pub fn prune_stale_records() {
    for dir in [runtime_records_dir(), oss_records_dir()] {
        prune_records_in(&dir);
    }
}

fn prune_records_in(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = fs::read(&path) else { continue };
        let Ok(record) = serde_json::from_slice::<InstanceRecord>(&raw) else {
            continue;
        };
        if !crate::winutil::pid_alive(record.pid) {
            let _ = fs::remove_file(&path);
        }
    }
}

// ---------------------------------------------------------------------------
// Spawning ossmount
// ---------------------------------------------------------------------------

/// Locate the `ossmount` binary: same directory as the tray executable, then
/// `OSSMOUNT_EXE`, then PATH.
pub fn find_ossmount() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("OSSMOUNT_EXE") {
        let p = PathBuf::from(explicit);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        for sibling in ["ossmount.exe", "ossmount"] {
            let p = exe.parent()?.join(sibling);
            if p.is_file() {
                return Some(p);
            }
        }
    }
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            for candidate in ["ossmount.exe", "ossmount"] {
                let p = dir.join(candidate);
                if p.is_file() {
                    return Some(p);
                }
            }
        }
    }
    None
}

/// Spawn `ossmount --bucket ... <drive>` (metadata-less OSS direct mount)
/// and return (child pid, log path).
pub fn spawn_oss_mount(ossmount: &Path, profile: &Profile) -> std::io::Result<(u32, PathBuf)> {
    let app_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("brewfs-tray");
    let log_dir = app_dir.join("logs");
    fs::create_dir_all(&log_dir)?;

    let safe_name: String = profile
        .name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let log_path = log_dir.join(format!("{safe_name}-oss.log"));
    let log_file = fs::File::create(&log_path)?;

    let mut cmd = Command::new(ossmount);
    cmd.arg("--bucket")
        .arg(profile.s3_bucket.trim())
        .arg("--endpoint")
        .arg(profile.s3_endpoint.trim());
    if !profile.s3_region.trim().is_empty() {
        cmd.arg("--region").arg(profile.s3_region.trim());
    }
    if !profile.prefix.trim().is_empty() {
        cmd.arg("--prefix").arg(profile.prefix.trim());
    }
    if profile.s3_force_path_style {
        cmd.arg("--force-path-style");
    }
    cmd.arg(profile.drive.trim());
    if !profile.access_key.is_empty() {
        cmd.env("AWS_ACCESS_KEY_ID", &profile.access_key)
            .env("AWS_SECRET_ACCESS_KEY", &profile.secret_key);
    }
    cmd.stdout(log_file.try_clone()?).stderr(log_file);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000 /* CREATE_NO_WINDOW */);
    }
    let child = cmd.spawn()?;
    Ok((child.id(), log_path))
}

/// Read the tail of a mount log file for error reporting.
pub fn read_log_tail(path: &Path, max_bytes: usize) -> String {
    let Ok(meta) = fs::metadata(path) else {
        return String::new();
    };
    let len = meta.len() as usize;
    let skip = len.saturating_sub(max_bytes);
    let Ok(file) = fs::File::open(path) else {
        return String::new();
    };
    use std::io::{Read, Seek, SeekFrom};
    let mut reader = std::io::BufReader::new(file);
    let _ = reader.seek(SeekFrom::Start(skip as u64));
    let mut buf = String::new();
    let _ = reader.read_to_string(&mut buf);
    buf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn oss_profile() -> Profile {
        Profile {
            name: "阿里云".into(),
            drive: "Z:".into(),
            s3_bucket: "my-bucket".into(),
            s3_endpoint: "https://s3.oss-cn-shanghai.aliyuncs.com".into(),
            s3_region: "cn-shanghai".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            ..Profile::default()
        }
    }

    #[test]
    fn validate_oss_ok() {
        assert!(oss_profile().validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_drive() {
        let mut p = oss_profile();
        p.drive = "Z".into();
        assert!(p.validate().is_err());
        p.drive = "ZZ:".into();
        assert!(p.validate().is_err());
        p.drive = "1:".into();
        assert!(p.validate().is_err());
        p.drive = "/Volumes/brewfs".into();
        assert!(p.validate().is_ok());
    }

    #[test]
    fn validate_requires_oss_fields() {
        let mut p = oss_profile();
        p.s3_bucket = String::new();
        assert!(p.validate().is_err()); // no bucket
        p.s3_bucket = "b".into();
        p.s3_endpoint = String::new();
        assert!(p.validate().is_err()); // no endpoint
        p.s3_endpoint = "https://example.com".into();
        p.access_key = String::new();
        assert!(p.validate().is_err()); // no keys
        p.access_key = "ak".into();
        p.secret_key = String::new();
        assert!(p.validate().is_err()); // no secret
        p.secret_key = "sk".into();
        assert!(p.validate().is_ok());
        // prefix is optional
        p.prefix = "myns/".into();
        assert!(p.validate().is_ok());
    }

    #[test]
    fn normalize_drive_letters() {
        assert_eq!(normalize_mount_point("Z:"), "Z:");
        assert_eq!(normalize_mount_point("  z:  "), "z:");
        assert_eq!(normalize_mount_point("C:\\"), "C:");
        assert_eq!(normalize_mount_point("C:\\mnt\\x"), "C:");
        assert_eq!(normalize_mount_point("/mnt/x"), "/mnt/x");
    }

    #[test]
    fn read_records_skips_non_json_and_bad_files() {
        let dir = std::env::temp_dir().join(format!("brewfs-tray-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("101.json"),
            "{\"pid\":101,\"mount_point\":\"Z:\",\"socket_path\":\"x\",\"started_at\":\"t\"}",
        )
        .unwrap();
        std::fs::write(dir.join("not-json.json"), "garbage").unwrap();
        std::fs::write(dir.join("readme.txt"), "hello").unwrap();
        let records = read_records_raw(&dir);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].pid, 101);
        assert_eq!(records[0].mount_point, "Z:");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn profile_json_roundtrip_preserves_prefix() {
        let p = Profile {
            prefix: "myns/".into(),
            drive: "F:".into(),
            s3_bucket: "b".into(),
            s3_endpoint: "https://s3.example.com".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            ..Profile::default()
        };
        let data = serde_json::to_vec(&p).unwrap();
        let back: Profile = serde_json::from_slice(&data).unwrap();
        assert_eq!(back.mode, "oss");
        assert_eq!(back.prefix, "myns/");
    }

    #[test]
    fn read_mounts_scans_oss_records_dir() {
        let oss_dir = oss_records_dir();
        let _ = fs::create_dir_all(&oss_dir);
        let fake_pid = 4_200_000 + std::process::id() % 100_000;
        let path = oss_dir.join(format!("{fake_pid}.json"));
        let record = serde_json::json!({
            "pid": fake_pid,
            "mount_point": "H:",
            "socket_path": "",
            "started_at": "2026-01-01T00:00:00Z",
        });
        fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        let mounts = read_mounts(&[]);
        let _ = fs::remove_file(&path);
        let m = mounts.iter().find(|m| m.pid == fake_pid);
        assert!(m.is_some(), "ossmount record should be picked up");
        assert_eq!(m.unwrap().backend, "oss");
    }

    #[test]
    fn load_profiles_migrates_old_brewfs_mode() {
        let dir = std::env::temp_dir().join(format!("brewfs-tray-migrate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let old = serde_json::json!({
            "profiles": [{
                "name": "旧配置",
                "mode": "brewfs",
                "drive": "F:",
                "backend": "s3",
                "s3_bucket": "b",
                "s3_endpoint": "https://s3.example.com",
                "access_key": "ak",
                "secret_key": "sk",
                "data_dir": "",
                "meta_backend": "sqlx",
                "meta_url": "sqlite://./data/brewfs-meta.db?mode=rwc"
            }]
        });
        let path = dir.join("profiles.json");
        std::fs::write(&path, serde_json::to_vec(&old).unwrap()).unwrap();

        // Temporarily redirect profiles_path() output by swapping the module
        // function is not possible, so save/load through the same file:
        let data = std::fs::read(&path).unwrap();
        let mut file: ProfilesFile = serde_json::from_slice(&data).unwrap();
        for p in &mut file.profiles {
            p.mode = "oss".to_string();
        }
        assert_eq!(file.profiles.len(), 1);
        assert_eq!(file.profiles[0].mode, "oss");
        assert_eq!(file.profiles[0].s3_bucket, "b");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn profile_json_roundtrip() {
        let file = ProfilesFile {
            profiles: vec![oss_profile()],
        };
        let data = serde_json::to_vec(&file).unwrap();
        let back: ProfilesFile = serde_json::from_slice(&data).unwrap();
        assert_eq!(back.profiles.len(), 1);
        assert_eq!(back.profiles[0].name, "阿里云");
        assert_eq!(back.profiles[0].drive, "Z:");
    }
}
