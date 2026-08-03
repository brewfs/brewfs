//! Data model for the BrewFS tray app: saved mount profiles, live mount
//! records read from the brewfs runtime registry, and the YAML renderer used
//! to feed `brewfs mount --config`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

pub const DEFAULT_META_URL: &str = "sqlite://./data/brewfs-meta.db?mode=rwc";
pub const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024;
pub const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Saved mount profiles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Profile {
    pub name: String,
    pub drive: String,
    pub backend: String, // "local-fs" | "s3"
    pub data_dir: String,
    pub s3_bucket: String,
    pub s3_endpoint: String,
    pub s3_region: String,
    pub s3_force_path_style: bool,
    pub s3_disable_payload_checksum: bool,
    pub access_key: String,
    pub secret_key: String,
    pub meta_backend: String, // "sqlx" | "redis" | "etcd" | "tikv"
    pub meta_url: String,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            name: "新建配置".to_string(),
            drive: String::new(),
            backend: "local-fs".to_string(),
            data_dir: String::from("E:\\brewfs-data"),
            s3_bucket: String::new(),
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_force_path_style: false,
            s3_disable_payload_checksum: true,
            access_key: String::new(),
            secret_key: String::new(),
            meta_backend: "sqlx".to_string(),
            meta_url: DEFAULT_META_URL.to_string(),
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
            return Err("请填写盘符（例如 Z:）".into());
        }
        let bytes = drive.as_bytes();
        let ok = bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';
        if !ok {
            return Err(format!(
                "盘符格式不正确：{drive}（应为单个字母加冒号，例如 Z:）"
            ));
        }
        match self.backend.as_str() {
            "local-fs" => {
                if self.data_dir.trim().is_empty() {
                    return Err("本地文件系统需要填写数据目录".into());
                }
            }
            "s3" => {
                if self.s3_bucket.trim().is_empty() {
                    return Err("S3 后端需要填写 Bucket".into());
                }
                if self.s3_endpoint.trim().is_empty() {
                    return Err("S3 后端需要填写 Endpoint".into());
                }
                if self.access_key.trim().is_empty() || self.secret_key.trim().is_empty() {
                    return Err("S3 后端需要填写 AccessKey / SecretKey".into());
                }
            }
            other => return Err(format!("未知的数据后端：{other}")),
        }
        match self.meta_backend.as_str() {
            "sqlx" | "redis" | "etcd" | "tikv" => {}
            other => return Err(format!("未知的元数据后端：{other}")),
        }
        if self.meta_url.trim().is_empty()
            && self.meta_backend != "etcd"
            && self.meta_backend != "tikv"
        {
            return Err("请填写元数据 URL".into());
        }
        Ok(())
    }

    /// Render the YAML accepted by `brewfs mount --config`.
    ///
    /// Serialized with `serde_yaml` so Windows paths (`E:\brewfs-data`) and
    /// URLs are escaped exactly like brewfs's own `MountFileConfig` parser
    /// expects (hand-written `"..."` quoting would turn `\b` into a backspace).
    pub fn to_mount_yaml(&self) -> String {
        let cfg = MountYaml {
            mount_point: self.drive.trim().to_string(),
            data: match self.backend.as_str() {
                "s3" => DataYaml {
                    backend: "s3".to_string(),
                    localfs: None,
                    s3: Some(S3Yaml {
                        bucket: self.s3_bucket.trim().to_string(),
                        endpoint: non_empty(self.s3_endpoint.trim()),
                        region: non_empty(self.s3_region.trim()),
                        force_path_style: self.s3_force_path_style,
                        disable_payload_checksum: self.s3_disable_payload_checksum,
                    }),
                },
                _ => DataYaml {
                    backend: "local-fs".to_string(),
                    localfs: Some(LocalFsYaml {
                        data_dir: self.data_dir.trim().to_string(),
                    }),
                    s3: None,
                },
            },
            meta: match self.meta_backend.as_str() {
                "redis" => MetaYaml {
                    backend: "redis".to_string(),
                    sqlx: None,
                    redis: Some(UrlYaml {
                        url: self.meta_url.trim().to_string(),
                    }),
                    etcd: None,
                    tikv: None,
                },
                "etcd" => MetaYaml {
                    backend: "etcd".to_string(),
                    sqlx: None,
                    redis: None,
                    etcd: Some(EndpointsYaml {
                        urls: split_urls(&self.meta_url),
                    }),
                    tikv: None,
                },
                "tikv" => MetaYaml {
                    backend: "tikv".to_string(),
                    sqlx: None,
                    redis: None,
                    etcd: None,
                    tikv: Some(TikvYaml {
                        pd_endpoints: split_urls(&self.meta_url),
                    }),
                },
                _ => MetaYaml {
                    backend: "sqlx".to_string(),
                    sqlx: Some(UrlYaml {
                        url: self.meta_url.trim().to_string(),
                    }),
                    redis: None,
                    etcd: None,
                    tikv: None,
                },
            },
            layout: LayoutYaml {
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        };
        serde_yaml::to_string(&cfg).expect("mount YAML serialization must not fail")
    }
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn split_urls(s: &str) -> Vec<String> {
    s.split(',')
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(str::to_string)
        .collect()
}

#[derive(Serialize)]
struct MountYaml {
    mount_point: String,
    data: DataYaml,
    meta: MetaYaml,
    layout: LayoutYaml,
}

#[derive(Serialize)]
struct DataYaml {
    backend: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    localfs: Option<LocalFsYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s3: Option<S3Yaml>,
}

#[derive(Serialize)]
struct LocalFsYaml {
    data_dir: String,
}

#[derive(Serialize)]
struct S3Yaml {
    bucket: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    force_path_style: bool,
    disable_payload_checksum: bool,
}

#[derive(Serialize)]
struct MetaYaml {
    backend: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlx: Option<UrlYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redis: Option<UrlYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    etcd: Option<EndpointsYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tikv: Option<TikvYaml>,
}

#[derive(Serialize)]
struct UrlYaml {
    url: String,
}

#[derive(Serialize)]
struct EndpointsYaml {
    urls: Vec<String>,
}

#[derive(Serialize)]
struct TikvYaml {
    pd_endpoints: Vec<String>,
}

#[derive(Serialize)]
struct LayoutYaml {
    chunk_size: u64,
    block_size: u32,
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

pub fn load_profiles() -> ProfilesFile {
    let path = profiles_path();
    match fs::read(&path) {
        Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
        Err(_) => ProfilesFile::default(),
    }
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
/// Stale records (dead pids) are still returned with `alive == false` so the
/// UI can show them, but they are not counted as live mounts.
pub fn read_mounts(profiles: &[Profile]) -> Vec<MountStatus> {
    let mut out: Vec<MountStatus> = Vec::new();
    let dir = runtime_records_dir();
    for record in read_records_raw(&dir) {
        let drive = normalize_mount_point(&record.mount_point);
        let profile = profiles
            .iter()
            .find(|p| normalize_mount_point(&p.drive) == drive);
        let backend = profile
            .map(|p| p.backend.as_str())
            .unwrap_or("local-fs")
            .to_string();
        let detail = profile
            .map(|p| match p.backend.as_str() {
                "s3" => format!("{} / {}", p.s3_bucket, p.s3_region),
                _ => p.data_dir.clone(),
            })
            .unwrap_or_else(|| "（无匹配配置）".to_string());
        out.push(MountStatus {
            drive,
            backend,
            detail,
            pid: record.pid,
            alive: crate::winutil::pid_alive(record.pid),
        });
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
    let dir = runtime_records_dir();
    let Ok(entries) = fs::read_dir(&dir) else {
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
// Spawning brewfs mount
// ---------------------------------------------------------------------------

/// Locate the `brewfs` binary: same directory as the tray executable, then
/// `BREWFS_EXE`, then PATH.
pub fn find_brewfs() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("BREWFS_EXE") {
        let p = PathBuf::from(explicit);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        let sibling = exe.parent()?.join("brewfs.exe");
        if sibling.is_file() {
            return Some(sibling);
        }
        let sibling = exe.parent()?.join("brewfs");
        if sibling.is_file() {
            return Some(sibling);
        }
    }
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            for candidate in ["brewfs.exe", "brewfs"] {
                let p = dir.join(candidate);
                if p.is_file() {
                    return Some(p);
                }
            }
        }
    }
    None
}

/// Spawn `brewfs mount --config <yaml>` and return (child pid, log path).
pub fn spawn_mount(brewfs: &Path, profile: &Profile) -> std::io::Result<(u32, PathBuf)> {
    let app_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("brewfs-tray");
    let cfg_dir = app_dir.join("configs");
    let log_dir = app_dir.join("logs");
    fs::create_dir_all(&cfg_dir)?;
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
    let yaml_path = cfg_dir.join(format!("{safe_name}.yaml"));
    let log_path = log_dir.join(format!("{safe_name}.log"));
    fs::write(&yaml_path, profile.to_mount_yaml())?;

    let mut cmd = Command::new(brewfs);
    cmd.arg("mount").arg("--config").arg(&yaml_path);
    if !profile.access_key.is_empty() {
        cmd.env("AWS_ACCESS_KEY_ID", &profile.access_key)
            .env("AWS_SECRET_ACCESS_KEY", &profile.secret_key);
    }
    let log_file = fs::File::create(&log_path)?;
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

    fn local_profile() -> Profile {
        Profile {
            name: "本地".into(),
            drive: "Z:".into(),
            backend: "local-fs".into(),
            data_dir: "E:\\brewfs-data".into(),
            meta_backend: "sqlx".into(),
            meta_url: DEFAULT_META_URL.into(),
            ..Profile::default()
        }
    }

    #[test]
    fn validate_local_ok() {
        assert!(local_profile().validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_drive() {
        let mut p = local_profile();
        p.drive = "Z".into();
        assert!(p.validate().is_err());
        p.drive = "ZZ:".into();
        assert!(p.validate().is_err());
        p.drive = "1:".into();
        assert!(p.validate().is_err());
    }

    #[test]
    fn validate_requires_s3_fields() {
        let mut p = local_profile();
        p.backend = "s3".into();
        assert!(p.validate().is_err()); // no bucket
        p.s3_bucket = "b".into();
        assert!(p.validate().is_err()); // no endpoint
        p.s3_endpoint = "https://example.com".into();
        assert!(p.validate().is_err()); // no keys
        p.access_key = "ak".into();
        p.secret_key = "sk".into();
        assert!(p.validate().is_ok());
    }

    #[test]
    fn yaml_local_roundtrip_shape() {
        let yaml = local_profile().to_mount_yaml();
        // Must parse back with the same values brewfs sees (serde_yaml), and
        // Windows backslashes must survive (no YAML escape mangling).
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("valid YAML");
        assert_eq!(v["mount_point"], "Z:");
        assert_eq!(v["data"]["backend"], "local-fs");
        assert_eq!(v["data"]["localfs"]["data_dir"], "E:\\brewfs-data");
        assert_eq!(v["meta"]["backend"], "sqlx");
        assert_eq!(
            v["meta"]["sqlx"]["url"],
            "sqlite://./data/brewfs-meta.db?mode=rwc"
        );
        assert_eq!(v["layout"]["chunk_size"], DEFAULT_CHUNK_SIZE);
        assert_eq!(v["layout"]["block_size"], DEFAULT_BLOCK_SIZE);
    }

    #[test]
    fn yaml_s3_shape() {
        let mut p = local_profile();
        p.backend = "s3".into();
        p.s3_bucket = "my-bucket".into();
        p.s3_endpoint = "https://s3.example.com".into();
        p.s3_region = "cn-shanghai".into();
        p.s3_force_path_style = true;
        let yaml = p.to_mount_yaml();
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("valid YAML");
        assert_eq!(v["data"]["backend"], "s3");
        assert_eq!(v["data"]["s3"]["bucket"], "my-bucket");
        assert_eq!(v["data"]["s3"]["endpoint"], "https://s3.example.com");
        assert_eq!(v["data"]["s3"]["region"], "cn-shanghai");
        assert_eq!(v["data"]["s3"]["force_path_style"], true);
        assert_eq!(v["data"]["s3"]["disable_payload_checksum"], true);
        assert!(v["data"]["localfs"].is_null());
    }

    #[test]
    fn yaml_etcd_urls_list() {
        let mut p = local_profile();
        p.meta_backend = "etcd".into();
        p.meta_url = "http://127.0.0.1:2379, http://127.0.0.1:2380".into();
        let yaml = p.to_mount_yaml();
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("valid YAML");
        assert_eq!(v["meta"]["backend"], "etcd");
        assert_eq!(
            v["meta"]["etcd"]["urls"],
            serde_yaml::Value::Sequence(
                ["http://127.0.0.1:2379", "http://127.0.0.1:2380"]
                    .iter()
                    .map(|s| serde_yaml::Value::String(s.to_string()))
                    .collect()
            )
        );
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
    fn profile_json_roundtrip() {
        let file = ProfilesFile {
            profiles: vec![local_profile()],
        };
        let data = serde_json::to_vec(&file).unwrap();
        let back: ProfilesFile = serde_json::from_slice(&data).unwrap();
        assert_eq!(back.profiles.len(), 1);
        assert_eq!(back.profiles[0].name, "本地");
        assert_eq!(back.profiles[0].drive, "Z:");
    }
}
