#![cfg(target_os = "linux")]

use brewfs_csi::{MountInfo, MountManager, MountOps, MountSpec, SystemMountOps, VolumeId};
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn script(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

// Exercise the production commands without mounts or process-wide PATH changes.
#[test]
fn readonly_remount_failure_rolls_back_bind() {
    let temp = tempfile::tempdir().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    script(
        &bin.join("mount"),
        r#"#!/bin/sh
echo "mount $*" >> "$CSI_BIND_TEST_ROOT/commands"
if [ "$1" = '--bind' ]; then
    : > "$CSI_BIND_TEST_ROOT/mounted"
    exit 0
fi
echo 'injected remount failure' >&2
exit 1
"#,
    );
    script(
        &bin.join("umount"),
        r#"#!/bin/sh
echo "umount $*" >> "$CSI_BIND_TEST_ROOT/commands"
if [ -e "$CSI_BIND_TEST_ROOT/fail-unmount" ]; then
    echo 'injected unmount failure' >&2
    exit 1
fi
rm "$CSI_BIND_TEST_ROOT/mounted"
"#,
    );
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "remount_subprocess", "--ignored", "--nocapture"])
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("CSI_BIND_TEST_ROOT", temp.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

// Kernel observations are simulated; bind/remount/rollback commands run in the
// isolated subprocess. Ownership and mode parsing have library regressions.
struct CommandOps {
    root: PathBuf,
    shared: PathBuf,
}

impl MountOps for CommandOps {
    fn is_mounted(&self, path: &Path) -> io::Result<bool> {
        Ok(if path == self.shared {
            self.root.join("shared-live").exists()
        } else {
            path == self.root.join("target") && self.root.join("mounted").exists()
        })
    }

    fn inspect_mount(&self, path: &Path) -> io::Result<Option<MountInfo>> {
        if !self.is_mounted(path)? {
            return Ok(None);
        }
        Ok(Some(serde_json::from_value(serde_json::json!({
            "device": "0:55", "root": "/", "fs_type": "fuse",
            "source": "brewfs", "read_only": false
        }))?))
    }

    fn process_alive(&self, _pid: u32) -> io::Result<bool> {
        Ok(self.root.join("process-live").exists())
    }

    fn process_start_time(&self, pid: u32) -> io::Result<Option<u64>> {
        Ok(self.process_alive(pid)?.then_some(42))
    }

    fn start_brewfs(&self, _spec: &MountSpec, target: &Path) -> io::Result<u32> {
        fs::create_dir_all(target)?;
        fs::write(self.root.join("shared-live"), "")?;
        fs::write(self.root.join("process-live"), "")?;
        Ok(42)
    }

    fn stop_brewfs(&self, pid: u32, start_time: u64) -> io::Result<()> {
        assert_eq!((pid, start_time), (42, 42));
        fs::remove_file(self.root.join("process-live"))
    }

    fn bind(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()> {
        SystemMountOps.bind(source, target, read_only)
    }

    fn unbind(&self, target: &Path, expected: MountInfo) -> io::Result<()> {
        assert_eq!(self.inspect_mount(target)?.as_ref(), Some(&expected));
        let output = Command::new("umount").arg(target).output()?;
        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }
        fs::remove_dir(target)
    }

    fn unmount_brewfs(&self, _target: &Path) -> io::Result<()> {
        fs::remove_file(self.root.join("shared-live"))
    }
}

#[tokio::test]
#[ignore = "invoked in an isolated subprocess by readonly_remount_failure_rolls_back_bind"]
async fn remount_subprocess() {
    let root = PathBuf::from(std::env::var_os("CSI_BIND_TEST_ROOT").unwrap());
    let spec = MountSpec {
        volume: VolumeId::parse("brewfs://cluster-a/00000000-0000-0000-0000-000000000007").unwrap(),
        config: root.join("config.yaml"),
        binary: root.join("bin/brewfs"),
        mount_root: root.join("mounts"),
        read_only: false,
    };
    let source = spec.shared_mount();
    let manager = || {
        MountManager::new_with_root(
            CommandOps {
                root: root.clone(),
                shared: source.clone(),
            },
            root.join("state"),
        )
    };
    let target = root.join("target");
    let error = manager().publish(&spec, &target, true).await.unwrap_err();
    assert!(error.to_string().contains("injected remount failure"));
    assert!(
        !root.join("mounted").exists(),
        "writable bind leaked after read-only remount failed"
    );
    assert_eq!(
        fs::read_to_string(root.join("commands")).unwrap(),
        format!(
            "mount --bind {} {}\nmount -o remount,bind,ro {}\numount {}\n",
            source.display(),
            target.display(),
            target.display(),
            target.display()
        )
    );
    assert!(!root.join("shared-live").exists());
    assert!(!root.join("process-live").exists());
    assert_eq!(fs::read_dir(root.join("state")).unwrap().count(), 0);

    fs::write(root.join("fail-unmount"), "").unwrap();
    let error = manager().publish(&spec, &target, true).await.unwrap_err();
    assert!(error.to_string().contains("injected remount failure"));
    assert!(error.to_string().contains("injected unmount failure"));
    assert!(root.join("mounted").exists());

    // A restart must reject the writable bind as a read-only publish, while
    // retaining enough source-verified recovery intent to unpublish it.
    let restarted = manager();
    assert!(restarted.publish(&spec, &target, true).await.is_err());
    fs::remove_file(root.join("fail-unmount")).unwrap();
    restarted
        .unpublish_by_id(&spec.volume.as_string(), &target)
        .await
        .unwrap();
    restarted
        .unstage_by_id(&spec.volume.as_string())
        .await
        .unwrap();
    assert!(!root.join("mounted").exists());
    assert!(!root.join("shared-live").exists());
    assert!(!root.join("process-live").exists());
}
