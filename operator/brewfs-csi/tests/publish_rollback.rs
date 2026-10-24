#![cfg(target_os = "linux")]

use brewfs_csi::{MountOps, SystemMountOps};
use std::fs;
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

#[test]
#[ignore = "invoked in an isolated subprocess by readonly_remount_failure_rolls_back_bind"]
fn remount_subprocess() {
    let root = PathBuf::from(std::env::var_os("CSI_BIND_TEST_ROOT").unwrap());
    let source = root.join("source");
    let target = root.join("target");
    let error = SystemMountOps.bind(&source, &target, true).unwrap_err();
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

    fs::write(root.join("fail-unmount"), "").unwrap();
    let error = SystemMountOps.bind(&source, &target, true).unwrap_err();
    assert!(error.to_string().contains("injected remount failure"));
    assert!(error.to_string().contains("injected unmount failure"));
    assert!(root.join("mounted").exists());

    // Supply mount options in the subprocess while using real inode identity.
    fs::create_dir(&source).unwrap();
    script(
        &root.join("bin/findmnt"),
        "#!/bin/sh\nprintf 'ro,relatime\\n'\n",
    );
    assert!(SystemMountOps.bind_matches(&source, &source, true).unwrap());
    assert!(
        !SystemMountOps
            .bind_matches(&source, &source, false)
            .unwrap()
    );
    assert!(!SystemMountOps.bind_matches(&source, &target, true).unwrap());
    script(
        &root.join("bin/findmnt"),
        "#!/bin/sh\nprintf 'rw,relatime\\n'\n",
    );
    assert!(
        SystemMountOps
            .bind_matches(&source, &source, false)
            .unwrap()
    );
    assert!(!SystemMountOps.bind_matches(&source, &source, true).unwrap());
    script(
        &root.join("bin/findmnt"),
        "#!/bin/sh\nprintf 'ro\\nrw\\n'\n",
    );
    assert!(!SystemMountOps.bind_matches(&source, &source, true).unwrap());
    script(&root.join("bin/findmnt"), "#!/bin/sh\nexit 1\n");
    assert!(SystemMountOps.bind_matches(&source, &source, true).is_err());
}
