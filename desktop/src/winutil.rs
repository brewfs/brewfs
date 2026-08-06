//! Platform helpers for the BrewFS tray app.
//!
//! Windows uses the Win32 API directly (via `windows-sys`) for drive-letter
//! enumeration and process liveness checks. Unix builds provide minimal
//! stubs so the crate still compiles for CI/development.

#[cfg(windows)]
use std::os::windows::process::CommandExt;

/// All drive letters currently in use on Windows, e.g. `["C:", "D:"]`.
/// Returns an empty list on non-Windows platforms.
#[cfg(windows)]
pub fn used_drives() -> Vec<String> {
    use windows_sys::Win32::Storage::FileSystem::GetLogicalDrives;

    // SAFETY: GetLogicalDrives takes no arguments and returns a bitmask.
    let mask = unsafe { GetLogicalDrives() };
    if mask == 0 {
        return Vec::new();
    }
    (0..26)
        .filter(|i| mask & (1 << i) != 0)
        .map(|i| format!("{}:", (b'A' + i as u8) as char))
        .collect()
}

#[cfg(not(windows))]
pub fn used_drives() -> Vec<String> {
    Vec::new()
}

/// Drive letters that are free (not in use), `C:` through `Z:` (`A:`/`B:`
/// are reserved for floppy drives and never offered).
pub fn free_drives() -> Vec<String> {
    let used = used_drives();
    (2..26)
        .map(|i| format!("{}:", (b'A' + i as u8) as char))
        .filter(|d| !used.iter().any(|u| u == d))
        .collect()
}

/// True when the process at `pid` is actually one of our mount binaries
/// (ossmount / brewfs). Guards against stale runtime records whose pid was
/// reused by an unrelated process, which would otherwise show phantom
/// mounted drives in the tray.
#[cfg(windows)]
pub fn pid_is_mount_process(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, QueryFullProcessImageNameW,
    };
    if pid == 0 {
        return false;
    }
    // SAFETY: OpenProcess/QueryFullProcessImageNameW/CloseHandle are standard
    // Win32 calls; we always close the handle we opened.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut buf = [0u16; 1024];
        let mut len = buf.len() as u32;
        let ok = QueryFullProcessImageNameW(handle, 0, buf.as_mut_ptr(), &mut len);
        CloseHandle(handle);
        if ok == 0 {
            return false;
        }
        let name = String::from_utf16_lossy(&buf[..len as usize]).replace('\\', "/");
        let base = name.rsplit('/').next().unwrap_or("");
        base.starts_with("ossmount") || base.starts_with("brewfs")
    }
}

#[cfg(not(windows))]
pub fn pid_is_mount_process(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    // `ps -p <pid> -o comm=` prints the executable name; match our binaries.
    let out = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "comm="])
        .output();
    match out {
        Ok(o) if o.status.success() => {
            // macOS `ps -o comm=` prints the full executable path (e.g.
            // /Applications/BrewFS.app/Contents/MacOS/ossmount), while Linux
            // prints the basename; match on the basename on both.
            let name = String::from_utf8_lossy(&o.stdout).trim().to_string();
            let base = name.rsplit('/').next().unwrap_or("");
            base.starts_with("ossmount") || base.starts_with("brewfs")
        }
        _ => false,
    }
}

/// True when `path` is currently a mount point at the kernel level (checks
/// the `mount` table and compares canonical paths, so `/tmp` == `/private/tmp`).
/// Used to refuse stacking a second mount on the same directory.
#[cfg(not(windows))]
pub fn path_is_mount_point(path: &std::path::Path) -> bool {
    let Ok(out) = std::process::Command::new("mount").output() else {
        return false;
    };
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    String::from_utf8_lossy(&out.stdout).lines().any(|line| {
        let mut it = line.split_whitespace();
        let _dev = it.next();
        let _on = it.next();
        match it.next() {
            Some(mp) => {
                let mp_c =
                    std::fs::canonicalize(mp).unwrap_or_else(|_| std::path::PathBuf::from(mp));
                mp_c == canon
            }
            None => false,
        }
    })
}

/// Whether a process with the given id is still running.
#[cfg(windows)]
pub fn pid_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    if pid == 0 {
        return false;
    }
    // SAFETY: OpenProcess/GetExitCodeProcess/CloseHandle are standard Win32
    // calls; we always close the handle we opened.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut exit_code: u32 = 0;
        let ok = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);
        ok != 0 && exit_code == STILL_ACTIVE as u32
    }
}

#[cfg(not(windows))]
pub fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    // Linux has /proc; macOS/BSD do not, so use `kill -0` (checks existence
    // without sending a signal).
    #[cfg(target_os = "linux")]
    {
        return std::path::Path::new(&format!("/proc/{pid}")).exists();
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

/// Terminate a process tree. On Windows uses `taskkill /T /F`; the brewfs
/// WinFsp volume is torn down by the kernel when the owning process exits.
/// On other platforms falls back to `kill`.
pub fn terminate_process(pid: u32) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        let output = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .creation_flags(0x08000000 /* CREATE_NO_WINDOW */)
            .output()?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        // A process that already exited is fine for our purposes.
        if stderr.contains("not found") || stderr.contains("not running") {
            return Ok(());
        }
        Err(std::io::Error::other(format!(
            "taskkill failed: {}",
            stderr.trim()
        )))
    }
    #[cfg(not(windows))]
    {
        let output = std::process::Command::new("kill")
            .arg(pid.to_string())
            .output()?;
        if output.status.success() {
            return Ok(());
        }
        Err(std::io::Error::other("kill failed"))
    }
}

// ---------------------------------------------------------------------------
// Single-instance protection (Windows named mutex, a kernel object)
// ---------------------------------------------------------------------------

/// Handle that keeps the single-instance named mutex alive for the whole
/// process lifetime. Dropping it (or process exit) releases the mutex.
#[cfg(windows)]
pub struct SingleInstanceGuard {
    _handle: std::os::windows::io::OwnedHandle,
}

/// Try to acquire the single-instance named mutex `name`. Returns `Some`
/// when this process is the only instance, `None` when another instance
/// already holds it.
///
/// Uses a per-session `Local\` mutex: tray apps run in the interactive user
/// session, and `Local\` avoids cross-session privilege issues that
/// `Global\` can hit.
#[cfg(windows)]
pub fn single_instance_guard(name: &str) -> Option<SingleInstanceGuard> {
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError};
    use windows_sys::Win32::System::Threading::CreateMutexW;

    let mutex_name = format!("Local\\{name}");
    let wide: Vec<u16> = mutex_name
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: CreateMutexW is called with a valid NUL-terminated name and no
    // initial-owner flag; the returned handle is owned by us and released via
    // CloseHandle / OwnedHandle drop.
    unsafe {
        let handle = CreateMutexW(std::ptr::null(), 0, wide.as_ptr());
        if handle.is_null() {
            return None;
        }
        if GetLastError() == ERROR_ALREADY_EXISTS {
            CloseHandle(handle);
            return None;
        }
        Some(SingleInstanceGuard {
            _handle: std::os::windows::io::OwnedHandle::from_raw_handle(handle as _),
        })
    }
}

/// Show a message box when a second instance is started.
#[cfg(windows)]
pub fn alert_single_instance() {
    #[link(name = "user32")]
    unsafe extern "system" {
        fn MessageBoxW(
            hwnd: *mut core::ffi::c_void,
            text: *const u16,
            caption: *const u16,
            flags: u32,
        ) -> i32;
    }
    let text: Vec<u16> = "BrewFS 已经在运行，请勿重复启动。"
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    let caption: Vec<u16> = "BrewFS".encode_utf16().chain(std::iter::once(0)).collect();
    // SAFETY: MessageBoxW is passed valid NUL-terminated wide strings.
    unsafe {
        MessageBoxW(
            std::ptr::null_mut(),
            text.as_ptr(),
            caption.as_ptr(),
            0x0000_0010 | 0x0004_0000 | 0x0001_0000, /* MB_ICONINFORMATION | MB_TOPMOST | MB_SETFOREGROUND */
        );
    }
}

#[cfg(not(windows))]
pub struct SingleInstanceGuard;

#[cfg(not(windows))]
pub fn single_instance_guard(_name: &str) -> Option<SingleInstanceGuard> {
    Some(SingleInstanceGuard)
}

#[cfg(not(windows))]
pub fn alert_single_instance() {}

/// Notify the Windows shell that `drive` (e.g. "T:") was removed, so
/// Explorer drops the stale icon from "This PC" immediately instead of
/// leaving a broken drive that errors when clicked.
#[cfg(windows)]
pub fn notify_drive_removed(drive: &str) {
    const SHCNE_DRIVEREMOVED: i32 = 0x0000_0080;
    const SHCNF_PATHW: u32 = 0x0005;
    const SHCNF_FLUSH: u32 = 0x1000;
    #[link(name = "shell32")]
    unsafe extern "system" {
        fn SHChangeNotify(
            w_event_id: i32,
            u_flags: u32,
            dw_item1: *const core::ffi::c_void,
            dw_item2: *const core::ffi::c_void,
        );
    }
    // SHCNE_DRIVEREMOVED expects the drive root path ("T:\").
    let path = format!("{drive}\\");
    let wide: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();
    // SAFETY: SHChangeNotify is a shell32 broadcast; the wide string is a
    // valid NUL-terminated buffer kept alive for the duration of the call.
    unsafe {
        SHChangeNotify(
            SHCNE_DRIVEREMOVED,
            SHCNF_PATHW | SHCNF_FLUSH,
            wide.as_ptr() as *const core::ffi::c_void,
            std::ptr::null(),
        );
    }
}

#[cfg(not(windows))]
pub fn notify_drive_removed(_drive: &str) {}

/// Enable/disable launching the tray on Windows sign-in via the HKCU Run
/// registry key.
#[cfg(windows)]
pub fn set_autostart(enabled: bool) -> std::io::Result<()> {
    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, GetLastError};
    use windows_sys::Win32::System::Registry::{
        HKEY_CURRENT_USER, KEY_SET_VALUE, REG_SZ, RegCloseKey, RegCreateKeyExW, RegDeleteValueW,
        RegSetValueExW,
    };
    const RUN_KEY: &[u16] = &[
        0x53, 0x6f, 0x66, 0x74, 0x77, 0x61, 0x72, 0x65, 0x5c, 0x4d, 0x69, 0x63, 0x72, 0x6f, 0x73,
        0x6f, 0x66, 0x74, 0x5c, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x73, 0x5c, 0x43, 0x75, 0x72,
        0x72, 0x65, 0x6e, 0x74, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5c, 0x52, 0x75, 0x6e,
        0,
    ]; // "Software\Microsoft\Windows\CurrentVersion\Run"
    let exe = std::env::current_exe()?;
    let val: Vec<u16> = "BrewFS-Tray"
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: standard registry API with valid NUL-terminated strings.
    unsafe {
        let mut key = std::ptr::null_mut();
        let rc = RegCreateKeyExW(
            HKEY_CURRENT_USER,
            RUN_KEY.as_ptr(),
            0,
            std::ptr::null(),
            0,
            KEY_SET_VALUE,
            std::ptr::null(),
            &mut key,
            std::ptr::null_mut(),
        );
        if rc != ERROR_SUCCESS {
            return Err(std::io::Error::from_raw_os_error(rc as i32));
        }
        let result = if enabled {
            let cmd: Vec<u16> = format!("\"{}\"", exe.display())
                .encode_utf16()
                .chain(std::iter::once(0))
                .collect();
            RegSetValueExW(
                key,
                val.as_ptr(),
                0,
                REG_SZ,
                cmd.as_ptr().cast(),
                (cmd.len() * 2) as u32,
            )
        } else {
            RegDeleteValueW(key, val.as_ptr())
        };
        RegCloseKey(key);
        let _ = GetLastError();
        if result == ERROR_SUCCESS {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(result as i32))
        }
    }
}

/// Whether the tray is registered to auto-start on sign-in.
#[cfg(windows)]
pub fn autostart_enabled() -> bool {
    use windows_sys::Win32::Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS};
    use windows_sys::Win32::System::Registry::{
        HKEY_CURRENT_USER, KEY_QUERY_VALUE, RegCloseKey, RegOpenKeyExW, RegQueryValueExW,
    };
    const RUN_KEY: &[u16] = &[
        0x53, 0x6f, 0x66, 0x74, 0x77, 0x61, 0x72, 0x65, 0x5c, 0x4d, 0x69, 0x63, 0x72, 0x6f, 0x73,
        0x6f, 0x66, 0x74, 0x5c, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x73, 0x5c, 0x43, 0x75, 0x72,
        0x72, 0x65, 0x6e, 0x74, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5c, 0x52, 0x75, 0x6e,
        0,
    ];
    let val: Vec<u16> = "BrewFS-Tray"
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: standard registry API.
    unsafe {
        let mut key = std::ptr::null_mut();
        if RegOpenKeyExW(
            HKEY_CURRENT_USER,
            RUN_KEY.as_ptr(),
            0,
            KEY_QUERY_VALUE,
            &mut key,
        ) != ERROR_SUCCESS
        {
            return false;
        }
        let mut len: u32 = 0;
        let rc = RegQueryValueExW(
            key,
            val.as_ptr(),
            std::ptr::null(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut len,
        );
        RegCloseKey(key);
        rc == ERROR_SUCCESS && rc != ERROR_FILE_NOT_FOUND
    }
}

#[cfg(not(windows))]
pub fn set_autostart(_enabled: bool) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(windows))]
pub fn autostart_enabled() -> bool {
    false
}

/// Ask a yes/no confirmation via a modal Windows message box. Returns `true`
/// only when the user clicks "Yes". Non-Windows builds return `true` (no-op).
#[cfg(test)]
mod tests {
    #[cfg(windows)]
    #[test]
    fn single_instance_mutex_blocks_second_acquirer() {
        let name = format!("BrewFS-Tray-Test-{}", std::process::id());
        let first = super::single_instance_guard(&name);
        assert!(first.is_some(), "first acquire must succeed");
        let second = super::single_instance_guard(&name);
        assert!(second.is_none(), "second acquire must be blocked");
        drop(first);
        let third = super::single_instance_guard(&name);
        assert!(third.is_some(), "after drop, acquire must succeed again");
    }
}
