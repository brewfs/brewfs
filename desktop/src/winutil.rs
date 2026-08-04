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

/// Drive letters that are free (not in use), `A:` through `Z:`.
pub fn free_drives() -> Vec<String> {
    let used = used_drives();
    (0..26)
        .map(|i| format!("{}:", (b'A' + i as u8) as char))
        .filter(|d| !used.iter().any(|u| u == d))
        .collect()
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
    // Linux /proc check; other unixes fall back to true (best effort).
    #[cfg(target_os = "linux")]
    {
        return std::path::Path::new(&format!("/proc/{pid}")).exists();
    }
    #[cfg(not(target_os = "linux"))]
    {
        true
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
            0x0000_0010, /* MB_ICONINFORMATION */
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
