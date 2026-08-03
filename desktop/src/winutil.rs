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
