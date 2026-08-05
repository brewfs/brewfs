//! BrewFS desktop tray manager (Slint 1.17), Windows + macOS.
//!
//! A system-tray app that keeps a list of saved OSS mount profiles (config
//! records), shows their live mount state, and lets the user open /
//! mount|unmount / delete each record from one list, edit the selected
//! profile in the form, and add new configs.
//!
//! OSS-only: every profile is a metadata-less S3/OSS direct mount spawned via
//! `ossmount` (WinFsp on Windows, FUSE/macFUSE on macOS/Linux). Binaries are
//! located next to this executable, via `OSSMOUNT_EXE`, or on PATH.

#![cfg_attr(windows, windows_subsystem = "windows")]
#![cfg_attr(not(windows), allow(dead_code))]

mod model;
mod winutil;

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use slint::{ModelRc, SharedString, Timer, TimerMode, VecModel};

slint::include_modules!();

/// An `ossmount` process we started that has not yet produced a runtime
/// registry record (still initializing) or failed to do so.
struct RecentSpawn {
    drive: String,
    pid: u32,
    log: PathBuf,
    at: Instant,
}

/// Configure the Slint backend before any window is created.
///
/// On macOS the window is created with a transparent, hidden titlebar while
/// keeping the native traffic-light buttons (close/minimize/zoom). This uses
/// winit's window-attributes hook (slint feature `unstable-winit-030`): a plain
/// `titlebar_hidden` would drop the `Closable` style mask and remove the buttons,
/// so we combine `titlebar_transparent` + `fullsize_content_view` + `title_hidden`
/// instead, which is the standard "hidden titlebar, buttons still visible" recipe.
fn configure_backend() -> Result<(), Box<dyn std::error::Error>> {
    let selector = slint::BackendSelector::new();
    #[cfg(target_os = "macos")]
    let selector = selector.with_winit_window_attributes_hook(|attributes| {
        use slint::winit_030::winit::platform::macos::WindowAttributesExtMacOS;
        attributes
            .with_titlebar_transparent(true)
            .with_fullsize_content_view(true)
            .with_title_hidden(true)
    });
    selector.select()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the winit backend (macOS: hidden titlebar, keep traffic lights)
    // before creating any Slint component.
    configure_backend()?;

    // Single-instance protection via a Windows named mutex (kernel object):
    // a second launch is shown a message box and exits immediately.
    let _single_instance = match winutil::single_instance_guard("BrewFS-Tray") {
        Some(guard) => guard,
        None => {
            winutil::alert_single_instance();
            return Ok(());
        }
    };

    let ui = MainWindow::new()?;
    let tray = Tray::new()?;

    let state = Rc::new(RefCell::new(model::load_profiles()));
    let recent = Rc::new(RefCell::new(Vec::<RecentSpawn>::new()));
    let ossmount = Rc::new(model::find_ossmount());

    // Drive letters are a Windows concept; macOS/Linux use mount directories.
    ui.set_show_free_drives(cfg!(windows));

    // On macOS the content extends under the (hidden) titlebar, so leave room
    // for the native traffic-light buttons in the top-left corner.
    ui.set_traffic_light_padding(cfg!(target_os = "macos"));

    // Clicking the Dock icon should re-show the tray window (macOS).
    #[cfg(target_os = "macos")]
    {
        let ui_weak = ui.as_weak();
        mac_dock_reopen::install(move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.show();
                raise_window_to_front();
            }
        });
    }

    // Drop stale runtime records from earlier crashed/force-killed mounts so
    // the tray status stays accurate.
    model::prune_stale_records();

    refresh(&ui, &tray, &state, &recent);

    // Preload the first saved profile into the form.
    if !state.borrow().profiles.is_empty() {
        profile_to_form(&ui, &state.borrow().profiles[0]);
    }

    wire_callbacks(&ui, &tray, &state, &recent, &ossmount);

    // Periodic status refresh (2s) driven from the UI thread.
    let timer = Timer::default();
    {
        let ui_weak = ui.as_weak();
        let tray_weak = tray.as_weak();
        let state = state.clone();
        let recent = recent.clone();
        timer.start(TimerMode::Repeated, Duration::from_secs(2), move || {
            if let (Some(ui), Some(tray)) = (ui_weak.upgrade(), tray_weak.upgrade()) {
                refresh(&ui, &tray, &state, &recent);
            }
        });
    }

    tray.show()?;
    ui.show()?;
    ui.set_status_text(SharedString::from("BrewFS 托盘已就绪"));
    refresh(&ui, &tray, &state, &recent);

    slint::run_event_loop()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn wire_callbacks(
    ui: &MainWindow,
    tray: &Tray,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
    ossmount: &Rc<Option<PathBuf>>,
) {
    let ui_weak = ui.as_weak();
    let tray_weak = tray.as_weak();
    let state = Rc::clone(state);
    let recent = Rc::clone(recent);
    let ossmount = Rc::clone(ossmount);

    // --- save the form back into a profile ---
    ui.on_save_form({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let p = form_to_profile(&ui);
            if let Err(e) = p.validate() {
                ui.set_status_text(format!("保存失败：{e}").into());
                return;
            }
            {
                let mut file = state.borrow_mut();
                upsert_profile(&mut file, &p);
                if let Err(e) = model::save_profiles(&file) {
                    ui.set_status_text(format!("保存失败：{e}").into());
                    return;
                }
            }
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent);
            }
            ui.set_status_text(format!("配置「{}」已保存", p.name).into());
        }
    });

    // --- add a new blank config ---
    ui.on_add_config({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let name = {
                let mut file = state.borrow_mut();
                let name = format!("新建配置 {}", file.profiles.len() + 1);
                // Base the new config on the current form so edits (e.g. a
                // mount point just changed) carry over instead of the form
                // being wiped to empty.
                let mut p = form_to_profile(&ui);
                p.name = name.clone();
                file.profiles.push(p.clone());
                if let Err(e) = model::save_profiles(&file) {
                    ui.set_status_text(format!("添加失败：{e}").into());
                    return;
                }
                profile_to_form(&ui, &p);
                name
            };
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent);
            }
            ui.set_status_text(format!("已添加配置「{name}」，填写后点保存").into());
        }
    });

    // --- open a record's drive in Explorer ---
    ui.on_open_record({
        let ui_weak = ui_weak.clone();
        let state = state.clone();
        move |index| {
            let Some(ui) = ui_weak.upgrade() else { return };
            let profiles = state.borrow().profiles.clone();
            let Some(p) = profiles.get(index as usize) else {
                ui.set_status_text("记录不存在".into());
                return;
            };
            open_in_explorer(&model::normalize_mount_point(&p.drive));
        }
    });

    // --- per-record mount / unmount toggle ---
    ui.on_toggle_record({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        let ossmount = ossmount.clone();
        move |index| {
            let Some(ui) = ui_weak.upgrade() else { return };
            let profiles = state.borrow().profiles.clone();
            let Some(p) = profiles.get(index as usize).cloned() else {
                ui.set_status_text("记录不存在".into());
                return;
            };
            let drive = model::normalize_mount_point(&p.drive);
            let mounts = model::read_mounts(&profiles);
            if let Some(m) = mounts.iter().find(|m| m.drive == drive && m.alive) {
                // mounted -> confirm then unmount
                if winutil::confirm_yes_no("BrewFS 卸载确认", &format!("确定要卸载 {drive} 吗？"))
                {
                    graceful_or_kill(&ui, m);
                } else {
                    ui.set_status_text(format!("已取消卸载 {drive}").into());
                }
            } else {
                // not mounted -> mount
                if let Err(e) = p.validate() {
                    ui.set_status_text(format!("挂载失败：{e}").into());
                } else {
                    mount_profile(&ui, &tray_weak, &state, &recent, &ossmount, &p);
                }
            }
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent);
            }
        }
    });

    // --- delete a config record ---
    ui.on_delete_record({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        move |index| {
            let Some(ui) = ui_weak.upgrade() else { return };
            let name = {
                let profiles = state.borrow();
                let Some(p) = profiles.profiles.get(index as usize) else {
                    ui.set_status_text("记录不存在".into());
                    return;
                };
                p.name.clone()
            };
            if !winutil::confirm_yes_no("BrewFS 删除确认", &format!("确定要删除配置「{name}」吗？"))
            {
                ui.set_status_text("已取消删除".into());
                return;
            }
            {
                let mut file = state.borrow_mut();
                if index >= 0 && (index as usize) < file.profiles.len() {
                    file.profiles.remove(index as usize);
                }
                if let Err(e) = model::save_profiles(&file) {
                    ui.set_status_text(format!("删除失败：{e}").into());
                    return;
                }
            }
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent);
            }
            ui.set_status_text(format!("已删除配置「{name}」").into());
        }
    });

    // --- select a record -> load into the form ---
    ui.on_select_record({
        let ui_weak = ui_weak.clone();
        let state = state.clone();
        move |index| {
            let Some(ui) = ui_weak.upgrade() else { return };
            let profiles = state.borrow();
            let Some(p) = profiles.profiles.get(index as usize) else {
                return;
            };
            let p = p.clone();
            drop(profiles);
            profile_to_form(&ui, &p);
        }
    });

    // --- tray: open a mounted drive ---
    {
        let state = state.clone();
        tray.on_open_mount(move |index| {
            let mounts = model::read_mounts(&state.borrow().profiles);
            if let Some(m) = mounts.get(index as usize) {
                open_in_explorer(&m.drive);
            }
        });
    }

    // --- tray: unmount all (with confirmation) ---
    tray.on_unmount_all({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let mounts = model::read_mounts(&state.borrow().profiles);
            let live: Vec<&model::MountStatus> = mounts.iter().filter(|m| m.alive).collect();
            if live.is_empty() {
                ui.set_status_text("当前没有活动挂载".into());
                return;
            }
            if !winutil::confirm_yes_no(
                "BrewFS 卸载确认",
                &format!("确定要卸载全部 {} 个挂载吗？", live.len()),
            ) {
                ui.set_status_text("已取消卸载".into());
                return;
            }
            for m in &live {
                graceful_or_kill(&ui, m);
            }
            ui.set_status_text(format!("已请求卸载 {} 个挂载", live.len()).into());
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent);
            }
        }
    });

    // --- window close -> hide to tray ---
    ui.window().on_close_requested({
        let ui_weak = ui_weak.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });
    tray.on_show_window({
        let ui_weak = ui_weak.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.show();
                // Slint has no public bring-to-front API; activate the app so
                // the window is shown and raised on top of other windows.
                #[cfg(target_os = "macos")]
                raise_window_to_front();
            }
        }
    });
    ui.on_quit_app(quit_app);
    tray.on_quit_app(quit_app);
}

/// Spawn `ossmount` for `p`, remember the profile, and report progress. Used
/// by the per-record mount action.
fn mount_profile(
    ui: &MainWindow,
    tray_weak: &slint::Weak<Tray>,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
    ossmount: &Rc<Option<PathBuf>>,
    p: &model::Profile,
) {
    let drive = model::normalize_mount_point(&p.drive);
    let Some(ossmount) = ossmount.as_ref() else {
        #[cfg(windows)]
        ui.set_status_text("未找到 ossmount.exe（OSS 直挂需要 Windows + WinFsp）".into());
        #[cfg(not(windows))]
        ui.set_status_text(
            "未找到 ossmount（OSS 直挂需要 macOS + macFUSE，请先安装 macFUSE）".into(),
        );
        return;
    };
    let spawned = model::spawn_oss_mount(ossmount, p);
    match spawned {
        Ok((pid, log)) => {
            {
                let mut file = state.borrow_mut();
                upsert_profile(&mut file, p);
                let _ = model::save_profiles(&file);
            }
            recent.borrow_mut().push(RecentSpawn {
                drive: drive.clone(),
                pid,
                log,
                at: Instant::now(),
            });
            ui.set_status_text(format!("正在挂载 {drive}（PID {pid}），等待就绪…").into());
        }
        Err(e) => ui.set_status_text(format!("挂载启动失败：{e}").into()),
    }
    if let Some(tray) = tray_weak.upgrade() {
        refresh(ui, &tray, state, recent);
    }
}

/// Insert or update `p` in the profile list (keyed by name).
fn upsert_profile(file: &mut model::ProfilesFile, p: &model::Profile) {
    let pos = file.profiles.iter().position(|x| x.name == p.name);
    match pos {
        Some(i) => file.profiles[i] = p.clone(),
        None => file.profiles.push(p.clone()),
    }
}

fn refresh(
    ui: &MainWindow,
    tray: &Tray,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
) {
    let profiles = state.borrow().profiles.clone();
    let mounts = model::read_mounts(&profiles);

    // Surface mounts we spawned that died before the runtime registry record
    // appeared (e.g. WinFsp missing, bad S3 credentials, invalid data dir).
    {
        let mut recent = recent.borrow_mut();
        recent.retain(|s| {
            // Give up tracking very slow mounts (long S3/meta init) silently.
            if s.at.elapsed() > Duration::from_secs(120) {
                return false;
            }
            let mounted = mounts
                .iter()
                .any(|m| m.drive == s.drive && m.alive && m.pid == s.pid);
            if mounted {
                return false;
            }
            if !winutil::pid_alive(s.pid) {
                let tail = model::read_log_tail(&s.log, 2048);
                let detail = tail.trim();
                let msg = if detail.is_empty() {
                    format!("挂载 {} 失败（PID {} 已退出）", s.drive, s.pid)
                } else {
                    format!("挂载 {} 失败：{}", s.drive, detail)
                };
                ui.set_status_text(msg.into());
                return false;
            }
            true
        });
    }

    // Main list: every saved profile, tagged with its mount state.
    let records: Vec<ProfileRecord> = profiles
        .iter()
        .map(|p| {
            let drive = model::normalize_mount_point(&p.drive);
            let m = mounts.iter().find(|m| m.drive == drive);
            let detail = format!("{} / {}", p.s3_bucket, p.s3_endpoint.trim_end_matches('/'));
            ProfileRecord {
                name: p.name.clone().into(),
                drive: drive.into(),
                backend: "oss".into(),
                detail: detail.into(),
                mounted: m.map(|m| m.alive).unwrap_or(false),
                pid: m.map(|m| m.pid as i32).unwrap_or(0),
            }
        })
        .collect();
    ui.set_records(ModelRc::new(Rc::new(VecModel::from(records))));

    // Tray menu: only live mounts.
    let tray_rows: Vec<MountInfo> = mounts
        .iter()
        .filter(|m| m.alive)
        .map(|m| MountInfo {
            drive: m.drive.clone().into(),
            backend: m.backend.clone().into(),
            detail: m.detail.clone().into(),
            pid: m.pid as i32,
            alive: m.alive,
        })
        .collect();
    tray.set_mounts(ModelRc::new(Rc::new(VecModel::from(tray_rows))));

    ui.set_free_drives_text(SharedString::from(winutil::free_drives().join(" ")));

    let live: Vec<&model::MountStatus> = mounts.iter().filter(|m| m.alive).collect();
    let status = if live.is_empty() {
        "当前没有活动挂载。".to_string()
    } else {
        let drives: Vec<&str> = live.iter().map(|m| m.drive.as_str()).collect();
        format!("已挂载 {} 个盘符：{}", live.len(), drives.join(", "))
    };
    ui.set_status_text(status.into());

    let tooltip = if live.is_empty() {
        "BrewFS（无挂载）".to_string()
    } else {
        let drives: Vec<&str> = live.iter().map(|m| m.drive.as_str()).collect();
        format!("BrewFS：已挂载 {}", drives.join(", "))
    };
    tray.set_tray_tooltip(tooltip.into());
}

fn profile_to_form(ui: &MainWindow, p: &model::Profile) {
    ui.set_cfg_name(p.name.clone().into());
    ui.set_cfg_drive(p.drive.clone().into());
    ui.set_cfg_s3_bucket(p.s3_bucket.clone().into());
    ui.set_cfg_s3_endpoint(p.s3_endpoint.clone().into());
    ui.set_cfg_s3_region(p.s3_region.clone().into());
    ui.set_cfg_s3_access_key(p.access_key.clone().into());
    ui.set_cfg_s3_secret_key(p.secret_key.clone().into());
    ui.set_cfg_s3_force_path_style(p.s3_force_path_style);
    ui.set_cfg_prefix(p.prefix.clone().into());
}

fn form_to_profile(ui: &MainWindow) -> model::Profile {
    model::Profile {
        name: ui.get_cfg_name().to_string(),
        mode: "oss".to_string(),
        drive: ui.get_cfg_drive().to_string(),
        s3_bucket: ui.get_cfg_s3_bucket().to_string(),
        s3_endpoint: ui.get_cfg_s3_endpoint().to_string(),
        s3_region: ui.get_cfg_s3_region().to_string(),
        s3_force_path_style: ui.get_cfg_s3_force_path_style(),
        s3_disable_payload_checksum: true,
        prefix: ui.get_cfg_prefix().to_string(),
        access_key: ui.get_cfg_s3_access_key().to_string(),
        secret_key: ui.get_cfg_s3_secret_key().to_string(),
    }
}

/// Unmount a live mount. All mounts are metadata-less `ossmount` instances:
/// data is flushed to the object store on close, so terminating the process
/// tears the volume down safely (WinFsp detaches the volume when the owner
/// exits; on macOS/Linux the FUSE process's SIGTERM handler umounts).
fn graceful_or_kill(ui: &MainWindow, m: &model::MountStatus) {
    match winutil::terminate_process(m.pid) {
        Ok(()) => {
            // Drop the stale drive icon from "This PC" right away instead of
            // leaving a broken drive that errors when clicked.
            winutil::notify_drive_removed(&m.drive);
            ui.set_status_text(format!("已卸载 {}", m.drive).into());
        }
        Err(e) => ui.set_status_text(format!("卸载 {} 失败：{e}", m.drive).into()),
    }
}

fn open_in_explorer(target: &str) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let _ = std::process::Command::new("explorer.exe")
            .arg(target)
            .creation_flags(0x08000000 /* CREATE_NO_WINDOW */)
            .spawn();
    }
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open").arg(target).spawn();
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    {
        let _ = target;
    }
}

/// Brings the app to the foreground on macOS (used by "显示窗口").
///
/// Slint 1.17 does not expose a window-activation API, so we activate the
/// Cocoa application natively; this raises all of its windows above other
/// apps and gives the window keyboard focus.
#[cfg(target_os = "macos")]
fn raise_window_to_front() {
    use objc::{class, msg_send, sel, sel_impl};
    #[allow(unexpected_cfgs)] // objc 0.2 macros emit cargo-clippy cfg noise
    unsafe {
        let app: *mut objc::runtime::Object = msg_send![class!(NSApplication), sharedApplication];
        let _: () = msg_send![app, activateIgnoringOtherApps: true];
    }
}

fn quit_app() {
    let _ = slint::quit_event_loop();
}

/// macOS: clicking the Dock icon must re-show the tray window.
///
/// winit installs its own `NSApplicationDelegate` and does not implement
/// `applicationShouldHandleReopen:hasVisibleWindows:`, so the default Cocoa
/// behaviour (activate, but leave a hidden window hidden) wins. Instead of
/// replacing winit's delegate (which would break its event handling), we add
/// that single method to the *existing* delegate class at runtime via
/// `class_addMethod`. The IMP calls a leaked callback that shows and raises
/// the Slint window, and returns YES so macOS proceeds with reactivation.
#[cfg(target_os = "macos")]
mod mac_dock_reopen {
    use std::ffi::CString;
    use std::sync::OnceLock;

    use objc2::ffi;
    use objc2::runtime::{AnyClass, AnyObject, Bool, Imp, Sel};
    use objc2::{MainThreadMarker, sel};
    use objc2_app_kit::NSApplication;

    type Callback = Box<dyn Fn()>;
    /// Raw pointer holder: the callback is only ever dereferenced on the main
    /// thread (from the Cocoa delegate method), so Send/Sync are safe.
    struct CallbackPtr(*mut Callback);
    unsafe impl Send for CallbackPtr {}
    unsafe impl Sync for CallbackPtr {}
    static CALLBACK: OnceLock<CallbackPtr> = OnceLock::new();

    unsafe extern "C-unwind" fn reopen_imp(
        _this: &AnyObject,
        _cmd: Sel,
        _sender: *mut AnyObject,
        _has_visible_windows: Bool,
    ) -> Bool {
        if let Some(CallbackPtr(ptr)) = CALLBACK.get() {
            // The callback is leaked for the app lifetime, so this is valid.
            let callback = unsafe { &**ptr };
            callback();
        }
        Bool::new(true)
    }

    pub fn install(callback: impl Fn() + 'static) {
        // Keep the callback alive for the whole app lifetime.
        let _ = CALLBACK.set(CallbackPtr(Box::into_raw(Box::new(
            Box::new(callback) as Callback
        ))));

        let mtm = MainThreadMarker::new().expect("Dock reopen hook must run on the main thread");
        let app = NSApplication::sharedApplication(mtm);
        let Some(delegate) = app.delegate() else {
            eprintln!("BrewFS: no NSApplication delegate yet; Dock reopen hook not installed");
            return;
        };
        let class_ptr = unsafe {
            ffi::object_getClass(
                objc2::rc::Retained::as_ptr(&delegate) as *const _ as *mut AnyObject
            )
        };
        let class = unsafe { &*class_ptr };

        // - (BOOL)applicationShouldHandleReopen:(NSApplication *)sender
        //                              hasVisibleWindows:(BOOL)flag
        let sel = sel!(applicationShouldHandleReopen:hasVisibleWindows:);
        let types = CString::new("c32@0:8@16c24").expect("valid type encoding");
        let imp: Imp = unsafe {
            std::mem::transmute::<
                unsafe extern "C-unwind" fn(&AnyObject, Sel, *mut AnyObject, Bool) -> Bool,
                Imp,
            >(
                reopen_imp
                    as unsafe extern "C-unwind" fn(&AnyObject, Sel, *mut AnyObject, Bool) -> Bool,
            )
        };

        let added = unsafe {
            ffi::class_addMethod(
                class as *const AnyClass as *mut AnyClass,
                sel,
                imp,
                types.as_ptr(),
            )
        };
        if !added.as_bool() {
            eprintln!("BrewFS: class_addMethod failed; Dock reopen may not show the window");
        }
    }
}
