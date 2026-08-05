//! BrewFS desktop tray manager (Slint 1.17), Windows + macOS.
//!
//! A system-tray app that keeps a list of saved BrewFS mount
//! profiles (config records), shows their live mount state, and lets the user
//! open / mount|unmount / delete each record from one list, edit the selected
//! profile in the form, and add new configs.
//!
//! Requires a brewfs build with the `fuse-winfsp` feature on Windows
//! (`ossmount` for the metadata-less OSS direct-mount mode; macOS uses
//! the FUSE-based `ossmount` with macFUSE). Binaries are located next
//! to this executable, via `BREWFS_EXE` / `OSSMOUNT_EXE`, or on PATH.

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

/// A `brewfs mount` process we started that has not yet produced a runtime
/// registry record (control plane still initializing) or failed to do so.
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

/// Desired mounts + auto-restart bookkeeping for the mount-process guard.
#[derive(Default)]
struct GuardState {
    /// Drive letters the user explicitly mounted; auto-restarted if the
    /// process dies unexpectedly.
    desired: std::collections::HashSet<String>,
    /// When we last spawned each drive (backoff against restart loops).
    last_spawn: std::collections::HashMap<String, Instant>,
    /// Drives that fast-failed (e.g. bad config) and must be retried only by
    /// the user manually.
    failed: std::collections::HashSet<String>,
}

static MOUNT_GUARD: std::sync::OnceLock<std::sync::Mutex<GuardState>> = std::sync::OnceLock::new();

fn guard() -> std::sync::MutexGuard<'static, GuardState> {
    MOUNT_GUARD
        .get_or_init(|| std::sync::Mutex::new(GuardState::default()))
        .lock()
        .unwrap()
}

/// Monitor desired mounts and auto-restart any whose process died without a
/// user-initiated unmount. Backs off 30s between attempts and stops retrying
/// after a fast failure (config error) until the user mounts manually.
fn auto_restart(
    ui: &MainWindow,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
    ossmount: &Rc<Option<PathBuf>>,
    brewfs: &Rc<Option<PathBuf>>,
) {
    let profiles = state.borrow().profiles.clone();
    let mounts = model::read_mounts(&profiles);
    let mut g = guard();
    for drive in g.desired.clone() {
        if mounts.iter().any(|m| m.alive && m.drive == drive) {
            g.failed.remove(&drive);
            continue;
        }
        if g.failed.contains(&drive) {
            continue;
        }
        let since = g
            .last_spawn
            .get(&drive)
            .map(|t| t.elapsed())
            .unwrap_or(Duration::from_secs(60));
        if since < Duration::from_secs(30) {
            continue;
        }
        let Some(p) = profiles
            .iter()
            .find(|p| model::normalize_mount_point(&p.drive) == drive)
            .cloned()
        else {
            g.failed.insert(drive);
            continue;
        };
        if p.validate().is_err() {
            g.failed.insert(drive);
            continue;
        }
        let spawned: Option<std::io::Result<(u32, PathBuf)>> = if p.mode == "oss" {
            ossmount
                .as_ref()
                .as_deref()
                .map(|o| model::spawn_oss_mount(o, &p))
        } else {
            brewfs
                .as_ref()
                .as_deref()
                .map(|b| model::spawn_mount(b, &p))
        };
        match spawned {
            Some(Ok((pid, log))) => {
                g.last_spawn.insert(drive.clone(), Instant::now());
                recent.borrow_mut().push(RecentSpawn {
                    drive: drive.clone(),
                    pid,
                    log,
                    at: Instant::now(),
                });
                ui.set_status_text(format!("检测到 {drive} 挂载进程退出，正在自动重启…").into());
            }
            Some(Err(e)) => {
                g.failed.insert(drive.clone());
                ui.set_status_text(format!("{drive} 自动重启失败：{e}").into());
            }
            None => {
                g.failed.insert(drive);
            }
        }
    }
    // Fast-fail guard: a desired mount whose spawned process died within 15s
    // is most likely a config/credential error; stop auto-retrying.
    for s in recent.borrow().iter() {
        if g.desired.contains(&s.drive)
            && g.last_spawn.get(&s.drive).is_some()
            && s.at.elapsed() < Duration::from_secs(15)
            && !winutil::pid_alive(s.pid)
            && !mounts.iter().any(|m| m.alive && m.drive == s.drive)
        {
            g.failed.insert(s.drive.clone());
        }
    }
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
    let edit = EditDialog::new()?;
    let tray = Tray::new()?;

    let state = Rc::new(RefCell::new(model::load_profiles()));
    let recent = Rc::new(RefCell::new(Vec::<RecentSpawn>::new()));
    let brewfs = Rc::new(model::find_brewfs());
    let ossmount = Rc::new(model::find_ossmount());

    // Drive letters are a Windows concept; macOS/Linux use mount directories.
    edit.set_show_free_drives(cfg!(windows));

    // On macOS the titlebar is hidden and content extends under it; leave room
    // for the native traffic-light buttons at the top of both windows.
    ui.set_traffic_light_padding(cfg!(target_os = "macos"));
    edit.set_traffic_light_padding(cfg!(target_os = "macos"));

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
    // both the tray status and `brewfs info` stay accurate.
    model::prune_stale_records();

    refresh(&ui, &tray, &state, &recent);

    // Preload the first saved profile into the form.
    if !state.borrow().profiles.is_empty() {
        profile_to_form(&edit, &state.borrow().profiles[0]);
    }

    // 模态确认：主窗口内的覆盖层（无第二个窗口、无第二个任务栏图标）。
    let pending: Rc<RefCell<Option<Box<dyn FnOnce()>>>> = Rc::new(RefCell::new(None));
    {
        let ui_weak = ui.as_weak();
        let pending_confirm = Rc::clone(&pending);
        ui.on_confirm_dialog(move || {
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_dlg_visible(false);
            }
            if let Some(f) = pending_confirm.borrow_mut().take() {
                f();
            }
        });
        let ui_weak = ui.as_weak();
        let pending_cancel = Rc::clone(&pending);
        ui.on_cancel_dialog(move || {
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_dlg_visible(false);
            }
            pending_cancel.borrow_mut().take();
        });
    }
    tray.set_autostart(winutil::autostart_enabled());

    wire_callbacks(
        &ui, &edit, &tray, &state, &recent, &brewfs, &ossmount, &pending,
    );

    // Periodic status refresh (2s) driven from the UI thread.
    let timer = Timer::default();
    {
        let ui_weak = ui.as_weak();
        let tray_weak = tray.as_weak();
        let state = state.clone();
        let recent = recent.clone();
        let ossmount = ossmount.clone();
        let brewfs = brewfs.clone();
        timer.start(TimerMode::Repeated, Duration::from_secs(2), move || {
            if let (Some(ui), Some(tray)) = (ui_weak.upgrade(), tray_weak.upgrade()) {
                auto_restart(&ui, &state, &recent, &ossmount, &brewfs);
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
    edit: &EditDialog,
    tray: &Tray,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
    brewfs: &Rc<Option<PathBuf>>,
    ossmount: &Rc<Option<PathBuf>>,
    pending: &Rc<RefCell<Option<Box<dyn FnOnce()>>>>,
) {
    let ui_weak = ui.as_weak();
    let edit_weak = edit.as_weak();
    let tray_weak = tray.as_weak();
    let state = Rc::clone(state);
    let recent = Rc::clone(recent);
    let brewfs = Rc::clone(brewfs);
    let ossmount = Rc::clone(ossmount);
    let pending = Rc::clone(pending);

    // --- save the form back into a profile ---
    edit.on_save_form({
        let ui_weak = ui_weak.clone();
        let edit_weak = edit_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let Some(edit) = edit_weak.upgrade() else {
                return;
            };
            let p = form_to_profile(&edit);
            if let Err(e) = p.validate() {
                ui.set_status_text(format!("保存失败：{e}").into());
                return;
            }
            let occupied = drive_occupied(&p.drive);
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
            close_edit_dialog(&ui, &edit);
            if occupied {
                ui.set_status_text(
                    format!("⚠️ 已保存，但盘符 {} 已被占用，挂载前请更换", p.drive).into(),
                );
            } else {
                ui.set_status_text(format!("配置「{}」已保存", p.name).into());
            }
        }
    });

    // --- add a new blank config -> open the edit window ---
    ui.on_add_config({
        let ui_weak = ui_weak.clone();
        let edit_weak = edit_weak.clone();
        let state = state.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let Some(edit) = edit_weak.upgrade() else {
                return;
            };
            let mut p = model::Profile::default();
            p.name = {
                let file = state.borrow();
                // 取第一个不冲突的默认名（避免「新建配置 2」这种依赖已有数量的叫法）。
                let mut n = 1;
                while file
                    .profiles
                    .iter()
                    .any(|q| q.name == format!("新建配置 {n}"))
                {
                    n += 1;
                }
                if n == 1 {
                    "新建配置".to_string()
                } else {
                    format!("新建配置 {n}")
                }
            };
            // 新配置默认 OSS 直挂 + 第一个空闲盘符。
            #[cfg(windows)]
            if let Some(d) = winutil::free_drives().first() {
                p.drive = d.clone();
            }
            profile_to_form(&edit, &p);
            open_edit_dialog(&ui, &edit, format!("添加配置「{}」", p.name));
            ui.set_status_text(format!("添加配置「{}」，填写后点保存", p.name).into());
        }
    });

    // --- edit a record -> load into the edit window ---
    ui.on_edit_record({
        let ui_weak = ui_weak.clone();
        let edit_weak = edit_weak.clone();
        let state = state.clone();
        move |index| {
            let Some(ui) = ui_weak.upgrade() else { return };
            let Some(edit) = edit_weak.upgrade() else {
                return;
            };
            let profiles = state.borrow();
            let Some(p) = profiles.profiles.get(index as usize) else {
                return;
            };
            let p = p.clone();
            drop(profiles);
            profile_to_form(&edit, &p);
            open_edit_dialog(&ui, &edit, format!("编辑配置「{}」", p.name));
        }
    });

    // --- cancel editing ---
    edit.on_cancel_edit({
        let ui_weak = ui_weak.clone();
        let edit_weak = edit_weak.clone();
        move || {
            if let (Some(ui), Some(edit)) = (ui_weak.upgrade(), edit_weak.upgrade()) {
                close_edit_dialog(&ui, &edit);
            }
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
        let brewfs = brewfs.clone();
        let ossmount = ossmount.clone();
        let pending = Rc::clone(&pending);
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
                // mounted -> confirm then unmount (Slint modal dialog)
                let m = m.clone();
                let ui_weak2 = ui_weak.clone();
                let brewfs2 = brewfs.clone();
                let tray_weak2 = tray_weak.clone();
                let state2 = state.clone();
                let recent2 = recent.clone();
                ask_confirm(
                    &ui,
                    &pending,
                    &format!("确定要卸载 {drive} 吗？"),
                    move || {
                        guard().desired.remove(&m.drive);
                        if let Some(ui) = ui_weak2.upgrade() {
                            graceful_or_kill(&ui, brewfs2.as_ref(), &m);
                        }
                        if let (Some(ui), Some(tray)) = (ui_weak2.upgrade(), tray_weak2.upgrade()) {
                            refresh(&ui, &tray, &state2, &recent2);
                        }
                    },
                );
            } else {
                // not mounted -> mount
                if let Err(e) = p.validate() {
                    ui.set_status_text(format!("挂载失败：{e}").into());
                } else {
                    mount_profile(&ui, &tray_weak, &state, &recent, &brewfs, &ossmount, &p);
                    // Ask the guard to auto-restart this mount if it dies.
                    guard().desired.insert(drive);
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
        let pending = Rc::clone(&pending);
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
            let state2 = state.clone();
            let recent2 = recent.clone();
            let tray_weak2 = tray_weak.clone();
            let ui_weak2 = ui_weak.clone();
            ask_confirm(
                &ui,
                &pending,
                &format!("确定要删除配置「{name}」吗？"),
                move || {
                    {
                        let mut file = state2.borrow_mut();
                        if index >= 0 && (index as usize) < file.profiles.len() {
                            file.profiles.remove(index as usize);
                        }
                        if let Err(e) = model::save_profiles(&file) {
                            if let Some(ui) = ui_weak2.upgrade() {
                                ui.set_status_text(format!("删除失败：{e}").into());
                            }
                            return;
                        }
                    }
                    if let (Some(ui), Some(tray)) = (ui_weak2.upgrade(), tray_weak2.upgrade()) {
                        refresh(&ui, &tray, &state2, &recent2);
                        ui.set_status_text(format!("已删除配置「{name}」").into());
                    }
                },
            );
        }
    });

    // --- select a record -> load into the form ---
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
        let brewfs = brewfs.clone();
        let pending = Rc::clone(&pending);
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let mounts = model::read_mounts(&state.borrow().profiles);
            let live: Vec<model::MountStatus> =
                mounts.iter().filter(|m| m.alive).cloned().collect();
            if live.is_empty() {
                ui.set_status_text("当前没有活动挂载".into());
                return;
            }
            let ui_weak2 = ui_weak.clone();
            let tray_weak2 = tray_weak.clone();
            let state2 = state.clone();
            let recent2 = recent.clone();
            let brewfs2 = brewfs.clone();
            ask_confirm(
                &ui,
                &pending,
                &format!("确定要卸载全部 {} 个挂载吗？", live.len()),
                move || {
                    for m in &live {
                        guard().desired.remove(&m.drive);
                        if let Some(ui) = ui_weak2.upgrade() {
                            graceful_or_kill(&ui, brewfs2.as_ref(), m);
                        }
                    }
                    if let (Some(ui), Some(tray)) = (ui_weak2.upgrade(), tray_weak2.upgrade()) {
                        refresh(&ui, &tray, &state2, &recent2);
                        ui.set_status_text(format!("已请求卸载 {} 个挂载", live.len()).into());
                    }
                },
            );
        }
    });

    // --- window close -> hide to tray (进程保留，双击托盘图标重新打开) ---
    ui.window().on_close_requested({
        let ui_weak = ui_weak.clone();
        let edit_weak = edit_weak.clone();
        move || {
            if let (Some(ui), Some(edit)) = (ui_weak.upgrade(), edit_weak.upgrade()) {
                close_edit_dialog(&ui, &edit);
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });
    // Edit dialog X button == cancel: close modally and re-enable the owner.
    edit.window().on_close_requested({
        let ui_weak = ui_weak.clone();
        let edit_weak = edit_weak.clone();
        move || {
            if let (Some(ui), Some(edit)) = (ui_weak.upgrade(), edit_weak.upgrade()) {
                close_edit_dialog(&ui, &edit);
            }
            slint::CloseRequestResponse::HideWindow
        }
    });
    tray.on_show_window({
        let ui_weak = ui_weak.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.show();
                // Slint has no public bring-to-front API; raise + focus the
                // window natively so it always lands on top (Windows needs
                // Win32 activation; macOS uses Cocoa activation).
                raise_window_to_front();
            }
        }
    });
    // --- 开机自启 ---
    tray.on_autostart_changed({
        let ui_weak = ui_weak.clone();
        move |enabled| {
            if let Err(e) = winutil::set_autostart(enabled) {
                if let Some(ui) = ui_weak.upgrade() {
                    ui.set_status_text(format!("设置开机自启失败：{e}").into());
                }
            }
        }
    });

    // 退出：先显式移除托盘图标（避免任何情况下残留），再退出事件循环。
    // Slint 的 SystemTrayIcon 在组件 drop 时也会 NIM_DELETE，这里 double-check。
    let tray_for_quit = tray_weak.clone();
    let do_quit = move || {
        if let Some(tray) = tray_for_quit.upgrade() {
            // 先移除托盘图标（Slint 组件 drop 时也会 NIM_DELETE，双保险）。
            let _ = tray.hide();
        }
        quit_app();
    };
    ui.on_quit_app(do_quit.clone());
    tray.on_quit_app(do_quit);
}

/// Spawn the right backend for `p` (brewfs vs ossmount), remember the profile,
/// and report progress. Used by the per-record mount action.
#[allow(clippy::too_many_arguments)]
fn mount_profile(
    ui: &MainWindow,
    tray_weak: &slint::Weak<Tray>,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
    brewfs: &Rc<Option<PathBuf>>,
    ossmount: &Rc<Option<PathBuf>>,
    p: &model::Profile,
) {
    let drive = model::normalize_mount_point(&p.drive);
    if drive_occupied(&drive) {
        ui.set_status_text(format!("盘符 {drive} 已被占用，请更换后挂载").into());
        return;
    }
    // Another saved config already mounts this drive -> conflict.
    if model::read_mounts(&state.borrow().profiles)
        .iter()
        .any(|m| m.alive && m.drive == drive)
    {
        ui.set_status_text(format!("盘符 {drive} 已被其他配置挂载，请更换").into());
        return;
    }
    let spawned = if p.mode == "oss" {
        let Some(ossmount) = ossmount.as_ref() else {
            #[cfg(windows)]
            ui.set_status_text("未找到 ossmount.exe（OSS 直挂需要 Windows + WinFsp）".into());
            #[cfg(not(windows))]
            ui.set_status_text(
                "未找到 ossmount（OSS 直挂需要 macOS + macFUSE，请先安装 macFUSE）".into(),
            );
            return;
        };
        model::spawn_oss_mount(ossmount, p)
    } else {
        let Some(brewfs) = brewfs.as_ref() else {
            ui.set_status_text("未找到 brewfs.exe（可用环境变量 BREWFS_EXE 指定）".into());
            return;
        };
        model::spawn_mount(brewfs, p)
    };
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
            let detail = if p.mode == "oss" || p.backend == "s3" {
                // 行内只展示“盘名”（对象存储即 bucket），其余参数在编辑表单里看。
                p.s3_bucket.clone()
            } else {
                p.data_dir.clone()
            };
            ProfileRecord {
                name: p.name.clone().into(),
                drive: drive.into(),
                detail: detail.into(),
                mounted: m.map(|m| m.alive).unwrap_or(false),
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

/// Whether a Windows drive letter is currently in use (system drive or
/// already-mounted). macOS/Linux mount points are directories, no check.
#[cfg(windows)]
fn drive_occupied(drive: &str) -> bool {
    let d = model::normalize_mount_point(drive);
    winutil::used_drives()
        .iter()
        .any(|u| u.eq_ignore_ascii_case(&d))
}

#[cfg(not(windows))]
fn drive_occupied(_drive: &str) -> bool {
    false
}

/// The mount point currently selected in the form: the chosen drive letter
/// on Windows (from the dropdown), the typed directory on macOS/Linux.
fn drive_from_form(edit: &EditDialog) -> String {
    #[cfg(windows)]
    {
        // Prefer the typed/selected value (the dropdown's selected() keeps
        // cfg-drive in sync, and profile_to_form sets it from a saved record,
        // so a saved drive is never silently replaced by the dropdown).
        let typed = edit.get_cfg_drive().to_string();
        if !typed.is_empty() {
            return typed;
        }
        // Fresh form: default to the first free drive.
        winutil::free_drives().first().cloned().unwrap_or_default()
    }
    #[cfg(not(windows))]
    {
        edit.get_cfg_drive().to_string()
    }
}

fn profile_to_form(edit: &EditDialog, p: &model::Profile) {
    edit.set_cfg_name(p.name.clone().into());
    edit.set_cfg_mode_index(if p.mode == "oss" { 0 } else { 1 });
    edit.set_cfg_drive(p.drive.clone().into());
    #[cfg(windows)]
    {
        let free = winutil::free_drives();
        let idx = free
            .iter()
            .position(|d| d.eq_ignore_ascii_case(&p.drive))
            .unwrap_or(0);
        edit.set_cfg_drive_index(idx as i32);
    }
    edit.set_cfg_backend_index(if p.backend == "s3" { 1 } else { 0 });
    edit.set_cfg_data_dir(p.data_dir.clone().into());
    edit.set_cfg_s3_bucket(p.s3_bucket.clone().into());
    edit.set_cfg_s3_endpoint(p.s3_endpoint.clone().into());
    edit.set_cfg_s3_region(p.s3_region.clone().into());
    edit.set_cfg_s3_access_key(p.access_key.clone().into());
    edit.set_cfg_s3_secret_key(p.secret_key.clone().into());
    edit.set_cfg_s3_force_path_style(p.s3_force_path_style);
    edit.set_cfg_prefix(p.prefix.clone().into());
    edit.set_cfg_meta_index(match p.meta_backend.as_str() {
        "redis" => 1,
        "etcd" => 2,
        "tikv" => 3,
        _ => 0,
    });
    edit.set_cfg_meta_url(p.meta_url.clone().into());
}

fn form_to_profile(edit: &EditDialog) -> model::Profile {
    let mode = if edit.get_cfg_mode_index() == 0 {
        "oss"
    } else {
        "brewfs"
    };
    let backend = if edit.get_cfg_backend_index() == 1 {
        "s3"
    } else {
        "local-fs"
    };
    let meta_backend = match edit.get_cfg_meta_index() {
        1 => "redis",
        2 => "etcd",
        3 => "tikv",
        _ => "sqlx",
    };
    model::Profile {
        name: edit.get_cfg_name().to_string(),
        mode: mode.to_string(),
        drive: drive_from_form(edit),
        backend: backend.to_string(),
        data_dir: edit.get_cfg_data_dir().to_string(),
        s3_bucket: edit.get_cfg_s3_bucket().to_string(),
        s3_endpoint: edit.get_cfg_s3_endpoint().to_string(),
        s3_region: edit.get_cfg_s3_region().to_string(),
        s3_force_path_style: edit.get_cfg_s3_force_path_style(),
        s3_disable_payload_checksum: true,
        prefix: edit.get_cfg_prefix().to_string(),
        access_key: edit.get_cfg_s3_access_key().to_string(),
        secret_key: edit.get_cfg_s3_secret_key().to_string(),
        meta_backend: meta_backend.to_string(),
        meta_url: edit.get_cfg_meta_url().to_string(),
    }
}

/// Show the Slint modal confirm dialog and run `on_yes` when the user
/// confirms. The dialog is a separate always-shown-on-top window, so it is
/// never hidden behind other windows (unlike the old Win32 MessageBox).
fn ask_confirm(
    ui: &MainWindow,
    pending: &Rc<RefCell<Option<Box<dyn FnOnce()>>>>,
    message: &str,
    on_yes: impl FnOnce() + 'static,
) {
    ui.set_dlg_message(message.into());
    ui.set_dlg_visible(true);
    *pending.borrow_mut() = Some(Box::new(on_yes));
}

/// Prefer a graceful control-plane unmount (`brewfs unmount <drive>`); only
/// fall back to force-killing the process when brewfs is missing or does not
/// accept the request (e.g. an older binary without the `unmount` subcommand).
fn graceful_or_kill(ui: &MainWindow, brewfs: &Option<PathBuf>, m: &model::MountStatus) {
    // Metadata-less ossmount instances have no control plane to shut down
    // gracefully; data is flushed on close, so terminating is safe.
    if m.is_oss {
        match winutil::terminate_process(m.pid) {
            Ok(()) => {
                guard().desired.remove(&m.drive);
                // Drop the stale drive icon from "This PC" right away.
                winutil::notify_drive_removed(&m.drive);
                ui.set_status_text(format!("已卸载 {}", m.drive).into());
            }
            Err(e) => ui.set_status_text(format!("卸载 {} 失败：{e}", m.drive).into()),
        }
        return;
    }
    if let Some(brewfs) = brewfs {
        match model::graceful_unmount(brewfs, &m.drive) {
            Ok(true) => {
                ui.set_status_text(format!("已请求优雅卸载 {}", m.drive).into());
                return;
            }
            Ok(false) => {
                ui.set_status_text(format!("{} 未接受优雅卸载请求，改用强制结束", m.drive).into());
            }
            Err(e) => {
                ui.set_status_text(format!("优雅卸载 {} 失败：{e}，改用强制结束", m.drive).into());
            }
        }
    }
    match winutil::terminate_process(m.pid) {
        Ok(()) => ui.set_status_text(format!("已强制结束 {}", m.drive).into()),
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

/// Windows: bring the tray's main window to the foreground.
///
/// `ui.show()` alone does not raise a hidden winit window, and Slint has no
/// public bring-to-front API, so we do it natively: find the largest top-level
/// window owned by this process, restore it if minimized, then use the
/// standard tray-app sequence (topmost-toggle + SetForegroundWindow, with an
/// AttachThreadInput fallback) so the window always lands on top and focused.
#[cfg(target_os = "windows")]
fn raise_window_to_front() {
    use std::cell::Cell;
    use std::ptr::null_mut;
    use windows_sys::Win32::Foundation::{BOOL, HWND, LPARAM};
    use windows_sys::Win32::System::Threading::{AttachThreadInput, GetCurrentThreadId};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        BringWindowToTop, EnumWindows, GetForegroundWindow, GetWindowTextW,
        GetWindowThreadProcessId, HWND_NOTOPMOST, HWND_TOPMOST, IsIconic, SW_RESTORE, SW_SHOW,
        SWP_NOMOVE, SWP_NOSIZE, SWP_SHOWWINDOW, SetForegroundWindow, SetWindowPos, ShowWindow,
    };

    // The tray's "show window" action targets the main window only. The edit
    // dialog is a separate hidden window and must never be raised by it, so we
    // match the main window by its title ("BrewFS") instead of picking the
    // largest top-level window (the edit dialog is larger than the main one).
    thread_local! {
        static MAIN_HWND: Cell<HWND> = const { Cell::new(null_mut()) };
    }

    unsafe extern "system" fn enum_proc(hwnd: HWND, _: LPARAM) -> BOOL {
        unsafe {
            let mut pid: u32 = 0;
            let _ = GetWindowThreadProcessId(hwnd, &mut pid);
            if pid == std::process::id() {
                let mut title = [0u16; 128];
                let len = GetWindowTextW(hwnd, title.as_mut_ptr(), title.len() as i32);
                if len > 0 {
                    let t = String::from_utf16_lossy(&title[..len as usize]);
                    if t == "BrewFS" {
                        MAIN_HWND.with(|c| c.set(hwnd));
                    }
                }
            }
        }
        1
    }

    unsafe {
        MAIN_HWND.with(|c| c.set(null_mut()));
        EnumWindows(Some(enum_proc), 0);

        let hwnd = MAIN_HWND.with(|c| c.get());
        if hwnd.is_null() {
            return;
        }
        if IsIconic(hwnd) != 0 {
            ShowWindow(hwnd, SW_RESTORE);
        }
        ShowWindow(hwnd, SW_SHOW);

        // Tray-app foreground trick: temporarily mark topmost to force
        // activation, then restore normal z-order so the flag does not stick.
        SetWindowPos(
            hwnd,
            HWND_TOPMOST,
            0,
            0,
            0,
            0,
            SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW,
        );
        SetWindowPos(hwnd, HWND_NOTOPMOST, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE);

        if SetForegroundWindow(hwnd) == 0 {
            let foreground = GetForegroundWindow();
            if !foreground.is_null() {
                let fg_thread = GetWindowThreadProcessId(foreground, null_mut());
                let cur_thread = GetCurrentThreadId();
                if AttachThreadInput(cur_thread, fg_thread, 1) != 0 {
                    SetForegroundWindow(hwnd);
                    AttachThreadInput(cur_thread, fg_thread, 0);
                }
            }
        }
        BringWindowToTop(hwnd);
    }
}

/// Non-Windows/non-macOS platforms: `show()` is enough.
#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn raise_window_to_front() {}

/// Native HWND of a Slint window (Windows). Slint exposes the underlying
/// winit window via `WinitWindowAccessor`; from there the raw window handle
/// yields the Win32 `HWND`.
#[cfg(windows)]
fn get_hwnd(window: &slint::Window) -> Option<isize> {
    use slint::winit_030::{WinitWindowAccessor, winit::raw_window_handle::HasWindowHandle};
    window.with_winit_window(|winit_window| {
        let raw = winit_window.window_handle().ok()?;
        match raw.as_raw() {
            slint::winit_030::winit::raw_window_handle::RawWindowHandle::Win32(h) => {
                Some(h.hwnd.get())
            }
            _ => None,
        }
    })?
}

/// Refresh the drive-letter dropdown of the edit dialog before showing it.
#[cfg(windows)]
fn update_drive_options(edit: &EditDialog) {
    use slint::{ModelRc, SharedString, VecModel};
    let free = winutil::free_drives();
    let options: Vec<SharedString> = free.iter().map(|d| SharedString::from(d.clone())).collect();
    edit.set_drive_options(ModelRc::new(Rc::new(VecModel::from(options))));
    let current = edit.get_cfg_drive().to_string();
    if let Some(idx) = free.iter().position(|d| d.eq_ignore_ascii_case(&current)) {
        edit.set_cfg_drive_index(idx as i32);
    }
}

/// Open the add/edit dialog as a real Win32 **owned window** of the main
/// window: no taskbar button, always above its owner, and the owner is
/// disabled so the dialog is genuinely modal (mouse/keyboard can only reach
/// the dialog).
fn open_edit_dialog(ui: &MainWindow, edit: &EditDialog, title: String) {
    edit.set_edit_title(title.into());
    #[cfg(windows)]
    update_drive_options(edit);
    // Secondary Slint windows can keep a stale default OS size; force the
    // dialog through Slint's own size API so the form renders at 620x560.
    edit.window()
        .set_size(slint::WindowSize::Logical(slint::LogicalSize::new(
            620.0, 560.0,
        )));
    let _ = edit.show();
    #[cfg(windows)]
    make_modal_child(edit.window(), ui.window());
}

/// Close the edit dialog: re-enable the owner window and hide the dialog.
fn close_edit_dialog(ui: &MainWindow, edit: &EditDialog) {
    #[cfg(windows)]
    restore_modal(ui.window());
    let _ = edit.hide();
}

/// Make `dialog` an owned window of `owner` (no taskbar entry, above owner),
/// center it over the owner, and disable the owner for modality.
#[cfg(windows)]
fn make_modal_child(dialog: &slint::Window, owner: &slint::Window) {
    use windows_sys::Win32::Foundation::{HWND, RECT};
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::EnableWindow;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GWLP_HWNDPARENT, GetSystemMetrics, GetWindowRect, HWND_TOP, SPI_GETWORKAREA, SWP_NOSIZE,
        SetForegroundWindow, SetWindowLongPtrW, SetWindowPos, SystemParametersInfoW,
    };
    unsafe {
        let Some(owner_hwnd) = get_hwnd(owner) else {
            return;
        };
        let Some(dlg_hwnd) = get_hwnd(dialog) else {
            return;
        };
        let owner_hwnd = owner_hwnd as HWND;
        let dlg_hwnd = dlg_hwnd as HWND;

        // Owned window: disappears from the taskbar, stays above the owner,
        // minimizes/hides together with it.
        SetWindowLongPtrW(dlg_hwnd, GWLP_HWNDPARENT, owner_hwnd as isize);
        // Modal: the owner cannot receive input while the dialog is open.
        EnableWindow(owner_hwnd, 0);

        // Center the dialog in the screen work area (excludes the taskbar),
        // not over the owner: it is an owned child but should read as a
        // centered modal dialog on the desktop.
        let mut dr = RECT {
            left: 0,
            top: 0,
            right: 0,
            bottom: 0,
        };
        let mut wa = RECT {
            left: 0,
            top: 0,
            right: 0,
            bottom: 0,
        };
        let work_area_ok = SystemParametersInfoW(
            SPI_GETWORKAREA,
            0,
            &mut wa as *mut RECT as *mut std::ffi::c_void,
            0,
        ) != 0;
        if GetWindowRect(dlg_hwnd, &mut dr) != 0 {
            let (w, h) = if work_area_ok && wa.right > wa.left && wa.bottom > wa.top {
                ((wa.right - wa.left), (wa.bottom - wa.top))
            } else {
                (
                    GetSystemMetrics(0), // SM_CXSCREEN
                    GetSystemMetrics(1), // SM_CYSCREEN
                )
            };
            let x = (w - (dr.right - dr.left)) / 2;
            let y = (h - (dr.bottom - dr.top)) / 2;
            SetWindowPos(dlg_hwnd, HWND_TOP, x, y, 0, 0, SWP_NOSIZE);
        }
        // Bring the dialog to the foreground so it is immediately visible and
        // focused (an owned window only stays above its owner, not other apps).
        SetForegroundWindow(dlg_hwnd);
    }
}

/// Re-enable the owner window after the edit dialog closes.
#[cfg(windows)]
fn restore_modal(owner: &slint::Window) {
    use windows_sys::Win32::Foundation::HWND;
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::EnableWindow;
    unsafe {
        if let Some(h) = get_hwnd(owner) {
            EnableWindow(h as HWND, 1);
        }
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
