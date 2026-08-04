//! BrewFS desktop tray manager (Slint 1.17).
//!
//! A small Windows system-tray app that keeps a list of saved BrewFS mount
//! profiles (config records), shows their live mount state, and lets the user
//! open / mount|unmount / delete each record from one list, edit the selected
//! profile in the form, and add new configs.
//!
//! Requires a brewfs build with the `fuse-winfsp` feature (and `ossmount` for
//! the metadata-less OSS direct-mount mode). Both binaries are located next
//! to this executable, via `BREWFS_EXE` / `OSSMOUNT_EXE`, or on PATH.

#![cfg_attr(windows, windows_subsystem = "windows")]
#![cfg_attr(not(windows), allow(dead_code))]

mod model;
mod winutil;

use std::cell::{Cell, RefCell};
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let window_visible = Rc::new(Cell::new(false));
    let brewfs = Rc::new(model::find_brewfs());
    let ossmount = Rc::new(model::find_ossmount());

    let brewfs_display = match brewfs.as_ref() {
        Some(p) => p.display().to_string(),
        None => "未找到 brewfs.exe（可用环境变量 BREWFS_EXE 指定）".to_string(),
    };
    ui.set_brewfs_path(SharedString::from(brewfs_display));

    let ossmount_display = match ossmount.as_ref() {
        Some(p) => p.display().to_string(),
        None => "未找到 ossmount.exe（可用环境变量 OSSMOUNT_EXE 指定）".to_string(),
    };
    ui.set_ossmount_path(SharedString::from(ossmount_display));

    // Drop stale runtime records from earlier crashed/force-killed mounts so
    // both the tray status and `brewfs info` stay accurate.
    model::prune_stale_records();

    refresh(&ui, &tray, &state, &recent, &window_visible);

    // Preload the first saved profile into the form.
    if !state.borrow().profiles.is_empty() {
        profile_to_form(&ui, &state.borrow().profiles[0]);
    }

    wire_callbacks(
        &ui,
        &tray,
        &state,
        &recent,
        &window_visible,
        &brewfs,
        &ossmount,
    );

    // Periodic status refresh (2s) driven from the UI thread.
    let timer = Timer::default();
    {
        let ui_weak = ui.as_weak();
        let tray_weak = tray.as_weak();
        let state = state.clone();
        let recent = recent.clone();
        let window_visible = window_visible.clone();
        timer.start(TimerMode::Repeated, Duration::from_secs(2), move || {
            if let (Some(ui), Some(tray)) = (ui_weak.upgrade(), tray_weak.upgrade()) {
                refresh(&ui, &tray, &state, &recent, &window_visible);
            }
        });
    }

    tray.show()?;
    ui.show()?;
    window_visible.set(true);
    tray.set_window_visible(true);
    ui.set_status_text(SharedString::from("BrewFS 托盘已就绪"));
    refresh(&ui, &tray, &state, &recent, &window_visible);

    slint::run_event_loop()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn wire_callbacks(
    ui: &MainWindow,
    tray: &Tray,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
    window_visible: &Rc<Cell<bool>>,
    brewfs: &Rc<Option<PathBuf>>,
    ossmount: &Rc<Option<PathBuf>>,
) {
    let ui_weak = ui.as_weak();
    let tray_weak = tray.as_weak();
    let state = Rc::clone(state);
    let recent = Rc::clone(recent);
    let window_visible = Rc::clone(window_visible);
    let brewfs = Rc::clone(brewfs);
    let ossmount = Rc::clone(ossmount);

    // --- save the form back into a profile ---
    ui.on_save_form({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        let window_visible = window_visible.clone();
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
                refresh(&ui, &tray, &state, &recent, &window_visible);
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
        let window_visible = window_visible.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let name = {
                let mut file = state.borrow_mut();
                let name = format!("新建配置 {}", file.profiles.len() + 1);
                let p = model::Profile {
                    name: name.clone(),
                    ..model::Profile::default()
                };
                file.profiles.push(p.clone());
                if let Err(e) = model::save_profiles(&file) {
                    ui.set_status_text(format!("添加失败：{e}").into());
                    return;
                }
                profile_to_form(&ui, &p);
                name
            };
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent, &window_visible);
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
        let window_visible = window_visible.clone();
        let brewfs = brewfs.clone();
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
                    graceful_or_kill(&ui, brewfs.as_ref(), m);
                } else {
                    ui.set_status_text(format!("已取消卸载 {drive}").into());
                }
            } else {
                // not mounted -> mount
                if let Err(e) = p.validate() {
                    ui.set_status_text(format!("挂载失败：{e}").into());
                } else {
                    mount_profile(
                        &ui,
                        &tray_weak,
                        &state,
                        &recent,
                        &window_visible,
                        &brewfs,
                        &ossmount,
                        &p,
                    );
                }
            }
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent, &window_visible);
            }
        }
    });

    // --- delete a config record ---
    ui.on_delete_record({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        let window_visible = window_visible.clone();
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
                refresh(&ui, &tray, &state, &recent, &window_visible);
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
        let window_visible = window_visible.clone();
        let brewfs = brewfs.clone();
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
                graceful_or_kill(&ui, brewfs.as_ref(), m);
            }
            ui.set_status_text(format!("已请求卸载 {} 个挂载", live.len()).into());
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent, &window_visible);
            }
        }
    });

    // --- window close -> hide to tray ---
    ui.window().on_close_requested({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let window_visible = window_visible.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.hide();
            }
            window_visible.set(false);
            if let Some(tray) = tray_weak.upgrade() {
                tray.set_window_visible(false);
            }
            slint::CloseRequestResponse::HideWindow
        }
    });
    tray.on_show_window({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let window_visible = window_visible.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.show();
            }
            window_visible.set(true);
            if let Some(tray) = tray_weak.upgrade() {
                tray.set_window_visible(true);
            }
        }
    });
    ui.on_quit_app(quit_app);
    tray.on_quit_app(quit_app);
}

/// Spawn the right backend for `p` (brewfs vs ossmount), remember the profile,
/// and report progress. Used by the per-record mount action.
#[allow(clippy::too_many_arguments)]
fn mount_profile(
    ui: &MainWindow,
    tray_weak: &slint::Weak<Tray>,
    state: &Rc<RefCell<model::ProfilesFile>>,
    recent: &Rc<RefCell<Vec<RecentSpawn>>>,
    window_visible: &Rc<Cell<bool>>,
    brewfs: &Rc<Option<PathBuf>>,
    ossmount: &Rc<Option<PathBuf>>,
    p: &model::Profile,
) {
    let drive = model::normalize_mount_point(&p.drive);
    let spawned = if p.mode == "oss" {
        let Some(ossmount) = ossmount.as_ref() else {
            ui.set_status_text("未找到 ossmount.exe（可用环境变量 OSSMOUNT_EXE 指定）".into());
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
        refresh(ui, &tray, state, recent, window_visible);
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
    window_visible: &Rc<Cell<bool>>,
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
            let (backend, detail) = if p.mode == "oss" {
                (
                    "oss".to_string(),
                    format!("{} / {}", p.s3_bucket, p.s3_endpoint.trim_end_matches('/')),
                )
            } else {
                let detail = if p.backend == "s3" {
                    format!("{} / {}", p.s3_bucket, p.s3_region)
                } else {
                    p.data_dir.clone()
                };
                (p.backend.clone(), detail)
            };
            ProfileRecord {
                name: p.name.clone().into(),
                drive: drive.into(),
                backend: backend.into(),
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
    tray.set_window_visible(window_visible.get());
}

fn profile_to_form(ui: &MainWindow, p: &model::Profile) {
    ui.set_cfg_name(p.name.clone().into());
    ui.set_cfg_mode_index(if p.mode == "oss" { 1 } else { 0 });
    ui.set_cfg_drive(p.drive.clone().into());
    ui.set_cfg_backend_index(if p.backend == "s3" { 1 } else { 0 });
    ui.set_cfg_data_dir(p.data_dir.clone().into());
    ui.set_cfg_s3_bucket(p.s3_bucket.clone().into());
    ui.set_cfg_s3_endpoint(p.s3_endpoint.clone().into());
    ui.set_cfg_s3_region(p.s3_region.clone().into());
    ui.set_cfg_s3_access_key(p.access_key.clone().into());
    ui.set_cfg_s3_secret_key(p.secret_key.clone().into());
    ui.set_cfg_s3_force_path_style(p.s3_force_path_style);
    ui.set_cfg_prefix(p.prefix.clone().into());
    ui.set_cfg_meta_index(match p.meta_backend.as_str() {
        "redis" => 1,
        "etcd" => 2,
        "tikv" => 3,
        _ => 0,
    });
    ui.set_cfg_meta_url(p.meta_url.clone().into());
}

fn form_to_profile(ui: &MainWindow) -> model::Profile {
    let mode = if ui.get_cfg_mode_index() == 1 {
        "oss"
    } else {
        "brewfs"
    };
    let backend = if ui.get_cfg_backend_index() == 1 {
        "s3"
    } else {
        "local-fs"
    };
    let meta_backend = match ui.get_cfg_meta_index() {
        1 => "redis",
        2 => "etcd",
        3 => "tikv",
        _ => "sqlx",
    };
    model::Profile {
        name: ui.get_cfg_name().to_string(),
        mode: mode.to_string(),
        drive: ui.get_cfg_drive().to_string(),
        backend: backend.to_string(),
        data_dir: ui.get_cfg_data_dir().to_string(),
        s3_bucket: ui.get_cfg_s3_bucket().to_string(),
        s3_endpoint: ui.get_cfg_s3_endpoint().to_string(),
        s3_region: ui.get_cfg_s3_region().to_string(),
        s3_force_path_style: ui.get_cfg_s3_force_path_style(),
        s3_disable_payload_checksum: true,
        prefix: ui.get_cfg_prefix().to_string(),
        access_key: ui.get_cfg_s3_access_key().to_string(),
        secret_key: ui.get_cfg_s3_secret_key().to_string(),
        meta_backend: meta_backend.to_string(),
        meta_url: ui.get_cfg_meta_url().to_string(),
    }
}

/// Prefer a graceful control-plane unmount (`brewfs unmount <drive>`); only
/// fall back to force-killing the process when brewfs is missing or does not
/// accept the request (e.g. an older binary without the `unmount` subcommand).
fn graceful_or_kill(ui: &MainWindow, brewfs: &Option<PathBuf>, m: &model::MountStatus) {
    // Metadata-less ossmount instances have no control plane to shut down
    // gracefully; data is flushed on close, so terminating is safe.
    if m.is_oss {
        match winutil::terminate_process(m.pid) {
            Ok(()) => ui.set_status_text(format!("已卸载 {}", m.drive).into()),
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
    #[cfg(not(windows))]
    {
        let _ = target;
    }
}

fn quit_app() {
    let _ = slint::quit_event_loop();
}
