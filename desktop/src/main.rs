//! BrewFS desktop tray manager (Slint 1.17).
//!
//! A small Windows system-tray app that edits BrewFS mount profiles, shows
//! the current drive-letter mappings (read from the brewfs runtime registry),
//! and starts/stops `brewfs mount` processes.
//!
//! Requires a brewfs build with the `fuse-winfsp` feature. The binary is
//! located next to this executable, via `BREWFS_EXE`, or on PATH.

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
    let ui = MainWindow::new()?;
    let tray = Tray::new()?;

    let state = Rc::new(RefCell::new(model::load_profiles()));
    let recent = Rc::new(RefCell::new(Vec::<RecentSpawn>::new()));
    let window_visible = Rc::new(Cell::new(false));
    let brewfs = Rc::new(model::find_brewfs());

    let brewfs_display = match brewfs.as_ref() {
        Some(p) => p.display().to_string(),
        None => "未找到 brewfs.exe（可用环境变量 BREWFS_EXE 指定）".to_string(),
    };
    ui.set_brewfs_path(SharedString::from(brewfs_display));

    // Drop stale runtime records from earlier crashed/force-killed mounts so
    // both the tray status and `brewfs info` stay accurate.
    model::prune_stale_records();

    refresh(&ui, &tray, &state, &recent, &window_visible);

    // Preload the first saved profile into the form.
    if !state.borrow().profiles.is_empty() {
        ui.set_cfg_profile_index(0);
        profile_to_form(&ui, &state.borrow().profiles[0]);
    }

    wire_callbacks(&ui, &tray, &state, &recent, &window_visible, &brewfs);

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
) {
    let ui_weak = ui.as_weak();
    let tray_weak = tray.as_weak();
    let state = Rc::clone(state);
    let recent = Rc::clone(recent);
    let window_visible = Rc::clone(window_visible);
    let brewfs = Rc::clone(brewfs);

    // --- profile selection ---
    ui.on_profile_selected({
        let ui_weak = ui_weak.clone();
        let state = state.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let idx = ui.get_cfg_profile_index();
            let profiles = state.borrow();
            if idx >= 0 && (idx as usize) < profiles.profiles.len() {
                let p = profiles.profiles[idx as usize].clone();
                drop(profiles);
                profile_to_form(&ui, &p);
            }
        }
    });

    // --- save ---
    ui.on_save_config({
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
            let idx = {
                let mut file = state.borrow_mut();
                let pos = file.profiles.iter().position(|x| x.name == p.name);
                match pos {
                    Some(i) => file.profiles[i] = p.clone(),
                    None => file.profiles.push(p.clone()),
                }
                if let Err(e) = model::save_profiles(&file) {
                    ui.set_status_text(format!("保存失败：{e}").into());
                    return;
                }
                file.profiles.iter().position(|x| x.name == p.name).unwrap() as i32
            };
            ui.set_cfg_profile_index(idx);
            if let Some(tray) = tray_weak.upgrade() {
                refresh(&ui, &tray, &state, &recent, &window_visible);
            }
            ui.set_status_text(format!("配置「{}」已保存", p.name).into());
        }
    });

    // --- mount ---
    ui.on_mount_current({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        let window_visible = window_visible.clone();
        let brewfs = brewfs.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let p = form_to_profile(&ui);
            if let Err(e) = p.validate() {
                ui.set_status_text(format!("挂载失败：{e}").into());
                return;
            }
            let Some(brewfs) = brewfs.as_ref() else {
                ui.set_status_text("未找到 brewfs.exe（可用环境变量 BREWFS_EXE 指定）".into());
                return;
            };
            let drive = model::normalize_mount_point(&p.drive);
            let mounts = model::read_mounts(&state.borrow().profiles);
            if mounts.iter().any(|m| m.drive == drive && m.alive) {
                ui.set_status_text(format!("{drive} 已被挂载，请先卸载").into());
                return;
            }
            match model::spawn_mount(brewfs, &p) {
                Ok((pid, log)) => {
                    // Remember the profile so the drive-letter mapping shows up
                    // with its backend / data details.
                    {
                        let mut file = state.borrow_mut();
                        let pos = file.profiles.iter().position(|x| x.name == p.name);
                        match pos {
                            Some(i) => file.profiles[i] = p.clone(),
                            None => file.profiles.push(p.clone()),
                        }
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
                refresh(&ui, &tray, &state, &recent, &window_visible);
            }
        }
    });

    // --- unmount current ---
    ui.on_unmount_current({
        let ui_weak = ui_weak.clone();
        let state = state.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let drive = model::normalize_mount_point(ui.get_cfg_drive().as_str());
            unmount_drive(&ui, &state, &drive);
        }
    });

    // --- open current in explorer ---
    ui.on_open_current({
        let ui_weak = ui_weak.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let drive = model::normalize_mount_point(ui.get_cfg_drive().as_str());
            if drive.is_empty() {
                ui.set_status_text("请先填写盘符".into());
            } else {
                open_in_explorer(&drive);
            }
        }
    });

    // --- open mount from list ---
    {
        let ui_weak = ui_weak.clone();
        let state = state.clone();
        ui.on_open_mount(move |index| {
            if let Some(ui) = ui_weak.upgrade() {
                open_mount_at(&ui, &state, index);
            }
        });
    }
    {
        let ui_weak = ui_weak.clone();
        let state = state.clone();
        tray.on_open_mount(move |index| {
            if let Some(ui) = ui_weak.upgrade() {
                open_mount_at(&ui, &state, index);
            }
        });
    }

    // --- unmount from list ---
    {
        let ui_weak = ui_weak.clone();
        let state = state.clone();
        ui.on_unmount_mount(move |index| {
            if let Some(ui) = ui_weak.upgrade() {
                unmount_at(&ui, &state, index);
            }
        });
    }

    // --- unmount all (tray) ---
    tray.on_unmount_all({
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        let window_visible = window_visible.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else { return };
            let mounts = model::read_mounts(&state.borrow().profiles);
            let live: Vec<u32> = mounts.iter().filter(|m| m.alive).map(|m| m.pid).collect();
            if live.is_empty() {
                ui.set_status_text("当前没有活动挂载".into());
                return;
            }
            for pid in &live {
                let _ = winutil::terminate_process(*pid);
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

    // --- refresh button ---
    {
        let ui_weak = ui_weak.clone();
        let tray_weak = tray_weak.clone();
        let state = state.clone();
        let recent = recent.clone();
        let window_visible = window_visible.clone();
        ui.on_refresh(move || {
            if let (Some(ui), Some(tray)) = (ui_weak.upgrade(), tray_weak.upgrade()) {
                refresh(&ui, &tray, &state, &recent, &window_visible);
            }
        });
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

    let rows: Vec<MountInfo> = mounts
        .iter()
        .map(|m| MountInfo {
            drive: m.drive.clone().into(),
            backend: m.backend.clone().into(),
            detail: m.detail.clone().into(),
            pid: m.pid as i32,
            alive: m.alive,
        })
        .collect();
    let model_rc: ModelRc<MountInfo> = ModelRc::new(Rc::new(VecModel::from(rows)));
    ui.set_mounts(model_rc.clone());
    tray.set_mounts(model_rc);

    let names: Vec<SharedString> = profiles.iter().map(|p| p.name.clone().into()).collect();
    ui.set_profile_names(ModelRc::new(Rc::new(VecModel::from(names))));

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
    ui.set_cfg_drive(p.drive.clone().into());
    ui.set_cfg_backend_index(if p.backend == "s3" { 1 } else { 0 });
    ui.set_cfg_data_dir(p.data_dir.clone().into());
    ui.set_cfg_s3_bucket(p.s3_bucket.clone().into());
    ui.set_cfg_s3_endpoint(p.s3_endpoint.clone().into());
    ui.set_cfg_s3_region(p.s3_region.clone().into());
    ui.set_cfg_s3_access_key(p.access_key.clone().into());
    ui.set_cfg_s3_secret_key(p.secret_key.clone().into());
    ui.set_cfg_s3_force_path_style(p.s3_force_path_style);
    ui.set_cfg_meta_index(match p.meta_backend.as_str() {
        "redis" => 1,
        "etcd" => 2,
        "tikv" => 3,
        _ => 0,
    });
    ui.set_cfg_meta_url(p.meta_url.clone().into());
}

fn form_to_profile(ui: &MainWindow) -> model::Profile {
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
        drive: ui.get_cfg_drive().to_string(),
        backend: backend.to_string(),
        data_dir: ui.get_cfg_data_dir().to_string(),
        s3_bucket: ui.get_cfg_s3_bucket().to_string(),
        s3_endpoint: ui.get_cfg_s3_endpoint().to_string(),
        s3_region: ui.get_cfg_s3_region().to_string(),
        s3_force_path_style: ui.get_cfg_s3_force_path_style(),
        s3_disable_payload_checksum: true,
        access_key: ui.get_cfg_s3_access_key().to_string(),
        secret_key: ui.get_cfg_s3_secret_key().to_string(),
        meta_backend: meta_backend.to_string(),
        meta_url: ui.get_cfg_meta_url().to_string(),
    }
}

fn open_mount_at(ui: &MainWindow, state: &Rc<RefCell<model::ProfilesFile>>, index: i32) {
    let mounts = model::read_mounts(&state.borrow().profiles);
    let Some(m) = mounts.get(index as usize) else {
        ui.set_status_text("挂载列表已变化，请刷新".into());
        return;
    };
    open_in_explorer(&m.drive);
}

fn unmount_at(ui: &MainWindow, state: &Rc<RefCell<model::ProfilesFile>>, index: i32) {
    let mounts = model::read_mounts(&state.borrow().profiles);
    let Some(m) = mounts.get(index as usize) else {
        ui.set_status_text("挂载列表已变化，请刷新".into());
        return;
    };
    if !m.alive {
        ui.set_status_text(format!("{} 已不在运行", m.drive).into());
        return;
    }
    match winutil::terminate_process(m.pid) {
        Ok(()) => ui.set_status_text(format!("已请求卸载 {}", m.drive).into()),
        Err(e) => ui.set_status_text(format!("卸载 {} 失败：{e}", m.drive).into()),
    }
}

fn unmount_drive(ui: &MainWindow, state: &Rc<RefCell<model::ProfilesFile>>, drive: &str) {
    let mounts = model::read_mounts(&state.borrow().profiles);
    let Some(m) = mounts.iter().find(|m| m.drive == drive && m.alive) else {
        ui.set_status_text(format!("{drive} 没有活动挂载").into());
        return;
    };
    match winutil::terminate_process(m.pid) {
        Ok(()) => ui.set_status_text(format!("已请求卸载 {drive}").into()),
        Err(e) => ui.set_status_text(format!("卸载 {drive} 失败：{e}").into()),
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
