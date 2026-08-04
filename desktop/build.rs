fn main() {
    slint_build::compile("ui/brewfs_tray.slint").expect("failed to compile Slint UI");

    // Windows: embed the application icon into the .exe so Explorer, the
    // taskbar and Alt-Tab all show the BrewFS icon. macOS uses the .icns in
    // the app bundle (and the runtime window icon set by Slint).
    #[cfg(windows)]
    {
        println!("cargo:rerun-if-changed=assets/brewfs.ico");
        embed_resource::compile("app.rc", embed_resource::NONE);
    }
}
