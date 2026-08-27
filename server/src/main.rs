#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
mod app {
    use fslock::LockFile;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tao::event::Event;
    use tao::event_loop::{ControlFlow, EventLoopBuilder};

    #[global_allocator]
    static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

    pub fn main() -> anyhow::Result<()> {
        #[cfg(windows)]
        let attached_console = unsafe {
            use windows::Win32::System::Console::{
                ATTACH_PARENT_PROCESS, AttachConsole, SetConsoleCtrlHandler,
            };
            let attached = AttachConsole(ATTACH_PARENT_PROCESS).is_ok();
            if attached {
                let _ = SetConsoleCtrlHandler(None, false);
            }
            attached
        };
        #[cfg(not(windows))]
        let attached_console = true;

        let lock_path = std::env::temp_dir().join("stream-server.lock");
        let mut lockfile = LockFile::open(&lock_path)?;

        if !lockfile.try_lock()? {
            if attached_console {
                eprintln!("Exiting, another instance is running.");
            }
            return Ok(());
        }

        let args: Vec<String> = std::env::args().collect();
        let no_tray = args.iter().any(|a| a == "--no-tray");
        let use_tui = args.iter().any(|a| a == "--tui");
        let is_release = !cfg!(debug_assertions);
        let silent_mode =
            !no_tray && (args.iter().any(|a| a == "--silent") || !attached_console || is_release);

        let result = if silent_mode {
            run_tray_mode(use_tui)
        } else {
            run_headless_mode(use_tui)
        };

        if let Err(err) = result {
            if is_missing_ffmpeg_error(&err) {
                show_desktop_error_and_exit(&err);
            }
            return Err(err);
        }

        Ok(())
    }

    fn run_tray_mode(use_tui: bool) -> anyhow::Result<()> {
        let event_loop =
            EventLoopBuilder::<stream_server::tray::UserEvent>::with_user_event().build();
        let tray_handle = match stream_server::tray::create_system_tray(&event_loop) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Failed to create tray icon: {}", e);
                return Err(e);
            }
        };

        let tray_stats = Arc::new(stream_server::tray::TrayStats::new());
        let server_control: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Sender<()>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let keep_running = Arc::new(AtomicBool::new(true));

        let server_control_clone = server_control.clone();
        let keep_running_clone = keep_running.clone();
        let tray_stats_clone = tray_stats.clone();

        std::thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("Failed to create Tokio runtime: {}", err);
                    return;
                }
            };

            loop {
                if !keep_running_clone.load(Ordering::Relaxed) {
                    break;
                }

                let (tx, rx) = tokio::sync::mpsc::channel(1);
                *server_control_clone.lock().unwrap() = Some(tx);

                let mut cfg = stream_server::ServerConfig::binary_default();
                cfg.use_tui = use_tui;
                cfg.exit_process_on_shutdown_timeout = false;

                match rt.block_on(stream_server::run_with_tray_stats(
                    cfg,
                    rx,
                    tray_stats_clone.clone(),
                    None,
                )) {
                    Ok(Some(stream_server::ShutdownSource::CtrlC)) => {
                        tracing::info!("Ctrl+C in tray mode, forcing process exit");
                        std::process::exit(0);
                    }
                    Ok(_) => {}
                    Err(e) if is_missing_ffmpeg_error(&e) => {
                        show_desktop_error_and_exit(&e);
                    }
                    Err(e) => {
                        eprintln!("Server crashed: {}", e);
                        std::thread::sleep(std::time::Duration::from_secs(2));
                    }
                }

                *server_control_clone.lock().unwrap() = None;
            }
        });

        let proxy = event_loop.create_proxy();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(2));
                if proxy
                    .send_event(stream_server::tray::UserEvent::UpdateStats)
                    .is_err()
                {
                    break;
                }
            }
        });

        let tray_icon = tray_handle.tray_icon;
        let stats_item = tray_handle.stats_item;
        let update_item = tray_handle.update_item;
        let auto_update_item = tray_handle.auto_update_item;
        let seeding_item = tray_handle.seeding_item;
        let install_update_item = tray_handle.install_update_item;

        event_loop.run(move |event, _, control_flow| {
            *control_flow = ControlFlow::Wait;

            if let Event::UserEvent(e) = event {
                match e {
                    stream_server::tray::UserEvent::OpenWeb => {
                        stream_server::tray::open_stremio_web();
                    }
                    stream_server::tray::UserEvent::OpenSettings => {
                        let url = format!("http://127.0.0.1:{}", stream_server::DEFAULT_HTTP_PORT);
                        if let Err(err) = stream_server::tray::open_settings_gui(&url) {
                            tracing::error!("Failed to open settings GUI: {}", err);
                            show_error_dialog(
                                "Stream Server Settings",
                                &format!(
                                    "Could not open the settings window.\n\n{err}\n\nBuild or install the settings GUI next to the server executable."
                                ),
                            );
                        }
                    }
                    stream_server::tray::UserEvent::OpenLogs => {
                        stream_server::tray::open_log_folder();
                    }
                    stream_server::tray::UserEvent::Restart => {
                        tracing::info!("Restart requested from tray");
                        let tx = server_control.lock().unwrap().take();
                        if let Some(tx) = tx {
                            let _ = tx.blocking_send(());
                        }
                    }
                    stream_server::tray::UserEvent::ToggleSeeding => {
                        let enabled = seeding_item.is_checked();
                        tray_stats.set_seeding_enabled(enabled);
                        stream_server::tray::trigger_seeding_toggle(enabled);
                    }
                    stream_server::tray::UserEvent::ToggleAutoUpdate => {
                        let enabled = auto_update_item.is_checked();
                        tray_stats.set_auto_update_enabled(enabled);
                        stream_server::tray::trigger_auto_update_toggle(enabled);
                    }
                    stream_server::tray::UserEvent::CheckUpdates => {
                        stream_server::tray::trigger_update_check();
                    }
                    stream_server::tray::UserEvent::InstallUpdate => {
                        stream_server::tray::trigger_update_install();
                    }
                    stream_server::tray::UserEvent::Quit => {
                        keep_running.store(false, Ordering::Relaxed);
                        tracing::info!("Quit requested from tray");
                        let tx = server_control.lock().unwrap().take();
                        if let Some(tx) = tx {
                            let _ = tx.blocking_send(());
                        }
                        *control_flow = ControlFlow::Exit;
                    }
                    stream_server::tray::UserEvent::UpdateStats => {
                        let tooltip = tray_stats.format_tooltip();
                        let stats_line = tray_stats.format_stats_line();

                        tray_icon.set_tooltip(Some(&tooltip)).ok();
                        stats_item.set_text(&stats_line);
                        update_item.set_text(tray_stats.format_update_line());
                        auto_update_item.set_checked(tray_stats.auto_update_enabled());
                        seeding_item.set_checked(tray_stats.seeding_enabled());
                        install_update_item.set_enabled(tray_stats.update_install_enabled());
                    }
                }
            }
        })
    }

    fn run_headless_mode(use_tui: bool) -> anyhow::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut cfg = stream_server::ServerConfig::binary_default();
        cfg.use_tui = use_tui;
        let _ = rt.block_on(stream_server::run(cfg, rx, None))?;
        Ok(())
    }

    fn is_missing_ffmpeg_error(err: &anyhow::Error) -> bool {
        err.downcast_ref::<stream_server::MissingFfmpegError>()
            .is_some()
    }

    fn show_desktop_error_and_exit(err: &anyhow::Error) -> ! {
        let details = err
            .downcast_ref::<stream_server::MissingFfmpegError>()
            .map(|err| err.details().to_string())
            .unwrap_or_else(|| err.to_string());
        let message = format!(
            "{details}\n\nInstall FFmpeg, make sure both ffmpeg and ffprobe are available in PATH, then start Stream Server again."
        );

        show_error_dialog("Stream Server requires FFmpeg", &message);
        std::process::exit(1);
    }

    #[cfg(windows)]
    fn show_error_dialog(title: &str, message: &str) {
        use windows::Win32::UI::WindowsAndMessaging::{MB_ICONERROR, MB_OK, MessageBoxW};
        use windows::core::PCWSTR;

        let title = title
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let message = message
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();

        unsafe {
            let _ = MessageBoxW(
                None,
                PCWSTR(message.as_ptr()),
                PCWSTR(title.as_ptr()),
                MB_OK | MB_ICONERROR,
            );
        }
    }

    #[cfg(target_os = "macos")]
    fn show_error_dialog(title: &str, message: &str) {
        let script = format!(
            "display dialog \"{}\" with title \"{}\" buttons {{\"Quit\"}} default button \"Quit\" with icon stop",
            escape_applescript(message),
            escape_applescript(title)
        );

        if std::process::Command::new("osascript")
            .arg("-e")
            .arg(script)
            .status()
            .is_err()
        {
            eprintln!("{title}: {message}");
        }
    }

    #[cfg(target_os = "macos")]
    fn escape_applescript(value: &str) -> String {
        value.replace('\\', "\\\\").replace('"', "\\\"")
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    fn show_error_dialog(title: &str, message: &str) {
        let attempts = [
            (
                "zenity",
                vec!["--error", "--title", title, "--text", message],
            ),
            ("kdialog", vec!["--title", title, "--error", message]),
            ("xmessage", vec!["-center", "-title", title, message]),
        ];

        for (program, args) in attempts {
            if let Ok(status) = std::process::Command::new(program).args(args).status()
                && status.success()
            {
                return;
            }
        }

        eprintln!("{title}: {message}");
    }

    #[cfg(not(any(windows, unix)))]
    fn show_error_dialog(title: &str, message: &str) {
        eprintln!("{title}: {message}");
    }
}

#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    app::main()
}

#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
fn main() -> anyhow::Result<()> {
    Ok(())
}
