use crate::state::AppState;
use anyhow::Result;
use crossbeam_channel::Receiver;
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use enginefs::backend::EngineStats;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs},
};

pub mod log_layer;

use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

enum View {
    Engines,
    Files,
}

#[derive(Clone, Debug)]
struct FileInfo {
    name: String,
    size: u64,
    is_dir: bool,
}

pub fn start_tui(
    app_state: Arc<AppState>, log_rx: Receiver<String>, shutdown_tx: tokio::sync::mpsc::Sender<()>,
) {
    let (stats_tx, stats_rx) = crossbeam_channel::bounded(1);
    let (files_tx, files_rx) = crossbeam_channel::bounded(1);

    // Capture the handle to the Tokio runtime
    let rt_handle = tokio::runtime::Handle::current();

    // Spawn background task to poll stats AND files
    let engine_state = app_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        let mut file_interval = tokio::time::interval(Duration::from_secs(2));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = engine_state.engine.get_all_statistics().await;
                    let _ = stats_tx.try_send(stats);
                }
                _ = file_interval.tick() => {
                    let mut files = Vec::new();
                    let path = &engine_state.engine.download_dir;
                    if let Ok(mut entries) = tokio::fs::read_dir(path).await {
                        while let Ok(Some(entry)) = entries.next_entry().await {
                            if let Ok(meta) = entry.metadata().await {
                                let name = entry.file_name().to_string_lossy().to_string();
                                files.push(FileInfo {
                                    name,
                                    size: meta.len(),
                                    is_dir: meta.is_dir(),
                                });
                            }
                        }
                    }
                    files.sort_by(|a, b| a.name.cmp(&b.name));
                    let _ = files_tx.try_send(files);
                }
            }
        }
    });

    thread::spawn(move || {
        if let Err(e) = run_tui(
            app_state,
            log_rx,
            stats_rx,
            files_rx,
            rt_handle,
            shutdown_tx,
        ) {
            eprintln!("TUI Error: {}", e);
        }
    });
}

fn run_tui(
    app_state: Arc<AppState>, log_rx: Receiver<String>,
    stats_rx: Receiver<HashMap<String, EngineStats>>, files_rx: Receiver<Vec<FileInfo>>,
    rt: tokio::runtime::Handle, shutdown_tx: tokio::sync::mpsc::Sender<()>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // State
    let mut logs = VecDeque::with_capacity(500);
    // Use a Vec for stable ordering in the UI, enabling selection by index
    let mut sorted_hashes: Vec<String> = Vec::new();
    let mut stats: HashMap<String, EngineStats> = HashMap::new();
    let mut files: Vec<FileInfo> = Vec::new();

    let mut view = View::Engines;
    let mut engine_list_state = ListState::default();
    let mut file_list_state = ListState::default();

    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();

    loop {
        // Drain logs
        while let Ok(msg) = log_rx.try_recv() {
            if logs.len() >= 500 {
                logs.pop_front();
            }
            logs.push_back(msg);
        }

        // Check stats
        if let Ok(new_stats) = stats_rx.try_recv() {
            stats = new_stats;
            // Update sorted list. We try to keep order stable if possible, or just sort by name?
            // Sorting by name is good UX.
            sorted_hashes = stats.keys().cloned().collect();
            sorted_hashes.sort_by(|a, b| {
                let name_a = &stats[a].name;
                let name_b = &stats[b].name;
                name_a.cmp(name_b)
            });

            // Adjust selection if out of bounds
            if let Some(selected) = engine_list_state.selected()
                && selected >= sorted_hashes.len()
            {
                engine_list_state.select(if sorted_hashes.is_empty() {
                    None
                } else {
                    Some(sorted_hashes.len() - 1)
                });
            }
        }

        // Check files
        if let Ok(new_files) = files_rx.try_recv() {
            files = new_files;
            if let Some(selected) = file_list_state.selected()
                && selected >= files.len()
            {
                file_list_state.select(if files.is_empty() {
                    None
                } else {
                    Some(files.len() - 1)
                });
            }
        }

        terminal.draw(|f| {
            ui(
                f,
                UiModel {
                    logs: &logs,
                    stats: &stats,
                    sorted_hashes: &sorted_hashes,
                    files: &files,
                    view: &view,
                    engine_list_state: &mut engine_list_state,
                    file_list_state: &mut file_list_state,
                },
            )
        })?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            break;
                        }
                        KeyCode::Tab => match view {
                            View::Engines => view = View::Files,
                            View::Files => view = View::Engines,
                        },
                        KeyCode::Down | KeyCode::Char('j') => match view {
                            View::Engines => {
                                let i = match engine_list_state.selected() {
                                    Some(i) => {
                                        if i >= sorted_hashes.len().saturating_sub(1) {
                                            i
                                        } else {
                                            i + 1
                                        }
                                    }
                                    None => 0,
                                };
                                engine_list_state.select(Some(i));
                            }
                            View::Files => {
                                let i = match file_list_state.selected() {
                                    Some(i) => {
                                        if i >= files.len().saturating_sub(1) {
                                            i
                                        } else {
                                            i + 1
                                        }
                                    }
                                    None => 0,
                                };
                                file_list_state.select(Some(i));
                            }
                        },
                        KeyCode::Up | KeyCode::Char('k') => match view {
                            View::Engines => {
                                let i = match engine_list_state.selected() {
                                    Some(i) => {
                                        if i == 0 {
                                            0
                                        } else {
                                            i - 1
                                        }
                                    }
                                    None => 0,
                                };
                                engine_list_state.select(Some(i));
                            }
                            View::Files => {
                                let i = match file_list_state.selected() {
                                    Some(i) => {
                                        if i == 0 {
                                            0
                                        } else {
                                            i - 1
                                        }
                                    }
                                    None => 0,
                                };
                                file_list_state.select(Some(i));
                            }
                        },
                        KeyCode::Char('d') | KeyCode::Delete => {
                            match view {
                                View::Engines => {
                                    if let Some(i) = engine_list_state.selected()
                                        && let Some(hash) = sorted_hashes.get(i)
                                    {
                                        // Trigger removal
                                        let hash = hash.clone();
                                        let app = app_state.clone();
                                        rt.spawn(async move {
                                            tracing::info!(
                                                "Users requested removal of engine: {}",
                                                hash
                                            );
                                            app.engine.remove_engine(&hash).await;
                                        });
                                    }
                                }
                                View::Files => {
                                    if let Some(i) = file_list_state.selected()
                                        && let Some(file) = files.get(i)
                                    {
                                        let name = file.name.clone();
                                        let is_dir = file.is_dir;
                                        let path = app_state.engine.download_dir.join(&name);
                                        rt.spawn(async move {
                                            tracing::info!(
                                                "Users requested removal of file/dir: {:?}",
                                                path
                                            );
                                            if is_dir {
                                                let _ = tokio::fs::remove_dir_all(path).await;
                                            } else {
                                                let _ = tokio::fs::remove_file(path).await;
                                            }
                                        });
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Event::Mouse(mouse)
                    if mouse.kind == MouseEventKind::Down(crossterm::event::MouseButton::Left) =>
                {
                    // Layout must match UI exactly for hit testing
                    let size = terminal.size()?;
                    let area = Rect::new(0, 0, size.width, size.height);
                    let chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Length(3),      // Header
                            Constraint::Length(3),      // Tabs
                            Constraint::Percentage(50), // Content
                            Constraint::Percentage(50), // Logs
                        ])
                        .split(area);

                    let tabs_area = chunks[1];
                    let list_area = chunks[2];

                    // Hit test Tabs
                    if mouse.column >= tabs_area.x
                        && mouse.column < tabs_area.x + tabs_area.width
                        && mouse.row >= tabs_area.y
                        && mouse.row < tabs_area.y + tabs_area.height
                    {
                        // Simple heuristic for 2 tabs left-aligned
                        // "Active Engines [Tab]" ~ 20 chars
                        // "Downloaded Files [Tab]" ~ 22 chars
                        let rel_x = mouse.column.saturating_sub(tabs_area.x);
                        if rel_x < 22 {
                            view = View::Engines;
                        } else if rel_x < 50 {
                            view = View::Files;
                        }
                    }

                    // Hit test List
                    if mouse.column >= list_area.x
                        && mouse.column < list_area.x + list_area.width
                        && mouse.row >= list_area.y
                        && mouse.row < list_area.y + list_area.height
                    {
                        // List has borders (1px)
                        let inner_y = mouse.row as i32 - list_area.y as i32 - 1; // -1 for top border
                        if inner_y >= 0 {
                            match view {
                                View::Engines => {
                                    let offset = engine_list_state.offset();
                                    let index = offset + inner_y as usize;
                                    if index < sorted_hashes.len() {
                                        engine_list_state.select(Some(index));
                                    }
                                }
                                View::Files => {
                                    let offset = file_list_state.offset();
                                    let index = offset + inner_y as usize;
                                    if index < files.len() {
                                        file_list_state.select(Some(index));
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }

    // Restore
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    // Signal shutdown immediately using blocking_send (not async spawn)
    // This ensures the shutdown signal is delivered before we return
    let _ = shutdown_tx.blocking_send(());

    // Force exit after a short grace period to handle any stuck connections
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(2));
        tracing::info!("Force exiting after grace period");
        std::process::exit(0);
    });

    Ok(())
}

struct UiModel<'a> {
    logs: &'a VecDeque<String>,
    stats: &'a HashMap<String, EngineStats>,
    sorted_hashes: &'a [String],
    files: &'a [FileInfo],
    view: &'a View,
    engine_list_state: &'a mut ListState,
    file_list_state: &'a mut ListState,
}

fn ui(f: &mut ratatui::Frame, model: UiModel<'_>) {
    let UiModel {
        logs,
        stats,
        sorted_hashes,
        files,
        view,
        engine_list_state,
        file_list_state,
    } = model;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),      // Header
            Constraint::Length(3),      // Tabs
            Constraint::Percentage(50), // Content (Engines/Files)
            Constraint::Percentage(50), // Logs
        ])
        .split(f.area());

    // 1. Header
    let total_dl_speed: f64 = stats.values().map(|s| s.download_speed).sum();
    let total_ul_speed: f64 = stats.values().map(|s| s.upload_speed).sum();
    let active_torrents = stats.len();

    let header_text = format!(
        "Stream Server | Active Engines: {} | Total DL: {:.2} MB/s | Total UL: {:.2} MB/s | 'd': Delete",
        active_torrents,
        total_dl_speed / 1024.0 / 1024.0,
        total_ul_speed / 1024.0 / 1024.0
    );

    let header = Paragraph::new(header_text)
        .block(Block::default().borders(Borders::ALL).title("Status"))
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(header, chunks[0]);

    // 2. Tabs
    let titles = vec!["Active Engines [Tab]", "Downloaded Files [Tab]"];
    let tab_index = match view {
        View::Engines => 0,
        View::Files => 1,
    };
    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title("View"))
        .select(tab_index)
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, chunks[1]);

    // 3. Content
    match view {
        View::Engines => {
            let items: Vec<ListItem> = sorted_hashes
                .iter()
                .filter_map(|hash| stats.get(hash))
                .map(|s| {
                    let dl = s.download_speed / 1024.0 / 1024.0;
                    let progress = s.stream_progress * 100.0;
                    let line = Line::from(vec![
                        Span::styled(
                            format!("{:.30}", s.name),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::raw(" | "),
                        Span::raw(format!("DL: {:.2} MB/s", dl)),
                        Span::raw(" | "),
                        Span::raw(format!("Peers: {}", s.peers)),
                        Span::raw(" | "),
                        Span::raw(format!("Progress: {:.1}%", progress)),
                    ]);
                    ListItem::new(line)
                })
                .collect();

            let list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title("Engines"))
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
                .highlight_symbol(">> ");

            f.render_stateful_widget(list, chunks[2], engine_list_state);
        }
        View::Files => {
            let items: Vec<ListItem> = files
                .iter()
                .map(|file| {
                    let size_mb = file.size as f64 / 1024.0 / 1024.0;
                    let line = Line::from(vec![
                        Span::styled(
                            format!("{:.40}", file.name),
                            if file.is_dir {
                                Style::default().fg(Color::Blue)
                            } else {
                                Style::default()
                            },
                        ),
                        Span::raw(" | "),
                        Span::raw(format!("{:.2} MB", size_mb)),
                    ]);
                    ListItem::new(line)
                })
                .collect();

            let list = List::new(items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Downloads (Press 'd' to delete)"),
                )
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
                .highlight_symbol(">> ");

            f.render_stateful_widget(list, chunks[2], file_list_state);
        }
    }

    // 4. Logs
    let log_height = chunks[3].height as usize;
    if log_height > 2 {
        let log_items: Vec<ListItem> = logs
            .iter()
            .rev()
            .take(log_height - 2)
            .map(|s| ListItem::new(Line::from(Span::raw(s))))
            .collect();

        let log_items: Vec<ListItem> = log_items.into_iter().rev().collect();

        let logs_widget =
            List::new(log_items).block(Block::default().borders(Borders::ALL).title("Logs"));
        f.render_widget(logs_widget, chunks[3]);
    }
}
