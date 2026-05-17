use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Terminal,
};
use std::io;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::metrics::MetricsTracker;

pub struct TuiServiceInfo {
    pub name: String,
    pub protocol: String,
    pub listen_addr: String,
}

pub struct TuiDashboard {
    services: Arc<RwLock<Vec<TuiServiceInfo>>>,
    metrics: MetricsTracker,
    start_time: Instant,
}

impl TuiDashboard {
    pub fn new(services: Arc<RwLock<Vec<TuiServiceInfo>>>, metrics: MetricsTracker) -> Self {
        Self {
            services,
            metrics,
            start_time: Instant::now(),
        }
    }

    pub fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        // Setup terminal
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let res = self.run_loop(&mut terminal);

        // Restore terminal
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;

        res
    }

    fn run_loop<B: ratatui::backend::Backend>(
        &self,
        terminal: &mut Terminal<B>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tick_rate = Duration::from_millis(250);
        let mut last_tick = Instant::now();

        loop {
            terminal.draw(|f| self.ui(f))?;

            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));

            if crossterm::event::poll(timeout)?
                && let Event::Key(key) = event::read()?
                    && (key.code == KeyCode::Char('q') || key.code == KeyCode::Char('Q')) {
                        return Ok(());
                    }

            if last_tick.elapsed() >= tick_rate {
                last_tick = Instant::now();
            }
        }
    }

    fn ui(&self, f: &mut ratatui::Frame) {
        let size = f.size();

        // Separate screen into Header, Table Content, and Footer
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Header
                Constraint::Min(5),    // Main Table
                Constraint::Length(3), // Footer
            ])
            .split(size);

        // 1. Render Header
        let uptime = format_duration(self.start_time.elapsed());
        let header_content = format!(
            " SPECTRA PROXY // TELEMETRY CENTER | Uptime: {} | Config: config.toml",
            uptime
        );
        let header = Paragraph::new(header_content)
            .style(Style::default().fg(Color::Black).bg(Color::Cyan).bold())
            .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)));
        f.render_widget(header, chunks[0]);

        // 2. Fetch metrics and prepare Table Rows
        let snapshots = self.metrics.get_all_snapshots();
        let mut rows = Vec::new();

        let services = self.services.read().unwrap();
        for s in services.iter() {
            let (active_conn, total_conn, rx_bytes, tx_bytes, requests, errors) =
                if let Some(metrics) = snapshots.get(&s.name) {
                    (
                        metrics.active_connections.to_string(),
                        metrics.total_connections.to_string(),
                        format_bytes(metrics.bytes_rx),
                        format_bytes(metrics.bytes_tx),
                        metrics.total_requests.to_string(),
                        metrics.total_errors.to_string(),
                    )
                } else {
                    (
                        "0".to_string(),
                        "0".to_string(),
                        "0 B".to_string(),
                        "0 B".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                    )
                };

            // Colorize protocol and active connection statuses
            let active_style = if active_conn != "0" {
                Style::default().fg(Color::Green).bold()
            } else {
                Style::default().fg(Color::Gray)
            };

            let error_style = if errors != "0" {
                Style::default().fg(Color::Red).bold()
            } else {
                Style::default().fg(Color::Gray)
            };

            let protocol_color = match s.protocol.to_lowercase().as_str() {
                "http" | "https" => Color::LightGreen,
                "tcp" | "tcps" => Color::LightBlue,
                "udp" => Color::Magenta,
                _ => Color::White,
            };

            rows.push(Row::new(vec![
                Cell::from(s.name.clone()).style(Style::default().bold()),
                Cell::from(s.protocol.to_uppercase()).style(Style::default().fg(protocol_color).bold()),
                Cell::from(s.listen_addr.clone()),
                Cell::from(active_conn).style(active_style),
                Cell::from(total_conn),
                Cell::from(requests),
                Cell::from(rx_bytes),
                Cell::from(tx_bytes),
                Cell::from(errors).style(error_style),
            ]));
        }

        let header_cells = [
            "Service Name",
            "Protocol",
            "Listen Address",
            "Active",
            "Total Handled",
            "Requests",
            "Rx Data",
            "Tx Data",
            "Errors",
        ]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow).bold()));

        let table_header = Row::new(header_cells).height(1).bottom_margin(1);

        let table = Table::new(
            rows,
            [
                Constraint::Percentage(20), // Name
                Constraint::Percentage(10), // Protocol
                Constraint::Percentage(20), // Listen addr
                Constraint::Percentage(8),  // Active
                Constraint::Percentage(10), // Total
                Constraint::Percentage(8),  // Requests
                Constraint::Percentage(8),  // RX
                Constraint::Percentage(8),  // TX
                Constraint::Percentage(8),  // Errors
            ],
        )
        .header(table_header)
        .block(
            Block::default()
                .title(" Active Services Telemetry ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );

        f.render_widget(table, chunks[1]);

        // 3. Render Footer
        let footer_text = " [q] Quit System | Engine: Asynchronous Tokio-based Proxy System v0.1.0 | Made with Rust 2024 🦀";
        let footer = Paragraph::new(footer_text)
            .style(Style::default().fg(Color::DarkGray))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::DarkGray)),
            );
        f.render_widget(footer, chunks[2]);
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }
    let k = 1024.0;
    let sizes = ["B", "KB", "MB", "GB", "TB"];
    let i = (bytes as f64).log(k).floor() as usize;
    let i = std::cmp::min(i, sizes.len() - 1);
    format!("{:.2} {}", bytes as f64 / k.powi(i as i32), sizes[i])
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let secs = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, secs)
}
