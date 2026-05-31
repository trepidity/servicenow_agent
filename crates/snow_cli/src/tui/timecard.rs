use std::collections::HashMap;
use std::io::{self, Stdout};
use std::sync::Arc;
use std::time::Duration;

use crossterm::cursor::{Hide, Show};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::{Frame, Terminal};
use snow_core::resource::timecard::{
    SetMode, TimeCard, TimeValue, TimecardSheet, WeekSelector, Weekday,
};

use crate::display;
use crate::error::SnowError;
use crate::tui_client::TuiClient;

const DAYS: [Weekday; 7] = [
    Weekday::Sun,
    Weekday::Mon,
    Weekday::Tue,
    Weekday::Wed,
    Weekday::Thu,
    Weekday::Fri,
    Weekday::Sat,
];
const TICK_RATE: Duration = Duration::from_millis(100);

type TimecardTerminal = Terminal<CrosstermBackend<Stdout>>;

pub async fn run_timecard_editor(
    client: Arc<TuiClient>,
    week: WeekSelector,
) -> Result<TimecardSheet, SnowError> {
    let sheet = client.timecard_list(week).await?;
    let mut app = TimecardEditor::new(client, week, sheet);
    let mut terminal = enter_terminal()?;
    let result = run_loop(&mut terminal, &mut app).await;
    let restore_result = leave_terminal(&mut terminal);
    result.and(restore_result)?;
    Ok(app.sheet)
}

async fn run_loop(
    terminal: &mut TimecardTerminal,
    app: &mut TimecardEditor,
) -> Result<(), SnowError> {
    terminal
        .draw(|frame| app.render(frame))
        .map_err(|e| SnowError::Api(e.to_string()))?;

    while !app.should_quit {
        if event::poll(TICK_RATE).map_err(|e| SnowError::Api(e.to_string()))?
            && let Event::Key(key) = event::read().map_err(|e| SnowError::Api(e.to_string()))?
        {
            app.handle_key(key).await?;
            terminal
                .draw(|frame| app.render(frame))
                .map_err(|e| SnowError::Api(e.to_string()))?;
        }
        tokio::task::yield_now().await;
    }

    Ok(())
}

fn enter_terminal() -> Result<TimecardTerminal, SnowError> {
    enable_raw_mode().map_err(|e| SnowError::Api(e.to_string()))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, Hide).map_err(|e| SnowError::Api(e.to_string()))?;
    Terminal::new(CrosstermBackend::new(stdout)).map_err(|e| SnowError::Api(e.to_string()))
}

fn leave_terminal(terminal: &mut TimecardTerminal) -> Result<(), SnowError> {
    disable_raw_mode().map_err(|e| SnowError::Api(e.to_string()))?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, Show)
        .map_err(|e| SnowError::Api(e.to_string()))?;
    terminal
        .show_cursor()
        .map_err(|e| SnowError::Api(e.to_string()))
}

struct TimecardEditor {
    client: Arc<TuiClient>,
    week: WeekSelector,
    sheet: TimecardSheet,
    selected_row: usize,
    selected_day: usize,
    dirty: HashMap<(String, usize), String>,
    status: String,
    should_quit: bool,
}

impl TimecardEditor {
    fn new(client: Arc<TuiClient>, week: WeekSelector, sheet: TimecardSheet) -> Self {
        Self {
            client,
            week,
            sheet,
            selected_row: 0,
            selected_day: 0,
            dirty: HashMap::new(),
            status: "Arrows move, digits replace a cell, Backspace edits, Delete sets 0, s saves, q exits.".to_string(),
            should_quit: false,
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<(), SnowError> {
        match key.code {
            KeyCode::Left | KeyCode::Char('h') => self.move_day(-1),
            KeyCode::Right | KeyCode::Char('l') => self.move_day(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_row(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_row(1),
            KeyCode::Tab => self.move_day(1),
            KeyCode::BackTab => self.move_day(-1),
            KeyCode::Backspace => self.backspace_current_cell(),
            KeyCode::Delete => self.set_current_cell("0"),
            KeyCode::Char('s') if key.modifiers.is_empty() => self.save().await?,
            KeyCode::Char('r') if key.modifiers.is_empty() => self.refresh().await?,
            KeyCode::Char('q') | KeyCode::Esc => {
                if self.dirty.is_empty() {
                    self.should_quit = true;
                } else {
                    self.status = "Unsaved changes remain; press Shift+Q to discard or s to save."
                        .to_string();
                }
            }
            KeyCode::Char('Q') => self.should_quit = true,
            KeyCode::Char(ch)
                if key.modifiers == KeyModifiers::NONE && (ch.is_ascii_digit() || ch == '.') =>
            {
                self.input_current_cell(ch);
            }
            _ => {}
        }
        Ok(())
    }

    fn render(&self, frame: &mut Frame<'_>) {
        let area = frame.area();
        let layout = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(area);

        let header = Paragraph::new(vec![
            Line::from(format!(
                "Timecard edit - week starting {} - {} card{}",
                self.sheet.week_starts_on,
                self.sheet.cards.len(),
                if self.sheet.cards.len() == 1 { "" } else { "s" }
            )),
            Line::from(format!(
                "Dirty cells: {}   Selected day: {}",
                self.dirty.len(),
                day_label(DAYS[self.selected_day])
            )),
        ])
        .block(Block::default().borders(Borders::ALL));
        frame.render_widget(header, layout[0]);

        let rows = self
            .sheet
            .cards
            .iter()
            .enumerate()
            .map(|(row_index, card)| self.render_row(row_index, card))
            .collect::<Vec<_>>();
        let table = Table::new(
            rows,
            [
                Constraint::Length(4),
                Constraint::Percentage(28),
                Constraint::Percentage(16),
                Constraint::Length(9),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(7),
            ],
        )
        .header(
            Row::new([
                Cell::from("#"),
                Cell::from("Task"),
                Cell::from("Category"),
                Cell::from("State"),
                Cell::from("Sun"),
                Cell::from("Mon"),
                Cell::from("Tue"),
                Cell::from("Wed"),
                Cell::from("Thu"),
                Cell::from("Fri"),
                Cell::from("Sat"),
                Cell::from("Total"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(Block::default().borders(Borders::ALL).title("Cards"));
        frame.render_widget(table, layout[1]);

        let status = Paragraph::new(self.status.clone())
            .block(Block::default().borders(Borders::ALL).title("Status"));
        frame.render_widget(status, layout[2]);
    }

    fn render_row(&self, row_index: usize, card: &TimeCard) -> Row<'static> {
        let mut cells = vec![
            Cell::from((row_index + 1).to_string()),
            Cell::from(truncate(&display::timecard_task_display(card), 36)),
            Cell::from(truncate(&card.category_label, 24)),
            Cell::from(truncate(&card.state, 12)),
        ];

        for (day_index, day) in DAYS.iter().enumerate() {
            let value = self.cell_value(card, *day);
            let mut style = if self.is_dirty(card, day_index) {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            if row_index == self.selected_row && day_index == self.selected_day {
                style = style.bg(Color::Blue).fg(Color::White);
            }
            cells.push(Cell::from(value).style(style));
        }
        cells.push(Cell::from(self.preview_total(card)));

        let style = if card.is_editable() {
            Style::default()
        } else {
            Style::default().fg(Color::DarkGray)
        };
        Row::new(cells).style(style)
    }

    fn move_row(&mut self, delta: isize) {
        if self.sheet.cards.is_empty() {
            self.selected_row = 0;
            return;
        }
        self.selected_row = offset_index(self.selected_row, delta, self.sheet.cards.len());
    }

    fn move_day(&mut self, delta: isize) {
        self.selected_day = offset_index(self.selected_day, delta, DAYS.len());
    }

    fn input_current_cell(&mut self, ch: char) {
        let Some(card) = self.current_card() else {
            return;
        };
        if !card.is_editable() {
            self.status =
                "Only Pending, Rejected, or Recalled time cards can be edited.".to_string();
            return;
        }
        let key = self.current_key(card);
        let entry = self.dirty.entry(key).or_default();
        entry.push(ch);
        self.status = "Cell changed; press s to save.".to_string();
    }

    fn backspace_current_cell(&mut self) {
        let Some(card) = self.current_card() else {
            return;
        };
        let key = self.current_key(card);
        if let Some(value) = self.dirty.get_mut(&key) {
            value.pop();
            if value.is_empty() {
                self.dirty.remove(&key);
            }
            self.status = "Cell changed; press s to save.".to_string();
        }
    }

    fn set_current_cell(&mut self, value: &str) {
        let Some(card) = self.current_card() else {
            return;
        };
        if !card.is_editable() {
            self.status =
                "Only Pending, Rejected, or Recalled time cards can be edited.".to_string();
            return;
        }
        let key = self.current_key(card);
        self.dirty.insert(key, value.to_string());
        self.status = "Cell changed; press s to save.".to_string();
    }

    async fn save(&mut self) -> Result<(), SnowError> {
        if self.dirty.is_empty() {
            self.status = "No pending changes.".to_string();
            return Ok(());
        }

        let updates = self.sorted_updates();
        for update in &updates {
            if let Err(err) = TimeValue::parse(&update.value) {
                self.status = format!(
                    "Invalid {} value on row {}: {}",
                    day_label(update.day),
                    update.row + 1,
                    err
                );
                return Ok(());
            }
        }

        self.status = format!(
            "Saving {} change{}...",
            updates.len(),
            plural(updates.len())
        );
        for update in updates {
            self.client
                .timecard_set_hours(&update.sys_id, update.day, &update.value, SetMode::Set)
                .await?;
        }
        self.sheet = self.client.timecard_list(self.week).await?;
        self.selected_row = self
            .selected_row
            .min(self.sheet.cards.len().saturating_sub(1));
        self.dirty.clear();
        self.status = "Saved and refreshed.".to_string();
        Ok(())
    }

    async fn refresh(&mut self) -> Result<(), SnowError> {
        if !self.dirty.is_empty() {
            self.status = "Unsaved changes remain; save or discard before refresh.".to_string();
            return Ok(());
        }
        self.sheet = self.client.timecard_list(self.week).await?;
        self.selected_row = self
            .selected_row
            .min(self.sheet.cards.len().saturating_sub(1));
        self.status = "Refreshed.".to_string();
        Ok(())
    }

    fn sorted_updates(&self) -> Vec<PendingUpdate> {
        let mut updates = self
            .sheet
            .cards
            .iter()
            .enumerate()
            .flat_map(|(row, card)| {
                DAYS.iter().enumerate().filter_map(move |(day_index, day)| {
                    self.dirty
                        .get(&(card.sys_id.clone(), day_index))
                        .map(|value| PendingUpdate {
                            row,
                            sys_id: card.sys_id.clone(),
                            day: *day,
                            value: value.clone(),
                        })
                })
            })
            .collect::<Vec<_>>();
        updates.sort_by_key(|update| (update.row, update.day.index()));
        updates
    }

    fn current_card(&self) -> Option<&TimeCard> {
        self.sheet.cards.get(self.selected_row)
    }

    fn current_key(&self, card: &TimeCard) -> (String, usize) {
        (card.sys_id.clone(), self.selected_day)
    }

    fn cell_value(&self, card: &TimeCard, day: Weekday) -> String {
        self.dirty
            .get(&(card.sys_id.clone(), day.index()))
            .cloned()
            .unwrap_or_else(|| card.hours[day.index()].clone())
    }

    fn is_dirty(&self, card: &TimeCard, day_index: usize) -> bool {
        self.dirty.contains_key(&(card.sys_id.clone(), day_index))
    }

    fn preview_total(&self, card: &TimeCard) -> String {
        let total = DAYS
            .iter()
            .map(|day| {
                self.cell_value(card, *day)
                    .trim()
                    .parse::<f64>()
                    .unwrap_or(0.0)
            })
            .sum::<f64>();
        format_hours(total)
    }
}

#[derive(Debug)]
struct PendingUpdate {
    row: usize,
    sys_id: String,
    day: Weekday,
    value: String,
}

fn offset_index(current: usize, delta: isize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let next = current as isize + delta;
    next.clamp(0, len as isize - 1) as usize
}

fn day_label(day: Weekday) -> &'static str {
    match day {
        Weekday::Sun => "Sun",
        Weekday::Mon => "Mon",
        Weekday::Tue => "Tue",
        Weekday::Wed => "Wed",
        Weekday::Thu => "Thu",
        Weekday::Fri => "Fri",
        Weekday::Sat => "Sat",
    }
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let mut result = value
        .chars()
        .take(max.saturating_sub(3))
        .collect::<String>();
    result.push_str("...");
    result
}

fn format_hours(value: f64) -> String {
    let rounded = (value * 100.0).round() / 100.0;
    if rounded.fract().abs() < f64::EPSILON {
        format!("{}", rounded as i64)
    } else {
        let mut value = format!("{rounded:.2}");
        while value.contains('.') && value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
        value
    }
}

fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}
