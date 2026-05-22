mod actions;
mod browser;
mod dashboard;
mod fetch;
mod keybinds;
mod knowledge;
mod timecard;

use std::collections::{HashMap, HashSet};
use std::io::{self, Stdout, Write};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Local};
use crossterm::cursor::{Hide, Show};
use crossterm::event::{self, Event};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{execute, queue};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use servicenow_rs::prelude::{JournalEntry, Record};
use snow_core::TaskSlaStatus;
use snow_core::resource::timecard::{TimecardSheet, WeekSelector};
use tokio::task::JoinSet;

use self::knowledge::KnowledgeState;
use crate::display;
use crate::error::SnowError;
use crate::tui_client::TuiClient;

const TICK_RATE: Duration = Duration::from_millis(25);
const MAX_WORK_NOTES: usize = 5;

type TuiTerminal = Terminal<CrosstermBackend<Stdout>>;

pub async fn run_tui(
    core: Arc<TuiClient>,
    env_name: &str,
    mode: &str,
    username: &str,
    refresh_interval: Option<Duration>,
    show_closed: bool,
) -> Result<(), SnowError> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let mut app = App::new(
                core,
                env_name,
                mode,
                username,
                refresh_interval,
                show_closed,
            );
            app.start_dashboard_refresh(false);

            let mut terminal = enter_terminal()?;
            let result = run_loop(&mut terminal, &mut app).await;
            let restore_result = leave_terminal(&mut terminal);
            result.and(restore_result)
        })
        .await
}

pub async fn run_timecard_editor(
    core: Arc<TuiClient>,
    week: WeekSelector,
) -> Result<TimecardSheet, SnowError> {
    timecard::run_timecard_editor(core, week).await
}

async fn run_loop(terminal: &mut TuiTerminal, app: &mut App) -> Result<(), SnowError> {
    terminal
        .draw(|frame| app.render(frame))
        .map_err(|e| SnowError::Api(e.to_string()))?;

    while !app.should_quit {
        yield_to_local_scheduler().await;
        let mut needs_draw = app.apply_completed_jobs()?;

        if event::poll(TICK_RATE).map_err(|e| SnowError::Api(e.to_string()))? {
            if let Event::Key(key) = event::read().map_err(|e| SnowError::Api(e.to_string()))? {
                app.handle_key(key).await?;
                needs_draw = true;
            }
        } else {
            if app.should_auto_refresh() && app.screen == Screen::Dashboard {
                app.start_dashboard_refresh(false);
                needs_draw = true;
            }
            if app.dispatch_background_work() {
                needs_draw = true;
            }
            if app.apply_completed_jobs()? {
                needs_draw = true;
            }
        }

        if needs_draw {
            terminal
                .draw(|frame| app.render(frame))
                .map_err(|e| SnowError::Api(e.to_string()))?;
        }

        yield_to_local_scheduler().await;
    }

    Ok(())
}

async fn yield_to_local_scheduler() {
    tokio::task::yield_now().await;
}

fn enter_terminal() -> Result<TuiTerminal, SnowError> {
    enable_raw_mode().map_err(|e| SnowError::Api(e.to_string()))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, Hide).map_err(|e| SnowError::Api(e.to_string()))?;
    Terminal::new(CrosstermBackend::new(stdout)).map_err(|e| SnowError::Api(e.to_string()))
}

fn leave_terminal(terminal: &mut TuiTerminal) -> Result<(), SnowError> {
    disable_raw_mode().map_err(|e| SnowError::Api(e.to_string()))?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, Show)
        .map_err(|e| SnowError::Api(e.to_string()))?;
    terminal
        .show_cursor()
        .map_err(|e| SnowError::Api(e.to_string()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Screen {
    Dashboard,
    Browser,
    Knowledge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum DashboardTab {
    PendingApprovals,
    MyTasks,
    MyStories,
    MyIncidents,
}

impl DashboardTab {
    fn all() -> [DashboardTab; 4] {
        [
            DashboardTab::PendingApprovals,
            DashboardTab::MyTasks,
            DashboardTab::MyStories,
            DashboardTab::MyIncidents,
        ]
    }

    fn title(self) -> &'static str {
        match self {
            DashboardTab::PendingApprovals => "Approvals",
            DashboardTab::MyTasks => "My Tasks",
            DashboardTab::MyStories => "My Stories",
            DashboardTab::MyIncidents => "My Incidents",
        }
    }

    fn index(self) -> usize {
        match self {
            DashboardTab::PendingApprovals => 0,
            DashboardTab::MyTasks => 1,
            DashboardTab::MyStories => 2,
            DashboardTab::MyIncidents => 3,
        }
    }

    fn next(self) -> Self {
        DashboardTab::all()[(self.index() + 1) % DashboardTab::all().len()]
    }

    fn previous(self) -> Self {
        DashboardTab::all()
            [(self.index() + DashboardTab::all().len() - 1) % DashboardTab::all().len()]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EntityKind {
    Task,
    ChangeRequest,
    ChangeTask,
    Demand,
    Project,
    ResourcePlan,
    Story,
    ScrumTask,
    RequestItem,
    CatalogTask,
    Incident,
}

#[derive(Clone, Copy, Debug)]
struct ChildRelation {
    #[allow(dead_code)]
    parent_kind: EntityKind,
    child_kind: Option<EntityKind>,
    child_table: Option<&'static str>,
    child_link_field: Option<&'static str>,
}

#[derive(Clone, Debug)]
struct ApprovalRef {
    #[allow(dead_code)]
    approval_sys_id: String,
    #[allow(dead_code)]
    document_id: String,
    source_table: String,
    source_sys_id: String,
    #[allow(dead_code)]
    source_number: String,
}

#[derive(Clone, Debug)]
struct RecordRef {
    kind: Option<EntityKind>,
    table: String,
    sys_id: String,
    number: String,
    short_description: String,
    assigned_to: Option<String>,
    planned_start: Option<String>,
    planned_end: Option<String>,
    planned_hours: Option<String>,
    allocated_hours: Option<String>,
    confirmed_hours: Option<String>,
    state_label: Option<String>,
    state_value: Option<String>,
    #[allow(dead_code)]
    record_class: Option<String>,
    #[allow(dead_code)]
    parent_table: Option<String>,
    #[allow(dead_code)]
    parent_sys_id: Option<String>,
    parent_number: Option<String>,
    approval: Option<ApprovalRef>,
}

#[derive(Clone, Debug)]
struct DashboardRow {
    record: RecordRef,
    cells: Vec<String>,
}

#[derive(Clone, Debug)]
struct ChoiceItem {
    label: String,
    value: String,
}

#[derive(Clone, Debug)]
struct UserChoice {
    sys_id: String,
    label: String,
}

#[derive(Clone, Debug)]
struct TextBuffer {
    value: String,
}

impl TextBuffer {
    fn new() -> Self {
        Self {
            value: String::new(),
        }
    }

    fn insert(&mut self, ch: char) {
        self.value.push(ch);
    }

    fn newline(&mut self) {
        self.value.push('\n');
    }

    fn backspace(&mut self) {
        self.value.pop();
    }

    fn is_empty(&self) -> bool {
        self.value.trim().is_empty()
    }

    fn lines(&self) -> Vec<Line<'static>> {
        if self.value.is_empty() {
            return vec![Line::from("")];
        }
        self.value
            .lines()
            .map(|line| Line::from(line.to_string()))
            .collect()
    }
}

#[derive(Clone, Debug)]
struct TextModal {
    title: &'static str,
    prompt: &'static str,
    buffer: TextBuffer,
}

#[derive(Clone, Debug)]
struct StateModal {
    title: &'static str,
    field_name: String,
    choices: Vec<ChoiceItem>,
    selected: usize,
    loading: bool,
}

#[derive(Clone, Debug)]
struct ReassignModal {
    query: TextBuffer,
    results: Vec<UserChoice>,
    selected: usize,
    last_searched: String,
    loading: bool,
}

#[derive(Clone, Debug)]
enum ModalState {
    Jump(TextModal),
    Note(TextModal),
    Reject(TextModal),
    ApproveConfirm,
    StatePicker(StateModal),
    Reassign(ReassignModal),
}

#[derive(Clone, Copy, Debug)]
enum BrowserSelection {
    Parent,
    Child(usize),
}

#[derive(Clone, Debug)]
struct RecordDetail {
    page: DetailPageModel,
    record: Record,
    description: Option<String>,
    extra_sections: Vec<(String, String)>,
    field_lines: Vec<(String, String)>,
    work_notes: Vec<JournalEntry>,
    notes_loaded: bool,
    task_sla: TaskSlaDetail,
}

#[derive(Clone, Debug)]
struct DetailPageModel {
    title: &'static str,
    field_specs: Vec<DetailFieldSpec>,
    section_specs: Vec<DetailSectionSpec>,
    sections_title: &'static str,
    child_summary: Option<DetailChildSummaryModel>,
}

#[derive(Clone, Debug)]
struct DetailFieldSpec {
    label: &'static str,
    fields: &'static [&'static str],
}

#[derive(Clone, Debug)]
struct DetailSectionSpec {
    label: &'static str,
    field: &'static str,
}

#[derive(Clone, Copy, Debug)]
struct DetailChildSummaryModel {
    title: &'static str,
    loading_text: &'static str,
    empty_text: &'static str,
    filtered_empty_text: &'static str,
    row_limit: usize,
    columns: &'static [DetailChildColumn],
}

#[derive(Clone, Copy, Debug)]
struct DetailChildColumn {
    header: &'static str,
    width: DetailColumnWidth,
    value: DetailChildValue,
}

#[derive(Clone, Copy, Debug)]
enum DetailColumnWidth {
    Length(u16),
    Percentage(u16),
}

#[derive(Clone, Copy, Debug)]
enum DetailChildValue {
    Number,
    ShortDescription,
    AssignedTo,
    Resource,
    PlannedStart,
    PlannedEnd,
    Hours,
    State,
}

#[derive(Clone, Debug)]
enum TaskSlaDetail {
    Loading,
    Status(Box<TaskSlaStatus>),
    Error(String),
}

#[derive(Clone, Debug)]
struct QuickSearch {
    query: String,
}

impl QuickSearch {
    fn new() -> Self {
        Self {
            query: String::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct BrowserState {
    parent: RecordRef,
    #[allow(dead_code)]
    parent_kind: EntityKind,
    #[allow(dead_code)]
    relation: ChildRelation,
    parent_detail: RecordDetail,
    children: Vec<RecordRef>,
    selected: BrowserSelection,
    task_sla_expanded: bool,
    detail_cache: HashMap<String, RecordDetail>,
    approval: Option<ApprovalRef>,
    search: QuickSearch,
    child_state_filter: Option<String>,
}

impl BrowserState {
    fn selected_record(&self) -> &RecordRef {
        match self.selected {
            BrowserSelection::Parent => &self.parent,
            BrowserSelection::Child(index) => self.children.get(index).unwrap_or(&self.parent),
        }
    }

    fn selected_detail(&self) -> Option<&RecordDetail> {
        match self.selected {
            BrowserSelection::Parent => Some(&self.parent_detail),
            BrowserSelection::Child(index) => self
                .children
                .get(index)
                .and_then(|child| self.detail_cache.get(&child.sys_id)),
        }
    }
}

#[derive(Clone, Debug)]
enum PrefetchTask {
    Detail(RecordRef),
    Children(RecordRef),
    BatchChildren {
        relation: ChildRelation,
        parents: Vec<RecordRef>,
    },
    Approval(RecordRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum BackgroundJobKey {
    DashboardRefresh(DashboardTab),
    Detail(String),
    Children(String),
    BatchChildren(String),
    Approval(String),
    KnowledgeBases,
    KnowledgeCategories(String),
    KnowledgeSearch(String),
    KnowledgeArticle(String),
    StateChoices(String),
    UserSearch(String),
    JumpLookup(String),
    Mutation(String),
}

#[derive(Debug)]
enum BackgroundJobResult {
    DashboardRefresh {
        tab: DashboardTab,
        result: Result<Vec<DashboardRow>, SnowError>,
    },
    Detail {
        record: RecordRef,
        detail: RecordDetail,
    },
    Children {
        record: RecordRef,
        children: Vec<RecordRef>,
    },
    BatchChildren {
        relation: ChildRelation,
        groups: Vec<(RecordRef, Vec<RecordRef>)>,
    },
    Approval {
        record: RecordRef,
        approval: Option<ApprovalRef>,
    },
    KnowledgeBases {
        result: Result<Vec<snow_core::KnowledgeBaseSummary>, SnowError>,
    },
    KnowledgeCategories {
        base_sys_id: String,
        result: Result<Vec<snow_core::KnowledgeCategorySummary>, SnowError>,
    },
    KnowledgeSearch {
        query: String,
        result: Result<Vec<snow_core::KnowledgeArticle>, SnowError>,
    },
    KnowledgeArticle {
        number: String,
        result: Result<Option<snow_core::KnowledgeArticle>, SnowError>,
    },
    StateChoices {
        table: String,
        field: String,
        choices: Result<Vec<ChoiceItem>, SnowError>,
    },
    UserSearch {
        query: String,
        results: Vec<UserChoice>,
    },
    JumpLookup {
        number: String,
        result: Result<(RecordRef, RecordDetail, Vec<RecordRef>, Option<ApprovalRef>), SnowError>,
    },
    Mutation {
        message: &'static str,
    },
}

impl PrefetchTask {
    fn same_target(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Detail(left), Self::Detail(right))
            | (Self::Children(left), Self::Children(right))
            | (Self::Approval(left), Self::Approval(right)) => {
                left.table == right.table && left.sys_id == right.sys_id
            }
            (
                Self::BatchChildren { parents: left, .. },
                Self::BatchChildren { parents: right, .. },
            ) => left
                .iter()
                .map(|r| r.sys_id.as_str())
                .eq(right.iter().map(|r| r.sys_id.as_str())),
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
struct TabData {
    rows: Vec<DashboardRow>,
    error: Option<String>,
    selected: usize,
}

impl TabData {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            error: None,
            selected: 0,
        }
    }

    fn clamp_selection(&mut self) {
        if self.rows.is_empty() {
            self.selected = 0;
        } else if self.selected >= self.rows.len() {
            self.selected = self.rows.len().saturating_sub(1);
        }
    }
}

#[derive(Clone, Debug)]
struct DashboardState {
    active_tab: DashboardTab,
    approvals: TabData,
    tasks: TabData,
    stories: TabData,
    incidents: TabData,
    search: QuickSearch,
}

impl DashboardState {
    fn new() -> Self {
        Self {
            active_tab: DashboardTab::PendingApprovals,
            approvals: TabData::new(),
            tasks: TabData::new(),
            stories: TabData::new(),
            incidents: TabData::new(),
            search: QuickSearch::new(),
        }
    }

    fn tab(&self, tab: DashboardTab) -> &TabData {
        match tab {
            DashboardTab::PendingApprovals => &self.approvals,
            DashboardTab::MyTasks => &self.tasks,
            DashboardTab::MyStories => &self.stories,
            DashboardTab::MyIncidents => &self.incidents,
        }
    }

    fn tab_mut(&mut self, tab: DashboardTab) -> &mut TabData {
        match tab {
            DashboardTab::PendingApprovals => &mut self.approvals,
            DashboardTab::MyTasks => &mut self.tasks,
            DashboardTab::MyStories => &mut self.stories,
            DashboardTab::MyIncidents => &mut self.incidents,
        }
    }

    fn active_tab(&self) -> &TabData {
        self.tab(self.active_tab)
    }

    fn active_tab_mut(&mut self) -> &mut TabData {
        self.tab_mut(self.active_tab)
    }

    fn set_rows(&mut self, tab: DashboardTab, rows: Vec<DashboardRow>) {
        let current = self.tab(tab).selected;
        let data = self.tab_mut(tab);
        data.rows = rows;
        data.error = None;
        data.selected = current;
        data.clamp_selection();
    }

    fn set_error(&mut self, tab: DashboardTab, error: String) {
        let data = self.tab_mut(tab);
        data.rows.clear();
        data.error = Some(error);
        data.selected = 0;
    }
}

#[derive(Clone, Debug)]
struct StatusLine {
    message: String,
    is_error: bool,
}

impl StatusLine {
    fn new() -> Self {
        Self {
            message: "Ready.".to_string(),
            is_error: false,
        }
    }
}

#[derive(Clone, Debug)]
struct DashboardRefreshProgress {
    completed: usize,
    total: usize,
    user_requested: bool,
}

impl DashboardRefreshProgress {
    fn new(user_requested: bool) -> Self {
        Self {
            completed: 0,
            total: DashboardTab::all().len(),
            user_requested,
        }
    }
}

struct App {
    core: Arc<TuiClient>,
    env_name: String,
    mode: String,
    username: String,
    screen: Screen,
    dashboard: DashboardState,
    browser: Option<BrowserState>,
    knowledge: KnowledgeState,
    modal: Option<ModalState>,
    last_refresh_at: Option<SystemTime>,
    last_refresh_tick: Instant,
    refresh_interval: Option<Duration>,
    choice_cache: HashMap<(String, String), Vec<ChoiceItem>>,
    detail_cache: HashMap<String, RecordDetail>,
    children_cache: HashMap<String, Vec<RecordRef>>,
    approval_cache: HashMap<String, Option<ApprovalRef>>,
    prefetch_queue: Vec<PrefetchTask>,
    background_jobs: JoinSet<Result<BackgroundJobResult, SnowError>>,
    inflight_jobs: HashSet<BackgroundJobKey>,
    dashboard_refresh_progress: Option<DashboardRefreshProgress>,
    status: StatusLine,
    show_closed: bool,
    should_quit: bool,
}

impl App {
    fn new(
        core: Arc<TuiClient>,
        env_name: &str,
        mode: &str,
        username: &str,
        refresh_interval: Option<Duration>,
        show_closed: bool,
    ) -> Self {
        Self {
            core,
            env_name: env_name.to_string(),
            mode: mode.to_string(),
            username: username.to_string(),
            screen: Screen::Dashboard,
            dashboard: DashboardState::new(),
            browser: None,
            knowledge: KnowledgeState::new(),
            modal: None,
            last_refresh_at: None,
            last_refresh_tick: Instant::now(),
            refresh_interval,
            choice_cache: HashMap::new(),
            detail_cache: HashMap::new(),
            children_cache: HashMap::new(),
            approval_cache: HashMap::new(),
            prefetch_queue: Vec::new(),
            background_jobs: JoinSet::new(),
            inflight_jobs: HashSet::new(),
            dashboard_refresh_progress: None,
            status: StatusLine::new(),
            show_closed,
            should_quit: false,
        }
    }

    fn render(&self, frame: &mut Frame<'_>) {
        let layout = ratatui::layout::Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(5),
        ])
        .split(frame.area());

        self.render_header(frame, layout[0]);

        match self.screen {
            Screen::Dashboard => dashboard::render_dashboard(frame, layout[1], self),
            Screen::Browser => browser::render_browser(frame, layout[1], self),
            Screen::Knowledge => knowledge::render_knowledge_browser(frame, layout[1], self),
        }

        self.render_footer(frame, layout[2]);

        if self.modal.is_some() {
            actions::render_modal(frame, self);
        }
    }

    fn render_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let last_refresh = self
            .last_refresh_at
            .map(|time| {
                let dt: DateTime<Local> = time.into();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .unwrap_or_else(|| "never".to_string());

        let header = Paragraph::new(Line::from(vec![
            Span::styled(
                "snow tui",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "  v{}  env:{}  mode:{}  username:{}  refreshed:{}  filter:{}",
                env!("CARGO_PKG_VERSION"),
                self.env_name,
                self.mode,
                self.username,
                last_refresh,
                if self.show_closed { "all" } else { "open" }
            )),
        ]))
        .block(Block::default().borders(Borders::ALL).title("Header"));
        frame.render_widget(header, area);
    }

    fn render_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let color = if self.status.is_error {
            Color::Red
        } else {
            Color::Green
        };
        let mut lines = self.key_hint_lines();
        lines.push(self.cache_hint_line());
        lines.push(Line::from(vec![
            Span::styled(
                "Status: ".to_string(),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(self.status.message.clone(), Style::default().fg(color)),
        ]));

        let footer = Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Keys"))
            .wrap(Wrap { trim: false });
        frame.render_widget(footer, area);
    }

    fn key_hint_lines(&self) -> Vec<Line<'static>> {
        let shortcuts = if let Some(modal) = self.modal.as_ref() {
            match modal {
                ModalState::Jump(_) => vec![("Enter", "jump"), ("Esc", "cancel")],
                ModalState::Note(_) | ModalState::Reject(_) => vec![
                    ("Ctrl+S", "submit"),
                    ("Esc", "cancel"),
                    ("Enter", "newline"),
                ],
                ModalState::ApproveConfirm => vec![("Enter", "confirm"), ("Esc", "cancel")],
                ModalState::StatePicker(_) => {
                    vec![("j/k", "move"), ("Enter", "select"), ("Esc", "cancel")]
                }
                ModalState::Reassign(_) => vec![
                    ("Type", "search"),
                    ("j/k", "move"),
                    ("Enter", "select"),
                    ("Esc", "cancel"),
                ],
            }
        } else {
            match self.screen {
                Screen::Dashboard => {
                    let mut hints = vec![
                        ("/", "jump"),
                        ("K", "kb"),
                        ("Tab", "next tab"),
                        ("Shift+Tab", "prev tab"),
                        ("j/k", "move"),
                        ("Enter", "open"),
                        ("C", "closed"),
                        ("n", "note"),
                        ("R", "refresh"),
                        ("q", "quit"),
                    ];
                    if self.dashboard.active_tab == DashboardTab::PendingApprovals {
                        hints.insert(4, ("a", "approve"));
                        hints.insert(5, ("r", "reject"));
                    }
                    hints
                }
                Screen::Browser => {
                    let mut hints = vec![
                        ("/", "jump"),
                        ("K", "kb"),
                        ("Esc", "back"),
                        ("j/k", "move"),
                        ("C", "closed"),
                        ("f", "state filter"),
                        ("n", "note"),
                        ("S", "sla"),
                        ("o", "browser"),
                        ("d", "json"),
                    ];
                    if !self.core.is_remote() {
                        hints.insert(4, ("s", "state"));
                        hints.insert(5, ("x", "reassign"));
                    }
                    if self
                        .browser
                        .as_ref()
                        .is_some_and(|browser| browser.approval.is_some())
                    {
                        hints.push(("a", "approve"));
                        hints.push(("r", "reject"));
                    }
                    hints
                }
                Screen::Knowledge => vec![
                    ("Type", "search"),
                    ("Tab", "results"),
                    ("Enter", "open"),
                    ("Esc", "back"),
                    ("K", "open"),
                ],
            }
        };

        vec![shortcut_line(&shortcuts)]
    }

    fn cache_hint_line(&self) -> Line<'static> {
        match self.screen {
            Screen::Browser => match self.browser.as_ref() {
                Some(browser) => {
                    let total_children = browser.children.len();
                    let loaded_children = browser.detail_cache.len();
                    let queued = self.prefetch_queue.len();
                    let state = if queued > 0 {
                        "warming"
                    } else if total_children > 0 {
                        "warm"
                    } else {
                        "idle"
                    };
                    Line::from(vec![
                        Span::styled(
                            "Cache: ".to_string(),
                            Style::default()
                                .fg(Color::Magenta)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(format!(
                            "{state}  loaded:{loaded_children}/{total_children}  queued:{queued}"
                        )),
                    ])
                }
                None => styled_cache_line("idle"),
            },
            Screen::Knowledge => {
                let queued = self.prefetch_queue.len();
                let state = if self.knowledge.loading {
                    "loading"
                } else {
                    "idle"
                };
                Line::from(vec![
                    Span::styled(
                        "Cache: ".to_string(),
                        Style::default()
                            .fg(Color::Magenta)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!(
                        "{state}  results:{}  bases:{}  queued:{queued}",
                        self.knowledge.results.len(),
                        self.knowledge.bases.len(),
                    )),
                ])
            }
            Screen::Dashboard => {
                let queued = self.prefetch_queue.len();
                let state = if queued > 0 { "warming" } else { "idle" };
                styled_cache_line(&format!("{state}  dashboard queued:{queued}"))
            }
        }
    }

    fn should_auto_refresh(&self) -> bool {
        self.refresh_interval
            .is_some_and(|interval| self.last_refresh_tick.elapsed() >= interval)
    }

    fn queue_prefetch(&mut self, task: PrefetchTask) {
        if self
            .prefetch_queue
            .iter()
            .any(|queued| queued.same_target(&task))
        {
            return;
        }
        self.prefetch_queue.push(task);
    }

    fn queue_prefetch_priority(&mut self, task: PrefetchTask) {
        if let Some(index) = self
            .prefetch_queue
            .iter()
            .position(|queued| queued.same_target(&task))
        {
            let existing = self.prefetch_queue.remove(index);
            self.prefetch_queue.push(existing);
        } else {
            self.prefetch_queue.push(task);
        }
    }

    fn schedule_dashboard_prefetch(&mut self) {
        if self.screen != Screen::Dashboard || self.modal.is_some() {
            return;
        }
        self.refill_prefetch_queue();
    }

    fn schedule_browser_prefetch(&mut self) {
        if self.screen != Screen::Browser || self.modal.is_some() {
            return;
        }
        let Some(browser) = self.browser.as_ref() else {
            return;
        };
        let BrowserSelection::Child(index) = browser.selected else {
            return;
        };

        let visible = self.visible_browser_child_indices();
        let Some(position) = visible
            .iter()
            .position(|visible_index| *visible_index == index)
        else {
            return;
        };

        let mut records = Vec::new();
        for visible_position in [
            Some(position),
            position.checked_sub(1),
            (position + 1 < visible.len()).then_some(position + 1),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(child) = visible
                .get(visible_position)
                .and_then(|child_index| browser.children.get(*child_index))
            {
                records.push(child.clone());
            }
        }

        for (position, record) in records.into_iter().enumerate() {
            let task = PrefetchTask::Detail(record);
            if position == 0 {
                self.queue_prefetch_priority(task);
            } else {
                self.queue_prefetch(task);
            }
        }
    }

    fn active_row(&self) -> Option<&DashboardRow> {
        let data = self.dashboard.active_tab();
        let visible = self.visible_dashboard_indices();
        let selected = visible
            .iter()
            .copied()
            .find(|index| *index == data.selected)
            .or_else(|| visible.first().copied())?;
        data.rows.get(selected)
    }

    fn set_status(&mut self, message: impl Into<String>, is_error: bool) {
        self.status = StatusLine {
            message: message.into(),
            is_error,
        };
    }

    fn toggle_closed_filter(&mut self) {
        self.show_closed = !self.show_closed;
        self.normalize_dashboard_selection();
        self.normalize_browser_selection();
        self.refill_prefetch_queue();
        let message = if self.show_closed {
            "Showing closed records."
        } else {
            "Hiding closed records."
        };
        self.set_status(message, false);
    }

    fn cycle_browser_child_state_filter(&mut self) {
        let Some(browser) = self.browser.as_mut() else {
            return;
        };

        let states = browser_child_state_options(browser);
        if states.is_empty() {
            browser.child_state_filter = None;
            self.set_status("No child states available to filter.", false);
            return;
        }

        browser.child_state_filter = match browser.child_state_filter.as_deref() {
            None => states.first().cloned(),
            Some(current) => states
                .iter()
                .position(|state| state.eq_ignore_ascii_case(current))
                .and_then(|position| states.get(position + 1).cloned()),
        };

        let filter = browser.child_state_filter.clone();
        let message = browser_child_state_filter_label(filter.as_deref()).to_string();
        self.normalize_browser_selection();
        self.refill_prefetch_queue();
        self.set_status(format!("Child state filter: {message}."), false);
    }

    fn set_error(&mut self, err: impl Into<String>) {
        self.set_status(err, true);
    }

    fn open_jump_modal(&mut self) {
        self.modal = Some(ModalState::Jump(TextModal {
            title: "Jump to Record or KB",
            prompt: "Enter a record number, KB number, or paste a ServiceNow record URL...",
            buffer: TextBuffer::new(),
        }));
    }

    fn selected_action_record(&self) -> Option<RecordRef> {
        match self.screen {
            Screen::Dashboard => self.active_row().map(|row| row.record.clone()),
            Screen::Browser => self
                .browser
                .as_ref()
                .map(|browser| browser.selected_record().clone()),
            Screen::Knowledge => None,
        }
    }

    async fn suspend_for_dump(&mut self) -> Result<(), SnowError> {
        let target = match self.selected_action_record() {
            Some(target) => target,
            None => {
                self.set_error("No record selected.");
                return Ok(());
            }
        };

        let Some(local_core) = self.core.as_local_core() else {
            self.set_error("Full JSON dump is not supported in daemon-backed TUI mode yet.");
            return Ok(());
        };
        let full = local_core
            .client()
            .table(&target.table)
            .display_value(servicenow_rs::prelude::DisplayValue::Both)
            .get(&target.sys_id)
            .await?;

        let mut stdout = io::stdout();
        disable_raw_mode().map_err(|e| SnowError::Api(e.to_string()))?;
        queue!(stdout, LeaveAlternateScreen, Show).map_err(|e| SnowError::Api(e.to_string()))?;
        stdout.flush().map_err(|e| SnowError::Api(e.to_string()))?;
        display::print_full_dump(&display::record_to_json(&full));
        print!("\nPress Enter to return to the TUI...");
        io::stdout()
            .flush()
            .map_err(|e| SnowError::Api(e.to_string()))?;
        let mut line = String::new();
        io::stdin()
            .read_line(&mut line)
            .map_err(|e| SnowError::Api(e.to_string()))?;

        enable_raw_mode().map_err(|e| SnowError::Api(e.to_string()))?;
        execute!(io::stdout(), EnterAlternateScreen, Hide)
            .map_err(|e| SnowError::Api(e.to_string()))?;
        self.set_status("Returned from JSON dump.", false);
        Ok(())
    }

    fn refill_prefetch_queue(&mut self) {
        self.prefetch_queue = match self.screen {
            Screen::Dashboard => build_dashboard_prefetch_queue(&self.dashboard, self.show_closed),
            Screen::Browser => self
                .browser
                .as_ref()
                .map(|browser| build_browser_prefetch_queue(browser, self.show_closed))
                .unwrap_or_default(),
            Screen::Knowledge => Vec::new(),
        };
    }

    fn open_browser_for_record(
        &mut self,
        record: RecordRef,
        detail: RecordDetail,
        children: Vec<RecordRef>,
        approval: Option<ApprovalRef>,
    ) {
        let relation = record
            .kind
            .map(fetch::child_relation)
            .unwrap_or(ChildRelation {
                parent_kind: EntityKind::Incident,
                child_kind: None,
                child_table: None,
                child_link_field: None,
            });
        let mut browser = BrowserState {
            parent: record.clone(),
            parent_kind: record.kind.unwrap_or(EntityKind::Incident),
            relation,
            parent_detail: detail.clone(),
            children,
            selected: BrowserSelection::Parent,
            task_sla_expanded: false,
            detail_cache: HashMap::new(),
            approval,
            search: QuickSearch::new(),
            child_state_filter: None,
        };
        if !browser.children.is_empty() && !matches!(record.kind, Some(EntityKind::Project)) {
            browser.selected = BrowserSelection::Child(0);
        }

        self.detail_cache.insert(
            fetch::detail_cache_key(&record.table, &record.sys_id),
            detail,
        );
        self.children_cache.insert(
            fetch::children_cache_key(&record.table, &record.sys_id),
            browser.children.clone(),
        );
        self.approval_cache.insert(
            fetch::approval_cache_key(&record.table, &record.sys_id),
            browser.approval.clone(),
        );
        self.browser = Some(browser);
        self.normalize_browser_selection();
        self.prefetch_queue = self
            .browser
            .as_ref()
            .map(|browser| build_browser_prefetch_queue(browser, self.show_closed))
            .unwrap_or_default();
        self.screen = Screen::Browser;
    }

    fn visible_dashboard_indices(&self) -> Vec<usize> {
        let data = self.dashboard.active_tab();
        data.rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| {
                row_is_visible(row, self.dashboard.search.query.as_str(), self.show_closed)
                    .then_some(index)
            })
            .collect()
    }

    fn dashboard_tab_visible_count(&self, tab: DashboardTab) -> usize {
        self.dashboard
            .tab(tab)
            .rows
            .iter()
            .filter(|row| {
                row_is_visible(row, self.dashboard.search.query.as_str(), self.show_closed)
            })
            .count()
    }

    fn visible_browser_child_indices(&self) -> Vec<usize> {
        let Some(browser) = self.browser.as_ref() else {
            return Vec::new();
        };

        browser
            .children
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                browser_child_is_visible(record, browser, self.show_closed).then_some(index)
            })
            .collect()
    }

    fn normalize_dashboard_selection(&mut self) {
        let visible = self.visible_dashboard_indices();
        let tab = self.dashboard.active_tab_mut();
        tab.selected = visible
            .iter()
            .copied()
            .find(|index| *index == tab.selected)
            .or_else(|| visible.first().copied())
            .unwrap_or(0);
    }

    fn normalize_browser_selection(&mut self) {
        let visible = self.visible_browser_child_indices();
        let Some(browser) = self.browser.as_mut() else {
            return;
        };

        browser.selected = match browser.selected {
            BrowserSelection::Parent => BrowserSelection::Parent,
            BrowserSelection::Child(current) => visible
                .iter()
                .copied()
                .find(|index| *index == current)
                .or_else(|| visible.first().copied())
                .map(BrowserSelection::Child)
                .unwrap_or(BrowserSelection::Parent),
        };
    }
}

fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let popup_width = width.min(area.width.saturating_sub(2)).max(10);
    let popup_height = height.min(area.height.saturating_sub(2)).max(5);
    Rect {
        x: area.x + (area.width.saturating_sub(popup_width)) / 2,
        y: area.y + (area.height.saturating_sub(popup_height)) / 2,
        width: popup_width,
        height: popup_height,
    }
}

fn modal_block(title: &str) -> Block<'static> {
    Block::default()
        .title(title.to_string())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
}

fn render_multiline_text(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    lines: Vec<Line<'static>>,
) {
    frame.render_widget(Clear, area);
    let paragraph = Paragraph::new(lines)
        .block(modal_block(title))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn build_dashboard_prefetch_queue(
    dashboard: &DashboardState,
    show_closed: bool,
) -> Vec<PrefetchTask> {
    let data = dashboard.active_tab();
    let visible: Vec<usize> = data
        .rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            row_is_visible(row, dashboard.search.query.as_str(), show_closed).then_some(index)
        })
        .collect();
    if visible.is_empty() {
        return Vec::new();
    }

    let selected = visible
        .iter()
        .position(|index| *index == data.selected)
        .unwrap_or(0);
    let mut indices = Vec::new();
    indices.push(visible[selected]);
    for distance in 1..=2 {
        if let Some(next) = selected.checked_add(distance)
            && next < visible.len()
        {
            indices.push(visible[next]);
        }
        if let Some(previous) = selected.checked_sub(distance) {
            indices.push(visible[previous]);
        }
    }

    type BatchGroupKey = (&'static str, &'static str, Option<EntityKind>);
    type BatchGroupValue = (ChildRelation, Vec<RecordRef>);

    let mut queue = Vec::new();
    let mut batch_groups: HashMap<BatchGroupKey, BatchGroupValue> = HashMap::new();
    for index in indices.into_iter().rev() {
        let Some(record) = data.rows.get(index).map(|row| row.record.clone()) else {
            continue;
        };
        if record.kind.is_none() {
            continue;
        }
        let relation = fetch::child_relation(record.kind.expect("kind checked"));
        if let (Some(child_table), Some(link_field)) =
            (relation.child_table, relation.child_link_field)
        {
            batch_groups
                .entry((child_table, link_field, relation.child_kind))
                .or_insert_with(|| (relation, Vec::new()))
                .1
                .push(record.clone());
        }
        queue.push(PrefetchTask::Detail(record));
    }
    for (_, (relation, parents)) in batch_groups {
        if parents.len() >= 2 {
            queue.insert(0, PrefetchTask::BatchChildren { relation, parents });
        } else if let Some(parent) = parents.into_iter().next() {
            queue.insert(0, PrefetchTask::Children(parent));
        }
    }
    queue
}

fn build_browser_prefetch_queue(browser: &BrowserState, show_closed: bool) -> Vec<PrefetchTask> {
    let visible_children: Vec<&RecordRef> = browser
        .children
        .iter()
        .filter(|child| browser_child_is_visible(child, browser, show_closed))
        .collect();
    let selected_child_sys_id = match browser.selected {
        BrowserSelection::Child(index) => browser
            .children
            .get(index)
            .map(|child| child.sys_id.as_str()),
        BrowserSelection::Parent => None,
    };

    let mut queue = Vec::new();
    if let Some(selected_sys_id) = selected_child_sys_id
        && let Some(selected_child) = visible_children
            .iter()
            .find(|child| child.sys_id == selected_sys_id)
    {
        queue.push(PrefetchTask::Detail((**selected_child).clone()));
    }
    for child in visible_children.into_iter().rev() {
        if Some(child.sys_id.as_str()) == selected_child_sys_id {
            continue;
        }
        queue.push(PrefetchTask::Detail(child.clone()));
    }
    queue
}

fn styled_cache_line(value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            "Cache: ".to_string(),
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(value.to_string()),
    ])
}

fn shortcut_line(shortcuts: &[(&str, &str)]) -> Line<'static> {
    let mut spans = Vec::new();
    for (index, (key, label)) in shortcuts.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(
            (*key).to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(format!(" {label}")));
    }
    Line::from(spans)
}

fn row_matches_query(row: &DashboardRow, query: &str) -> bool {
    if query.trim().is_empty() {
        return true;
    }

    let needle = query.to_ascii_lowercase();
    row.cells
        .iter()
        .any(|cell| cell.to_ascii_lowercase().contains(&needle))
        || record_matches_query(&row.record, query)
}

fn row_is_visible(row: &DashboardRow, query: &str, show_closed: bool) -> bool {
    row_matches_query(row, query) && (show_closed || !record_is_terminal(&row.record))
}

fn record_matches_query(record: &RecordRef, query: &str) -> bool {
    if query.trim().is_empty() {
        return true;
    }

    let needle = query.to_ascii_lowercase();
    [
        record.number.as_str(),
        record.short_description.as_str(),
        record.table.as_str(),
        record.state_label.as_deref().unwrap_or(""),
        record.parent_number.as_deref().unwrap_or(""),
    ]
    .into_iter()
    .any(|value| value.to_ascii_lowercase().contains(&needle))
}

fn record_is_visible(record: &RecordRef, query: &str, show_closed: bool) -> bool {
    record_matches_query(record, query) && (show_closed || !record_is_terminal(record))
}

fn browser_child_is_visible(record: &RecordRef, browser: &BrowserState, show_closed: bool) -> bool {
    record_is_visible(record, browser.search.query.as_str(), show_closed)
        && record_matches_state_filter(record, browser.child_state_filter.as_deref())
}

fn record_matches_state_filter(record: &RecordRef, state_filter: Option<&str>) -> bool {
    let Some(state_filter) = state_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return true;
    };
    [record.state_label.as_deref(), record.state_value.as_deref()]
        .into_iter()
        .flatten()
        .any(|state| state.eq_ignore_ascii_case(state_filter))
}

fn browser_child_state_options(browser: &BrowserState) -> Vec<String> {
    let mut states = Vec::new();
    for child in &browser.children {
        let Some(state) = child
            .state_label
            .as_deref()
            .or(child.state_value.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if !states
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(state))
        {
            states.push(state.to_string());
        }
    }
    states.sort_by_key(|state| state.to_ascii_lowercase());
    states
}

fn browser_child_state_filter_label(state_filter: Option<&str>) -> &str {
    state_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("All")
}

fn record_is_terminal(record: &RecordRef) -> bool {
    [record.state_label.as_deref(), record.state_value.as_deref()]
        .into_iter()
        .flatten()
        .any(state_text_is_terminal)
}

fn state_text_is_terminal(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    [
        "closed",
        "resolved",
        "cancelled",
        "canceled",
        "complete",
        "completed",
        "rejected",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(kind: EntityKind, sys_id: &str, number: &str) -> RecordRef {
        RecordRef {
            kind: Some(kind),
            table: match kind {
                EntityKind::Task => "task",
                EntityKind::ChangeRequest => "change_request",
                EntityKind::ChangeTask => "change_task",
                EntityKind::Demand => "dmn_demand",
                EntityKind::Project => "pm_project",
                EntityKind::ResourcePlan => "resource_plan",
                EntityKind::Story => "rm_story",
                EntityKind::ScrumTask => "rm_scrum_task",
                EntityKind::RequestItem => "sc_req_item",
                EntityKind::CatalogTask => "sc_task",
                EntityKind::Incident => "incident",
            }
            .to_string(),
            sys_id: sys_id.to_string(),
            number: number.to_string(),
            short_description: "Sample".to_string(),
            assigned_to: None,
            planned_start: None,
            planned_end: None,
            planned_hours: None,
            allocated_hours: None,
            confirmed_hours: None,
            state_label: Some("Open".to_string()),
            state_value: Some("1".to_string()),
            record_class: None,
            parent_table: None,
            parent_sys_id: None,
            parent_number: None,
            approval: None,
        }
    }

    fn sample_dashboard_row(record: RecordRef) -> DashboardRow {
        DashboardRow {
            cells: vec![
                record.number.clone(),
                record.short_description.clone(),
                record.state_label.clone().unwrap_or_default(),
            ],
            record,
        }
    }

    #[test]
    fn closed_records_are_hidden_unless_show_closed_is_enabled() {
        let mut record = sample_record(EntityKind::Incident, "closed-sys", "INC0010001");
        record.state_label = Some("Closed Complete".to_string());

        assert!(!record_is_visible(&record, "", false));
        assert!(record_is_visible(&record, "", true));
    }

    #[test]
    fn dashboard_prefetch_skips_closed_rows_by_default() {
        let mut closed = sample_record(EntityKind::Incident, "closed-sys", "INC0010001");
        closed.state_label = Some("Resolved".to_string());
        let open = sample_record(EntityKind::Incident, "open-sys", "INC0010002");

        let mut dashboard = DashboardState::new();
        dashboard.approvals.rows = vec![
            sample_dashboard_row(closed.clone()),
            sample_dashboard_row(open.clone()),
        ];

        let queue = build_dashboard_prefetch_queue(&dashboard, false);

        assert!(queue.iter().any(
            |task| matches!(task, PrefetchTask::Detail(record) if record.sys_id == open.sys_id)
        ));
        assert!(!queue.iter().any(
            |task| matches!(task, PrefetchTask::Detail(record) if record.sys_id == closed.sys_id)
        ));
    }

    #[test]
    fn browser_prefetch_prioritizes_selected_child_detail() {
        let parent = sample_record(EntityKind::ChangeRequest, "parent-sys", "CHG0329219");
        let mut child_one = sample_record(EntityKind::ChangeTask, "child-1", "CTASK0660518");
        child_one.parent_table = Some("change_request".to_string());
        child_one.parent_sys_id = Some(parent.sys_id.clone());
        child_one.parent_number = Some(parent.number.clone());

        let mut child_two = sample_record(EntityKind::ChangeTask, "child-2", "CTASK0660519");
        child_two.parent_table = Some("change_request".to_string());
        child_two.parent_sys_id = Some(parent.sys_id.clone());
        child_two.parent_number = Some(parent.number.clone());

        let browser = BrowserState {
            parent: parent.clone(),
            parent_kind: EntityKind::ChangeRequest,
            relation: ChildRelation {
                parent_kind: EntityKind::ChangeRequest,
                child_kind: Some(EntityKind::ChangeTask),
                child_table: Some("change_task"),
                child_link_field: Some("change_request"),
            },
            parent_detail: fetch::placeholder_detail(&parent),
            children: vec![child_one.clone(), child_two],
            selected: BrowserSelection::Child(0),
            task_sla_expanded: false,
            detail_cache: HashMap::new(),
            approval: None,
            search: QuickSearch::new(),
            child_state_filter: None,
        };

        let queue = build_browser_prefetch_queue(&browser, false);
        assert!(
            matches!(queue.first(), Some(PrefetchTask::Detail(record)) if record.sys_id == child_one.sys_id)
        );
    }

    #[test]
    fn browser_child_state_filter_matches_resource_plan_states() {
        let parent = sample_record(EntityKind::Project, "project-sys", "PRJ0161206");
        let mut allocated = sample_record(EntityKind::ResourcePlan, "rpln-1", "RPLN0089255");
        allocated.state_label = Some("Allocated".to_string());
        let mut requested = sample_record(EntityKind::ResourcePlan, "rpln-2", "RPLN0089256");
        requested.state_label = Some("Requested".to_string());

        let browser = BrowserState {
            parent,
            parent_kind: EntityKind::Project,
            relation: ChildRelation {
                parent_kind: EntityKind::Project,
                child_kind: Some(EntityKind::ResourcePlan),
                child_table: Some("resource_plan"),
                child_link_field: Some("task"),
            },
            parent_detail: fetch::placeholder_detail(&sample_record(
                EntityKind::Project,
                "project-sys",
                "PRJ0161206",
            )),
            children: vec![allocated.clone(), requested.clone()],
            selected: BrowserSelection::Child(0),
            task_sla_expanded: false,
            detail_cache: HashMap::new(),
            approval: None,
            search: QuickSearch::new(),
            child_state_filter: Some("Allocated".to_string()),
        };

        assert!(browser_child_is_visible(&allocated, &browser, true));
        assert!(!browser_child_is_visible(&requested, &browser, true));
        assert_eq!(
            browser_child_state_options(&browser),
            vec!["Allocated".to_string(), "Requested".to_string()]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn yield_to_local_scheduler_runs_spawned_local_tasks() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let flag = std::rc::Rc::new(std::cell::Cell::new(false));
                let flag_task = flag.clone();
                tokio::task::spawn_local(async move {
                    flag_task.set(true);
                });
                assert!(!flag.get());
                yield_to_local_scheduler().await;
                assert!(flag.get());
            })
            .await;
    }
}
