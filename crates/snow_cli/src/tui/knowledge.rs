use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap};
use snow_core::{KnowledgeArticle, KnowledgeBaseSummary, KnowledgeCategorySummary};

use super::{App, TextBuffer};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum KnowledgeFocus {
    Search,
    Results,
}

#[derive(Clone, Debug)]
pub(super) struct KnowledgeState {
    pub query: TextBuffer,
    pub results: Vec<KnowledgeArticle>,
    pub selected: usize,
    pub focus: KnowledgeFocus,
    pub loading: bool,
    pub error: Option<String>,
    pub bases: Vec<KnowledgeBaseSummary>,
    pub categories: Vec<KnowledgeCategorySummary>,
    pub categories_base_sys_id: Option<String>,
}

impl KnowledgeState {
    pub fn new() -> Self {
        Self {
            query: TextBuffer::new(),
            results: Vec::new(),
            selected: 0,
            focus: KnowledgeFocus::Search,
            loading: false,
            error: None,
            bases: Vec::new(),
            categories: Vec::new(),
            categories_base_sys_id: None,
        }
    }

    pub fn selected_article(&self) -> Option<&KnowledgeArticle> {
        self.results.get(self.selected)
    }

    pub fn selected_base_sys_id(&self) -> Option<&str> {
        self.selected_article()
            .map(|article| article.knowledge_base.sys_id.as_str())
    }

    pub fn clamp_selection(&mut self) {
        if self.results.is_empty() {
            self.selected = 0;
        } else if self.selected >= self.results.len() {
            self.selected = self.results.len().saturating_sub(1);
        }
    }

    pub fn set_results(&mut self, results: Vec<KnowledgeArticle>) {
        self.results = results;
        self.selected = 0;
        self.clamp_selection();
        self.error = None;
    }

    pub fn set_bases(&mut self, bases: Vec<KnowledgeBaseSummary>) {
        self.bases = bases;
    }

    pub fn set_categories(
        &mut self,
        base_sys_id: String,
        categories: Vec<KnowledgeCategorySummary>,
    ) {
        self.categories_base_sys_id = Some(base_sys_id);
        self.categories = categories;
    }

    pub fn visible_bases(&self) -> &[KnowledgeBaseSummary] {
        &self.bases
    }

    pub fn visible_categories(&self) -> &[KnowledgeCategorySummary] {
        &self.categories
    }
}

impl App {
    pub(super) fn open_knowledge_browser(&mut self) {
        self.screen = super::Screen::Knowledge;
        self.knowledge.focus = KnowledgeFocus::Search;
        self.knowledge.clamp_selection();
        self.prefetch_queue.clear();
        self.start_knowledge_bases_load();
        if !self.knowledge.query.value.trim().is_empty() {
            let query = self.knowledge.query.value.clone();
            self.start_knowledge_search(&query);
        } else {
            self.set_status("Knowledge browser opened.", false);
        }
    }

    pub(super) fn knowledge_query_text(&self) -> &str {
        self.knowledge.query.value.as_str()
    }

    pub(super) fn knowledge_selected_number(&self) -> Option<String> {
        self.knowledge
            .selected_article()
            .map(|article| article.record.number.clone())
    }

    pub(super) fn move_knowledge_selection(&mut self, delta: isize) {
        if self.knowledge.results.is_empty() {
            return;
        }
        let len = self.knowledge.results.len() as isize;
        let next = (self.knowledge.selected as isize + delta).clamp(0, len.saturating_sub(1));
        self.knowledge.selected = next as usize;
        self.schedule_knowledge_category_load();
    }

    pub(super) fn knowledge_focus_results(&mut self) {
        self.knowledge.focus = KnowledgeFocus::Results;
        self.schedule_knowledge_category_load();
    }

    pub(super) fn knowledge_focus_search(&mut self) {
        self.knowledge.focus = KnowledgeFocus::Search;
    }

    pub(super) fn append_knowledge_query(&mut self, ch: char) {
        self.knowledge.query.insert(ch);
        let query = self.knowledge.query.value.clone();
        self.start_knowledge_search(&query);
    }

    pub(super) fn backspace_knowledge_query(&mut self) {
        self.knowledge.query.backspace();
        let query = self.knowledge.query.value.clone();
        self.start_knowledge_search(&query);
    }

    pub(super) fn schedule_knowledge_category_load(&mut self) {
        let Some(base_sys_id) = self
            .knowledge
            .selected_base_sys_id()
            .map(ToString::to_string)
        else {
            return;
        };
        if self
            .knowledge
            .categories_base_sys_id
            .as_deref()
            .is_some_and(|loaded| loaded == base_sys_id)
        {
            return;
        }
        self.start_knowledge_categories_load(&base_sys_id);
    }
}

pub(super) fn is_knowledge_number(input: &str) -> bool {
    let value = input.trim();
    value.len() > 2 && value.to_ascii_uppercase().starts_with("KB")
}

pub(super) fn render_knowledge_browser(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let sections =
        Layout::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)]).split(area);
    render_left(frame, sections[0], app);
    render_right(frame, sections[1], app);
}

fn render_left(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let kb = &app.knowledge;
    let sections = Layout::vertical([
        Constraint::Length(5),
        Constraint::Length(8),
        Constraint::Min(8),
    ])
    .split(area);

    let search_lines = vec![
        Line::from(vec![
            Span::styled(
                "Search".to_string(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(if kb.loading { "  loading..." } else { "" }),
        ]),
        Line::from(""),
        Line::from(kb.query.value.clone()),
        Line::from(""),
        Line::from("Type to search  Tab results"),
    ];
    let search = Paragraph::new(search_lines)
        .block(
            Block::default()
                .title("Knowledge Browser")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(search, sections[0]);

    let mut context_lines: Vec<Line<'static>> = Vec::new();
    if let Some(article) = kb.selected_article() {
        context_lines.push(Line::from(vec![
            Span::styled(
                article.knowledge_base.display_name.clone(),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("  ({})", article.knowledge_base.sys_id)),
        ]));
        context_lines.push(Line::from(format!(
            "Category: {}",
            article.category.display_name
        )));
        context_lines.push(Line::from(format!(
            "Article type: {}",
            article.article_type
        )));
        if kb
            .categories_base_sys_id
            .as_deref()
            .is_some_and(|base_sys_id| base_sys_id == article.knowledge_base.sys_id.as_str())
            && !kb.visible_categories().is_empty()
        {
            context_lines.push(Line::from(""));
            context_lines.push(Line::from("Categories"));
            for category in kb.visible_categories().iter().take(4) {
                context_lines.push(Line::from(format!(
                    "{} - {} articles",
                    category.display_name, category.article_count
                )));
            }
        }
    } else if !kb.visible_bases().is_empty() {
        context_lines.push(Line::from("Knowledge bases"));
        for base in kb.visible_bases().iter().take(4) {
            context_lines.push(Line::from(format!(
                "{} ({}) - {} articles",
                base.display_name, base.sys_id, base.article_count
            )));
        }
    } else {
        context_lines.push(Line::from("No knowledge metadata loaded."));
    }

    let context = Paragraph::new(context_lines)
        .block(Block::default().title("Context").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(context, sections[1]);

    let visible_rows = kb.results.iter().collect::<Vec<_>>();
    if visible_rows.is_empty() {
        let empty = Paragraph::new(
            kb.error
                .clone()
                .unwrap_or_else(|| "Type a query to search knowledge articles.".to_string()),
        )
        .block(Block::default().title("Results").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
        frame.render_widget(empty, sections[2]);
        return;
    }

    let rows: Vec<Row<'static>> = visible_rows
        .into_iter()
        .map(|article| {
            Row::new(vec![
                Cell::from(article.record.number.clone()),
                Cell::from(article.record.short_description.clone()),
                Cell::from(article.knowledge_base.display_name.clone()),
                Cell::from(article.category.display_name.clone()),
                Cell::from(article.record.state.clone()),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Percentage(42),
            Constraint::Length(16),
            Constraint::Length(16),
            Constraint::Length(12),
        ],
    )
    .header(
        Row::new(vec!["Number", "Title", "Base", "Category", "State"]).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().title("Results").borders(Borders::ALL))
    .row_highlight_style(Style::default().bg(Color::Blue).fg(Color::Black));

    let mut state = TableState::default();
    match kb.focus {
        KnowledgeFocus::Results if !kb.results.is_empty() => state.select(Some(kb.selected)),
        _ => state.select(None),
    }
    frame.render_stateful_widget(table, sections[2], &mut state);
}

fn render_right(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let kb = &app.knowledge;
    let Some(article) = kb.selected_article() else {
        let empty = Paragraph::new("No knowledge article selected.")
            .block(Block::default().title("Article").borders(Borders::ALL))
            .wrap(Wrap { trim: false });
        frame.render_widget(empty, area);
        return;
    };

    let sections = Layout::vertical([
        Constraint::Length(4),
        Constraint::Length(8),
        Constraint::Min(8),
    ])
    .split(area);

    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            article.record.number.clone(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  {}", article.record.short_description)),
    ]))
    .block(Block::default().title("Article").borders(Borders::ALL));
    frame.render_widget(header, sections[0]);

    let mut metadata_lines = vec![
        Line::from(format!(
            "Knowledge base: {}",
            article.knowledge_base.display_name
        )),
        Line::from(format!("Category: {}", article.category.display_name)),
        Line::from(format!("Article type: {}", article.article_type)),
        Line::from(format!("State: {}", article.record.state)),
    ];
    if let Some(author) = article.author.as_ref() {
        metadata_lines.push(Line::from(format!("Author: {}", author.display_name)));
    }
    if let Some(published_at) = article.published_at.as_ref() {
        metadata_lines.push(Line::from(format!("Published: {published_at}")));
    }
    if let Some(valid_to) = article.valid_to.as_ref() {
        metadata_lines.push(Line::from(format!("Valid to: {valid_to}")));
    }
    let metadata = Paragraph::new(metadata_lines)
        .block(Block::default().title("Metadata").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(metadata, sections[1]);

    let body = Paragraph::new(format_article_body(article))
        .block(Block::default().title("Body").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(body, sections[2]);
}

fn format_article_body(article: &KnowledgeArticle) -> String {
    let body = article.content.trim();
    if body.is_empty() {
        return String::new();
    }
    if !body.contains('<') {
        return normalize_rendered_output(body.to_string());
    }

    render_html_to_text(body)
}

#[derive(Clone, Debug)]
enum ListKind {
    Unordered,
    Ordered { next: usize },
}

#[derive(Clone, Debug)]
struct HtmlTag {
    name: String,
    closing: bool,
    self_closing: bool,
    attrs: String,
}

fn render_html_to_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut text = String::new();
    let mut list_stack: Vec<ListKind> = Vec::new();
    let mut in_pre = false;
    let mut in_code = false;
    let mut active_link: Option<String> = None;
    let mut chars = html.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '<' {
            push_html_text(&mut out, &text, in_pre);
            text.clear();

            let mut raw_tag = String::new();
            for next in chars.by_ref() {
                if next == '>' {
                    break;
                }
                raw_tag.push(next);
            }

            let Some(tag) = parse_html_tag(&raw_tag) else {
                continue;
            };

            match (tag.name.as_str(), tag.closing) {
                ("br", _) => ensure_line_breaks(&mut out, 1),
                ("p" | "div" | "section" | "article" | "blockquote", false) => {
                    ensure_line_breaks(&mut out, 2);
                }
                ("p" | "div" | "section" | "article" | "blockquote", true) => {
                    ensure_line_breaks(&mut out, 2);
                }
                ("ul", false) => {
                    ensure_line_breaks(&mut out, 1);
                    list_stack.push(ListKind::Unordered);
                }
                ("ul", true) => {
                    list_stack.pop();
                    ensure_line_breaks(&mut out, 1);
                }
                ("ol", false) => {
                    ensure_line_breaks(&mut out, 1);
                    list_stack.push(ListKind::Ordered { next: 1 });
                }
                ("ol", true) => {
                    list_stack.pop();
                    ensure_line_breaks(&mut out, 1);
                }
                ("li", false) => {
                    ensure_line_breaks(&mut out, 1);
                    let indent = "  ".repeat(list_stack.len().saturating_sub(1));
                    out.push_str(&indent);
                    match list_stack.last_mut() {
                        Some(ListKind::Unordered) => out.push_str("• "),
                        Some(ListKind::Ordered { next }) => {
                            out.push_str(&format!("{}. ", *next));
                            *next += 1;
                        }
                        None => out.push_str("• "),
                    }
                }
                ("li", true) => ensure_line_breaks(&mut out, 1),
                ("pre", false) => {
                    ensure_line_breaks(&mut out, 2);
                    in_pre = true;
                }
                ("pre", true) => {
                    in_pre = false;
                    ensure_line_breaks(&mut out, 2);
                }
                ("code", false) if !in_pre => {
                    if !out.ends_with([' ', '\n']) && !out.is_empty() {
                        out.push(' ');
                    }
                    out.push('`');
                    in_code = true;
                }
                ("code", true) if !in_pre && in_code => {
                    out.push('`');
                    in_code = false;
                }
                ("h1" | "h2" | "h3" | "h4" | "h5" | "h6", false) => {
                    ensure_line_breaks(&mut out, 2);
                    let level = tag
                        .name
                        .strip_prefix('h')
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(2)
                        .clamp(1, 6);
                    out.push_str(&"#".repeat(level));
                    out.push(' ');
                }
                ("h1" | "h2" | "h3" | "h4" | "h5" | "h6", true) => {
                    ensure_line_breaks(&mut out, 2);
                }
                ("td" | "th", true) => {
                    trim_trailing_spaces(&mut out);
                    if !out.ends_with('\n') {
                        out.push_str(" | ");
                    }
                }
                ("tr", false) => ensure_line_breaks(&mut out, 1),
                ("tr", true) => ensure_line_breaks(&mut out, 1),
                ("a", false) => active_link = href_from_attrs(&tag.attrs),
                ("a", true) => {
                    if let Some(href) = active_link.take()
                        && !href.is_empty()
                        && !out.trim_end().ends_with(&href)
                    {
                        out.push_str(" <");
                        out.push_str(&href);
                        out.push('>');
                    }
                }
                _ => {}
            }

            if tag.self_closing {
                match tag.name.as_str() {
                    "p" | "div" | "section" | "article" => ensure_line_breaks(&mut out, 2),
                    "li" => ensure_line_breaks(&mut out, 1),
                    _ => {}
                }
            }
            continue;
        }

        text.push(ch);
    }

    push_html_text(&mut out, &text, in_pre);
    normalize_rendered_output(out)
}

fn parse_html_tag(raw: &str) -> Option<HtmlTag> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.starts_with('!') || trimmed.starts_with('?') {
        return None;
    }

    let closing = trimmed.starts_with('/');
    let without_prefix = trimmed.trim_start_matches('/').trim();
    if without_prefix.is_empty() {
        return None;
    }

    let self_closing = without_prefix.ends_with('/');
    let core = without_prefix.trim_end_matches('/').trim();
    let mut parts = core.splitn(2, char::is_whitespace);
    let name = parts.next()?.to_ascii_lowercase();
    let attrs = parts.next().unwrap_or_default().trim().to_string();

    Some(HtmlTag {
        name,
        closing,
        self_closing,
        attrs,
    })
}

fn href_from_attrs(attrs: &str) -> Option<String> {
    let lower = attrs.to_ascii_lowercase();
    let href_pos = lower.find("href=")?;
    let value = attrs[href_pos + 5..].trim_start();
    let mut chars = value.chars();
    let quote = chars.next()?;
    if quote == '"' || quote == '\'' {
        let end = value[1..].find(quote)?;
        Some(decode_html_entities(&value[1..1 + end]).trim().to_string())
    } else {
        let end = value.find(char::is_whitespace).unwrap_or(value.len());
        Some(decode_html_entities(&value[..end]).trim().to_string())
    }
}

fn push_html_text(out: &mut String, text: &str, in_pre: bool) {
    if text.is_empty() {
        return;
    }

    let decoded = decode_html_entities(text);
    if in_pre {
        out.push_str(&decoded);
        return;
    }

    let mut pending_space = false;
    for ch in decoded.chars() {
        if ch.is_whitespace() {
            pending_space = true;
            continue;
        }

        if pending_space && !out.is_empty() && !out.ends_with([' ', '\n']) {
            out.push(' ');
        }
        out.push(ch);
        pending_space = false;
    }

    if pending_space && !out.is_empty() && !out.ends_with(' ') && !out.ends_with('\n') {
        out.push(' ');
    }
}

fn ensure_line_breaks(out: &mut String, count: usize) {
    trim_trailing_spaces(out);
    let existing = out.chars().rev().take_while(|ch| *ch == '\n').count();
    if out.is_empty() {
        return;
    }
    for _ in existing..count {
        out.push('\n');
    }
}

fn trim_trailing_spaces(out: &mut String) {
    while out.ends_with(' ') || out.ends_with('\t') {
        out.pop();
    }
}

fn decode_html_entities(input: &str) -> String {
    input
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&ndash;", "-")
        .replace("&mdash;", "--")
}

fn normalize_rendered_output(rendered: String) -> String {
    let mut lines = Vec::new();
    let mut blank_count = 0usize;

    for line in rendered.lines() {
        let cleaned = line.trim_end().trim_end_matches('|').trim_end().to_string();
        if cleaned.is_empty() {
            blank_count += 1;
            if blank_count <= 1 {
                lines.push(String::new());
            }
            continue;
        }

        blank_count = 0;
        lines.push(cleaned);
    }

    lines.join("\n").trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use snow_core::{CacheSource, KnowledgeArticle, Reference, ResourceType, SnowRecord};

    #[test]
    fn knowledge_number_detection_is_case_insensitive() {
        assert!(is_knowledge_number("KB0105015"));
        assert!(is_knowledge_number("kb0105015"));
        assert!(!is_knowledge_number("INC0012345"));
    }

    #[test]
    fn knowledge_state_clamps_selection() {
        let mut state = KnowledgeState::new();
        state.results = vec![sample_article("KB1"), sample_article("KB2")];
        state.selected = 9;
        state.clamp_selection();
        assert_eq!(state.selected, 1);
    }

    #[test]
    fn formats_html_body_for_tui_rendering() {
        let mut article = sample_article("KB1");
        article.content =
            "<p>Line one</p><ul><li>First</li><li>Second</li></ul><div>Done &amp; verified</div>"
                .to_string();

        let formatted = format_article_body(&article);
        assert!(formatted.contains("Line one"));
        assert!(formatted.contains("• First"));
        assert!(formatted.contains("• Second"));
        assert!(formatted.contains("Done & verified"));
        assert!(!formatted.contains("<p>"));
    }

    #[test]
    fn smart_formatter_preserves_structure() {
        let mut article = sample_article("KB2");
        article.content = concat!(
            "<h2>Steps</h2>",
            "<ol><li>Open the portal</li><li>Follow the <a href=\"https://example.test/runbook\">runbook</a></li></ol>",
            "<pre>line1\n  line2</pre>"
        )
        .to_string();

        let formatted = format_article_body(&article);
        assert!(formatted.contains("## Steps"));
        assert!(formatted.contains("1. Open the portal"));
        assert!(formatted.contains("2. Follow the runbook <https://example.test/runbook>"));
        assert!(formatted.contains("line1\n  line2"));
    }

    fn sample_article(number: &str) -> KnowledgeArticle {
        KnowledgeArticle {
            record: SnowRecord {
                sys_id: format!("{number}-sys"),
                number: number.to_string(),
                table: "kb_knowledge".to_string(),
                resource_type: ResourceType::Knowledge,
                state: "published".to_string(),
                short_description: "Sample article".to_string(),
                description: "Sample body".to_string(),
                fields: Default::default(),
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references: Default::default(),
                synced_at: chrono::Utc::now(),
                source: CacheSource::Disk,
            },
            knowledge_base: Reference {
                sys_id: "kb-base".to_string(),
                table: "kb_knowledge_base".to_string(),
                display_name: "Base".to_string(),
                extra: Default::default(),
            },
            category: Reference {
                sys_id: "kb-cat".to_string(),
                table: "kb_category".to_string(),
                display_name: "Category".to_string(),
                extra: Default::default(),
            },
            article_type: "text".to_string(),
            content: "Sample body".to_string(),
            sn_tags: vec!["sample".to_string()],
            auto_tags: vec!["article".to_string()],
            user_tags: vec!["runbook".to_string()],
            body_cached: true,
            published_at: None,
            author: None,
            valid_to: None,
        }
    }
}
