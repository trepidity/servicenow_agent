use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap};

use super::{App, ModalState, centered_rect, modal_block, render_multiline_text};

pub(super) fn render_modal(frame: &mut Frame<'_>, app: &App) {
    let Some(modal) = app.modal.as_ref() else {
        return;
    };

    match modal {
        ModalState::Jump(modal) => {
            let area = centered_rect(frame.area(), 70, 8);
            let lines = vec![
                Line::from(modal.prompt),
                Line::from(""),
                Line::from(modal.buffer.value.clone()),
                Line::from(""),
                Line::from("Enter jump  Esc cancel"),
            ];
            render_multiline_text(frame, area, modal.title, lines);
        }
        ModalState::Note(modal) => {
            let area = centered_rect(frame.area(), 80, 14);
            let mut lines = vec![Line::from(modal.prompt)];
            lines.push(Line::from(""));
            lines.extend(modal.buffer.lines());
            lines.push(Line::from(""));
            lines.push(Line::from("Ctrl+S submit  Esc cancel"));
            render_multiline_text(frame, area, modal.title, lines);
        }
        ModalState::Reject(modal) => {
            let area = centered_rect(frame.area(), 80, 14);
            let mut lines = vec![Line::from(modal.prompt)];
            lines.push(Line::from(""));
            lines.extend(modal.buffer.lines());
            lines.push(Line::from(""));
            lines.push(Line::from("Ctrl+S submit  Esc cancel"));
            render_multiline_text(frame, area, modal.title, lines);
        }
        ModalState::ApproveConfirm => {
            let area = centered_rect(frame.area(), 60, 7);
            frame.render_widget(ratatui::widgets::Clear, area);
            let prompt = Paragraph::new(vec![
                Line::from("Approve the selected record?"),
                Line::from(""),
                Line::from("Enter confirm  Esc cancel"),
            ])
            .block(modal_block("Approve"))
            .wrap(Wrap { trim: false });
            frame.render_widget(prompt, area);
        }
        ModalState::StatePicker(modal) => {
            let area = centered_rect(frame.area(), 60, 14);
            frame.render_widget(ratatui::widgets::Clear, area);
            let sections =
                Layout::vertical([Constraint::Length(3), Constraint::Min(6)]).split(area);
            let header_text = if modal.loading {
                format!("Loading {} choices...", modal.field_name)
            } else {
                format!("Select {} value and press Enter.", modal.field_name)
            };
            let header = Paragraph::new(header_text)
                .block(Block::default().title(modal.title).borders(Borders::ALL));
            frame.render_widget(header, sections[0]);

            let rows: Vec<Row<'static>> = if modal.loading {
                vec![Row::new(vec![Cell::from("Loading..."), Cell::from("")])]
            } else if modal.choices.is_empty() {
                vec![Row::new(vec![Cell::from("No choices"), Cell::from("")])]
            } else {
                modal
                    .choices
                    .iter()
                    .map(|choice| {
                        Row::new(vec![
                            Cell::from(choice.label.clone()),
                            Cell::from(choice.value.clone()),
                        ])
                    })
                    .collect()
            };
            let table = Table::new(
                rows,
                [Constraint::Percentage(70), Constraint::Percentage(30)],
            )
            .header(
                Row::new(vec!["Label", "Raw Value"]).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            )
            .block(Block::default().title("Choices").borders(Borders::ALL))
            .row_highlight_style(Style::default().bg(Color::Blue).fg(Color::Black));
            let mut state = TableState::default();
            if !modal.loading && !modal.choices.is_empty() {
                state.select(Some(modal.selected));
            }
            frame.render_stateful_widget(table, sections[1], &mut state);
        }
        ModalState::Reassign(modal) => {
            let area = centered_rect(frame.area(), 80, 16);
            frame.render_widget(ratatui::widgets::Clear, area);
            let sections = Layout::vertical([
                Constraint::Length(4),
                Constraint::Min(6),
                Constraint::Length(2),
            ])
            .split(area);
            let input = Paragraph::new(vec![
                Line::from("Search user by name"),
                Line::from(""),
                Line::from(modal.query.value.clone()),
            ])
            .block(modal_block("Reassign"))
            .wrap(Wrap { trim: false });
            frame.render_widget(input, sections[0]);

            let rows: Vec<Row<'static>> = if modal.loading {
                vec![Row::new(vec![Cell::from("Searching...")])]
            } else if modal.results.is_empty() {
                vec![Row::new(vec![Cell::from("Type at least 2 characters")])]
            } else {
                modal
                    .results
                    .iter()
                    .map(|user| Row::new(vec![Cell::from(user.label.clone())]))
                    .collect()
            };
            let table = Table::new(rows, [Constraint::Percentage(100)])
                .block(Block::default().title("Matches").borders(Borders::ALL))
                .row_highlight_style(Style::default().bg(Color::Blue).fg(Color::Black));
            let mut state = TableState::default();
            if !modal.loading && !modal.results.is_empty() {
                state.select(Some(modal.selected));
            }
            frame.render_stateful_widget(table, sections[1], &mut state);

            let footer = Paragraph::new(Line::from(vec![
                Span::raw("Enter select  Esc cancel  "),
                Span::styled(
                    "typing updates search",
                    Style::default().fg(Color::DarkGray),
                ),
            ]))
            .block(Block::default().borders(Borders::ALL));
            frame.render_widget(footer, sections[2]);
        }
    }
}
