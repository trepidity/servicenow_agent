use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::fetch::lifecycle_field;
use super::{
    App, BrowserSelection, ModalState, ReassignModal, Screen, StateModal, TextBuffer, TextModal,
};
use crate::error::SnowError;

impl App {
    pub(super) async fn handle_key(&mut self, key: KeyEvent) -> Result<(), SnowError> {
        if self.modal.is_some() {
            return self.handle_modal_key(key).await;
        }

        match self.screen {
            Screen::Dashboard => self.handle_dashboard_key(key).await,
            Screen::Browser => self.handle_browser_key(key).await,
            Screen::Knowledge => self.handle_knowledge_key(key).await,
        }
    }

    async fn handle_dashboard_key(&mut self, key: KeyEvent) -> Result<(), SnowError> {
        match key.code {
            KeyCode::Char('/') => self.open_jump_modal(),
            KeyCode::Char('K') => self.open_knowledge_browser(),
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => {
                self.dashboard.active_tab = self.dashboard.active_tab.next();
                self.schedule_dashboard_prefetch();
            }
            KeyCode::BackTab => {
                self.dashboard.active_tab = self.dashboard.active_tab.previous();
                self.schedule_dashboard_prefetch();
            }
            KeyCode::Char('j') | KeyCode::Down => {
                move_dashboard_selection(&mut self.dashboard, self.show_closed, 1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                move_dashboard_selection(&mut self.dashboard, self.show_closed, -1);
            }
            KeyCode::Enter => self.open_selected_record()?,
            KeyCode::Char('R') => {
                self.start_dashboard_refresh(true);
            }
            KeyCode::Char('C') => self.toggle_closed_filter(),
            KeyCode::Char('n') => self.open_note_modal(),
            KeyCode::Char('a') => self.open_approve_modal_if_available(),
            KeyCode::Char('r') => self.open_reject_modal_if_available(),
            _ => {}
        }
        Ok(())
    }

    async fn handle_browser_key(&mut self, key: KeyEvent) -> Result<(), SnowError> {
        match key.code {
            KeyCode::Char('/') => self.open_jump_modal(),
            KeyCode::Char('K') => self.open_knowledge_browser(),
            KeyCode::Esc => {
                self.browser = None;
                self.prefetch_queue.clear();
                self.screen = Screen::Dashboard;
                self.schedule_dashboard_prefetch();
                self.set_status("Returned to dashboard.", false);
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.move_browser_selection(1);
                self.schedule_browser_prefetch();
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.move_browser_selection(-1);
                self.schedule_browser_prefetch();
            }
            KeyCode::Char('n') => self.open_note_modal(),
            KeyCode::Char('a') => self.open_approve_modal_if_available(),
            KeyCode::Char('r') => self.open_reject_modal_if_available(),
            KeyCode::Char('C') => self.toggle_closed_filter(),
            KeyCode::Char('f') => self.cycle_browser_child_state_filter(),
            KeyCode::Char('s') => self.open_state_modal().await?,
            KeyCode::Char('S') => self.toggle_browser_task_sla(),
            KeyCode::Char('x') => self.open_reassign_modal().await?,
            KeyCode::Char('o') => self.open_in_browser()?,
            KeyCode::Char('d') => self.suspend_for_dump().await?,
            _ => {}
        }
        Ok(())
    }

    async fn handle_knowledge_key(&mut self, key: KeyEvent) -> Result<(), SnowError> {
        match key.code {
            KeyCode::Char('/') => self.open_jump_modal(),
            KeyCode::Char('K') => self.open_knowledge_browser(),
            KeyCode::Esc => {
                self.screen = Screen::Dashboard;
                self.set_status("Returned to dashboard.", false);
            }
            KeyCode::Tab => self.knowledge_focus_results(),
            KeyCode::BackTab => self.knowledge_focus_search(),
            KeyCode::Down => {
                self.knowledge_focus_results();
                self.move_knowledge_selection(1);
            }
            KeyCode::Up => {
                self.knowledge_focus_results();
                self.move_knowledge_selection(-1);
            }
            KeyCode::Enter => {
                if matches!(
                    self.knowledge.focus,
                    super::knowledge::KnowledgeFocus::Results
                ) {
                    if let Some(number) = self.knowledge_selected_number() {
                        self.start_knowledge_lookup(&number);
                    }
                } else {
                    let query = self.knowledge_query_text().to_string();
                    if super::knowledge::is_knowledge_number(&query) {
                        self.start_knowledge_lookup(&query);
                    } else {
                        self.start_knowledge_search(&query);
                        self.knowledge_focus_results();
                    }
                }
            }
            KeyCode::Backspace => {
                self.knowledge_focus_search();
                self.backspace_knowledge_query();
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.knowledge_focus_search();
                self.append_knowledge_query(ch);
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_modal_key(&mut self, key: KeyEvent) -> Result<(), SnowError> {
        let mut modal = match self.modal.take() {
            Some(modal) => modal,
            None => return Ok(()),
        };

        let keep_modal = match &mut modal {
            ModalState::Jump(text) => handle_jump_modal_key(text, key),
            ModalState::Note(text) => handle_text_modal_key(text, key),
            ModalState::Reject(text) => handle_text_modal_key(text, key),
            ModalState::ApproveConfirm => handle_confirm_modal_key(key),
            ModalState::StatePicker(state) => handle_state_modal_key(state, key),
            ModalState::Reassign(reassign) => handle_reassign_modal_key(reassign, key),
        };

        match keep_modal {
            ModalResult::Keep => {
                if let ModalState::Reassign(reassign) = &mut modal {
                    let query = reassign.query.value.clone();
                    let should_search =
                        query.trim() != reassign.last_searched.trim() || reassign.loading;
                    self.modal = Some(modal);
                    if should_search {
                        self.start_user_search(&query);
                    }
                    return Ok(());
                }
                self.modal = Some(modal);
            }
            ModalResult::Close => {
                self.set_status("Cancelled.", false);
            }
            ModalResult::Submit => match modal {
                ModalState::Jump(text) => {
                    let number = text.buffer.value.trim().to_string();
                    if number.is_empty() {
                        self.set_error("Record number cannot be empty.");
                    } else if super::knowledge::is_knowledge_number(&number) {
                        self.start_knowledge_lookup(&number);
                    } else {
                        self.start_jump_lookup(&number);
                    }
                }
                ModalState::Note(text) => {
                    if text.buffer.is_empty() {
                        self.set_error("Work note cannot be empty.");
                    } else {
                        let Some(target) = self.selected_action_record() else {
                            self.set_error("No record selected.");
                            return Ok(());
                        };
                        self.start_mutation(super::fetch::MutationRequest::Note {
                            target,
                            message: text.buffer.value.trim().to_string(),
                        });
                        self.set_status("Submitting work note...", false);
                    }
                }
                ModalState::Reject(text) => {
                    if text.buffer.is_empty() {
                        self.set_error("Rejection reason cannot be empty.");
                    } else {
                        let Some(approval) = self.active_approval() else {
                            self.set_error("No pending approval is available for this record.");
                            return Ok(());
                        };
                        self.start_mutation(super::fetch::MutationRequest::Reject {
                            approval,
                            reason: text.buffer.value.trim().to_string(),
                        });
                        self.set_status("Submitting rejection...", false);
                    }
                }
                ModalState::ApproveConfirm => {
                    let Some(approval) = self.active_approval() else {
                        self.set_error("No pending approval is available for this record.");
                        return Ok(());
                    };
                    self.start_mutation(super::fetch::MutationRequest::Approve { approval });
                    self.set_status("Submitting approval...", false);
                }
                ModalState::StatePicker(state) => {
                    if state.loading {
                        self.modal = Some(ModalState::StatePicker(state));
                        return Ok(());
                    }
                    if let Some(choice) = state.choices.get(state.selected) {
                        let Some(target) = self.selected_action_record() else {
                            self.set_error("No record selected.");
                            return Ok(());
                        };
                        self.start_mutation(super::fetch::MutationRequest::StateUpdate {
                            target,
                            field_name: state.field_name.clone(),
                            raw_value: choice.value.clone(),
                        });
                        self.set_status("Submitting state update...", false);
                    }
                }
                ModalState::Reassign(reassign) => {
                    if reassign.loading {
                        self.modal = Some(ModalState::Reassign(reassign));
                        return Ok(());
                    }
                    if let Some(user) = reassign.results.get(reassign.selected) {
                        let Some(target) = self.selected_action_record() else {
                            self.set_error("No record selected.");
                            return Ok(());
                        };
                        self.start_mutation(super::fetch::MutationRequest::Reassign {
                            target,
                            assignee_sys_id: user.sys_id.clone(),
                        });
                        self.set_status("Submitting reassignment...", false);
                    } else {
                        self.set_error("No assignee selected.");
                    }
                }
            },
        }
        Ok(())
    }

    fn open_note_modal(&mut self) {
        if self.selected_action_record().is_none() {
            self.set_error("No record selected.");
            return;
        }
        self.modal = Some(ModalState::Note(TextModal {
            title: "Add Work Note",
            prompt: "Enter a work note:",
            buffer: TextBuffer::new(),
        }));
    }

    fn open_approve_modal_if_available(&mut self) {
        if self.active_approval().is_none() {
            self.set_error("No pending approval is available for this record.");
            return;
        }
        self.modal = Some(ModalState::ApproveConfirm);
    }

    fn open_reject_modal_if_available(&mut self) {
        if self.active_approval().is_none() {
            self.set_error("No pending approval is available for this record.");
            return;
        }
        self.modal = Some(ModalState::Reject(TextModal {
            title: "Reject",
            prompt: "Enter a rejection reason:",
            buffer: TextBuffer::new(),
        }));
    }

    async fn open_state_modal(&mut self) -> Result<(), SnowError> {
        let Some(target) = self.selected_action_record() else {
            self.set_error("No record selected.");
            return Ok(());
        };
        let Some(kind) = target.kind else {
            self.set_error("State updates are not supported for this record type.");
            return Ok(());
        };
        let field = lifecycle_field(kind);
        self.modal = Some(ModalState::StatePicker(StateModal {
            title: "Update State",
            field_name: field.to_string(),
            choices: Vec::new(),
            selected: 0,
            loading: true,
        }));
        self.start_state_choices_load(&target.table, field);
        Ok(())
    }

    async fn open_reassign_modal(&mut self) -> Result<(), SnowError> {
        if self.core.is_remote() {
            self.set_error("Reassignment is not supported in daemon-backed TUI mode yet.");
            return Ok(());
        }
        if self.selected_action_record().is_none() {
            self.set_error("No record selected.");
            return Ok(());
        }
        self.modal = Some(ModalState::Reassign(ReassignModal {
            query: TextBuffer::new(),
            results: Vec::new(),
            selected: 0,
            last_searched: String::new(),
            loading: false,
        }));
        Ok(())
    }

    fn toggle_browser_task_sla(&mut self) {
        let Some(browser) = self.browser.as_mut() else {
            return;
        };
        browser.task_sla_expanded = !browser.task_sla_expanded;
        let message = if browser.task_sla_expanded {
            "Task SLA expanded."
        } else {
            "Task SLA collapsed."
        };
        self.set_status(message, false);
    }

    fn open_in_browser(&mut self) -> Result<(), SnowError> {
        let Some(target) = self.selected_action_record() else {
            self.set_error("No record selected.");
            return Ok(());
        };
        let url = self.core.browser_url_by_id(&target.table, &target.sys_id)?;
        open::that(url).map_err(|e| SnowError::Api(e.to_string()))?;
        self.set_status("Opened record in browser.", false);
        Ok(())
    }

    fn move_browser_selection(&mut self, delta: isize) {
        let visible = self.visible_browser_child_indices();
        let Some(browser) = self.browser.as_mut() else {
            return;
        };
        if visible.is_empty() {
            browser.selected = BrowserSelection::Parent;
            return;
        }
        if matches!(browser.selected, BrowserSelection::Parent) {
            browser.selected = if delta > 0 {
                BrowserSelection::Child(visible[0])
            } else {
                BrowserSelection::Parent
            };
            return;
        }

        let current = match browser.selected {
            BrowserSelection::Parent => unreachable!("handled above"),
            BrowserSelection::Child(index) => {
                visible.iter().position(|item| *item == index).unwrap_or(0)
            }
        };
        if delta < 0 && current == 0 {
            browser.selected = BrowserSelection::Parent;
            return;
        }
        let max = visible.len().saturating_sub(1);
        let next = current.saturating_add_signed(delta).clamp(0, max);
        browser.selected = BrowserSelection::Child(visible[next]);
    }
}

fn move_dashboard_selection(
    dashboard: &mut super::DashboardState,
    show_closed: bool,
    delta: isize,
) {
    let visible: Vec<usize> = dashboard
        .active_tab()
        .rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            super::row_is_visible(row, dashboard.search.query.as_str(), show_closed)
                .then_some(index)
        })
        .collect();
    let tab = dashboard.active_tab_mut();
    if visible.is_empty() {
        tab.selected = 0;
        return;
    }
    let current = visible
        .iter()
        .position(|index| *index == tab.selected)
        .unwrap_or(0);
    let max = visible.len().saturating_sub(1);
    let next = current.saturating_add_signed(delta).clamp(0, max);
    tab.selected = visible[next];
}

enum ModalResult {
    Keep,
    Close,
    Submit,
}

fn handle_text_modal_key(modal: &mut TextModal, key: KeyEvent) -> ModalResult {
    match key.code {
        KeyCode::Esc => ModalResult::Close,
        KeyCode::Backspace => {
            modal.buffer.backspace();
            ModalResult::Keep
        }
        KeyCode::Enter => {
            if key.modifiers.contains(KeyModifiers::CONTROL)
                || key.modifiers.contains(KeyModifiers::ALT)
            {
                ModalResult::Keep
            } else {
                modal.buffer.newline();
                ModalResult::Keep
            }
        }
        KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => ModalResult::Submit,
        KeyCode::Char(ch) => {
            modal.buffer.insert(ch);
            ModalResult::Keep
        }
        _ => ModalResult::Keep,
    }
}

fn handle_jump_modal_key(modal: &mut TextModal, key: KeyEvent) -> ModalResult {
    match key.code {
        KeyCode::Esc => ModalResult::Close,
        KeyCode::Enter => ModalResult::Submit,
        KeyCode::Backspace => {
            modal.buffer.backspace();
            ModalResult::Keep
        }
        KeyCode::Char(ch) => {
            modal.buffer.insert(ch);
            ModalResult::Keep
        }
        _ => ModalResult::Keep,
    }
}

fn handle_confirm_modal_key(key: KeyEvent) -> ModalResult {
    match key.code {
        KeyCode::Esc => ModalResult::Close,
        KeyCode::Enter => ModalResult::Submit,
        _ => ModalResult::Keep,
    }
}

fn handle_state_modal_key(modal: &mut StateModal, key: KeyEvent) -> ModalResult {
    match key.code {
        KeyCode::Esc => ModalResult::Close,
        KeyCode::Enter => ModalResult::Submit,
        KeyCode::Char('j') | KeyCode::Down => {
            if !modal.choices.is_empty() {
                modal.selected = (modal.selected + 1).min(modal.choices.len().saturating_sub(1));
            }
            ModalResult::Keep
        }
        KeyCode::Char('k') | KeyCode::Up => {
            modal.selected = modal.selected.saturating_sub(1);
            ModalResult::Keep
        }
        _ => ModalResult::Keep,
    }
}

fn handle_reassign_modal_key(modal: &mut ReassignModal, key: KeyEvent) -> ModalResult {
    match key.code {
        KeyCode::Esc => ModalResult::Close,
        KeyCode::Enter => ModalResult::Submit,
        KeyCode::Backspace => {
            modal.query.backspace();
            ModalResult::Keep
        }
        KeyCode::Char('j') | KeyCode::Down => {
            if !modal.results.is_empty() {
                modal.selected = (modal.selected + 1).min(modal.results.len().saturating_sub(1));
            }
            ModalResult::Keep
        }
        KeyCode::Char('k') | KeyCode::Up => {
            modal.selected = modal.selected.saturating_sub(1);
            ModalResult::Keep
        }
        KeyCode::Char(ch) => {
            modal.query.insert(ch);
            ModalResult::Keep
        }
        _ => ModalResult::Keep,
    }
}
