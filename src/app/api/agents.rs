use std::time::Duration;

use bytes::Bytes;

use crate::api::schema::{
    AgentPromptModelParams, AgentPromptParams, AgentRenameParams, AgentSendKeysParams,
    AgentSessionRotateV1Params, AgentSessionRotationResult, AgentStartParams, AgentTarget,
    PaneReadResult, ResponseResult,
};
use crate::app::App;

use super::responses::{encode_error, encode_error_body, encode_success};

const AGENT_PROMPT_SUBMIT_DELAY: Duration = Duration::from_millis(300);
type QueuedAgentPrompt = (
    String,
    crate::api::schema::AgentInfo,
    Option<String>,
    std::sync::mpsc::Receiver<std::io::Result<()>>,
);

// Codex's Windows input reader does not surface bracketed paste. It detects the prompt as a
// "paste burst" and, while that burst is buffered, rewrites a following Enter into a newline
// instead of submitting. The burst only flushes after an idle timeout, so any size-based delay is
// a timing guess that fails when ConPTY delivery lags it. Codex flushes a buffered burst
// synchronously when it receives a non-character key, so appending one after the paste gives the
// submission a deterministic paste boundary regardless of prompt size or delivery speed.
#[cfg(windows)]
fn append_codex_paste_boundary(runtime: &crate::terminal::TerminalRuntime, text: &mut Vec<u8>) {
    let keys = match crate::app::api_helpers::encode_api_keys(runtime, &["right".to_string()]) {
        Ok(keys) => keys,
        Err(key) => {
            tracing::warn!(key = %key, "failed to encode Codex paste boundary key");
            return;
        }
    };
    if let Some(key) = keys.into_iter().find(|bytes| !bytes.is_empty()) {
        text.extend_from_slice(&key);
    }
}

impl App {
    pub(super) fn handle_agent_list(&mut self, id: String) -> String {
        encode_success(
            id,
            ResponseResult::AgentList {
                agents: self.collect_agent_infos(),
            },
        )
    }

    pub(super) fn handle_agent_get(&mut self, id: String, target: AgentTarget) -> String {
        self.reconcile_managed_agent_target(&target.target);
        let agent = match self.agent_info_for_target(&target.target) {
            Ok(agent) => agent,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_focus(&mut self, id: String, target: AgentTarget) -> String {
        let agent = match self.focus_agent_target(&target.target) {
            Ok(agent) => agent,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_rename(&mut self, id: String, params: AgentRenameParams) -> String {
        let agent = match self.rename_agent_target(&params.target, params.name) {
            Ok(agent) => agent,
            Err(err) => return encode_error_body(id, self.agent_rename_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_start(&mut self, id: String, params: AgentStartParams) -> String {
        let (agent, argv) = match self.start_agent(params) {
            Ok(started) => started,
            Err(err) => return encode_error_body(id, self.agent_start_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentStarted { agent, argv })
    }

    pub(super) fn handle_agent_session_rotate_v1(
        &mut self,
        id: String,
        params: AgentSessionRotateV1Params,
    ) -> String {
        let fence_lost = |reason: &str| AgentSessionRotationResult::FenceLost {
            operation_id: params.operation_id.clone(),
            pane_id: params.pane_id.clone(),
            reason: reason.into(),
        };
        let Some((ws_idx, pane_id)) = self.parse_current_public_pane_id(&params.pane_id) else {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("pane_not_found"),
                },
            );
        };
        let Some(agent) = self.agent_info(ws_idx, pane_id) else {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("native_session_changed"),
                },
            );
        };
        if agent.terminal_id != params.expected_terminal_id {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("terminal_changed"),
                },
            );
        }
        let launch_argv = match managed_rotation_argv(&params) {
            Ok(argv) => argv,
            Err(reason) => {
                return encode_success(
                    id,
                    ResponseResult::AgentSessionRotation {
                        rotation: AgentSessionRotationResult::UnsupportedLaunch {
                            operation_id: params.operation_id,
                            pane_id: params.pane_id,
                            reason,
                        },
                    },
                );
            }
        };
        let supervisor_request = crate::agent_session_supervisor::SupervisorRotationRequest {
            operation_id: params.operation_id.clone(),
            pane_id: params.pane_id.clone(),
            terminal_id: params.expected_terminal_id.clone(),
            expected_session: params.expected_session.clone(),
            expected_state_change_seq: params.expected_state_change_seq,
            launch_argv,
        };
        let supervisor_status =
            crate::agent_session_supervisor::query_rotation(&supervisor_request);
        if let Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Completed {
            receipt,
        }) = &supervisor_status
        {
            let rotation = if receipt.pane_id == agent.pane_id
                && receipt.terminal_id == agent.terminal_id
                && receipt.operation_id == params.operation_id
            {
                AgentSessionRotationResult::AlreadyApplied {
                    receipt: receipt.as_ref().clone(),
                }
            } else {
                AgentSessionRotationResult::Uncertain {
                    operation_id: params.operation_id,
                    pane_id: params.pane_id,
                    reason: "stored_receipt_identity_mismatch".into(),
                }
            };
            return encode_success(id, ResponseResult::AgentSessionRotation { rotation });
        }
        if matches!(
            supervisor_status,
            Ok(
                crate::agent_session_supervisor::SupervisorRotationResponse::Launched
                    | crate::agent_session_supervisor::SupervisorRotationResponse::AlreadyApplied
            )
        ) {
            if let Some(new_session) = agent
                .agent_session
                .clone()
                .filter(|session| session != &params.expected_session)
            {
                let receipt = crate::api::schema::AgentSessionRotationReceipt {
                    operation_id: params.operation_id.clone(),
                    pane_id: params.pane_id.clone(),
                    terminal_id: params.expected_terminal_id.clone(),
                    old_session: params.expected_session.clone(),
                    new_session,
                };
                let rotation = match crate::agent_session_supervisor::complete_rotation(
                    &supervisor_request,
                    &receipt,
                ) {
                    Ok(
                        crate::agent_session_supervisor::SupervisorRotationResponse::Completed {
                            ..
                        },
                    ) => AgentSessionRotationResult::Rotated { receipt },
                    Ok(_) => AgentSessionRotationResult::Uncertain {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                        reason: "receipt_not_persisted".into(),
                    },
                    Err(err) => AgentSessionRotationResult::Uncertain {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                        reason: format!("receipt_transport_failed:{err}"),
                    },
                };
                return encode_success(id, ResponseResult::AgentSessionRotation { rotation });
            }
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: AgentSessionRotationResult::Rotating {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                    },
                },
            );
        }
        if let Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Pending) =
            supervisor_status
        {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: AgentSessionRotationResult::Uncertain {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                        reason: "supervisor_operation_interrupted".into(),
                    },
                },
            );
        }
        if let Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Conflict) =
            supervisor_status
        {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("operation_id_reused"),
                },
            );
        }
        if agent.agent_session.as_ref() != Some(&params.expected_session) {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("native_session_changed"),
                },
            );
        }
        if agent.state_change_seq != params.expected_state_change_seq {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("state_changed"),
                },
            );
        }
        if agent.agent_status != crate::api::schema::AgentStatus::Idle || !agent.interactive_ready {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("agent_not_idle"),
                },
            );
        }
        if agent.name.as_deref() != Some(params.launch.name.as_str()) {
            return encode_success(
                id,
                ResponseResult::AgentSessionRotation {
                    rotation: fence_lost("agent_name_changed"),
                },
            );
        }
        let rotation = match supervisor_status {
            Err(err) if supervisor_endpoint_is_absent(&err) => AgentSessionRotationResult::UnsupportedLaunch {
                operation_id: params.operation_id,
                pane_id: params.pane_id,
                reason: "pane_not_rotatable".into(),
            },
            Err(err) => AgentSessionRotationResult::Uncertain {
                operation_id: params.operation_id,
                pane_id: params.pane_id,
                reason: format!("supervisor_status_failed:{err}"),
            },
            Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Unknown) => {
                match crate::agent_session_supervisor::request_rotation(&supervisor_request) {
                    Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Launched
                    | crate::agent_session_supervisor::SupervisorRotationResponse::AlreadyApplied) => {
                        AgentSessionRotationResult::Rotating {
                            operation_id: params.operation_id,
                            pane_id: params.pane_id,
                        }
                    }
                    Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Conflict) => {
                        fence_lost("operation_id_reused")
                    }
                    Ok(crate::agent_session_supervisor::SupervisorRotationResponse::DefinitelyNotStarted {
                        reason,
                    }) => AgentSessionRotationResult::DefinitelyNotStarted {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                        reason,
                    },
                    Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Uncertain {
                        reason,
                    }) => AgentSessionRotationResult::Uncertain {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                        reason,
                    },
                    Ok(_) => AgentSessionRotationResult::Uncertain {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                        reason: "unexpected_supervisor_response".into(),
                    },
                    Err(err) => AgentSessionRotationResult::Uncertain {
                        operation_id: params.operation_id,
                        pane_id: params.pane_id,
                        reason: format!("supervisor_transport_failed:{err}"),
                    },
                }
            }
            Ok(crate::agent_session_supervisor::SupervisorRotationResponse::DefinitelyNotStarted {
                reason,
            }) => AgentSessionRotationResult::DefinitelyNotStarted {
                operation_id: params.operation_id,
                pane_id: params.pane_id,
                reason,
            },
            Ok(crate::agent_session_supervisor::SupervisorRotationResponse::Uncertain {
                reason,
            }) => AgentSessionRotationResult::Uncertain {
                operation_id: params.operation_id,
                pane_id: params.pane_id,
                reason,
            },
            Ok(_) => AgentSessionRotationResult::Uncertain {
                operation_id: params.operation_id,
                pane_id: params.pane_id,
                reason: "unexpected_supervisor_state".into(),
            },
        };
        encode_success(id, ResponseResult::AgentSessionRotation { rotation })
    }

    pub(crate) fn handle_deferred_agent_api_request(
        &mut self,
        request: crate::api::schema::Request,
        respond_to: std::sync::mpsc::Sender<String>,
    ) -> bool {
        match request.method {
            crate::api::schema::Method::AgentPrompt(params) => {
                match self.queue_agent_prompt(request.id, params) {
                    Ok((id, agent, submission_id, completion)) => {
                        std::thread::spawn(move || {
                            let response = match completion.recv() {
                                Ok(Ok(())) => encode_success(
                                    id,
                                    ResponseResult::AgentPrompted {
                                        agent,
                                        submission_id,
                                    },
                                ),
                                Ok(Err(err)) if err.kind() == std::io::ErrorKind::TimedOut => {
                                    encode_error(id, "timeout", err.to_string())
                                }
                                Ok(Err(err)) => {
                                    encode_error(id, "agent_prompt_failed", err.to_string())
                                }
                                Err(_) => {
                                    encode_error(id, "agent_prompt_failed", "pty actor closed")
                                }
                            };
                            let _ = respond_to.send(response);
                        });
                    }
                    Err(response) => {
                        let _ = respond_to.send(response);
                    }
                }
            }
            crate::api::schema::Method::AgentPromptModel(params) => {
                let response = self.begin_agent_model_resume(request.id, params);
                let _ = respond_to.send(response);
            }
            _ => return false,
        }
        true
    }

    fn begin_agent_model_resume(&mut self, id: String, params: AgentPromptModelParams) -> String {
        if params.text.is_empty()
            || params.submission_id.is_empty()
            || params.submission_id.len() > 256
            || params.expected_session_id.is_empty()
            || params.expected_session_id.len() > 512
        {
            return encode_error(
                id,
                "invalid_agent_prompt",
                "model prompt identity or text is invalid",
            );
        }
        let resolved = match self.resolve_agent_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
            .cloned()
        else {
            return agent_not_found(id, &params.target);
        };
        let Some(terminal) = self.state.terminals.get(&terminal_id) else {
            return agent_not_found(id, &params.target);
        };
        if terminal.state != crate::detect::AgentState::Idle
            || terminal.effective_known_agent() != Some(crate::detect::Agent::Traex)
            || terminal.pending_agent_resume_plan.is_some()
        {
            return agent_not_ready(id, &params.target);
        }
        let Some(session) = terminal.persisted_agent_session.as_ref() else {
            return encode_error(
                id,
                "agent_session_changed",
                "agent native session is unavailable",
            );
        };
        if session.agent != "traex"
            || session.session_ref.value != params.expected_session_id
            || session.session_ref.kind != crate::agent_resume::AgentSessionRefKind::Id
        {
            return encode_error(
                id,
                "agent_session_changed",
                "agent native session no longer matches the model prompt precondition",
            );
        }
        let Some(runtime) = self.lookup_runtime_sender(resolved.ws_idx, resolved.pane_id) else {
            return agent_not_found(id, &params.target);
        };
        if !super::super::agents::runtime_hosts_agent(runtime, crate::detect::Agent::Traex) {
            return agent_not_ready(id, &params.target);
        }
        let Some(plan) =
            crate::agent_resume::plan_traex_with_model(&session.session_ref, &params.model)
        else {
            return encode_error(id, "invalid_agent_prompt", "model is invalid");
        };
        let Some(terminal) = self.state.terminals.get_mut(&terminal_id) else {
            return agent_not_found(id, &params.target);
        };
        terminal.prepare_agent_runtime_replacement(plan);
        self.shutdown_terminal_runtime(terminal_id);
        encode_success(id, ResponseResult::Ok {})
    }

    fn queue_agent_prompt(
        &mut self,
        id: String,
        params: AgentPromptParams,
    ) -> Result<QueuedAgentPrompt, String> {
        if params.text.is_empty() {
            return Err(encode_error(
                id,
                "empty_agent_prompt",
                "agent prompt must not be empty",
            ));
        }
        if params
            .submission_id
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 256)
            || params
                .expected_session_id
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 512)
        {
            return Err(encode_error(
                id,
                "invalid_agent_prompt",
                "agent prompt submission identity is invalid",
            ));
        }
        let resolved = match self.resolve_agent_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return Err(encode_error_body(id, self.agent_target_error_body(err))),
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
            .cloned()
        else {
            return Err(agent_not_found(id, &params.target));
        };
        let Some(terminal) = self.state.terminals.get(&terminal_id) else {
            return Err(agent_not_found(id, &params.target));
        };
        if terminal.state == crate::detect::AgentState::Blocked {
            return Err(encode_error(
                id,
                "agent_blocked",
                format!(
                    "agent {} is blocked and requires interactive input",
                    params.target
                ),
            ));
        }
        let Some(expected_agent) = terminal.effective_known_agent() else {
            return Err(agent_not_ready(id, &params.target));
        };
        if terminal.managed_agent_launch_pending() {
            return Err(agent_not_ready(id, &params.target));
        }
        let Some(runtime) = self.lookup_runtime_sender(resolved.ws_idx, resolved.pane_id) else {
            return Err(agent_not_found(id, &params.target));
        };
        if !super::super::agents::runtime_hosts_agent(runtime, expected_agent) {
            return Err(encode_error(
                id,
                "agent_not_ready",
                format!(
                    "agent {} is no longer the pane foreground process",
                    params.target
                ),
            ));
        }
        #[cfg(windows)]
        let submit_deadline = params
            .wait
            .as_ref()
            .and_then(|wait| wait.submission_deadline);
        #[cfg(not(windows))]
        let submit_deadline = None;
        let Some(agent) = self.agent_info(resolved.ws_idx, resolved.pane_id) else {
            return Err(agent_not_found(id, &params.target));
        };
        if let Some(expected_session_id) = params.expected_session_id.as_deref() {
            if agent
                .agent_session
                .as_ref()
                .map(|session| session.value.as_str())
                != Some(expected_session_id)
            {
                return Err(encode_error(
                    id,
                    "agent_session_changed",
                    "agent native session no longer matches the prompt precondition",
                ));
            }
        }
        if expected_agent == crate::detect::Agent::GithubCopilot {
            // Copilot ignores synthetic Enter after focus loss until it receives focus gained.
            let focus = match crate::ghostty::encode_focus(crate::ghostty::FocusEvent::Gained) {
                Ok(focus) => focus,
                Err(err) => {
                    return Err(encode_error(id, "agent_prompt_failed", err.to_string()));
                }
            };
            if let Err(err) = runtime.try_send_bytes(Bytes::from(focus)) {
                return Err(encode_error(id, "agent_prompt_failed", err.to_string()));
            }
        }
        let (text, enter) =
            crate::app::api_helpers::encode_api_submission_parts(runtime, &params.text);
        #[cfg(windows)]
        let text = if expected_agent == crate::detect::Agent::Codex {
            let mut text = text;
            append_codex_paste_boundary(runtime, &mut text);
            text
        } else {
            text
        };
        let completion = runtime
            .queue_user_input_submission(
                Bytes::from(text),
                Bytes::from(enter),
                AGENT_PROMPT_SUBMIT_DELAY,
                submit_deadline,
            )
            .map_err(|err| encode_error(id.clone(), "agent_prompt_failed", err.to_string()))?;
        Ok((id, agent, params.submission_id, completion))
    }

    pub(super) fn handle_agent_read(
        &mut self,
        id: String,
        params: crate::api::schema::AgentReadParams,
    ) -> String {
        let resolved = match self.resolve_agent_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some((pane, workspace_id)) = self.lookup_runtime(resolved.ws_idx, resolved.pane_id)
        else {
            return agent_not_found(id, &params.target);
        };
        let snapshot = crate::app::api_helpers::read_terminal_snapshot(
            pane,
            params.source,
            params.format,
            params.lines,
        );

        encode_success(
            id,
            ResponseResult::PaneRead {
                read: PaneReadResult {
                    pane_id: self
                        .public_pane_id(resolved.ws_idx, resolved.pane_id)
                        .unwrap_or_else(|| params.target.clone()),
                    workspace_id,
                    tab_id: self
                        .public_tab_id(resolved.ws_idx, resolved.tab_idx)
                        .unwrap(),
                    source: params.source,
                    format: params.format,
                    text: snapshot.text,
                    revision: 0,
                    truncated: snapshot.truncated,
                },
            },
        )
    }

    pub(super) fn handle_agent_explain(&mut self, id: String, target: AgentTarget) -> String {
        let resolved = match self.resolve_agent_target(&target.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some((pane, _workspace_id)) = self.lookup_runtime(resolved.ws_idx, resolved.pane_id)
        else {
            return agent_not_found(id, &target.target);
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
        else {
            return agent_not_found(id, &target.target);
        };
        let Some(terminal) = self.state.terminals.get(terminal_id) else {
            return agent_not_found(id, &target.target);
        };
        if terminal.full_lifecycle_hook_authority_active() {
            let explain = serde_json::json!({
                "agent": terminal.effective_agent_label().unwrap_or("unknown"),
                "state": crate::detect::manifest::agent_state_label(terminal.state),
                "manifest_source": null,
                "manifest_version": null,
                "cached_remote_version": null,
                "local_override_shadowing_remote": false,
                "remote_update_status": null,
                "remote_update_error": null,
                "matched_rule": null,
                "visible_idle": false,
                "visible_blocker": false,
                "visible_working": false,
                "screen_detection_skipped": true,
                "screen_detection_skip_reason": "full_lifecycle_hook_authority",
                "skip_state_update": false,
                "skipped_update_reason": null,
                "fallback_reason": null,
                "warning": null,
                "evaluated_rules": [],
            });
            return encode_success(id, ResponseResult::AgentExplain { explain });
        }
        let Some(agent) = terminal.effective_known_agent().or(terminal.detected_agent) else {
            return encode_error(
                id,
                "agent_explain_unavailable",
                format!(
                    "agent target {} does not have a detected agent label",
                    target.target
                ),
            );
        };

        let screen = pane.detection_text();
        let osc_title = pane.agent_osc_title();
        let osc_progress = pane.agent_osc_progress();
        let explain = crate::detect::manifest::explain_with_input(
            agent,
            crate::detect::manifest::DetectionInput {
                screen: &screen,
                osc_title: &osc_title,
                osc_progress: &osc_progress,
            },
        );
        let value = crate::detect::manifest::explain_to_json_value(&explain);

        encode_success(id, ResponseResult::AgentExplain { explain: value })
    }

    pub(super) fn handle_agent_send_keys(
        &mut self,
        id: String,
        params: AgentSendKeysParams,
    ) -> String {
        let resolved = match self.resolve_agent_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
        else {
            return agent_not_found(id, &params.target);
        };
        let Some(expected_agent) = self
            .state
            .terminals
            .get(terminal_id)
            .and_then(|terminal| terminal.effective_known_agent())
        else {
            return agent_not_ready(id, &params.target);
        };
        let Some(runtime) = self.lookup_runtime_sender(resolved.ws_idx, resolved.pane_id) else {
            return agent_not_found(id, &params.target);
        };
        if !super::super::agents::runtime_hosts_agent(runtime, expected_agent) {
            return agent_not_ready(id, &params.target);
        }
        let encoded = match super::super::api_helpers::encode_api_keys(runtime, &params.keys) {
            Ok(encoded) => encoded,
            Err(key) => {
                return encode_error(id, "invalid_key", format!("unsupported key {key}"));
            }
        };
        let bytes: Vec<u8> = encoded.into_iter().flatten().collect();
        if let Err(err) = runtime.try_send_bytes(Bytes::from(bytes)) {
            return encode_error(id, "agent_send_keys_failed", err.to_string());
        }

        encode_success(id, ResponseResult::Ok {})
    }
}

fn agent_not_ready(id: String, target: &str) -> String {
    encode_error(
        id,
        "agent_not_ready",
        format!("agent {target} is not an active named agent"),
    )
}

fn supervisor_endpoint_is_absent(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::NotFound
}

fn managed_rotation_argv(params: &AgentSessionRotateV1Params) -> Result<Vec<String>, String> {
    if params.operation_id.is_empty()
        || params.operation_id.len() > 128
        || !params
            .operation_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '.'))
    {
        return Err("invalid_operation_id".into());
    }
    let Some(agent) = crate::detect::parse_agent_label(&params.launch.kind) else {
        return Err("unsupported_agent_kind".into());
    };
    if agent != crate::detect::Agent::Traex {
        return Err("unsupported_agent_kind".into());
    }
    if params
        .launch
        .args
        .iter()
        .any(|arg| arg.chars().any(char::is_control))
    {
        return Err("invalid_launch_argument".into());
    }
    let timeout = params.launch.timeout_ms.unwrap_or(30_000);
    if timeout <= super::super::agents::AGENT_START_SETTLE_DELAY.as_millis() as u64
        || timeout > super::super::agents::MAX_AGENT_START_TIMEOUT.as_millis() as u64
    {
        return Err("invalid_launch_timeout".into());
    }
    let mut argv = vec![crate::detect::interactive_agent_executable(agent).to_string()];
    argv.extend(params.launch.args.iter().cloned());
    Ok(argv)
}

fn agent_not_found(id: String, target: &str) -> String {
    encode_error(
        id,
        "agent_not_found",
        format!("agent target {target} not found"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::schema::{AgentStatus, SuccessResponse},
        app::Mode,
        config::Config,
        detect::{Agent, AgentState},
        workspace::Workspace,
    };

    fn app_with_agent() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.workspaces = vec![Workspace::test_new("agent")];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app.state.selected = 0;
        app.state.mode = Mode::Terminal;
        app
    }

    #[test]
    fn only_an_absent_supervisor_endpoint_means_the_launch_is_unsupported() {
        assert!(supervisor_endpoint_is_absent(&std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "missing socket",
        )));
        assert!(!supervisor_endpoint_is_absent(&std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "stale socket",
        )));
        assert!(!supervisor_endpoint_is_absent(&std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "live supervisor did not answer",
        )));
        assert!(!supervisor_endpoint_is_absent(&std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed supervisor response",
        )));
    }

    fn start_deferred_agent_prompt(
        app: &mut App,
        id: &str,
        params: AgentPromptParams,
    ) -> std::sync::mpsc::Receiver<String> {
        let (respond_to, response_rx) = std::sync::mpsc::channel();
        assert!(app.handle_deferred_agent_api_request(
            crate::api::schema::Request {
                id: id.into(),
                method: crate::api::schema::Method::AgentPrompt(params),
            },
            respond_to,
        ));
        response_rx
    }

    fn run_deferred_agent_prompt(app: &mut App, id: &str, params: AgentPromptParams) -> String {
        start_deferred_agent_prompt(app, id, params)
            .recv_timeout(Duration::from_secs(1))
            .expect("agent prompt responds after submission")
    }

    #[tokio::test]
    async fn session_rotation_rejects_a_legacy_launch_without_mutating_the_pane() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        let now = std::time::Instant::now();
        terminal.begin_managed_agent(
            "orchestrator".into(),
            Agent::Traex,
            now,
            Duration::ZERO,
            Duration::from_secs(30),
        );
        terminal.set_detected_state(Some(Agent::Traex), AgentState::Idle);
        assert!(terminal.reconcile_managed_agent_at(now, false));
        terminal.set_persisted_agent_session(crate::agent_resume::PersistedAgentSession {
            source: "herdr:traex".into(),
            agent: "traex".into(),
            session_ref: crate::agent_resume::AgentSessionRef::id("session-old").unwrap(),
        });
        let public_pane_id = app.public_pane_id(0, pane_id).unwrap();
        let before = app.pane_info(0, pane_id).unwrap();

        let response = app.handle_api_request(crate::api::schema::Request {
            id: "rotate-legacy".into(),
            method: crate::api::schema::Method::AgentSessionRotateV1(
                crate::api::schema::AgentSessionRotateV1Params {
                    operation_id: "clear-legacy".into(),
                    pane_id: public_pane_id,
                    expected_terminal_id: terminal_id.to_string(),
                    expected_session: crate::api::schema::AgentSessionInfo {
                        source: "herdr:traex".into(),
                        agent: "traex".into(),
                        kind: crate::agent_resume::AgentSessionRefKind::Id,
                        value: "session-old".into(),
                    },
                    expected_state: crate::api::schema::AgentSessionRotationExpectedState::Idle,
                    expected_state_change_seq: 0,
                    launch: crate::api::schema::ManagedAgentLaunch {
                        name: "orchestrator".into(),
                        kind: "traex".into(),
                        args: Vec::new(),
                        timeout_ms: Some(30_000),
                    },
                },
            ),
        });
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::AgentSessionRotation { rotation } = success.result else {
            panic!("expected session rotation response");
        };
        assert!(matches!(
            rotation,
            crate::api::schema::AgentSessionRotationResult::UnsupportedLaunch {
                ref operation_id,
                ref pane_id,
                ref reason,
            } if operation_id == "clear-legacy"
                && pane_id == &before.pane_id
                && reason == "pane_not_rotatable"
        ));

        let after = app.pane_info(0, pane_id).unwrap();
        assert_eq!(after.workspace_id, before.workspace_id);
        assert_eq!(after.tab_id, before.tab_id);
        assert_eq!(after.pane_id, before.pane_id);
        assert_eq!(after.terminal_id, before.terminal_id);
        assert_eq!(after.token, before.token);
        assert_eq!(after.label, before.label);
        assert_eq!(after.agent_session, before.agent_session);
    }

    #[tokio::test]
    async fn session_rotation_checks_exact_identity_and_idle_fences_before_launch_support() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        let now = std::time::Instant::now();
        terminal.begin_managed_agent(
            "orchestrator".into(),
            Agent::Traex,
            now,
            Duration::ZERO,
            Duration::from_secs(30),
        );
        terminal.set_detected_state(Some(Agent::Traex), AgentState::Idle);
        assert!(terminal.reconcile_managed_agent_at(now, false));
        terminal.set_persisted_agent_session(crate::agent_resume::PersistedAgentSession {
            source: "herdr:traex".into(),
            agent: "traex".into(),
            session_ref: crate::agent_resume::AgentSessionRef::id("session-old").unwrap(),
        });
        let public_pane_id = app.public_pane_id(0, pane_id).unwrap();

        let params = |operation_id: &str| crate::api::schema::AgentSessionRotateV1Params {
            operation_id: operation_id.into(),
            pane_id: public_pane_id.clone(),
            expected_terminal_id: terminal_id.to_string(),
            expected_session: crate::api::schema::AgentSessionInfo {
                source: "herdr:traex".into(),
                agent: "traex".into(),
                kind: crate::agent_resume::AgentSessionRefKind::Id,
                value: "session-old".into(),
            },
            expected_state: crate::api::schema::AgentSessionRotationExpectedState::Idle,
            expected_state_change_seq: 0,
            launch: crate::api::schema::ManagedAgentLaunch {
                name: "orchestrator".into(),
                kind: "traex".into(),
                args: Vec::new(),
                timeout_ms: Some(30_000),
            },
        };
        let rotate = |app: &mut App, params| {
            let response = app.handle_api_request(crate::api::schema::Request {
                id: "rotate".into(),
                method: crate::api::schema::Method::AgentSessionRotateV1(params),
            });
            let success: SuccessResponse = serde_json::from_str(&response).unwrap();
            let ResponseResult::AgentSessionRotation { rotation } = success.result else {
                panic!("expected session rotation response");
            };
            rotation
        };

        let mut missing_pane = params("clear-pane");
        missing_pane.pane_id = "missing:pane".into();
        assert!(matches!(
            rotate(&mut app, missing_pane),
            crate::api::schema::AgentSessionRotationResult::FenceLost { reason, .. }
                if reason == "pane_not_found"
        ));

        let mut stale_terminal = params("clear-terminal");
        stale_terminal.expected_terminal_id = "different-terminal".into();
        assert!(matches!(
            rotate(&mut app, stale_terminal),
            crate::api::schema::AgentSessionRotationResult::FenceLost { reason, .. }
                if reason == "terminal_changed"
        ));

        let mut stale_session = params("clear-session");
        stale_session.expected_session.value = "different-session".into();
        assert!(matches!(
            rotate(&mut app, stale_session),
            crate::api::schema::AgentSessionRotationResult::FenceLost { reason, .. }
                if reason == "native_session_changed"
        ));

        let mut stale_sequence = params("clear-sequence");
        stale_sequence.expected_state_change_seq = 1;
        assert!(matches!(
            rotate(&mut app, stale_sequence),
            crate::api::schema::AgentSessionRotationResult::FenceLost { reason, .. }
                if reason == "state_changed"
        ));

        let mut stale_name = params("clear-name");
        stale_name.launch.name = "different-name".into();
        assert!(matches!(
            rotate(&mut app, stale_name),
            crate::api::schema::AgentSessionRotationResult::FenceLost { reason, .. }
                if reason == "agent_name_changed"
        ));

        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(Some(Agent::Traex), AgentState::Working);
        assert!(matches!(
            rotate(&mut app, params("clear-working")),
            crate::api::schema::AgentSessionRotationResult::FenceLost { reason, .. }
                if reason == "agent_not_idle"
        ));
    }

    #[tokio::test]
    async fn session_rotation_recovers_the_same_receipt_without_replacing_the_pane() {
        use interprocess::local_socket::traits::Listener as _;
        use std::io::{Read as _, Write as _};

        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        let now = std::time::Instant::now();
        terminal.begin_managed_agent(
            "orchestrator".into(),
            Agent::Traex,
            now,
            Duration::ZERO,
            Duration::from_secs(30),
        );
        terminal.set_detected_state(Some(Agent::Traex), AgentState::Idle);
        assert!(terminal.reconcile_managed_agent_at(now, false));
        terminal.set_persisted_agent_session(crate::agent_resume::PersistedAgentSession {
            source: "herdr:traex".into(),
            agent: "traex".into(),
            session_ref: crate::agent_resume::AgentSessionRef::id("session-old").unwrap(),
        });
        let public_pane_id = app.public_pane_id(0, pane_id).unwrap();
        let before = app.pane_info(0, pane_id).unwrap();
        let terminal_id_text = terminal_id.to_string();
        let endpoint = crate::agent_session_supervisor::endpoint_path(&terminal_id_text);
        crate::ipc::prepare_socket_path(&endpoint, |_| "busy".into()).unwrap();
        let listener = crate::ipc::bind_private_local_listener(&endpoint).unwrap();
        let completed_receipt = crate::api::schema::AgentSessionRotationReceipt {
            operation_id: "clear-success".into(),
            pane_id: public_pane_id.clone(),
            terminal_id: terminal_id_text.clone(),
            old_session: crate::api::schema::AgentSessionInfo {
                source: "herdr:traex".into(),
                agent: "traex".into(),
                kind: crate::agent_resume::AgentSessionRefKind::Id,
                value: "session-old".into(),
            },
            new_session: crate::api::schema::AgentSessionInfo {
                source: "herdr:traex".into(),
                agent: "traex".into(),
                kind: crate::agent_resume::AgentSessionRefKind::Id,
                value: "session-new".into(),
            },
        };
        let supervisor = std::thread::spawn({
            let completed_receipt = completed_receipt.clone();
            move || {
                let responses = [
                    serde_json::json!({"outcome": "unknown"}),
                    serde_json::json!({"outcome": "launched"}),
                    serde_json::json!({"outcome": "launched"}),
                    serde_json::json!({
                        "outcome": "completed",
                        "receipt": completed_receipt,
                    }),
                ];
                for response in responses {
                    let mut stream = listener.accept().unwrap();
                    let mut request = [0u8; 4096];
                    assert!(stream.read(&mut request).unwrap() > 0);
                    serde_json::to_writer(&mut stream, &response).unwrap();
                    stream.write_all(b"\n").unwrap();
                }
            }
        });
        let params = crate::api::schema::AgentSessionRotateV1Params {
            operation_id: "clear-success".into(),
            pane_id: public_pane_id,
            expected_terminal_id: terminal_id_text,
            expected_session: completed_receipt.old_session.clone(),
            expected_state: crate::api::schema::AgentSessionRotationExpectedState::Idle,
            expected_state_change_seq: 0,
            launch: crate::api::schema::ManagedAgentLaunch {
                name: "orchestrator".into(),
                kind: "traex".into(),
                args: vec!["--model".into(), "default".into()],
                timeout_ms: Some(30_000),
            },
        };

        let first = app.handle_api_request(crate::api::schema::Request {
            id: "rotate-first".into(),
            method: crate::api::schema::Method::AgentSessionRotateV1(params.clone()),
        });
        let first: SuccessResponse = serde_json::from_str(&first).unwrap();
        assert!(matches!(
            first.result,
            ResponseResult::AgentSessionRotation {
                rotation: crate::api::schema::AgentSessionRotationResult::Rotating { .. }
            }
        ));

        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_persisted_agent_session(crate::agent_resume::PersistedAgentSession {
                source: "herdr:traex".into(),
                agent: "traex".into(),
                session_ref: crate::agent_resume::AgentSessionRef::id("session-new").unwrap(),
            });
        let second = app.handle_api_request(crate::api::schema::Request {
            id: "rotate-second".into(),
            method: crate::api::schema::Method::AgentSessionRotateV1(params),
        });
        let second: SuccessResponse = serde_json::from_str(&second).unwrap();
        assert_eq!(
            second.result,
            ResponseResult::AgentSessionRotation {
                rotation: crate::api::schema::AgentSessionRotationResult::Rotated {
                    receipt: completed_receipt,
                },
            }
        );

        supervisor.join().unwrap();
        let _ = std::fs::remove_file(endpoint);
        let after = app.pane_info(0, pane_id).unwrap();
        assert_eq!(after.workspace_id, before.workspace_id);
        assert_eq!(after.tab_id, before.tab_id);
        assert_eq!(after.pane_id, before.pane_id);
        assert_eq!(after.terminal_id, before.terminal_id);
        assert_eq!(after.token, before.token);
        assert_eq!(after.label, before.label);
        assert_ne!(after.agent_session, before.agent_session);
    }

    #[tokio::test]
    async fn model_prompt_replaces_the_exact_idle_traex_runtime_with_a_model_resume() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("primary".into());
        terminal.set_detected_state(Some(Agent::Traex), AgentState::Idle);
        terminal.set_persisted_agent_session(crate::agent_resume::PersistedAgentSession {
            source: "herdr:traex".into(),
            agent: "traex".into(),
            session_ref: crate::agent_resume::AgentSessionRef::id("traex-session").unwrap(),
        });
        let (runtime, _rx) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
                80, 24, 0, b"", 2,
            );
        app.state.insert_test_runtime(pane_id, runtime);
        let public_pane_id = app.public_pane_id(0, pane_id).unwrap();
        let (respond_to, response_rx) = std::sync::mpsc::channel();

        assert!(app.handle_deferred_agent_api_request(
            crate::api::schema::Request {
                id: "model-prompt".into(),
                method: crate::api::schema::Method::AgentPromptModel(
                    crate::api::schema::AgentPromptModelParams {
                        target: public_pane_id,
                        text: "continue".into(),
                        model: "gpt-5.4".into(),
                        submission_id: "prompt-1".into(),
                        expected_session_id: "traex-session".into(),
                    },
                ),
            },
            respond_to,
        ));
        let response = response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("model resume request responds after the old runtime is stopped");
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(success.result, ResponseResult::Ok {}));

        assert!(app.terminal_runtimes.get(&terminal_id).is_none());
        let terminal = app.state.terminals.get(&terminal_id).unwrap();
        assert_eq!(
            terminal
                .pending_agent_resume_plan
                .as_ref()
                .map(|plan| plan.argv.as_slice()),
            Some(
                ["traex", "resume", "traex-session", "--model", "gpt-5.4"]
                    .map(String::from)
                    .as_slice()
            )
        );
        assert!(terminal.respawn_shell_on_exit);
        assert_eq!(terminal.agent_name.as_deref(), Some("primary"));
        assert_eq!(
            terminal
                .persisted_agent_session
                .as_ref()
                .map(|session| session.session_ref.value.as_str()),
            Some("traex-session")
        );
        assert_eq!(terminal.state, AgentState::Unknown);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_codex_prompt_flushes_paste_burst_before_enter() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::Codex), AgentState::Idle);
        let (runtime, mut rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(pane_id, runtime);

        let response = run_deferred_agent_prompt(
            &mut app,
            "req",
            AgentPromptParams {
                target: "reviewer".into(),
                text: "A != B".into(),
                submission_id: None,
                expected_session_id: None,
                wait: None,
            },
        );
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(
            success.result,
            ResponseResult::AgentPrompted { .. }
        ));
        // The non-character key must precede Enter so Codex commits the paste burst first.
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"A != B\x1b[C"));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\r"));
    }

    #[tokio::test]
    async fn a_false_process_exit_makes_a_named_live_agent_unreachable_by_name() {
        // Reproduces the registration loss reported on #3225 by rszrszrsz:
        // a live agent pane with an assigned name stops resolving by that name
        // while its process keeps running, and renaming is the only recovery.
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let observed_at = std::time::Instant::now();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_detected_state(Some(Agent::Pi), AgentState::Working);
        terminal.set_agent_name("reviewer".into());

        let found = app.handle_agent_get(
            "req:before".into(),
            AgentTarget {
                target: "reviewer".into(),
            },
        );
        assert!(
            serde_json::from_str::<SuccessResponse>(&found).is_ok(),
            "the assigned name must resolve while the agent is running: {found}"
        );

        // One process-exit observation, then the same agent is observed alive
        // again on the next probe - the process never actually went away.
        app.handle_internal_event(crate::events::AppEvent::StateChanged {
            pane_id,
            agent: Some(Agent::Pi),
            state: AgentState::Idle,
            visible_blocker: false,
            visible_working: false,
            process_exited: true,
            observed_at,
        });
        app.handle_internal_event(crate::events::AppEvent::AgentProcessDetected {
            pane_id,
            agent: Agent::Pi,
            observed_at: observed_at + std::time::Duration::from_secs(1),
        });

        let terminal = &app.state.terminals[&terminal_id];
        assert_eq!(
            terminal.detected_agent,
            Some(Agent::Pi),
            "the agent process is still there"
        );

        let after = app.handle_agent_get(
            "req:after".into(),
            AgentTarget {
                target: "reviewer".into(),
            },
        );
        assert!(
            serde_json::from_str::<SuccessResponse>(&after).is_ok(),
            "a live agent must stay reachable by its assigned name: {after}"
        );
    }

    #[tokio::test]
    async fn agent_prompt_sends_text_then_delays_enter() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::OpenCode), AgentState::Working);
        let (runtime, mut rx) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
                80, 24, 0, b"", 2,
            );
        runtime.test_process_pty_bytes(b"\x1b[?2004h");
        app.state.insert_test_runtime(pane_id, runtime);

        let public_pane_id = app.public_pane_id(0, pane_id).unwrap();
        let bracketed_started = std::time::Instant::now();
        let response_rx = start_deferred_agent_prompt(
            &mut app,
            "req",
            AgentPromptParams {
                target: public_pane_id,
                text: "A != B".into(),
                submission_id: Some("prompt-1".into()),
                expected_session_id: None,
                wait: None,
            },
        );
        assert!(response_rx.try_recv().is_err());
        let response = response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("agent prompt responds after submission");
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::AgentPrompted {
            agent,
            submission_id,
        } = success.result
        else {
            panic!("expected prompted response");
        };
        assert_eq!(submission_id.as_deref(), Some("prompt-1"));
        assert_eq!(agent.name.as_deref(), Some("reviewer"));
        assert_eq!(
            rx.try_recv().unwrap(),
            Bytes::from_static(b"\x1b[200~A != B\x1b[201~")
        );
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\r"));
        assert!(bracketed_started.elapsed() >= AGENT_PROMPT_SUBMIT_DELAY);

        app.lookup_runtime_sender(0, pane_id)
            .unwrap()
            .test_process_pty_bytes(b"\x1b[?2004l");
        let raw_started = std::time::Instant::now();
        let raw = run_deferred_agent_prompt(
            &mut app,
            "req-raw",
            AgentPromptParams {
                target: "reviewer".into(),
                text: "A != B".into(),
                submission_id: None,
                expected_session_id: None,
                wait: None,
            },
        );
        let raw: SuccessResponse = serde_json::from_str(&raw).unwrap();
        assert!(matches!(raw.result, ResponseResult::AgentPrompted { .. }));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"A != B"));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\r"));
        assert!(raw_started.elapsed() >= AGENT_PROMPT_SUBMIT_DELAY);

        let rejected = run_deferred_agent_prompt(
            &mut app,
            "req-label",
            AgentPromptParams {
                target: "opencode".into(),
                text: "wrong target".into(),
                submission_id: None,
                expected_session_id: None,
                wait: None,
            },
        );
        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&rejected).unwrap();
        assert_eq!(error.error.code, "agent_not_found");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn agent_prompt_rejects_blocked_agent_without_writing() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::GithubCopilot), AgentState::Blocked);
        let (runtime, mut rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(pane_id, runtime);

        let response = run_deferred_agent_prompt(
            &mut app,
            "req",
            AgentPromptParams {
                target: "reviewer".into(),
                text: "unrelated prompt".into(),
                submission_id: None,
                expected_session_id: None,
                wait: None,
            },
        );

        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_blocked");
        assert!(
            tokio::time::timeout(
                AGENT_PROMPT_SUBMIT_DELAY + Duration::from_millis(100),
                rx.recv()
            )
            .await
            .is_err(),
            "blocked prompt wrote or scheduled terminal input"
        );
    }

    #[tokio::test]
    async fn agent_prompt_focuses_copilot_before_submitting() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::GithubCopilot), AgentState::Idle);
        let (runtime, mut rx) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
                80, 24, 0, b"", 3,
            );
        runtime.test_process_pty_bytes(b"\x1b[?2004h");
        app.state.insert_test_runtime(pane_id, runtime);

        let response = run_deferred_agent_prompt(
            &mut app,
            "req",
            AgentPromptParams {
                target: "reviewer".into(),
                text: "A != B".into(),
                submission_id: None,
                expected_session_id: None,
                wait: None,
            },
        );
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(
            success.result,
            ResponseResult::AgentPrompted { .. }
        ));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\x1b[I"));
        assert_eq!(
            rx.try_recv().unwrap(),
            Bytes::from_static(b"\x1b[200~A != B\x1b[201~")
        );
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\r"));
    }

    #[tokio::test]
    async fn agent_send_keys_validates_every_key_before_writing() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::Pi), AgentState::Idle);
        let (runtime, mut rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(pane_id, runtime);

        let rejected = app.handle_agent_send_keys(
            "req-invalid".into(),
            AgentSendKeysParams {
                target: "reviewer".into(),
                keys: vec!["enter".into(), "not-a-key".into()],
            },
        );
        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&rejected).unwrap();
        assert_eq!(error.error.code, "invalid_key");
        assert!(rx.try_recv().is_err());

        let sent = app.handle_agent_send_keys(
            "req-valid".into(),
            AgentSendKeysParams {
                target: "reviewer".into(),
                keys: vec!["up".into(), "enter".into()],
            },
        );
        let success: SuccessResponse = serde_json::from_str(&sent).unwrap();
        assert!(matches!(success.result, ResponseResult::Ok {}));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\x1b[A\r"));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn agent_prompt_rejects_managed_agent_while_startup_is_pending() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        let now = std::time::Instant::now();
        terminal.begin_managed_agent(
            "reviewer".into(),
            Agent::OpenCode,
            now,
            std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(10),
        );
        terminal.set_detected_state(Some(Agent::OpenCode), AgentState::Idle);
        let (runtime, mut rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(pane_id, runtime);

        let response = run_deferred_agent_prompt(
            &mut app,
            "req-pending",
            AgentPromptParams {
                target: "reviewer".into(),
                text: "A != B".into(),
                submission_id: None,
                expected_session_id: None,
                wait: None,
            },
        );
        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_not_ready");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn agent_focus_marks_already_focused_done_agent_seen() {
        let mut app = app_with_agent();
        app.state.outer_terminal_focus = Some(false);

        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(Some(Agent::Pi), AgentState::Idle);
        app.state.workspaces[0].tabs[0]
            .panes
            .get_mut(&pane_id)
            .unwrap()
            .seen = false;
        app.state.workspaces[0].tabs[0].layout.focus_pane(pane_id);

        let response = app.handle_agent_focus(
            "req".into(),
            AgentTarget {
                target: app.public_pane_id(0, pane_id).unwrap(),
            },
        );

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::AgentInfo { agent } = success.result else {
            panic!("expected agent info response");
        };
        assert_eq!(agent.agent_status, AgentStatus::Idle);
    }

    #[test]
    fn agent_rename_does_not_replace_the_pane_label() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_manual_label("shell-pane".into());
        terminal.set_detected_state(Some(Agent::Pi), AgentState::Idle);
        let target = app.public_pane_id(0, pane_id).unwrap();

        for name in [Some("reviewer".to_string()), None] {
            let response = app.handle_agent_rename(
                "req".into(),
                AgentRenameParams {
                    target: target.clone(),
                    name,
                },
            );
            let success: SuccessResponse = serde_json::from_str(&response).unwrap();
            assert!(matches!(success.result, ResponseResult::AgentInfo { .. }));
            assert_eq!(
                app.state.terminals[&terminal_id].manual_label.as_deref(),
                Some("shell-pane")
            );
        }
    }
}
