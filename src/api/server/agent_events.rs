use super::{dispatch_to_app_with_timeout, write_json_line_allow_disconnect};
#[cfg(test)]
use crate::agent_events::Checkpoint;
use crate::agent_events::{
    EventError, Journal, RegisteredSource, SourceReader, SubmissionPrepareResult,
};
use crate::api::schema::agent_events::{
    AgentEventsAttachParams, AgentEventsBatch, AgentEventsCapabilities, AgentEventsListTurnsParams,
    AgentEventsLocateParams, AgentEventsReadParams, AgentEventsRecoverTurnParams,
    AgentEventsRecoveryBatch, AgentEventsSubmissionParams, AgentEventsSubmissionReceipt,
    AgentEventsTurnCursor, AgentEventsTurnList, TranscriptKind,
};
use crate::api::schema::{EmptyParams, Method, PaneProcessInfoParams, Request, ResponseResult};
use crate::api::ApiRequestSender;
use crate::ipc::{local_stream_peer_closed, LocalStream};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub(super) struct ReplyStreams {
    journal: Mutex<Option<Journal>>,
    directory: PathBuf,
    subscribers: AtomicUsize,
    running: Arc<AtomicBool>,
    revision: Mutex<u64>,
    changed: Condvar,
}

impl ReplyStreams {
    pub fn start(api_tx: ApiRequestSender, running: Arc<AtomicBool>) -> Arc<Self> {
        let service = Arc::new(Self {
            journal: Mutex::new(None),
            directory: crate::session::data_dir().join("agent-events"),
            subscribers: AtomicUsize::new(0),
            running: running.clone(),
            revision: Mutex::new(0),
            changed: Condvar::new(),
        });
        let worker = service.clone();
        std::thread::spawn(move || {
            let mut tick = 0u32;
            while running.load(Ordering::Acquire) {
                if worker.directory.join("events.db").exists() {
                    if let Err(error) = worker.collect(&api_tx, tick.is_multiple_of(240)) {
                        if tick.is_multiple_of(240) {
                            tracing::warn!(code = error.0, "agent event collection unavailable");
                        }
                    }
                }
                tick = tick.wrapping_add(1);
                std::thread::sleep(Duration::from_millis(250));
            }
            if let Ok(mut journal) = worker.journal.lock() {
                *journal = None;
            }
        });
        service
    }
    fn with_journal<T>(
        &self,
        create: bool,
        f: impl FnOnce(&mut Journal) -> crate::agent_events::Result<T>,
    ) -> crate::agent_events::Result<T> {
        let mut guard = self
            .journal
            .lock()
            .map_err(|_| EventError("journal_unavailable"))?;
        if !self.running.load(Ordering::Acquire) {
            return Err(EventError("events_stopped"));
        }
        if guard.is_none() {
            if !create && !self.directory.join("events.db").exists() {
                return Err(EventError("events_disabled"));
            }
            crate::platform::prepare_agent_event_directory(&self.directory)?;
            let path = self.directory.join("events.db");
            if !path.exists() {
                crate::platform::create_private_state_file(&path)?;
            }
            if !std::fs::symlink_metadata(&path)?.is_file() {
                return Err(EventError("journal_unavailable"));
            }
            *guard = Some(Journal::open(&path)?);
        }
        f(guard.as_mut().ok_or(EventError("journal_unavailable"))?)
    }
    pub fn attach(
        &self,
        params: AgentEventsAttachParams,
        api_tx: &ApiRequestSender,
    ) -> crate::agent_events::Result<ResponseResult> {
        let snapshot = request_snapshot(api_tx)?;
        let pane = snapshot["panes"]
            .as_array()
            .and_then(|panes| panes.iter().find(|p| p["pane_id"] == params.pane_id))
            .ok_or(EventError("pane_not_found"))?;
        let terminal = pane["terminal_id"]
            .as_str()
            .ok_or(EventError("terminal_identity_missing"))?;
        let session = pane["agent_session"]
            .as_object()
            .ok_or(EventError("session_identity_missing"))?;
        if session.get("agent").and_then(Value::as_str) != Some(agent_name(params.agent_kind)) {
            return Err(EventError("session_identity_mismatch"));
        }
        {
            let matches = match session.get("kind").and_then(Value::as_str) {
                Some("path") => {
                    let expected = session
                        .get("value")
                        .and_then(Value::as_str)
                        .ok_or(EventError("session_identity_mismatch"))?;
                    std::fs::canonicalize(expected)? == std::fs::canonicalize(&params.path)?
                }
                Some("id") => {
                    session.get("value").and_then(Value::as_str) == Some(params.session_id.as_str())
                }
                _ => false,
            };
            if !matches {
                return Err(EventError("session_identity_mismatch"));
            }
        }
        let pid = foreground_pid(api_tx, &params.pane_id, params.agent_kind)?;
        let roots = source_roots(params.agent_kind);
        let source = SourceReader::register(
            terminal.to_owned(),
            params.agent_kind,
            params.session_id,
            std::path::Path::new(&params.path),
            pid,
            &roots,
        )?;
        let initial = match params.from {
            crate::api::schema::agent_events::AgentEventsAttachFrom::Start => Default::default(),
            crate::api::schema::agent_events::AgentEventsAttachFrom::End => {
                SourceReader::checkpoint_at_end(&source)?
            }
        };
        self.with_journal(true, |j| {
            let checkpoint = j.attach(&source, &initial)?;
            Ok(ResponseResult::AgentEventsAttached {
                source_id: source.id,
                attach_offset: checkpoint.offset,
                sources: j.sources()?,
            })
        })
    }
    pub fn sources(&self) -> crate::agent_events::Result<ResponseResult> {
        match self.with_journal(false, |j| {
            Ok(ResponseResult::AgentEventsSources {
                enabled: true,
                sources: j.sources()?,
            })
        }) {
            Err(EventError("events_disabled")) => Ok(ResponseResult::AgentEventsSources {
                enabled: false,
                sources: vec![],
            }),
            result => result,
        }
    }

    pub fn capabilities(&self) -> ResponseResult {
        ResponseResult::AgentEventsCapabilities {
            capabilities: AgentEventsCapabilities {
                exact_turn_recovery_v1: true,
                historical_turn_index_recovery_v1: true,
                session_turn_enumeration_v1: true,
            },
        }
    }

    pub fn submission(
        &self,
        params: &AgentEventsSubmissionParams,
    ) -> crate::agent_events::Result<AgentEventsSubmissionReceipt> {
        self.with_journal(false, |journal| journal.submission(params))
    }

    pub fn recover_turn(
        &self,
        params: &AgentEventsRecoverTurnParams,
    ) -> crate::agent_events::Result<AgentEventsRecoveryBatch> {
        self.with_journal(false, |journal| journal.recover_turn(params))
    }

    pub fn turns(
        &self,
        params: &AgentEventsListTurnsParams,
    ) -> crate::agent_events::Result<AgentEventsTurnList> {
        self.with_journal(false, |journal| journal.turns(params))
    }

    pub fn prepare_submission(
        &self,
        params: &crate::api::schema::AgentPromptParams,
        api_tx: &ApiRequestSender,
    ) -> crate::agent_events::Result<SubmissionPrepareResult> {
        let submission_id = params
            .submission_id
            .as_deref()
            .ok_or(EventError("invalid_submission_id"))?;
        let expected_session_id = params
            .expected_session_id
            .as_deref()
            .ok_or(EventError("session_identity_missing"))?;
        let snapshot = request_snapshot(api_tx)?;
        let pane = snapshot["panes"]
            .as_array()
            .and_then(|panes| panes.iter().find(|pane| pane["pane_id"] == params.target))
            .ok_or(EventError("pane_not_found"))?;
        let terminal_id = pane["terminal_id"]
            .as_str()
            .ok_or(EventError("terminal_identity_missing"))?;
        let session = pane["agent_session"]
            .as_object()
            .ok_or(EventError("session_identity_missing"))?;
        if session.get("value").and_then(Value::as_str) != Some(expected_session_id) {
            return Err(EventError("session_identity_mismatch"));
        }
        let kind = match session.get("agent").and_then(Value::as_str) {
            Some("traex") => TranscriptKind::Traex,
            Some("pi") => TranscriptKind::Pi,
            _ => return Err(EventError("agent_kind_mismatch")),
        };
        self.with_journal(true, |journal| {
            journal.prepare_submission(
                submission_id,
                terminal_id,
                kind,
                expected_session_id,
                &params.text,
            )
        })
    }

    pub fn cancel_prepared_submission(
        &self,
        submission_id: &str,
    ) -> crate::agent_events::Result<()> {
        self.with_journal(false, |journal| {
            journal.cancel_prepared_submission(submission_id)
        })
    }
    pub fn read(
        &self,
        params: &AgentEventsReadParams,
    ) -> crate::agent_events::Result<AgentEventsBatch> {
        self.with_journal(false, |j| {
            j.read(&params.source_id, &params.after, params.limit)
        })
    }
    pub fn locate(
        &self,
        params: &AgentEventsLocateParams,
    ) -> crate::agent_events::Result<AgentEventsTurnCursor> {
        self.with_journal(false, |j| j.locate(params))
    }
    pub fn read_failure(&self, id: &str, code: &str, source_id: &str) -> Value {
        let mut response = failure(id, code);
        if code == "cursor_expired" {
            if let Ok(batch) = self.read(&AgentEventsReadParams {
                source_id: source_id.into(),
                after: "latest".into(),
                limit: 1,
            }) {
                response["error"]["earliest_cursor"] = json!(batch.earliest_cursor);
                response["error"]["latest_cursor"] = json!(batch.latest_cursor);
            }
        }
        response
    }
    fn collect(&self, api_tx: &ApiRequestSender, prune: bool) -> crate::agent_events::Result<()> {
        let active = self.with_journal(false, |j| {
            if prune {
                j.prune()?;
            }
            j.pending(prune)
        })?;
        if active.is_empty() {
            return Ok(());
        }
        let snapshot = request_snapshot(api_tx)?;
        let panes = snapshot["panes"]
            .as_array()
            .ok_or(EventError("runtime_unavailable"))?;
        for (source, cp) in active {
            let pane = panes
                .iter()
                .find(|p| p["terminal_id"] == source.terminal_id);
            let identity = match pane.and_then(|p| p["pane_id"].as_str()) {
                Some(pane_id) => foreground_pid(api_tx, pane_id, source.kind)
                    .map(|pid| pid == source.foreground_pid),
                None => Ok(false),
            };
            match identity {
                Ok(true) => {}
                Ok(false) | Err(EventError("agent_not_running" | "agent_kind_mismatch")) => {
                    self.fail(&source, "runtime_identity_changed")?;
                    continue;
                }
                Err(_) => continue,
            }
            if let Some(session) = pane
                .and_then(|p| p.get("agent_session"))
                .and_then(Value::as_object)
            {
                let matches = match session["kind"].as_str() {
                    Some("id") => session["value"] == source.session_id,
                    Some("path") => {
                        session["value"]
                            .as_str()
                            .and_then(|p| std::fs::canonicalize(p).ok())
                            .as_ref()
                            == Some(&source.path)
                    }
                    _ => false,
                };
                if session.get("agent").and_then(Value::as_str) != Some(agent_name(source.kind))
                    || !matches
                {
                    self.fail(&source, "session_identity_changed")?;
                    continue;
                }
            } else {
                self.fail(&source, "session_identity_changed")?;
                continue;
            }
            match SourceReader::read(&source, &cp) {
                Ok((next, events)) => {
                    if next != cp {
                        self.with_journal(false, |j| j.commit(&source, &cp, &next, events))?;
                        self.notify();
                    }
                }
                Err(error) => self.fail(&source, error.0)?,
            }
        }
        Ok(())
    }
    fn fail(
        &self,
        source: &RegisteredSource,
        code: &'static str,
    ) -> crate::agent_events::Result<()> {
        self.with_journal(false, |j| j.fail(source, code))?;
        self.notify();
        Ok(())
    }
    fn notify(&self) {
        if let Ok(mut revision) = self.revision.lock() {
            *revision = revision.wrapping_add(1);
            self.changed.notify_all();
        }
    }
    pub fn subscribe(
        &self,
        mut stream: LocalStream,
        id: String,
        mut params: AgentEventsReadParams,
        running: &AtomicBool,
    ) -> std::io::Result<()> {
        let admitted = self
            .subscribers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < 32).then_some(n + 1)
            })
            .is_ok();
        if !admitted {
            return write_json_line_allow_disconnect(
                &mut stream,
                &failure(&id, "subscriber_limit"),
            );
        }
        let _permit = SubscriberPermit(&self.subscribers);
        let mut first = true;
        while running.load(Ordering::Acquire) && !local_stream_peer_closed(&mut stream)? {
            let observed = *self
                .revision
                .lock()
                .map_err(|_| std::io::Error::other("event notifier unavailable"))?;
            match self.read(&params) {
                Ok(batch) => {
                    params.after = batch.next_cursor.clone();
                    if first {
                        write_json_line_allow_disconnect(
                            &mut stream,
                            &json!({"id":id,"result":ResponseResult::AgentEventsBatch { batch: batch.clone() }}),
                        )?;
                        first = false;
                    } else if !batch.events.is_empty() {
                        write_json_line_allow_disconnect(
                            &mut stream,
                            &json!({"event":"agent.events.batch","data":batch}),
                        )?;
                    }
                    if params.after != batch.latest_cursor {
                        continue;
                    }
                }
                Err(error) => {
                    write_json_line_allow_disconnect(
                        &mut stream,
                        &self.read_failure(&id, error.0, &params.source_id),
                    )?;
                    break;
                }
            }
            let revision = self
                .revision
                .lock()
                .map_err(|_| std::io::Error::other("event notifier unavailable"))?;
            let _wait = self
                .changed
                .wait_timeout_while(revision, Duration::from_secs(1), |current| {
                    *current == observed
                })
                .map_err(|_| std::io::Error::other("event notifier unavailable"))?;
        }
        Ok(())
    }
}

fn agent_name(kind: TranscriptKind) -> &'static str {
    match kind {
        TranscriptKind::Traex => "traex",
        TranscriptKind::Pi => "pi",
    }
}
struct SubscriberPermit<'a>(&'a AtomicUsize);
impl Drop for SubscriberPermit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn request_snapshot(api_tx: &ApiRequestSender) -> crate::agent_events::Result<Value> {
    let raw = dispatch_to_app_with_timeout(
        Request {
            id: "agent-events:snapshot".into(),
            method: Method::SessionSnapshot(EmptyParams {}),
        },
        api_tx,
        Some(Duration::from_secs(2)),
    );
    let value: Value = serde_json::from_str(&raw)?;
    if value["result"]["snapshot"].is_object() {
        Ok(value["result"]["snapshot"].clone())
    } else {
        Err(EventError("runtime_unavailable"))
    }
}
fn foreground_pid(
    api_tx: &ApiRequestSender,
    pane_id: &str,
    kind: TranscriptKind,
) -> crate::agent_events::Result<u32> {
    let raw = dispatch_to_app_with_timeout(
        Request {
            id: "agent-events:process".into(),
            method: Method::PaneProcessInfo(PaneProcessInfoParams {
                pane_id: Some(pane_id.into()),
            }),
        },
        api_tx,
        Some(Duration::from_secs(2)),
    );
    let value: Value = serde_json::from_str(&raw)?;
    validate_foreground(&value["result"]["process_info"], kind)
}
fn validate_foreground(info: &Value, kind: TranscriptKind) -> crate::agent_events::Result<u32> {
    let pid = info["foreground_process_group_id"]
        .as_u64()
        .and_then(|p| u32::try_from(p).ok())
        .ok_or(EventError("foreground_identity_unavailable"))?;
    if info["shell_pid"].as_u64() == Some(u64::from(pid)) {
        return Err(EventError("agent_not_running"));
    }
    let matches = info["foreground_processes"]
        .as_array()
        .is_some_and(|processes| {
            processes.iter().any(|process| {
                process["argv"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .take(2)
                    .filter_map(Value::as_str)
                    .chain(process["argv0"].as_str())
                    .chain(process["name"].as_str())
                    .any(|arg| match kind {
                        TranscriptKind::Traex => matches!(
                            std::path::Path::new(arg)
                                .file_name()
                                .and_then(|n| n.to_str()),
                            Some("traex" | "traecli" | "trae")
                        ),
                        TranscriptKind::Pi => {
                            matches!(
                                std::path::Path::new(arg)
                                    .file_name()
                                    .and_then(|n| n.to_str()),
                                Some("pi")
                            ) || arg.ends_with("/pi-coding-agent/dist/cli.js")
                        }
                    })
            })
        });
    if !matches {
        return Err(EventError("agent_kind_mismatch"));
    }
    Ok(pid)
}
fn source_roots(kind: TranscriptKind) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        let home = PathBuf::from(home);
        roots.push(match kind {
            TranscriptKind::Traex => home.join(".trae/cli/sessions"),
            TranscriptKind::Pi => home.join(".pi/agent/sessions"),
        });
    }
    let key = match kind {
        TranscriptKind::Traex => "HERDR_TRAEX_TRANSCRIPT_ROOT",
        TranscriptKind::Pi => "HERDR_PI_TRANSCRIPT_ROOT",
    };
    if let Some(root) = std::env::var_os(key) {
        roots.push(PathBuf::from(root));
    }
    roots
}
pub(super) fn failure(id: &str, code: &str) -> Value {
    json!({"id":id,"error":{"code":code,"message":code}})
}

#[cfg(test)]
mod tests {
    use super::*;
    use interprocess::local_socket::traits::Listener as _;
    use std::io::{BufRead, BufReader};

    #[test]
    fn agent_events_foreground_rejects_shell_and_mismatched_agent() {
        let mut info = json!({"shell_pid":1,"foreground_process_group_id":2,"foreground_processes":[{"name":"node","argv":["node","/opt/pi-coding-agent/dist/cli.js"]}]});
        assert_eq!(validate_foreground(&info, TranscriptKind::Pi).unwrap(), 2);
        assert_eq!(
            validate_foreground(&info, TranscriptKind::Traex)
                .unwrap_err()
                .0,
            "agent_kind_mismatch"
        );
        info["foreground_process_group_id"] = json!(1);
        assert_eq!(
            validate_foreground(&info, TranscriptKind::Pi)
                .unwrap_err()
                .0,
            "agent_not_running"
        );
        info["foreground_process_group_id"] = json!(2);
        info["foreground_processes"] =
            json!([{"name":"python","argv":["python","script.py","pi"]}]);
        assert_eq!(
            validate_foreground(&info, TranscriptKind::Pi)
                .unwrap_err()
                .0,
            "agent_kind_mismatch"
        );
    }

    #[test]
    fn agent_events_socket_replays_and_resumes_without_duplicates() {
        let root = std::env::temp_dir().join(format!(
            "herdr-event-stream-{}-{}",
            std::process::id(),
            crate::agent_events::now()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let transcript = root.join("pi.jsonl");
        std::fs::write(&transcript, "{\"type\":\"session\",\"id\":\"s\"}\n{\"type\":\"message\",\"id\":\"u\",\"parentId\":null,\"message\":{\"role\":\"user\"}}\n").unwrap();
        let source = SourceReader::register(
            "t".into(),
            TranscriptKind::Pi,
            "s".into(),
            &transcript,
            1,
            std::slice::from_ref(&root),
        )
        .unwrap();
        let service = Arc::new(ReplyStreams {
            journal: Mutex::new(None),
            directory: root.join("events"),
            subscribers: AtomicUsize::new(0),
            running: Arc::new(AtomicBool::new(true)),
            revision: Mutex::new(0),
            changed: Condvar::new(),
        });
        service
            .with_journal(true, |j| {
                j.attach(&source, &Checkpoint::default())?;
                let (_, cp) = j.active()?.remove(0);
                let (next, events) = SourceReader::read(&source, &cp)?;
                j.commit(&source, &cp, &next, events)
            })
            .unwrap();
        let cursor = service
            .read(&AgentEventsReadParams {
                source_id: source.id.clone(),
                after: "start".into(),
                limit: 64,
            })
            .unwrap()
            .next_cursor;
        for (index, after, count) in [(0, "start".to_owned(), 1), (1, cursor, 0)] {
            let socket = root.join(format!("{index}.sock"));
            let listener = crate::ipc::bind_local_listener(&socket).unwrap();
            let client = crate::ipc::connect_local_stream(&socket).unwrap();
            let server = listener.accept().unwrap();
            let subscriber_service = service.clone();
            let source_id = source.id.clone();
            let running = Arc::new(AtomicBool::new(true));
            let server_running = running.clone();
            let worker = std::thread::spawn(move || {
                subscriber_service
                    .subscribe(
                        server,
                        "test".into(),
                        AgentEventsReadParams {
                            source_id,
                            after,
                            limit: 64,
                        },
                        &server_running,
                    )
                    .unwrap()
            });
            let mut reader = BufReader::new(client);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let response: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(
                response["result"]["events"].as_array().unwrap().len(),
                count
            );
            let typed: crate::api::schema::SuccessResponse = serde_json::from_str(&line).unwrap();
            assert!(matches!(
                typed.result,
                ResponseResult::AgentEventsBatch { .. }
            ));
            if index == 1 {
                service.fail(&source, "test_source_error").unwrap();
                line.clear();
                reader.read_line(&mut line).unwrap();
                let pushed: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(pushed["event"], "agent.events.batch");
                assert_eq!(
                    pushed["data"]["events"][0]["payload"]["code"],
                    "test_source_error"
                );
            }
            running.store(false, Ordering::Release);
            service.notify();
            drop(reader);
            worker.join().unwrap();
        }
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}
