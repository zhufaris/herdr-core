use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

#[cfg(unix)]
use std::time::Instant;

use interprocess::local_socket::traits::{ListenerExt as _, Stream as _};

const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const CHILD_STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SupervisorRotationRequest {
    pub operation_id: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub expected_session: crate::api::schema::AgentSessionInfo,
    pub expected_state_change_seq: u64,
    pub launch_argv: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub(crate) enum SupervisorRotationResponse {
    Unknown,
    Pending,
    Launched,
    AlreadyApplied,
    Completed {
        receipt: Box<crate::api::schema::AgentSessionRotationReceipt>,
    },
    Conflict,
    DefinitelyNotStarted {
        reason: String,
    },
    Uncertain {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum SupervisorControlAction {
    Status,
    Rotate,
    Complete,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SupervisorControlRequest {
    action: SupervisorControlAction,
    rotation: SupervisorRotationRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt: Option<crate::api::schema::AgentSessionRotationReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedSupervisorOperationState {
    Accepted,
    Launched,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PersistedSupervisorOperation {
    request: SupervisorRotationRequest,
    state: PersistedSupervisorOperationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt: Option<crate::api::schema::AgentSessionRotationReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<SupervisorRotationResponse>,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct PersistedSupervisorJournal {
    operations: BTreeMap<String, PersistedSupervisorOperation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisorOperationState {
    New,
    Pending,
    Launched,
    Conflict,
}

struct SupervisorJournal {
    path: PathBuf,
    persisted: PersistedSupervisorJournal,
}

impl SupervisorJournal {
    fn open(path: PathBuf) -> io::Result<Self> {
        let persisted = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                PersistedSupervisorJournal::default()
            }
            Err(err) => return Err(err),
        };
        Ok(Self { path, persisted })
    }

    fn begin(
        &mut self,
        request: &SupervisorRotationRequest,
    ) -> io::Result<SupervisorOperationState> {
        if let Some(operation) = self.persisted.operations.get(&request.operation_id) {
            return Ok(if operation.request != *request {
                SupervisorOperationState::Conflict
            } else {
                match operation.state {
                    PersistedSupervisorOperationState::Accepted => {
                        SupervisorOperationState::Pending
                    }
                    PersistedSupervisorOperationState::Launched => {
                        SupervisorOperationState::Launched
                    }
                    PersistedSupervisorOperationState::Completed => {
                        SupervisorOperationState::Launched
                    }
                }
            });
        }

        self.persisted.operations.insert(
            request.operation_id.clone(),
            PersistedSupervisorOperation {
                request: request.clone(),
                state: PersistedSupervisorOperationState::Accepted,
                receipt: None,
                result: None,
            },
        );
        self.save()?;
        Ok(SupervisorOperationState::New)
    }

    fn mark_launched(&mut self, operation_id: &str) -> io::Result<()> {
        let operation = self
            .persisted
            .operations
            .get_mut(operation_id)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "supervisor operation not found")
            })?;
        operation.state = PersistedSupervisorOperationState::Launched;
        self.save()
    }

    fn mark_result(
        &mut self,
        operation_id: &str,
        result: SupervisorRotationResponse,
    ) -> io::Result<()> {
        let operation = self
            .persisted
            .operations
            .get_mut(operation_id)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "supervisor operation not found")
            })?;
        operation.result = Some(result);
        self.save()
    }

    fn status(&self, request: &SupervisorRotationRequest) -> SupervisorRotationResponse {
        let Some(operation) = self.persisted.operations.get(&request.operation_id) else {
            return SupervisorRotationResponse::Unknown;
        };
        if operation.request != *request {
            return SupervisorRotationResponse::Conflict;
        }
        if let Some(result) = operation.result.as_ref() {
            return result.clone();
        }
        match operation.state {
            PersistedSupervisorOperationState::Accepted => SupervisorRotationResponse::Pending,
            PersistedSupervisorOperationState::Launched => SupervisorRotationResponse::Launched,
            PersistedSupervisorOperationState::Completed => operation
                .receipt
                .as_ref()
                .map(|receipt| SupervisorRotationResponse::Completed {
                    receipt: Box::new(receipt.clone()),
                })
                .unwrap_or_else(|| SupervisorRotationResponse::Uncertain {
                    reason: "completed_operation_missing_receipt".into(),
                }),
        }
    }

    fn complete(
        &mut self,
        request: &SupervisorRotationRequest,
        receipt: crate::api::schema::AgentSessionRotationReceipt,
    ) -> io::Result<SupervisorRotationResponse> {
        let Some(operation) = self.persisted.operations.get_mut(&request.operation_id) else {
            return Ok(SupervisorRotationResponse::Conflict);
        };
        if operation.request != *request {
            return Ok(SupervisorRotationResponse::Conflict);
        }
        if operation.state == PersistedSupervisorOperationState::Accepted {
            return Ok(SupervisorRotationResponse::Uncertain {
                reason: "completion_before_launch".into(),
            });
        }
        if operation.state == PersistedSupervisorOperationState::Completed {
            return Ok(if operation.receipt.as_ref() == Some(&receipt) {
                SupervisorRotationResponse::Completed {
                    receipt: Box::new(receipt),
                }
            } else {
                SupervisorRotationResponse::Conflict
            });
        }
        operation.state = PersistedSupervisorOperationState::Completed;
        operation.receipt = Some(receipt.clone());
        operation.result = None;
        self.save()?;
        Ok(SupervisorRotationResponse::Completed {
            receipt: Box::new(receipt),
        })
    }

    fn save(&self) -> io::Result<()> {
        let Some(parent) = self.path.parent() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "supervisor journal path has no parent",
            ));
        };
        std::fs::create_dir_all(parent)?;
        let temporary = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&self.persisted)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &self.path)?;
        if let Ok(directory) = std::fs::File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    }
}

pub(crate) fn endpoint_path(terminal_id: &str) -> PathBuf {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    terminal_id.hash(&mut hasher);
    #[cfg(test)]
    let root = std::env::temp_dir().join(format!(
        "herdr-agent-supervisor-tests-{}",
        std::process::id()
    ));
    #[cfg(not(test))]
    let root = crate::session::data_dir();
    root.join(format!("agent-supervisor-{:016x}.sock", hasher.finish()))
}

fn journal_path(terminal_id: &str) -> PathBuf {
    endpoint_path(terminal_id).with_extension("json")
}

pub(crate) fn request_rotation(
    request: &SupervisorRotationRequest,
) -> io::Result<SupervisorRotationResponse> {
    send_control(SupervisorControlAction::Rotate, request)
}

pub(crate) fn query_rotation(
    request: &SupervisorRotationRequest,
) -> io::Result<SupervisorRotationResponse> {
    match send_control(SupervisorControlAction::Status, request) {
        Ok(response) => Ok(response),
        Err(transport_error) => {
            let path = journal_path(&request.terminal_id);
            if !path.exists() {
                return Err(transport_error);
            }
            let response = SupervisorJournal::open(path)?.status(request);
            if response == SupervisorRotationResponse::Unknown {
                Err(transport_error)
            } else {
                Ok(response)
            }
        }
    }
}

fn send_control(
    action: SupervisorControlAction,
    request: &SupervisorRotationRequest,
) -> io::Result<SupervisorRotationResponse> {
    let mut stream = crate::ipc::connect_local_stream(&endpoint_path(&request.terminal_id))?;
    stream.set_send_timeout(Some(CONTROL_TIMEOUT))?;
    stream.set_recv_timeout(Some(CONTROL_TIMEOUT))?;
    let mut encoded = serde_json::to_vec(&SupervisorControlRequest {
        action,
        rotation: request.clone(),
        receipt: None,
    })
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    encoded.push(b'\n');
    stream.write_all(&encoded)?;
    stream.flush()?;

    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response)?;
    if response.len() > 64 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "agent supervisor response exceeds 64 KiB",
        ));
    }
    serde_json::from_str(&response).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

pub(crate) fn complete_rotation(
    request: &SupervisorRotationRequest,
    receipt: &crate::api::schema::AgentSessionRotationReceipt,
) -> io::Result<SupervisorRotationResponse> {
    let mut stream = crate::ipc::connect_local_stream(&endpoint_path(&request.terminal_id))?;
    stream.set_send_timeout(Some(CONTROL_TIMEOUT))?;
    stream.set_recv_timeout(Some(CONTROL_TIMEOUT))?;
    let mut encoded = serde_json::to_vec(&SupervisorControlRequest {
        action: SupervisorControlAction::Complete,
        rotation: request.clone(),
        receipt: Some(receipt.clone()),
    })
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    encoded.push(b'\n');
    stream.write_all(&encoded)?;
    stream.flush()?;

    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response)?;
    serde_json::from_str(&response).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

pub(crate) fn run_from_args(args: &[String]) -> io::Result<()> {
    let (pane_id, terminal_id, initial_argv) = parse_args(args)?;
    let endpoint = endpoint_path(&terminal_id);
    crate::ipc::prepare_socket_path(&endpoint, |path| {
        format!("agent supervisor is already running at {}", path.display())
    })?;
    let listener = crate::ipc::bind_private_local_listener(&endpoint)?;
    crate::ipc::restrict_socket_permissions(&endpoint, 0o600)?;
    let _socket_guard = SocketPathGuard(endpoint);
    let mut journal = SupervisorJournal::open(journal_path(&terminal_id))?;
    let mut child = spawn_agent_child(&initial_argv)?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        stream.set_recv_timeout(Some(CONTROL_TIMEOUT))?;
        stream.set_send_timeout(Some(CONTROL_TIMEOUT))?;
        let mut line = String::new();
        BufReader::new(&mut stream).read_line(&mut line)?;
        let response = match serde_json::from_str::<SupervisorControlRequest>(&line) {
            Ok(request)
                if request.rotation.pane_id != pane_id
                    || request.rotation.terminal_id != terminal_id =>
            {
                SupervisorRotationResponse::Conflict
            }
            Ok(request) if request.action == SupervisorControlAction::Status => {
                journal.status(&request.rotation)
            }
            Ok(request) if request.action == SupervisorControlAction::Complete => {
                match request.receipt {
                    Some(receipt)
                        if receipt.operation_id == request.rotation.operation_id
                            && receipt.pane_id == request.rotation.pane_id
                            && receipt.terminal_id == request.rotation.terminal_id =>
                    {
                        match journal.complete(&request.rotation, receipt) {
                            Ok(response) => response,
                            Err(err) => SupervisorRotationResponse::Uncertain {
                                reason: format!("receipt_write_failed:{err}"),
                            },
                        }
                    }
                    _ => SupervisorRotationResponse::Conflict,
                }
            }
            Ok(request) => rotate_child(&mut journal, &mut child, &request.rotation),
            Err(err) => SupervisorRotationResponse::DefinitelyNotStarted {
                reason: format!("invalid_request:{err}"),
            },
        };
        serde_json::to_writer(&mut stream, &response)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
    }
    Ok(())
}

fn parse_args(args: &[String]) -> io::Result<(String, String, Vec<String>)> {
    let separator = args.iter().position(|arg| arg == "--").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing supervisor argv separator",
        )
    })?;
    let options = &args[..separator];
    let mut pane_id = None;
    let mut terminal_id = None;
    let mut index = 0;
    while index < options.len() {
        let target = match options[index].as_str() {
            "--pane-id" => &mut pane_id,
            "--terminal-id" => &mut terminal_id,
            option => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown supervisor option {option}"),
                ));
            }
        };
        let value = options.get(index + 1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing supervisor option value",
            )
        })?;
        *target = Some(value.clone());
        index += 2;
    }
    let initial_argv = args[separator + 1..].to_vec();
    if initial_argv.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing supervised Agent argv",
        ));
    }
    Ok((
        pane_id.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing pane ID"))?,
        terminal_id
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing terminal ID"))?,
        initial_argv,
    ))
}

fn rotate_child(
    journal: &mut SupervisorJournal,
    child: &mut Child,
    request: &SupervisorRotationRequest,
) -> SupervisorRotationResponse {
    let state = match journal.begin(request) {
        Ok(state) => state,
        Err(err) => {
            return SupervisorRotationResponse::DefinitelyNotStarted {
                reason: format!("journal_write_failed:{err}"),
            };
        }
    };
    match state {
        SupervisorOperationState::Launched => {
            return SupervisorRotationResponse::AlreadyApplied;
        }
        SupervisorOperationState::Pending => {
            return SupervisorRotationResponse::Uncertain {
                reason: "operation_was_interrupted_after_acceptance".into(),
            };
        }
        SupervisorOperationState::Conflict => return SupervisorRotationResponse::Conflict,
        SupervisorOperationState::New => {}
    }

    if let Err(err) = stop_agent_child(child) {
        let result = SupervisorRotationResponse::Uncertain {
            reason: format!("child_stop_failed:{err}"),
        };
        let _ = journal.mark_result(&request.operation_id, result.clone());
        return result;
    }
    let replacement = match spawn_agent_child(&request.launch_argv) {
        Ok(child) => child,
        Err(err) => {
            let result = SupervisorRotationResponse::Uncertain {
                reason: format!("replacement_launch_failed:{err}"),
            };
            let _ = journal.mark_result(&request.operation_id, result.clone());
            return result;
        }
    };
    *child = replacement;
    if let Err(err) = journal.mark_launched(&request.operation_id) {
        return SupervisorRotationResponse::Uncertain {
            reason: format!("launch_receipt_write_failed:{err}"),
        };
    }
    SupervisorRotationResponse::Launched
}

fn spawn_agent_child(argv: &[String]) -> io::Result<Child> {
    let Some((program, args)) = argv.split_first() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty Agent argv",
        ));
    };
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                for signal in [
                    libc::SIGINT,
                    libc::SIGQUIT,
                    libc::SIGTERM,
                    libc::SIGTSTP,
                    libc::SIGTTIN,
                    libc::SIGTTOU,
                ] {
                    libc::signal(signal, libc::SIG_DFL);
                }
                Ok(())
            });
        }
    }
    let child = command.spawn()?;
    #[cfg(unix)]
    set_foreground_process_group(child.id());
    Ok(child)
}

fn stop_agent_child(child: &mut Child) -> io::Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        let group = -(child.id() as libc::pid_t);
        if unsafe { libc::kill(group, libc::SIGTERM) } == -1 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ESRCH) {
                return Err(err);
            }
        }
        let deadline = Instant::now() + CHILD_STOP_TIMEOUT;
        while Instant::now() < deadline {
            if child.try_wait()?.is_some() {
                set_foreground_process_group(unsafe { libc::getpgrp() } as u32);
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        if unsafe { libc::kill(group, libc::SIGKILL) } == -1 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ESRCH) {
                return Err(err);
            }
        }
        let _ = child.wait()?;
        set_foreground_process_group(unsafe { libc::getpgrp() } as u32);
        Ok(())
    }
    #[cfg(not(unix))]
    {
        child.kill()?;
        let _ = child.wait()?;
        Ok(())
    }
}

#[cfg(unix)]
fn set_foreground_process_group(process_group: u32) {
    if unsafe { libc::isatty(libc::STDIN_FILENO) } == 1 {
        unsafe {
            libc::signal(libc::SIGTTOU, libc::SIG_IGN);
            let _ = libc::tcsetpgrp(libc::STDIN_FILENO, process_group as libc::pid_t);
        }
    }
}

struct SocketPathGuard(PathBuf);

impl Drop for SocketPathGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "herdr-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn supervisor_journal_deduplicates_operations_across_reopen() {
        let path = temp_path("agent-session-supervisor-journal");
        let request = test_request(vec!["traex".into(), "--model".into(), "default".into()]);
        let replacement = test_request(vec!["traex".into(), "--model".into(), "other".into()]);

        let mut journal = SupervisorJournal::open(path.clone()).unwrap();
        assert_eq!(
            journal.begin(&request).unwrap(),
            SupervisorOperationState::New
        );

        let mut reopened = SupervisorJournal::open(path.clone()).unwrap();
        assert_eq!(
            reopened.begin(&request).unwrap(),
            SupervisorOperationState::Pending
        );
        reopened.mark_launched("clear-42").unwrap();
        let receipt = crate::api::schema::AgentSessionRotationReceipt {
            operation_id: "clear-42".into(),
            pane_id: "w1:p1".into(),
            terminal_id: "terminal-1".into(),
            old_session: crate::api::schema::AgentSessionInfo {
                source: "herdr:traex".into(),
                agent: "traex".into(),
                kind: crate::agent_resume::AgentSessionRefKind::Id,
                value: "old".into(),
            },
            new_session: crate::api::schema::AgentSessionInfo {
                source: "herdr:traex".into(),
                agent: "traex".into(),
                kind: crate::agent_resume::AgentSessionRefKind::Id,
                value: "new".into(),
            },
        };
        assert_eq!(
            reopened.complete(&request, receipt.clone()).unwrap(),
            SupervisorRotationResponse::Completed {
                receipt: Box::new(receipt.clone())
            }
        );

        let mut completed = SupervisorJournal::open(path.clone()).unwrap();
        assert_eq!(
            completed.begin(&request).unwrap(),
            SupervisorOperationState::Launched
        );
        assert_eq!(
            completed.status(&request),
            SupervisorRotationResponse::Completed {
                receipt: Box::new(receipt)
            }
        );
        assert_eq!(
            completed.begin(&replacement).unwrap(),
            SupervisorOperationState::Conflict
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn supervisor_journal_rejects_a_mismatched_completion_request() {
        let path = temp_path("agent-session-supervisor-completion-conflict");
        let request = test_request(vec!["traex".into(), "--model".into(), "default".into()]);
        let replacement = test_request(vec!["traex".into(), "--model".into(), "other".into()]);
        let receipt = crate::api::schema::AgentSessionRotationReceipt {
            operation_id: "clear-42".into(),
            pane_id: "w1:p1".into(),
            terminal_id: "terminal-1".into(),
            old_session: test_session("old"),
            new_session: test_session("new"),
        };

        let mut journal = SupervisorJournal::open(path.clone()).unwrap();
        assert_eq!(
            journal.begin(&request).unwrap(),
            SupervisorOperationState::New
        );
        journal.mark_launched("clear-42").unwrap();

        assert_eq!(
            journal.complete(&replacement, receipt).unwrap(),
            SupervisorRotationResponse::Conflict
        );
        assert_eq!(
            journal.status(&request),
            SupervisorRotationResponse::Launched
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rotation_status_recovers_from_journal_when_the_supervisor_socket_is_gone() {
        let terminal_id = format!(
            "terminal-recovery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut request = test_request(vec!["traex".into()]);
        request.terminal_id = terminal_id;
        let endpoint = endpoint_path(&request.terminal_id);
        let path = journal_path(&request.terminal_id);
        let _ = std::fs::remove_file(&endpoint);
        let _ = std::fs::remove_file(&path);

        let mut journal = SupervisorJournal::open(path.clone()).unwrap();
        assert_eq!(
            journal.begin(&request).unwrap(),
            SupervisorOperationState::New
        );
        journal.mark_launched(&request.operation_id).unwrap();
        drop(journal);

        assert_eq!(
            query_rotation(&request).unwrap(),
            SupervisorRotationResponse::Launched
        );

        let _ = std::fs::remove_file(path);
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_duplicate_rotation_callers_launch_only_one_replacement() {
        use interprocess::local_socket::traits::Listener as _;

        let terminal_id = format!(
            "terminal-concurrent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut request = test_request(vec!["sh".into(), "-c".into(), "sleep 30".into()]);
        request.terminal_id = terminal_id;
        let endpoint = endpoint_path(&request.terminal_id);
        let path = journal_path(&request.terminal_id);
        let _ = std::fs::remove_file(&endpoint);
        let _ = std::fs::remove_file(&path);
        std::fs::create_dir_all(endpoint.parent().unwrap()).unwrap();
        let listener = crate::ipc::bind_private_local_listener(&endpoint).unwrap();

        let server_request = request.clone();
        let server = std::thread::spawn(move || {
            let mut journal = SupervisorJournal::open(path.clone()).unwrap();
            let initial = vec!["sh".into(), "-c".into(), "sleep 30".into()];
            let mut child = spawn_agent_child(&initial).unwrap();
            for _ in 0..2 {
                let mut stream = listener.accept().unwrap();
                let mut line = String::new();
                BufReader::new(&mut stream).read_line(&mut line).unwrap();
                let control: SupervisorControlRequest = serde_json::from_str(&line).unwrap();
                assert_eq!(control.rotation, server_request);
                let response = rotate_child(&mut journal, &mut child, &control.rotation);
                serde_json::to_writer(&mut stream, &response).unwrap();
                stream.write_all(b"\n").unwrap();
            }
            let replacement_pid = child.id();
            stop_agent_child(&mut child).unwrap();
            let _ = std::fs::remove_file(path);
            replacement_pid
        });

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let callers: Vec<_> = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                let request = request.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    request_rotation(&request).unwrap()
                })
            })
            .collect();
        barrier.wait();
        let responses: Vec<_> = callers
            .into_iter()
            .map(|caller| caller.join().unwrap())
            .collect();

        assert_eq!(
            responses
                .iter()
                .filter(|response| **response == SupervisorRotationResponse::Launched)
                .count(),
            1
        );
        assert_eq!(
            responses
                .iter()
                .filter(|response| **response == SupervisorRotationResponse::AlreadyApplied)
                .count(),
            1
        );
        assert_ne!(server.join().unwrap(), 0);
        let _ = std::fs::remove_file(endpoint);
    }

    #[cfg(unix)]
    #[test]
    fn supervisor_rotates_only_its_child_process_group_and_deduplicates() {
        let path = temp_path("agent-session-supervisor-child");
        let mut journal = SupervisorJournal::open(path.clone()).unwrap();
        let initial = vec!["sh".into(), "-c".into(), "sleep 30".into()];
        let replacement = vec!["sh".into(), "-c".into(), "sleep 31".into()];
        let mut child = spawn_agent_child(&initial).unwrap();
        let old_pid = child.id();
        let request = SupervisorRotationRequest {
            operation_id: "clear-child".into(),
            pane_id: "w1:p1".into(),
            terminal_id: "terminal-1".into(),
            expected_session: test_session("old"),
            expected_state_change_seq: 7,
            launch_argv: replacement,
        };

        assert_eq!(
            rotate_child(&mut journal, &mut child, &request),
            SupervisorRotationResponse::Launched
        );
        let new_pid = child.id();
        assert_ne!(new_pid, old_pid);
        assert_eq!(unsafe { libc::kill(old_pid as libc::pid_t, 0) }, -1);
        assert_eq!(
            rotate_child(&mut journal, &mut child, &request),
            SupervisorRotationResponse::AlreadyApplied
        );
        assert_eq!(child.id(), new_pid);

        stop_agent_child(&mut child).unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[cfg(unix)]
    #[test]
    fn replacement_launch_failure_is_uncertain_after_the_old_child_stops() {
        let path = temp_path("agent-session-supervisor-launch-failure");
        let mut journal = SupervisorJournal::open(path.clone()).unwrap();
        let initial = vec!["sh".into(), "-c".into(), "sleep 30".into()];
        let mut child = spawn_agent_child(&initial).unwrap();
        let old_pid = child.id();
        let request = SupervisorRotationRequest {
            operation_id: "clear-launch-failure".into(),
            pane_id: "w1:p1".into(),
            terminal_id: "terminal-1".into(),
            expected_session: test_session("old"),
            expected_state_change_seq: 7,
            launch_argv: vec!["/definitely/missing/traex".into()],
        };

        assert!(matches!(
            rotate_child(&mut journal, &mut child, &request),
            SupervisorRotationResponse::Uncertain { ref reason }
                if reason.starts_with("replacement_launch_failed:")
        ));
        assert_eq!(unsafe { libc::kill(old_pid as libc::pid_t, 0) }, -1);
        assert!(matches!(
            journal.status(&request),
            SupervisorRotationResponse::Uncertain { ref reason }
                if reason.starts_with("replacement_launch_failed:")
        ));

        let _ = std::fs::remove_file(path);
    }

    fn test_request(launch_argv: Vec<String>) -> SupervisorRotationRequest {
        SupervisorRotationRequest {
            operation_id: "clear-42".into(),
            pane_id: "w1:p1".into(),
            terminal_id: "terminal-1".into(),
            expected_session: test_session("old"),
            expected_state_change_seq: 7,
            launch_argv,
        }
    }

    fn test_session(value: &str) -> crate::api::schema::AgentSessionInfo {
        crate::api::schema::AgentSessionInfo {
            source: "herdr:traex".into(),
            agent: "traex".into(),
            kind: crate::agent_resume::AgentSessionRefKind::Id,
            value: value.into(),
        }
    }
}
