use super::codec::Decoded;
use super::source::Checkpoint;
use super::{digest, now, EventError, RegisteredSource, Result, SourceReader};
use crate::api::schema::agent_events::{
    AgentEventSource, AgentEventsBatch, AgentEventsListTurnsParams, AgentEventsLocateParams,
    AgentEventsRecoverTurnParams, AgentEventsRecoveryBatch, AgentEventsRecoveryOutcome,
    AgentEventsSubmissionParams, AgentEventsSubmissionReceipt, AgentEventsSubmissionState,
    AgentEventsTurnBoundary, AgentEventsTurnCursor, AgentEventsTurnList, AgentEventsTurnSummary,
    AgentReplyEvent, ReplyPayload, TranscriptKind,
};
use base64::Engine as _;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;

const MAX_SOURCES: i64 = 256;
const RETENTION_SECONDS: i64 = 7 * 24 * 3600;
const MAX_EVENT_BYTES: i64 = 192 * 1024 * 1024;
const JOURNAL_VERSION: i64 = 2;
const HISTORY_INDEX_BATCHES_PER_LOOKUP: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecoveryCursor {
    transcript_id: String,
    turn_id: String,
    started_at: String,
    checkpoint: Checkpoint,
    event_skip: usize,
    ordinal: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TurnListCursor {
    agent_kind: TranscriptKind,
    session_id: String,
    started_at: String,
    transcript_id: String,
    turn_id: String,
}

struct IndexedTurn {
    transcript_id: String,
    source: RegisteredSource,
    turn_id: String,
    started_at: String,
    start: Checkpoint,
    end: Checkpoint,
}

type SubmissionRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);
type RecoverableTurnRow = (String, String, String, String, String, String);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoricalIndexState {
    checkpoint: Checkpoint,
    complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmissionPrepareResult {
    Prepared,
    Duplicate,
}

pub(crate) struct Journal {
    db: Connection,
    id: String,
    _writer_lock: std::fs::File,
}
impl Journal {
    pub fn open(path: &Path) -> Result<Self> {
        let lock_path = path.with_extension("lock");
        let writer_lock = match crate::platform::create_private_state_file(&lock_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&lock_path)?
            }
            Err(error) => return Err(error.into()),
        };
        writer_lock
            .try_lock()
            .map_err(|_| EventError("journal_writer_busy"))?;
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(2))?;
        let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > JOURNAL_VERSION {
            return Err(EventError("unsupported_journal_version"));
        }
        db.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA max_page_count=65536;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT OR IGNORE INTO metadata VALUES ('id', lower(hex(randomblob(16))));
            CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY, definition TEXT NOT NULL, checkpoint TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'active', error TEXT, floor INTEGER NOT NULL DEFAULT 0, latest INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, created INTEGER NOT NULL, UNIQUE(source,key));
            CREATE INDEX IF NOT EXISTS events_source_seq ON events(source,seq);")?;
        let pending_submission_columns = {
            let mut statement = db.prepare("PRAGMA table_info(pending_submissions)")?;
            let columns = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            columns
        };
        let has_pending_submission_column = |name: &str| {
            pending_submission_columns
                .iter()
                .any(|column| column == name)
        };
        if version == 0 {
            db.execute_batch("CREATE TABLE pending_submissions (
                submission_id TEXT PRIMARY KEY, terminal_id TEXT NOT NULL, agent_kind TEXT NOT NULL,
                session_id TEXT NOT NULL, text_digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('prepared','observed','terminal','cancelled','legacy_unavailable')),
                transcript_id TEXT, turn_id TEXT, turn_started_at TEXT, human_event_key TEXT,
                terminal_state TEXT, created INTEGER NOT NULL, updated INTEGER NOT NULL);
                CREATE INDEX pending_submissions_match ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,created);
                CREATE TABLE turn_index (
                  transcript_id TEXT NOT NULL, agent_kind TEXT NOT NULL, session_id TEXT NOT NULL,
                  source_definition TEXT NOT NULL, turn_id TEXT NOT NULL, started_at TEXT NOT NULL,
                  start_checkpoint TEXT NOT NULL, end_checkpoint TEXT, terminal_state TEXT,
                  human_event_key TEXT, human_text_digest TEXT, created INTEGER NOT NULL, updated INTEGER NOT NULL,
                  PRIMARY KEY(transcript_id,turn_id,started_at));
                CREATE INDEX turn_index_session_order ON turn_index(agent_kind,session_id,started_at,turn_id);
                PRAGMA user_version=2;")?;
        } else if version == 1 {
            db.execute_batch("BEGIN IMMEDIATE;
                DROP INDEX IF EXISTS pending_submissions_match;
                ALTER TABLE pending_submissions RENAME TO pending_submissions_v1;
                CREATE TABLE pending_submissions (
                  submission_id TEXT PRIMARY KEY, terminal_id TEXT NOT NULL, agent_kind TEXT NOT NULL,
                  session_id TEXT NOT NULL, text_digest TEXT NOT NULL,
                  state TEXT NOT NULL CHECK(state IN ('prepared','observed','terminal','cancelled','legacy_unavailable')),
                  transcript_id TEXT, turn_id TEXT, turn_started_at TEXT, human_event_key TEXT,
                  terminal_state TEXT, created INTEGER NOT NULL, updated INTEGER NOT NULL);
                INSERT INTO pending_submissions(submission_id,terminal_id,agent_kind,session_id,text_digest,state,created,updated)
                  SELECT submission_id,terminal_id,agent_kind,session_id,text_digest,
                    CASE state WHEN 'prepared' THEN 'prepared' ELSE 'legacy_unavailable' END,created,created
                  FROM pending_submissions_v1;
                DROP TABLE pending_submissions_v1;
                CREATE INDEX pending_submissions_match ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,created);
                CREATE TABLE turn_index (
                  transcript_id TEXT NOT NULL, agent_kind TEXT NOT NULL, session_id TEXT NOT NULL,
                  source_definition TEXT NOT NULL, turn_id TEXT NOT NULL, started_at TEXT NOT NULL,
                  start_checkpoint TEXT NOT NULL, end_checkpoint TEXT, terminal_state TEXT,
                  human_event_key TEXT, human_text_digest TEXT, created INTEGER NOT NULL, updated INTEGER NOT NULL,
                  PRIMARY KEY(transcript_id,turn_id,started_at));
                CREATE INDEX turn_index_session_order ON turn_index(agent_kind,session_id,started_at,turn_id);
                PRAGMA user_version=2;
                COMMIT;")?;
        } else if has_pending_submission_column("correlated")
            && !has_pending_submission_column("transcript_id")
        {
            // The stream-only v2 converted any prepared receipt to uncertain on reopen,
            // so none of its states prove that a prompt is safe to submit again.
            db.execute_batch("BEGIN IMMEDIATE;
                DROP INDEX IF EXISTS pending_submissions_match;
                ALTER TABLE pending_submissions RENAME TO pending_submissions_stream_v2;
                CREATE TABLE pending_submissions (
                  submission_id TEXT PRIMARY KEY, terminal_id TEXT NOT NULL, agent_kind TEXT NOT NULL,
                  session_id TEXT NOT NULL, text_digest TEXT NOT NULL,
                  state TEXT NOT NULL CHECK(state IN ('prepared','observed','terminal','cancelled','legacy_unavailable')),
                  transcript_id TEXT, turn_id TEXT, turn_started_at TEXT, human_event_key TEXT,
                  terminal_state TEXT, created INTEGER NOT NULL, updated INTEGER NOT NULL);
                INSERT INTO pending_submissions(submission_id,terminal_id,agent_kind,session_id,text_digest,state,created,updated)
                  SELECT submission_id,terminal_id,agent_kind,session_id,text_digest,
                    'legacy_unavailable',created,updated
                  FROM pending_submissions_stream_v2;
                DROP TABLE pending_submissions_stream_v2;
                CREATE INDEX pending_submissions_match ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,created);
                CREATE TABLE IF NOT EXISTS turn_index (
                  transcript_id TEXT NOT NULL, agent_kind TEXT NOT NULL, session_id TEXT NOT NULL,
                  source_definition TEXT NOT NULL, turn_id TEXT NOT NULL, started_at TEXT NOT NULL,
                  start_checkpoint TEXT NOT NULL, end_checkpoint TEXT, terminal_state TEXT,
                  human_event_key TEXT, human_text_digest TEXT, created INTEGER NOT NULL, updated INTEGER NOT NULL,
                  PRIMARY KEY(transcript_id,turn_id,started_at));
                CREATE INDEX IF NOT EXISTS turn_index_session_order ON turn_index(agent_kind,session_id,started_at,turn_id);
                COMMIT;")?;
        } else {
            if ![
                "transcript_id",
                "turn_id",
                "turn_started_at",
                "human_event_key",
                "terminal_state",
                "updated",
            ]
            .into_iter()
            .all(has_pending_submission_column)
            {
                return Err(EventError("journal_unavailable"));
            }
            db.execute_batch("CREATE INDEX IF NOT EXISTS pending_submissions_match ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,created);
                CREATE INDEX IF NOT EXISTS turn_index_session_order ON turn_index(agent_kind,session_id,started_at,turn_id);")?;
        }
        let id = db.query_row("SELECT value FROM metadata WHERE key='id'", [], |r| {
            r.get(0)
        })?;
        Ok(Self {
            db,
            id,
            _writer_lock: writer_lock,
        })
    }
    pub fn attach(
        &mut self,
        source: &RegisteredSource,
        initial: &Checkpoint,
    ) -> Result<Checkpoint> {
        let existing: Option<(String, String)> = self
            .db
            .query_row(
                "SELECT definition,checkpoint FROM sources WHERE id=?",
                [&source.id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let definition = serde_json::to_string(source)?;
        if let Some((existing_definition, existing_checkpoint)) = existing {
            if definition != existing_definition {
                return Err(EventError("source_identity_conflict"));
            }
            self.db.execute(
                "UPDATE sources SET state='active',error=NULL WHERE id=?",
                [&source.id],
            )?;
            return Ok(serde_json::from_str(&existing_checkpoint)?);
        }
        let count: i64 = self
            .db
            .query_row("SELECT count(*) FROM sources", [], |r| r.get(0))?;
        if count >= MAX_SOURCES {
            return Err(EventError("source_limit"));
        }
        self.db.execute(
            "INSERT INTO sources(id,definition,checkpoint) VALUES (?,?,?)",
            params![source.id, definition, serde_json::to_string(initial)?],
        )?;
        Ok(initial.clone())
    }

    pub(crate) fn prepare_submission(
        &mut self,
        submission_id: &str,
        terminal_id: &str,
        agent_kind: crate::api::schema::agent_events::TranscriptKind,
        session_id: &str,
        text: &str,
    ) -> Result<SubmissionPrepareResult> {
        if submission_id.is_empty() || submission_id.len() > 256 {
            return Err(EventError("invalid_submission_id"));
        }
        let kind = match agent_kind {
            crate::api::schema::agent_events::TranscriptKind::Traex => "traex",
            crate::api::schema::agent_events::TranscriptKind::Pi => "pi",
        };
        let text_digest = super::codec::human_message_digest(text);
        let existing: Option<(String, String, String, String)> = self
            .db
            .query_row(
                "SELECT terminal_id,agent_kind,session_id,text_digest FROM pending_submissions WHERE submission_id=?",
                [submission_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing
                == (
                    terminal_id.to_owned(),
                    kind.to_owned(),
                    session_id.to_owned(),
                    text_digest,
                )
            {
                return Ok(SubmissionPrepareResult::Duplicate);
            }
            return Err(EventError("submission_identity_conflict"));
        }
        let timestamp = now();
        self.db.execute(
            "INSERT INTO pending_submissions(submission_id,terminal_id,agent_kind,session_id,text_digest,state,created,updated) VALUES (?,?,?,?,?,'prepared',?,?)",
            params![submission_id, terminal_id, kind, session_id, text_digest, timestamp, timestamp],
        )?;
        Ok(SubmissionPrepareResult::Prepared)
    }

    pub(crate) fn cancel_prepared_submission(&mut self, submission_id: &str) -> Result<()> {
        self.db.execute(
            "UPDATE pending_submissions SET state='cancelled',updated=? WHERE submission_id=? AND state='prepared'",
            params![now(), submission_id],
        )?;
        Ok(())
    }

    pub fn submission(
        &self,
        params: &AgentEventsSubmissionParams,
    ) -> Result<AgentEventsSubmissionReceipt> {
        if params.submission_id.is_empty() || params.submission_id.len() > 256 {
            return Err(EventError("invalid_submission_id"));
        }
        let row: Option<SubmissionRow> =
            self.db
                .query_row(
                    "SELECT state,agent_kind,session_id,turn_id,turn_started_at,terminal_state FROM pending_submissions WHERE submission_id=?",
                    [&params.submission_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
                )
                .optional()?;
        let Some((state, agent_kind, session_id, turn_id, started_at, terminal_state)) = row else {
            return Err(EventError("submission_not_found"));
        };
        let state = match state.as_str() {
            "prepared" => AgentEventsSubmissionState::Prepared,
            "observed" => AgentEventsSubmissionState::Observed,
            "terminal" => AgentEventsSubmissionState::Terminal,
            "cancelled" => AgentEventsSubmissionState::Cancelled,
            "legacy_unavailable" => AgentEventsSubmissionState::LegacyUnavailable,
            _ => return Err(EventError("journal_unavailable")),
        };
        let agent_kind = match agent_kind.as_str() {
            "traex" => TranscriptKind::Traex,
            "pi" => TranscriptKind::Pi,
            _ => return Err(EventError("journal_unavailable")),
        };
        Ok(AgentEventsSubmissionReceipt {
            submission_id: params.submission_id.clone(),
            state,
            agent_kind,
            session_id,
            turn_id,
            started_at,
            terminal_state,
        })
    }
    pub(crate) fn pending(&self, verify_all: bool) -> Result<Vec<(RegisteredSource, Checkpoint)>> {
        let mut stmt = self.db.prepare("SELECT definition,json_extract(checkpoint,'$.offset') FROM sources WHERE state='active'")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        let mut pending = Vec::new();
        for row in rows {
            let (raw, offset) = row?;
            let source: RegisteredSource = serde_json::from_str(&raw)?;
            if verify_all
                || std::fs::metadata(&source.path)
                    .map(|m| i64::try_from(m.len()).ok() != Some(offset))
                    .unwrap_or(true)
            {
                let raw: String = self.db.query_row(
                    "SELECT checkpoint FROM sources WHERE id=?",
                    [&source.id],
                    |r| r.get(0),
                )?;
                pending.push((source, serde_json::from_str(&raw)?));
            }
        }
        Ok(pending)
    }
    #[cfg(test)]
    pub(crate) fn active(&self) -> Result<Vec<(RegisteredSource, Checkpoint)>> {
        let mut stmt = self
            .db
            .prepare("SELECT definition,checkpoint FROM sources WHERE state='active'")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        rows.map(|r| {
            let (source, cp) = r?;
            Ok((serde_json::from_str(&source)?, serde_json::from_str(&cp)?))
        })
        .collect()
    }
    pub(crate) fn commit(
        &mut self,
        source: &RegisteredSource,
        expected: &Checkpoint,
        next: &Checkpoint,
        events: Vec<Decoded>,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        let changed = tx.execute(
            "UPDATE sources SET checkpoint=? WHERE id=? AND checkpoint=? AND state='active'",
            params![
                serde_json::to_string(next)?,
                source.id,
                serde_json::to_string(expected)?
            ],
        )?;
        if changed != 1 {
            return Err(EventError("checkpoint_conflict"));
        }
        let transcript_id = transcript_identity(source)?;
        let source_definition = serde_json::to_string(source)?;
        let kind = transcript_kind_name(source.kind);
        for mut event in events {
            let turn_id = event.turn.clone();
            let is_start = matches!(event.payload, ReplyPayload::TurnStarted);
            let terminal_state = match &event.payload {
                ReplyPayload::TurnCompleted => Some("completed"),
                ReplyPayload::TurnAborted { .. } => Some("aborted"),
                _ => None,
            };
            if is_start {
                let Some(turn_id) = turn_id.as_deref() else {
                    return Err(EventError("missing_turn_identity"));
                };
                tx.execute(
                    "INSERT INTO turn_index(transcript_id,agent_kind,session_id,source_definition,turn_id,started_at,start_checkpoint,created,updated)
                     VALUES (?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(transcript_id,turn_id,started_at) DO UPDATE SET
                       source_definition=excluded.source_definition,
                       start_checkpoint=excluded.start_checkpoint,
                       end_checkpoint=NULL,
                       terminal_state=NULL,
                       updated=excluded.updated",
                    params![transcript_id, kind, source.session_id, source_definition, turn_id, event.time, serde_json::to_string(expected)?, now(), now()],
                )?;
            }
            let key = format!("{}:{}", turn_id.as_deref().unwrap_or(""), event.key);
            let mut matched_submission = None;
            if let ReplyPayload::HumanMessage {
                text,
                submission_id,
                ..
            } = &mut event.payload
            {
                if submission_id.is_none() {
                    let text_digest = super::codec::human_message_digest(text);
                    let matched: Option<String> = tx
                        .query_row(
                            "SELECT submission_id FROM pending_submissions WHERE terminal_id=? AND agent_kind=? AND session_id=? AND text_digest=? AND state='prepared' ORDER BY created,rowid LIMIT 1",
                            params![source.terminal_id, kind, source.session_id, text_digest],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if let Some(matched) = matched {
                        *submission_id = Some(matched.clone());
                        matched_submission = Some((matched, text_digest));
                    }
                }
            }
            let body = AgentReplyEvent {
                schema_version: 1,
                event_id: digest(format!("{transcript_id}:{key}").as_bytes()),
                cursor: String::new(),
                source_id: source.id.clone(),
                agent_kind: source.kind,
                session_id: source.session_id.clone(),
                turn_id: turn_id.clone(),
                occurred_at: event.time,
                payload: event.payload,
            };
            let serialized = serde_json::to_string(&body)?;
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO events(source,key,body,created) VALUES (?,?,?,?)",
                params![source.id, key, serialized, now()],
            )?;
            if inserted == 0 && matched_submission.is_some() {
                tx.execute(
                    "UPDATE events SET body=? WHERE source=? AND key=?",
                    params![serialized, source.id, key],
                )?;
            }
            if inserted > 0 {
                tx.execute(
                    "UPDATE sources SET latest=? WHERE id=?",
                    params![tx.last_insert_rowid(), source.id],
                )?;
            }
            if let (Some(turn_id), Some((submission_id, text_digest))) =
                (turn_id.as_deref(), matched_submission)
            {
                let started_at: Option<String> = tx
                    .query_row(
                        "SELECT started_at FROM turn_index WHERE transcript_id=? AND turn_id=? ORDER BY started_at DESC LIMIT 1",
                        params![transcript_id, turn_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(started_at) = started_at else {
                    return Err(EventError("turn_boundary_incomplete"));
                };
                tx.execute(
                    "UPDATE turn_index SET human_event_key=?,human_text_digest=?,updated=? WHERE transcript_id=? AND turn_id=? AND started_at=?",
                    params![key, text_digest, now(), transcript_id, turn_id, started_at],
                )?;
                let linked = tx.execute(
                    "UPDATE pending_submissions SET state='observed',transcript_id=?,turn_id=?,turn_started_at=?,human_event_key=?,updated=? WHERE submission_id=? AND state='prepared'",
                    params![transcript_id, turn_id, started_at, key, now(), submission_id],
                )?;
                if linked != 1 {
                    return Err(EventError("submission_identity_conflict"));
                }
            }
            if let (Some(turn_id), Some(terminal_state)) = (turn_id.as_deref(), terminal_state) {
                let updated = tx.execute(
                    "UPDATE turn_index SET end_checkpoint=?,terminal_state=?,updated=? WHERE transcript_id=? AND turn_id=?",
                    params![serde_json::to_string(next)?, terminal_state, now(), transcript_id, turn_id],
                )?;
                if updated > 0 {
                    tx.execute(
                        "UPDATE pending_submissions SET state='terminal',terminal_state=?,updated=? WHERE transcript_id=? AND turn_id=? AND state='observed'",
                        params![terminal_state, now(), transcript_id, turn_id],
                    )?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn fail(&mut self, source: &RegisteredSource, code: &'static str) -> Result<()> {
        let tx = self.db.transaction()?;
        let body = AgentReplyEvent {
            schema_version: 1,
            event_id: digest(format!("{}:{}:error:{code}", self.id, source.id).as_bytes()),
            cursor: String::new(),
            source_id: source.id.clone(),
            agent_kind: source.kind,
            session_id: source.session_id.clone(),
            turn_id: None,
            occurred_at: super::timestamp(),
            payload: ReplyPayload::SourceError { code: code.into() },
        };
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO events(source,key,body,created) VALUES (?, ?, ?, ?)",
            params![
                source.id,
                format!("error:{code}"),
                serde_json::to_string(&body)?,
                now()
            ],
        )?;
        tx.execute(
            "UPDATE sources SET state='error',error=? WHERE id=?",
            params![code, source.id],
        )?;
        if inserted > 0 {
            tx.execute(
                "UPDATE sources SET latest=? WHERE id=?",
                params![tx.last_insert_rowid(), source.id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn sources(&self) -> Result<Vec<AgentEventSource>> {
        let mut stmt = self
            .db
            .prepare("SELECT definition,state,error,floor,latest,json_extract(checkpoint,'$.offset') FROM sources ORDER BY id")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })?;
        rows.map(|row| {
            let (raw, state, error, floor, latest, checkpoint_offset) = row?;
            let source: RegisteredSource = serde_json::from_str(&raw)?;
            Ok(AgentEventSource {
                earliest_cursor: self.cursor(&source.id, floor),
                latest_cursor: self.cursor(&source.id, latest),
                source_id: source.id,
                terminal_id: source.terminal_id,
                agent_kind: source.kind,
                session_id: source.session_id,
                state,
                error,
                checkpoint_offset: u64::try_from(checkpoint_offset)
                    .map_err(|_| EventError("journal_unavailable"))?,
            })
        })
        .collect()
    }
    fn cursor(&self, source: &str, seq: i64) -> String {
        format!("{}:{source}:{seq}", self.id)
    }
    pub fn read(&self, source: &str, after: &str, limit: u32) -> Result<AgentEventsBatch> {
        if !(1..=128).contains(&limit) {
            return Err(EventError("invalid_limit"));
        }
        let (floor, latest): (i64, i64) = self
            .db
            .query_row(
                "SELECT floor,latest FROM sources WHERE id=?",
                [source],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or(EventError("source_not_found"))?;
        let seq = if after == "start" {
            0
        } else if after == "latest" {
            latest
        } else {
            let prefix = format!("{}:{source}:", self.id);
            after
                .strip_prefix(&prefix)
                .and_then(|s| s.parse::<i64>().ok())
                .filter(|s| *s >= 0)
                .ok_or(EventError("invalid_cursor"))?
        };
        if seq < floor {
            return Err(EventError("cursor_expired"));
        }
        if seq > latest {
            return Err(EventError("invalid_cursor"));
        }
        let mut stmt = self
            .db
            .prepare("SELECT seq,body FROM events WHERE source=? AND seq>? ORDER BY seq LIMIT ?")?;
        let rows = stmt.query_map(params![source, seq, limit], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut events = Vec::new();
        let mut next = seq;
        let mut bytes = 0;
        for row in rows {
            let (sequence, body) = row?;
            if bytes + body.len() > 1024 * 1024 && !events.is_empty() {
                break;
            }
            bytes += body.len();
            let mut event: AgentReplyEvent = serde_json::from_str(&body)?;
            event.cursor = self.cursor(source, sequence);
            events.push(event);
            next = sequence;
        }
        Ok(AgentEventsBatch {
            events,
            next_cursor: self.cursor(source, next),
            earliest_cursor: self.cursor(source, floor),
            latest_cursor: self.cursor(source, latest),
        })
    }

    pub fn recover_turn(
        &mut self,
        params: &AgentEventsRecoverTurnParams,
    ) -> Result<AgentEventsRecoveryBatch> {
        if params.session_id.is_empty()
            || params.turn_id.is_empty()
            || params.started_at.is_empty()
            || !(1..=128).contains(&params.limit)
        {
            return Err(EventError("invalid_turn_boundary"));
        }
        let mut rows = self.find_recoverable_turn(params)?;
        if rows.is_empty() {
            let complete = self.advance_historical_index(params.agent_kind, &params.session_id)?;
            rows = self.find_recoverable_turn(params)?;
            if rows.is_empty() {
                return Ok(AgentEventsRecoveryBatch {
                    outcome: if complete {
                        AgentEventsRecoveryOutcome::NotFound
                    } else {
                        AgentEventsRecoveryOutcome::Indexing
                    },
                    canonical_started_at: None,
                    events: Vec::new(),
                    next_cursor: String::new(),
                    complete,
                });
            }
        }
        if rows.len() != 1 {
            return Ok(AgentEventsRecoveryBatch {
                outcome: AgentEventsRecoveryOutcome::Ambiguous,
                canonical_started_at: None,
                events: Vec::new(),
                next_cursor: String::new(),
                complete: true,
            });
        }
        let (transcript_id, source_definition, turn_id, started_at, start, end) =
            rows.pop().ok_or(EventError("turn_not_found"))?;
        if !recovery_start_fence_matches(&started_at, &params.started_at) {
            return Ok(AgentEventsRecoveryBatch {
                outcome: AgentEventsRecoveryOutcome::Ambiguous,
                canonical_started_at: Some(started_at),
                events: Vec::new(),
                next_cursor: String::new(),
                complete: true,
            });
        }
        let indexed = IndexedTurn {
            transcript_id,
            source: serde_json::from_str(&source_definition)?,
            turn_id,
            started_at,
            start: serde_json::from_str(&start)?,
            end: serde_json::from_str(&end)?,
        };
        let mut cursor = match params.after.as_deref() {
            None => RecoveryCursor {
                transcript_id: indexed.transcript_id.clone(),
                turn_id: indexed.turn_id.clone(),
                started_at: indexed.started_at.clone(),
                checkpoint: indexed.start.clone(),
                event_skip: 0,
                ordinal: 0,
            },
            Some(value) => decode_recovery_cursor(value)?,
        };
        if cursor.transcript_id != indexed.transcript_id
            || cursor.turn_id != indexed.turn_id
            || cursor.started_at != indexed.started_at
            || cursor.checkpoint.offset < indexed.start.offset
            || cursor.checkpoint.offset > indexed.end.offset
            || indexed.source.kind != params.agent_kind
        {
            return Err(EventError("invalid_cursor"));
        }
        let mut recovered = Vec::new();
        while recovered.len() < params.limit as usize
            && cursor.checkpoint.offset < indexed.end.offset
        {
            let batch_start = cursor.checkpoint.clone();
            let (next, decoded) =
                SourceReader::read_until(&indexed.source, &batch_start, indexed.end.offset)?;
            if next.offset <= batch_start.offset || next.offset > indexed.end.offset {
                return Err(EventError("turn_boundary_changed"));
            }
            if next.offset == indexed.end.offset && next != indexed.end {
                return Err(EventError("turn_boundary_changed"));
            }
            let matching = decoded
                .into_iter()
                .filter(|event| event.turn.as_deref() == Some(indexed.turn_id.as_str()))
                .collect::<Vec<_>>();
            if cursor.event_skip > matching.len() {
                return Err(EventError("invalid_cursor"));
            }
            let available = &matching[cursor.event_skip..];
            let take = available.len().min(params.limit as usize - recovered.len());
            for event in available.iter().take(take) {
                let key = format!("{}:{}", indexed.turn_id, event.key);
                let mut payload = event.payload.clone();
                if let ReplyPayload::HumanMessage { submission_id, .. } = &mut payload {
                    *submission_id = self
                        .db
                        .query_row(
                            "SELECT submission_id FROM pending_submissions WHERE transcript_id=? AND turn_id=? AND human_event_key=? AND state IN ('observed','terminal') ORDER BY created LIMIT 1",
                            params![indexed.transcript_id, indexed.turn_id, key],
                            |row| row.get(0),
                        )
                        .optional()?;
                }
                cursor.ordinal = cursor.ordinal.saturating_add(1);
                recovered.push(AgentReplyEvent {
                    schema_version: 1,
                    event_id: digest(format!("{}:{key}", indexed.transcript_id).as_bytes()),
                    cursor: format!("recovery:{}:{}", indexed.transcript_id, cursor.ordinal),
                    source_id: format!("recovery:{}", indexed.transcript_id),
                    agent_kind: indexed.source.kind,
                    session_id: indexed.source.session_id.clone(),
                    turn_id: event.turn.clone(),
                    occurred_at: event.time.clone(),
                    payload,
                });
            }
            if cursor.event_skip + take < matching.len() {
                cursor.event_skip += take;
                break;
            }
            cursor.checkpoint = next;
            cursor.event_skip = 0;
        }
        let complete = cursor.checkpoint.offset >= indexed.end.offset;
        Ok(AgentEventsRecoveryBatch {
            outcome: AgentEventsRecoveryOutcome::Recovered,
            canonical_started_at: Some(indexed.started_at),
            events: recovered,
            next_cursor: encode_recovery_cursor(&cursor)?,
            complete,
        })
    }

    fn find_recoverable_turn(
        &self,
        params: &AgentEventsRecoverTurnParams,
    ) -> Result<Vec<RecoverableTurnRow>> {
        let mut statement = self.db.prepare(
            "SELECT transcript_id,source_definition,turn_id,started_at,start_checkpoint,end_checkpoint
             FROM turn_index WHERE agent_kind=? AND session_id=? AND turn_id=? AND end_checkpoint IS NOT NULL
             ORDER BY transcript_id,started_at LIMIT 2",
        )?;
        let rows = statement
            .query_map(
                params![
                    transcript_kind_name(params.agent_kind),
                    params.session_id,
                    params.turn_id
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn advance_historical_index(
        &mut self,
        agent_kind: TranscriptKind,
        session_id: &str,
    ) -> Result<bool> {
        let kind = transcript_kind_name(agent_kind);
        let mut statement = self.db.prepare(
            "SELECT definition FROM sources
             WHERE json_extract(definition,'$.kind')=? AND json_extract(definition,'$.session_id')=?
             ORDER BY CASE state WHEN 'active' THEN 0 ELSE 1 END,id DESC",
        )?;
        let definitions = statement
            .query_map(params![kind, session_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);
        let mut candidates = definitions
            .into_iter()
            .filter_map(|raw| serde_json::from_str::<RegisteredSource>(&raw).ok())
            .filter(|source| transcript_identity(source).is_ok())
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(true);
        }
        let transcript_id = transcript_identity(&candidates[0])?;
        candidates.retain(|source| {
            transcript_identity(source).is_ok_and(|candidate| candidate == transcript_id)
        });
        let state_key = format!("history_index:{transcript_id}");
        let raw: Option<String> = self
            .db
            .query_row(
                "SELECT value FROM metadata WHERE key=?",
                [&state_key],
                |row| row.get(0),
            )
            .optional()?;
        let mut state = raw
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or(HistoricalIndexState {
                checkpoint: Checkpoint::default(),
                complete: false,
            });
        for _ in 0..HISTORY_INDEX_BATCHES_PER_LOOKUP {
            let before = state.checkpoint.clone();
            let mut first_error = None;
            let mut readable = None;
            for source in &candidates {
                match SourceReader::read(source, &before) {
                    Ok(batch) => {
                        readable = Some((source, batch));
                        break;
                    }
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            let (source, (next, events)) =
                readable.ok_or_else(|| first_error.unwrap_or(EventError("source_unavailable")))?;
            state.complete = next.offset == before.offset;
            if !state.complete {
                self.commit_historical_index_batch(
                    source,
                    &transcript_id,
                    &state_key,
                    &before,
                    &next,
                    &events,
                )?;
                state.checkpoint = next;
            } else {
                self.db.execute(
                    "INSERT INTO metadata(key,value) VALUES (?,?)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![state_key, serde_json::to_string(&state)?],
                )?;
                break;
            }
        }
        Ok(state.complete)
    }

    fn commit_historical_index_batch(
        &mut self,
        source: &RegisteredSource,
        transcript_id: &str,
        state_key: &str,
        before: &Checkpoint,
        next: &Checkpoint,
        events: &[Decoded],
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        let source_definition = serde_json::to_string(source)?;
        let kind = transcript_kind_name(source.kind);
        for event in events {
            let Some(turn_id) = event.turn.as_deref() else {
                continue;
            };
            if matches!(event.payload, ReplyPayload::TurnStarted) {
                tx.execute(
                    "INSERT INTO turn_index(transcript_id,agent_kind,session_id,source_definition,turn_id,started_at,start_checkpoint,created,updated)
                     VALUES (?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(transcript_id,turn_id,started_at) DO UPDATE SET
                       source_definition=excluded.source_definition,
                       start_checkpoint=excluded.start_checkpoint,
                       updated=excluded.updated",
                    params![transcript_id, kind, source.session_id, source_definition, turn_id, event.time, serde_json::to_string(before)?, now(), now()],
                )?;
            }
            let terminal_state = match event.payload {
                ReplyPayload::TurnCompleted => Some("completed"),
                ReplyPayload::TurnAborted { .. } => Some("aborted"),
                _ => None,
            };
            if let Some(terminal_state) = terminal_state {
                tx.execute(
                    "UPDATE turn_index SET source_definition=?,end_checkpoint=?,terminal_state=?,updated=?
                     WHERE transcript_id=? AND turn_id=?",
                    params![source_definition, serde_json::to_string(next)?, terminal_state, now(), transcript_id, turn_id],
                )?;
            }
        }
        let state = HistoricalIndexState {
            checkpoint: next.clone(),
            complete: false,
        };
        tx.execute(
            "INSERT INTO metadata(key,value) VALUES (?,?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![state_key, serde_json::to_string(&state)?],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn turns(&self, params: &AgentEventsListTurnsParams) -> Result<AgentEventsTurnList> {
        if params.session_id.is_empty()
            || params.session_id.len() > 512
            || !(1..=128).contains(&params.limit)
        {
            return Err(EventError("invalid_turn_boundary"));
        }
        let kind = transcript_kind_name(params.agent_kind);
        let after = match params.after.as_deref() {
            Some(value) => {
                let cursor = decode_turn_list_cursor(value)?;
                if cursor.agent_kind != params.agent_kind || cursor.session_id != params.session_id
                {
                    return Err(EventError("invalid_cursor"));
                }
                cursor
            }
            None => TurnListCursor {
                agent_kind: params.agent_kind,
                session_id: params.session_id.clone(),
                started_at: String::new(),
                transcript_id: String::new(),
                turn_id: String::new(),
            },
        };
        let mut statement = self.db.prepare(
            "SELECT transcript_id,turn_id,started_at,terminal_state,human_event_key
             FROM turn_index
             WHERE agent_kind=?1 AND session_id=?2 AND
               (started_at>?3 OR
                (started_at=?3 AND transcript_id>?4) OR
                (started_at=?3 AND transcript_id=?4 AND turn_id>?5))
             ORDER BY started_at,transcript_id,turn_id LIMIT ?6",
        )?;
        let rows = statement.query_map(
            params![
                kind,
                params.session_id,
                after.started_at,
                after.transcript_id,
                after.turn_id,
                i64::from(params.limit) + 1,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )?;
        let mut indexed = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        let complete = indexed.len() <= params.limit as usize;
        indexed.truncate(params.limit as usize);
        let next_cursor = indexed
            .last()
            .map(|(transcript_id, turn_id, started_at, _, _)| {
                encode_turn_list_cursor(&TurnListCursor {
                    agent_kind: params.agent_kind,
                    session_id: params.session_id.clone(),
                    started_at: started_at.clone(),
                    transcript_id: transcript_id.clone(),
                    turn_id: turn_id.clone(),
                })
            })
            .transpose()?
            .or_else(|| params.after.clone());
        let mut turns = Vec::with_capacity(indexed.len());
        for (transcript_id, turn_id, started_at, terminal_state, human_event_key) in indexed {
            let submission_id = self
                .db
                .query_row(
                    "SELECT submission_id FROM pending_submissions
                     WHERE transcript_id=? AND turn_id=? AND turn_started_at=?
                       AND state IN ('observed','terminal')
                     ORDER BY created LIMIT 1",
                    params![transcript_id, turn_id, started_at],
                    |row| row.get(0),
                )
                .optional()?;
            turns.push(AgentEventsTurnSummary {
                turn_id,
                started_at,
                terminal_state,
                has_human_message: human_event_key.is_some(),
                submission_id,
            });
        }
        Ok(AgentEventsTurnList {
            turns,
            next_cursor,
            complete,
        })
    }

    pub fn locate(&self, params: &AgentEventsLocateParams) -> Result<AgentEventsTurnCursor> {
        let (floor, latest, checkpoint): (i64, i64, String) = self
            .db
            .query_row(
                "SELECT floor,latest,checkpoint FROM sources WHERE id=?",
                [&params.source_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .ok_or(EventError("source_not_found"))?;
        let exact = match (&params.turn_id, &params.started_at) {
            (Some(turn_id), Some(started_at)) if !turn_id.is_empty() && !started_at.is_empty() => {
                Some((turn_id.as_str(), started_at.as_str()))
            }
            (None, None) => None,
            _ => return Err(EventError("invalid_turn_boundary")),
        };
        let start = match params.boundary {
            AgentEventsTurnBoundary::Active => {
                if exact.is_some() {
                    return Err(EventError("invalid_turn_boundary"));
                }
                let checkpoint: serde_json::Value = serde_json::from_str(&checkpoint)?;
                let Some(turn_id) = checkpoint
                    .pointer("/decoder/turn")
                    .and_then(serde_json::Value::as_str)
                else {
                    return Ok(AgentEventsTurnCursor {
                        source_id: params.source_id.clone(),
                        found: false,
                        after_cursor: self.cursor(&params.source_id, latest),
                    });
                };
                let start = self.turn_event(&params.source_id, turn_id, "start")?;
                let completed = self.turn_event(&params.source_id, turn_id, "complete")?;
                let aborted = self.turn_event(&params.source_id, turn_id, "abort")?;
                if completed.is_some() || aborted.is_some() {
                    None
                } else {
                    start.and_then(|(seq, event)| {
                        matches!(event.payload, ReplyPayload::TurnStarted).then_some(seq)
                    })
                }
            }
            AgentEventsTurnBoundary::At | AgentEventsTurnBoundary::After => {
                let Some((turn_id, started_at)) = exact else {
                    return Err(EventError("invalid_turn_boundary"));
                };
                self.turn_event(&params.source_id, turn_id, "start")?
                    .and_then(|(seq, event)| {
                        (matches!(event.payload, ReplyPayload::TurnStarted)
                            && event.turn_id.as_deref() == Some(turn_id)
                            && timestamps_equal(&event.occurred_at, started_at))
                        .then_some(seq)
                    })
            }
        };
        let Some(start) = start.filter(|seq| *seq > floor) else {
            return Ok(AgentEventsTurnCursor {
                source_id: params.source_id.clone(),
                found: false,
                after_cursor: self.cursor(&params.source_id, latest),
            });
        };
        if params.boundary == AgentEventsTurnBoundary::After {
            let Some((turn_id, _)) = exact else {
                return Err(EventError("invalid_turn_boundary"));
            };
            let completed = self.turn_event(&params.source_id, turn_id, "complete")?;
            let aborted = self.turn_event(&params.source_id, turn_id, "abort")?;
            let terminal = [completed, aborted]
                .into_iter()
                .flatten()
                .filter(|(seq, _)| *seq > start)
                .map(|(seq, _)| seq)
                .min();
            let Some(terminal) = terminal.filter(|seq| *seq > floor) else {
                return Ok(AgentEventsTurnCursor {
                    source_id: params.source_id.clone(),
                    found: false,
                    after_cursor: self.cursor(&params.source_id, latest),
                });
            };
            return Ok(AgentEventsTurnCursor {
                source_id: params.source_id.clone(),
                found: true,
                after_cursor: self.cursor(&params.source_id, terminal),
            });
        }
        let previous: i64 = self.db.query_row(
            "SELECT COALESCE(MAX(seq),?) FROM events WHERE source=? AND seq<?",
            params![floor, params.source_id, start],
            |r| r.get(0),
        )?;
        Ok(AgentEventsTurnCursor {
            source_id: params.source_id.clone(),
            found: true,
            after_cursor: self.cursor(&params.source_id, previous.max(floor)),
        })
    }
    fn turn_event(
        &self,
        source_id: &str,
        turn_id: &str,
        kind: &str,
    ) -> Result<Option<(i64, AgentReplyEvent)>> {
        let key = format!("{turn_id}:{kind}:{turn_id}");
        let row: Option<(i64, String)> = self
            .db
            .query_row(
                "SELECT seq,body FROM events WHERE source=? AND key=?",
                params![source_id, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(seq, body)| Ok((seq, serde_json::from_str(&body)?)))
            .transpose()
    }
    pub fn prune(&mut self) -> Result<()> {
        self.prune_to(MAX_EVENT_BYTES, now() - RETENTION_SECONDS)
    }
    fn prune_to(&mut self, max_bytes: i64, cutoff: i64) -> Result<()> {
        let tx = self.db.transaction()?;
        let expired: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM events WHERE created < ?",
            [cutoff],
            |r| r.get(0),
        )?;
        let oversized: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM (SELECT seq,SUM(length(CAST(body AS BLOB))) OVER (ORDER BY seq DESC) AS retained_bytes FROM events) WHERE retained_bytes > ?",
            [max_bytes], |r| r.get(0))?;
        let through = expired.max(oversized);
        tx.execute("UPDATE sources SET floor=MAX(floor,COALESCE((SELECT MAX(seq) FROM events WHERE source=sources.id AND seq<=?),0))", [through])?;
        tx.execute("DELETE FROM events WHERE seq<=?", [through])?;
        tx.commit()?;
        Ok(())
    }
}

fn timestamps_equal(left: &str, right: &str) -> bool {
    let format = &time::format_description::well_known::Rfc3339;
    match (
        time::OffsetDateTime::parse(left, format),
        time::OffsetDateTime::parse(right, format),
    ) {
        (Ok(left), Ok(right)) => left.unix_timestamp() == right.unix_timestamp(),
        _ => false,
    }
}

fn recovery_start_fence_matches(canonical: &str, requested: &str) -> bool {
    if canonical == requested {
        return true;
    }
    let format = &time::format_description::well_known::Rfc3339;
    match (
        time::OffsetDateTime::parse(canonical, format),
        time::OffsetDateTime::parse(requested, format),
    ) {
        (Ok(canonical), Ok(requested)) => {
            requested.nanosecond() == 0 && canonical.unix_timestamp() == requested.unix_timestamp()
        }
        _ => false,
    }
}

fn transcript_kind_name(kind: TranscriptKind) -> &'static str {
    match kind {
        TranscriptKind::Traex => "traex",
        TranscriptKind::Pi => "pi",
    }
}

fn transcript_identity(source: &RegisteredSource) -> Result<String> {
    Ok(digest(
        serde_json::to_string(&(source.kind, &source.session_id, &source.header_hash))?.as_bytes(),
    ))
}

fn encode_recovery_cursor(cursor: &RecoveryCursor) -> Result<String> {
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor)?))
}

fn decode_recovery_cursor(value: &str) -> Result<RecoveryCursor> {
    if value.is_empty() || value.len() > 64 * 1024 {
        return Err(EventError("invalid_cursor"));
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| EventError("invalid_cursor"))?;
    serde_json::from_slice(&decoded).map_err(|_| EventError("invalid_cursor"))
}

fn encode_turn_list_cursor(cursor: &TurnListCursor) -> Result<String> {
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor)?))
}

fn decode_turn_list_cursor(value: &str) -> Result<TurnListCursor> {
    if value.is_empty() || value.len() > 16 * 1024 {
        return Err(EventError("invalid_cursor"));
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| EventError("invalid_cursor"))?;
    serde_json::from_slice(&decoded).map_err(|_| EventError("invalid_cursor"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::agent_events::{GoalStatus, TranscriptKind};

    #[test]
    fn timestamp_comparison_tolerates_only_subsecond_precision_loss() {
        assert!(timestamps_equal(
            "2026-09-27T11:08:31.000Z",
            "2026-09-27T11:08:31.667Z"
        ));
        assert!(!timestamps_equal(
            "2026-09-27T11:08:31.999Z",
            "2026-09-27T11:08:32.000Z"
        ));
    }

    #[test]
    fn recovery_start_fence_accepts_only_exact_or_seconds_precision() {
        assert!(recovery_start_fence_matches(
            "2026-09-30T04:13:43.159Z",
            "2026-09-30T04:13:43.000Z"
        ));
        assert!(!recovery_start_fence_matches(
            "2026-09-30T04:13:43.159Z",
            "2026-09-30T04:13:43.500Z"
        ));
        assert!(!recovery_start_fence_matches(
            "2026-09-30T04:13:43.159Z",
            "2026-09-30T04:13:44.000Z"
        ));
    }

    #[test]
    fn transcript_identity_survives_source_and_terminal_replacement() {
        let source = RegisteredSource {
            id: "old-source".into(),
            terminal_id: "old-terminal".into(),
            kind: TranscriptKind::Traex,
            session_id: "session".into(),
            path: "/old/session.jsonl".into(),
            foreground_pid: 1,
            header_hash: "header".into(),
            file_identity: "old-file".into(),
        };
        let mut replacement = source.clone();
        replacement.id = "new-source".into();
        replacement.terminal_id = "new-terminal".into();
        replacement.path = "/new/session.jsonl".into();
        replacement.foreground_pid = 2;
        replacement.file_identity = "new-file".into();
        assert_eq!(
            transcript_identity(&source).unwrap(),
            transcript_identity(&replacement).unwrap()
        );
    }

    #[test]
    fn byte_retention_prunes_more_than_one_old_batch() {
        let root =
            std::env::temp_dir().join(format!("herdr-retention-{}-{}", std::process::id(), now()));
        std::fs::create_dir_all(&root).unwrap();
        let mut journal = Journal::open(&root.join("events.db")).unwrap();
        journal
            .db
            .execute(
                "INSERT INTO sources(id,definition,checkpoint) VALUES ('s','{}','{}')",
                [],
            )
            .unwrap();
        journal.db.execute("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<5000) INSERT INTO events(source,key,body,created) SELECT 's',CAST(i AS TEXT),'界',? FROM n", [now()]).unwrap();
        journal.prune_to(9, 0).unwrap();
        let (count, bytes): (i64, i64) = journal
            .db
            .query_row(
                "SELECT count(*),SUM(length(CAST(body AS BLOB))) FROM events",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((count, bytes), (3, 9));
        let floor: i64 = journal
            .db
            .query_row("SELECT floor FROM sources WHERE id='s'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(floor, 4997);
        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn transaction_replay_retention_and_cursor_identity() {
        let path =
            std::env::temp_dir().join(format!("herdr-journal-{}-{}.db", std::process::id(), now()));
        let source = RegisteredSource {
            id: "source".into(),
            terminal_id: "term".into(),
            kind: TranscriptKind::Pi,
            session_id: "session".into(),
            path: "/unused".into(),
            foreground_pid: 1,
            header_hash: "header".into(),
            file_identity: "file".into(),
        };
        let mut j = Journal::open(&path).unwrap();
        j.attach(&source, &Checkpoint::default()).unwrap();
        assert!(matches!(
            Journal::open(&path),
            Err(EventError("journal_writer_busy"))
        ));
        let before = Checkpoint::default();
        let mut next = before.clone();
        next.offset = 10;
        let event = || Decoded {
            key: "start".into(),
            turn: Some("turn".into()),
            time: "now".into(),
            payload: ReplyPayload::TurnStarted,
        };
        j.commit(&source, &before, &next, vec![event()]).unwrap();
        let first = j.read("source", "start", 64).unwrap();
        assert_eq!(first.events.len(), 1);
        assert!(j.commit(&source, &before, &next, vec![event()]).is_err());
        drop(j);
        let mut j = Journal::open(&path).unwrap();
        assert_eq!(j.active().unwrap()[0].1.offset, 10);
        j.commit(&source, &next, &next, vec![event()]).unwrap();
        assert_eq!(j.read("source", "start", 64).unwrap(), first);
        assert!(j
            .read("source", &first.next_cursor, 64)
            .unwrap()
            .events
            .is_empty());
        assert_eq!(
            j.read("source", "foreign:source:0", 64).unwrap_err().0,
            "invalid_cursor"
        );
        let mut failed = next.clone();
        failed.offset = 20;
        j.db.execute_batch("CREATE TRIGGER reject_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'test write failure'); END;").unwrap();
        assert!(j.commit(&source, &next, &failed, vec![event()]).is_err());
        assert_eq!(j.active().unwrap()[0].1.offset, 10);
        j.db.execute_batch("DROP TRIGGER reject_event").unwrap();
        j.db.execute("UPDATE events SET created=0", []).unwrap();
        j.prune().unwrap();
        assert_eq!(
            j.read("source", "start", 64).unwrap_err().0,
            "cursor_expired"
        );
        assert!(j
            .read("source", &first.next_cursor, 64)
            .unwrap()
            .events
            .is_empty());
        j.fail(&source, "source_io_error").unwrap();
        let error_cursor = j.read("source", "latest", 64).unwrap().latest_cursor;
        let mut other = source.clone();
        other.id = "other".into();
        j.attach(&other, &Checkpoint::default()).unwrap();
        j.commit(&other, &before, &next, vec![event()]).unwrap();
        j.fail(&source, "source_io_error").unwrap();
        assert_eq!(
            j.read("source", "latest", 64).unwrap().latest_cursor,
            error_cursor
        );
        j.attach(&source, &Checkpoint::default()).unwrap();
        assert_eq!(
            j.sources()
                .unwrap()
                .iter()
                .find(|s| s.source_id == "source")
                .unwrap()
                .state,
            "active"
        );
        drop(j);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn frozen_high_water_cursor_does_not_commit_a_later_batch_tail() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-high-water-{}-{}.db",
            std::process::id(),
            now()
        ));
        let source = RegisteredSource {
            id: "source".into(),
            terminal_id: "term".into(),
            kind: TranscriptKind::Traex,
            session_id: "session".into(),
            path: "/unused".into(),
            foreground_pid: 1,
            header_hash: "header".into(),
            file_identity: "file".into(),
        };
        let event = |key: &str, text: &str, truncated: bool| Decoded {
            key: key.into(),
            turn: Some("turn".into()),
            time: "2026-09-30T00:00:00Z".into(),
            payload: ReplyPayload::ToolResult {
                call_id: key.into(),
                text: text.into(),
                is_error: false,
                truncated,
            },
        };

        let mut journal = Journal::open(&path).unwrap();
        let before = journal.attach(&source, &Checkpoint::default()).unwrap();
        let mut first_checkpoint = before.clone();
        first_checkpoint.offset = 10;
        journal
            .commit(
                &source,
                &before,
                &first_checkpoint,
                vec![
                    event("one", "full payload one", false),
                    event("two", "full payload two", false),
                    event("three", "bounded payload three", true),
                ],
            )
            .unwrap();

        let first = journal.read("source", "start", 2).unwrap();
        let frozen_high_water = first.latest_cursor.clone();
        assert_eq!(first.events.len(), 2);
        assert_ne!(first.next_cursor, frozen_high_water);

        let mut second_checkpoint = first_checkpoint.clone();
        second_checkpoint.offset = 20;
        journal
            .commit(
                &source,
                &first_checkpoint,
                &second_checkpoint,
                vec![
                    event("four", "later payload four", false),
                    event("five", "later payload five", false),
                ],
            )
            .unwrap();

        let crossing = journal.read("source", &first.next_cursor, 3).unwrap();
        assert_eq!(crossing.events.len(), 3);
        assert_eq!(crossing.events[0].cursor, frozen_high_water);
        assert_ne!(crossing.next_cursor, frozen_high_water);
        assert!(matches!(
            &crossing.events[0].payload,
            ReplyPayload::ToolResult {
                text,
                truncated: true,
                ..
            } if text == "bounded payload three"
        ));

        let resumed = journal.read("source", &frozen_high_water, 64).unwrap();
        assert_eq!(resumed.events.len(), 2);
        assert_eq!(
            resumed
                .events
                .iter()
                .map(|event| event.payload.clone())
                .collect::<Vec<_>>(),
            crossing.events[1..]
                .iter()
                .map(|event| event.payload.clone())
                .collect::<Vec<_>>()
        );

        drop(journal);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn accepted_submission_is_bound_once_to_the_matching_human_message() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-submission-{}-{}.db",
            std::process::id(),
            now()
        ));
        let source = RegisteredSource {
            id: "source".into(),
            terminal_id: "term".into(),
            kind: TranscriptKind::Traex,
            session_id: "session".into(),
            path: "/unused".into(),
            foreground_pid: 1,
            header_hash: "header".into(),
            file_identity: "file".into(),
        };
        let mut journal = Journal::open(&path).unwrap();
        let before = journal.attach(&source, &Checkpoint::default()).unwrap();
        assert_eq!(
            journal
                .prepare_submission(
                    "prompt-1",
                    "term",
                    TranscriptKind::Traex,
                    "session",
                    "continue"
                )
                .unwrap(),
            SubmissionPrepareResult::Prepared
        );
        assert_eq!(
            journal
                .prepare_submission(
                    "prompt-1",
                    "term",
                    TranscriptKind::Traex,
                    "session",
                    "continue"
                )
                .unwrap(),
            SubmissionPrepareResult::Duplicate
        );
        let mut next = before.clone();
        next.offset = 10;
        journal
            .commit(
                &source,
                &before,
                &next,
                vec![
                    Decoded {
                        key: "start:turn-1".into(),
                        turn: Some("turn-1".into()),
                        time: "2026-09-28T00:00:00Z".into(),
                        payload: ReplyPayload::TurnStarted,
                    },
                    Decoded {
                        key: "human:user-1".into(),
                        turn: Some("turn-1".into()),
                        time: "2026-09-28T00:00:00Z".into(),
                        payload: ReplyPayload::HumanMessage {
                            message_id: "user-1".into(),
                            text: "continue".into(),
                            truncated: false,
                            submission_id: None,
                        },
                    },
                ],
            )
            .unwrap();

        let batch = journal.read("source", "start", 64).unwrap();
        assert!(matches!(
            &batch.events[1].payload,
            ReplyPayload::HumanMessage { submission_id: Some(id), .. } if id == "prompt-1"
        ));
        let receipt = journal
            .submission(&AgentEventsSubmissionParams {
                submission_id: "prompt-1".into(),
            })
            .unwrap();
        assert_eq!(receipt.state, AgentEventsSubmissionState::Observed);
        assert_eq!(receipt.turn_id.as_deref(), Some("turn-1"));
        assert_eq!(receipt.started_at.as_deref(), Some("2026-09-28T00:00:00Z"));
        drop(journal);
        let mut reopened = Journal::open(&path).unwrap();
        assert_eq!(
            reopened
                .prepare_submission(
                    "prompt-1",
                    "term",
                    TranscriptKind::Traex,
                    "session",
                    "continue"
                )
                .unwrap(),
            SubmissionPrepareResult::Duplicate
        );
        drop(reopened);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn version_one_consumed_submissions_migrate_without_fabricated_turn_identity() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-v1-migration-{}-{}.db",
            std::process::id(),
            now()
        ));
        {
            let db = Connection::open(&path).unwrap();
            db.execute_batch(
                "CREATE TABLE pending_submissions (
                    submission_id TEXT PRIMARY KEY, terminal_id TEXT NOT NULL,
                    agent_kind TEXT NOT NULL, session_id TEXT NOT NULL,
                    text_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('prepared','consumed')),
                    created INTEGER NOT NULL);
                 CREATE INDEX pending_submissions_match ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,created);
                 INSERT INTO pending_submissions VALUES ('legacy','term','traex','session','digest','consumed',1);
                 PRAGMA user_version=1;",
            )
            .unwrap();
        }

        let journal = Journal::open(&path).unwrap();
        let receipt = journal
            .submission(&AgentEventsSubmissionParams {
                submission_id: "legacy".into(),
            })
            .unwrap();
        assert_eq!(receipt.state, AgentEventsSubmissionState::LegacyUnavailable);
        assert!(receipt.turn_id.is_none());
        assert!(receipt.started_at.is_none());
        drop(journal);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn stream_only_version_two_migrates_without_fabricating_turn_identity() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-stream-v2-migration-{}-{}.db",
            std::process::id(),
            now()
        ));
        {
            let db = Connection::open(&path).unwrap();
            db.execute_batch(
                "CREATE TABLE pending_submissions (
                    submission_id TEXT PRIMARY KEY, terminal_id TEXT NOT NULL,
                    agent_kind TEXT NOT NULL, session_id TEXT NOT NULL,
                    text_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('prepared','accepted','rejected','uncertain')),
                    correlated INTEGER NOT NULL DEFAULT 0 CHECK(correlated IN (0,1)),
                    created INTEGER NOT NULL, updated INTEGER NOT NULL);
                 CREATE INDEX pending_submissions_match
                    ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,correlated,created);
                 INSERT INTO pending_submissions VALUES
                    ('prepared','term','traex','session','digest-1','prepared',0,1,2),
                    ('accepted','term','traex','session','digest-2','accepted',1,3,4),
                    ('rejected','term','traex','session','digest-3','rejected',0,5,6),
                    ('uncertain','term','traex','session','digest-4','uncertain',0,7,8);
                 PRAGMA user_version=2;",
            )
            .unwrap();
        }

        let mut journal = Journal::open(&path).unwrap();
        let columns = journal
            .db
            .prepare("PRAGMA table_info(pending_submissions)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(columns.contains(&"transcript_id".to_owned()));
        assert!(columns.contains(&"turn_id".to_owned()));
        assert!(columns.contains(&"turn_started_at".to_owned()));
        assert!(columns.contains(&"human_event_key".to_owned()));
        assert!(columns.contains(&"terminal_state".to_owned()));
        assert!(!columns.contains(&"correlated".to_owned()));

        for submission_id in ["prepared", "accepted", "rejected", "uncertain"] {
            let receipt = journal
                .submission(&AgentEventsSubmissionParams {
                    submission_id: submission_id.into(),
                })
                .unwrap();
            assert_eq!(receipt.state, AgentEventsSubmissionState::LegacyUnavailable);
            assert!(receipt.turn_id.is_none());
            assert!(receipt.started_at.is_none());
            assert!(receipt.terminal_state.is_none());
        }

        let turns = journal
            .turns(&AgentEventsListTurnsParams {
                agent_kind: TranscriptKind::Traex,
                session_id: "session".into(),
                after: None,
                limit: 128,
            })
            .unwrap();
        assert!(turns.turns.is_empty());
        assert!(turns.complete);
        let recovered = journal
            .recover_turn(&AgentEventsRecoverTurnParams {
                agent_kind: TranscriptKind::Traex,
                session_id: "session".into(),
                turn_id: "missing-turn".into(),
                started_at: "2026-10-02T00:00:00Z".into(),
                after: None,
                limit: 128,
            })
            .unwrap();
        assert_eq!(recovered.outcome, AgentEventsRecoveryOutcome::NotFound);
        drop(journal);

        let reopened = Journal::open(&path).unwrap();
        assert_eq!(
            reopened
                .submission(&AgentEventsSubmissionParams {
                    submission_id: "accepted".into(),
                })
                .unwrap()
                .state,
            AgentEventsSubmissionState::LegacyUnavailable
        );
        drop(reopened);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn recovers_an_exact_pruned_turn_without_reading_later_transcript_content() {
        use std::io::Write as _;

        let root = std::env::temp_dir().join(format!(
            "herdr-journal-recovery-{}-{}",
            std::process::id(),
            now()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let transcript_path = root.join("session.jsonl");
        let first_turn = concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session\"}}\n",
            "{\"type\":\"response_item\",\"timestamp\":\"2026-09-30T00:00:00Z\",\"payload\":{\"type\":\"message\",\"id\":\"user-1\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"continue\"}]}}\n",
            "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T00:00:01Z\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-1\"}}\n",
            "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T00:00:01.500Z\",\"payload\":{\"type\":\"thread_goal_updated\",\"threadId\":\"session\",\"goal\":{\"threadId\":\"session\",\"objective\":\"Ship recovery\",\"status\":\"active\",\"createdAt\":100,\"updatedAt\":101}}}\n",
            "{\"type\":\"response_item\",\"timestamp\":\"2026-09-30T00:00:02Z\",\"payload\":{\"type\":\"message\",\"id\":\"answer-1\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n",
            "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T00:00:03Z\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-1\",\"last_agent_message\":\"answer\"}}\n"
        );
        std::fs::write(&transcript_path, first_turn).unwrap();
        let source = SourceReader::register(
            "term".into(),
            TranscriptKind::Traex,
            "session".into(),
            &transcript_path,
            1,
            std::slice::from_ref(&root),
        )
        .unwrap();
        let db_path = root.join("events.db");
        let mut journal = Journal::open(&db_path).unwrap();
        let before = journal.attach(&source, &Checkpoint::default()).unwrap();
        journal
            .prepare_submission(
                "prompt-1",
                "term",
                TranscriptKind::Traex,
                "session",
                "continue",
            )
            .unwrap();
        let (after, events) = SourceReader::read(&source, &before).unwrap();
        journal.commit(&source, &before, &after, events).unwrap();
        let original_ids = journal
            .read(&source.id, "start", 128)
            .unwrap()
            .events
            .into_iter()
            .filter(|event| event.turn_id.as_deref() == Some("turn-1"))
            .map(|event| event.event_id)
            .collect::<Vec<_>>();

        let mut transcript = std::fs::OpenOptions::new()
            .append(true)
            .open(&transcript_path)
            .unwrap();
        transcript
            .write_all(
                concat!(
                    "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T00:01:01Z\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-2\"}}\n",
                    "{\"type\":\"response_item\",\"timestamp\":\"2026-09-30T00:01:02Z\",\"payload\":{\"type\":\"message\",\"id\":\"answer-2\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"later\"}]}}\n"
                )
                .as_bytes(),
            )
            .unwrap();
        drop(transcript);
        journal
            .db
            .execute("UPDATE events SET created=0", [])
            .unwrap();
        journal.prune().unwrap();

        let mut recovered = Vec::new();
        let mut cursor = None;
        loop {
            let batch = journal
                .recover_turn(&AgentEventsRecoverTurnParams {
                    agent_kind: TranscriptKind::Traex,
                    session_id: "session".into(),
                    turn_id: "turn-1".into(),
                    started_at: "2026-09-30T00:00:01Z".into(),
                    after: cursor,
                    limit: 2,
                })
                .unwrap();
            recovered.extend(batch.events);
            if batch.complete {
                break;
            }
            cursor = Some(batch.next_cursor);
        }
        assert_eq!(
            recovered
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<Vec<_>>(),
            original_ids
        );
        assert!(recovered
            .iter()
            .all(|event| event.turn_id.as_deref() == Some("turn-1")));
        assert!(recovered.iter().any(|event| matches!(
            &event.payload,
            ReplyPayload::HumanMessage {
                submission_id: Some(id),
                ..
            } if id == "prompt-1"
        )));
        assert!(recovered.iter().any(|event| matches!(
            &event.payload,
            ReplyPayload::GoalChanged {
                objective,
                status: GoalStatus::Active,
                source_updated_at: 101,
                ..
            } if objective == "Ship recovery"
        )));

        std::fs::write(&transcript_path, first_turn.replace("answer", "tamper")).unwrap();
        assert_eq!(
            journal
                .recover_turn(&AgentEventsRecoverTurnParams {
                    agent_kind: TranscriptKind::Traex,
                    session_id: "session".into(),
                    turn_id: "turn-1".into(),
                    started_at: "2026-09-30T00:00:01Z".into(),
                    after: None,
                    limit: 128,
                })
                .unwrap_err()
                .0,
            "turn_boundary_changed"
        );

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovers_exact_native_turn_missing_from_live_index_after_end_attach() {
        let root = std::env::temp_dir().join(format!(
            "herdr-journal-history-index-{}-{}",
            std::process::id(),
            now()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let transcript_path = root.join("session.jsonl");
        std::fs::write(
            &transcript_path,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session\"}}\n",
                "{\"type\":\"response_item\",\"timestamp\":\"2026-09-30T04:13:42.900Z\",\"payload\":{\"type\":\"message\",\"id\":\"user-1\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"continue\"}]}}\n",
                "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T04:13:43.159Z\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-native\"}}\n",
                "{\"type\":\"response_item\",\"timestamp\":\"2026-09-30T04:13:44Z\",\"payload\":{\"type\":\"message\",\"id\":\"answer-1\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n",
                "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T04:13:45Z\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-native\",\"last_agent_message\":\"answer\"}}\n"
            ),
        )
        .unwrap();
        let source = SourceReader::register(
            "term-new".into(),
            TranscriptKind::Traex,
            "session".into(),
            &transcript_path,
            2,
            std::slice::from_ref(&root),
        )
        .unwrap();
        let db_path = root.join("events.db");
        let mut journal = Journal::open(&db_path).unwrap();
        let at_end = SourceReader::checkpoint_at_end(&source).unwrap();
        journal.attach(&source, &at_end).unwrap();
        assert_eq!(
            journal
                .db
                .query_row("SELECT count(*) FROM turn_index", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );

        let batch = journal
            .recover_turn(&AgentEventsRecoverTurnParams {
                agent_kind: TranscriptKind::Traex,
                session_id: "session".into(),
                turn_id: "turn-native".into(),
                started_at: "2026-09-30T04:13:43.000Z".into(),
                after: None,
                limit: 128,
            })
            .unwrap();

        assert_eq!(batch.outcome, AgentEventsRecoveryOutcome::Recovered);
        assert_eq!(
            batch.canonical_started_at.as_deref(),
            Some("2026-09-30T04:13:43.159Z")
        );
        assert!(batch.complete);
        assert!(batch.events.iter().any(|event| {
            matches!(event.payload, ReplyPayload::TurnStarted)
                && event.turn_id.as_deref() == Some("turn-native")
        }));
        assert!(batch
            .events
            .iter()
            .any(|event| matches!(event.payload, ReplyPayload::TurnCompleted)));

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_historical_turn_returns_a_typed_terminal_outcome() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-history-missing-{}-{}.db",
            std::process::id(),
            now()
        ));
        let mut journal = Journal::open(&path).unwrap();

        let batch = journal
            .recover_turn(&AgentEventsRecoverTurnParams {
                agent_kind: TranscriptKind::Traex,
                session_id: "missing-session".into(),
                turn_id: "missing-turn".into(),
                started_at: "2026-09-30T04:13:43Z".into(),
                after: None,
                limit: 128,
            })
            .unwrap();

        assert_eq!(batch.outcome, AgentEventsRecoveryOutcome::NotFound);
        assert!(batch.canonical_started_at.is_none());
        assert!(batch.events.is_empty());
        assert!(batch.complete);
        drop(journal);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn historical_index_falls_back_to_another_canonical_source() {
        let root = std::env::temp_dir().join(format!(
            "herdr-journal-history-source-fallback-{}-{}",
            std::process::id(),
            now()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let transcript = concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session\"}}\n",
            "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T04:13:43.159Z\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-native\"}}\n",
            "{\"type\":\"response_item\",\"timestamp\":\"2026-09-30T04:13:44Z\",\"payload\":{\"type\":\"message\",\"id\":\"answer-1\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n",
            "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-30T04:13:45Z\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-native\",\"last_agent_message\":\"answer\"}}\n"
        );
        let stale_path = root.join("stale.jsonl");
        let canonical_path = root.join("canonical.jsonl");
        std::fs::write(&stale_path, transcript).unwrap();
        std::fs::write(&canonical_path, transcript).unwrap();
        let mut stale = SourceReader::register(
            "term-old".into(),
            TranscriptKind::Traex,
            "session".into(),
            &stale_path,
            1,
            std::slice::from_ref(&root),
        )
        .unwrap();
        stale.id = "zz-stale".into();
        let mut canonical = SourceReader::register(
            "term-new".into(),
            TranscriptKind::Traex,
            "session".into(),
            &canonical_path,
            2,
            std::slice::from_ref(&root),
        )
        .unwrap();
        canonical.id = "aa-canonical".into();
        let db_path = root.join("events.db");
        let mut journal = Journal::open(&db_path).unwrap();
        journal
            .attach(&stale, &SourceReader::checkpoint_at_end(&stale).unwrap())
            .unwrap();
        journal
            .attach(
                &canonical,
                &SourceReader::checkpoint_at_end(&canonical).unwrap(),
            )
            .unwrap();
        std::fs::write(
            &stale_path,
            transcript.replacen("\"id\":\"session\"", "\"id\":\"replaced\"", 1),
        )
        .unwrap();

        let batch = journal
            .recover_turn(&AgentEventsRecoverTurnParams {
                agent_kind: TranscriptKind::Traex,
                session_id: "session".into(),
                turn_id: "turn-native".into(),
                started_at: "2026-09-30T04:13:43.000Z".into(),
                after: None,
                limit: 128,
            })
            .unwrap();

        assert!(batch.complete);
        assert!(batch
            .events
            .iter()
            .any(|event| matches!(event.payload, ReplyPayload::TurnCompleted)));

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn locates_exact_next_and_active_turns_without_scanning_history() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-boundary-{}-{}.db",
            std::process::id(),
            now()
        ));
        let source = RegisteredSource {
            id: "source".into(),
            terminal_id: "term".into(),
            kind: TranscriptKind::Traex,
            session_id: "session".into(),
            path: "/unused".into(),
            foreground_pid: 1,
            header_hash: "header".into(),
            file_identity: "file".into(),
        };
        let mut journal = Journal::open(&path).unwrap();
        let before = journal.attach(&source, &Checkpoint::default()).unwrap();
        let mut next = before.clone();
        next.offset = 10;
        next.decoder
            .decode(
                TranscriptKind::Traex,
                &serde_json::json!({
                    "type": "event_msg",
                    "timestamp": "2026-09-27T00:00:02Z",
                    "payload": { "type": "task_started", "turn_id": "turn-2" }
                }),
            )
            .unwrap();
        let decoded = |key: &str, turn: &str, time: &str, payload| Decoded {
            key: key.into(),
            turn: Some(turn.into()),
            time: time.into(),
            payload,
        };
        journal
            .commit(
                &source,
                &before,
                &next,
                vec![
                    decoded(
                        "start:turn-1",
                        "turn-1",
                        "2026-09-27T00:00:00Z",
                        ReplyPayload::TurnStarted,
                    ),
                    decoded(
                        "complete:turn-1",
                        "turn-1",
                        "2026-09-27T00:00:01Z",
                        ReplyPayload::TurnCompleted,
                    ),
                    decoded(
                        "start:turn-2",
                        "turn-2",
                        "2026-09-27T00:00:02Z",
                        ReplyPayload::TurnStarted,
                    ),
                ],
            )
            .unwrap();

        {
            let locate = |boundary, turn_id: Option<&str>, started_at: Option<&str>| {
                journal
                    .locate(&AgentEventsLocateParams {
                        source_id: "source".into(),
                        boundary,
                        turn_id: turn_id.map(str::to_owned),
                        started_at: started_at.map(str::to_owned),
                    })
                    .unwrap()
            };
            let at_first = locate(
                AgentEventsTurnBoundary::At,
                Some("turn-1"),
                Some("2026-09-27T00:00:00.000Z"),
            );
            assert!(at_first.found);
            assert_eq!(
                journal
                    .read("source", &at_first.after_cursor, 1)
                    .unwrap()
                    .events[0]
                    .turn_id
                    .as_deref(),
                Some("turn-1")
            );
            let after_first = locate(
                AgentEventsTurnBoundary::After,
                Some("turn-1"),
                Some("2026-09-27T00:00:00Z"),
            );
            assert!(after_first.found);
            assert_eq!(
                journal
                    .read("source", &after_first.after_cursor, 1)
                    .unwrap()
                    .events[0]
                    .turn_id
                    .as_deref(),
                Some("turn-2")
            );
            let active = locate(AgentEventsTurnBoundary::Active, None, None);
            assert!(active.found);
            assert_eq!(
                journal
                    .read("source", &active.after_cursor, 1)
                    .unwrap()
                    .events[0]
                    .turn_id
                    .as_deref(),
                Some("turn-2")
            );

            let plan: String = journal
                .db
                .query_row(
                    "EXPLAIN QUERY PLAN SELECT seq,body FROM events WHERE source=? AND key=?",
                    params!["source", "turn-1:start:turn-1"],
                    |r| r.get(3),
                )
                .unwrap();
            assert!(plan.contains("sqlite_autoindex_events_1"), "{plan}");
        }

        let previous = next.clone();
        next.offset = 20;
        journal
            .commit(
                &source,
                &previous,
                &next,
                vec![decoded(
                    "complete:turn-2",
                    "turn-2",
                    "2026-09-27T00:00:03Z",
                    ReplyPayload::TurnCompleted,
                )],
            )
            .unwrap();
        assert!(
            !journal
                .locate(&AgentEventsLocateParams {
                    source_id: "source".into(),
                    boundary: AgentEventsTurnBoundary::Active,
                    turn_id: None,
                    started_at: None,
                })
                .unwrap()
                .found
        );

        let first_page = journal
            .turns(&AgentEventsListTurnsParams {
                agent_kind: TranscriptKind::Traex,
                session_id: "session".into(),
                after: None,
                limit: 1,
            })
            .unwrap();
        assert_eq!(first_page.turns.len(), 1);
        assert_eq!(first_page.turns[0].turn_id, "turn-1");
        assert_eq!(
            first_page.turns[0].terminal_state.as_deref(),
            Some("completed")
        );
        assert!(!first_page.complete);
        let second_page = journal
            .turns(&AgentEventsListTurnsParams {
                agent_kind: TranscriptKind::Traex,
                session_id: "session".into(),
                after: first_page.next_cursor.clone(),
                limit: 1,
            })
            .unwrap();
        assert_eq!(second_page.turns.len(), 1);
        assert_eq!(second_page.turns[0].turn_id, "turn-2");
        assert!(second_page.complete);
        assert_eq!(
            journal
                .turns(&AgentEventsListTurnsParams {
                    agent_kind: TranscriptKind::Traex,
                    session_id: "other-session".into(),
                    after: first_page.next_cursor,
                    limit: 1,
                })
                .unwrap_err()
                .0,
            "invalid_cursor"
        );

        drop(journal);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
