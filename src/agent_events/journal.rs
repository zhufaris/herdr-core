use super::codec::Decoded;
use super::source::Checkpoint;
use super::{digest, now, EventError, RegisteredSource, Result};
use crate::api::schema::agent_events::{
    AgentEventSource, AgentEventsBatch, AgentEventsLocateParams, AgentEventsSubmissionReceipt,
    AgentEventsSubmissionState, AgentEventsTurnBoundary, AgentEventsTurnCursor, AgentReplyEvent,
    ReplyPayload, TranscriptKind,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

const MAX_SOURCES: i64 = 256;
const RETENTION_SECONDS: i64 = 7 * 24 * 3600;
const MAX_EVENT_BYTES: i64 = 192 * 1024 * 1024;
const JOURNAL_VERSION: i64 = 2;

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
            CREATE INDEX IF NOT EXISTS events_source_seq ON events(source,seq);
            CREATE TABLE IF NOT EXISTS pending_submissions (submission_id TEXT PRIMARY KEY, terminal_id TEXT NOT NULL, agent_kind TEXT NOT NULL, session_id TEXT NOT NULL, text_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','consumed')), created INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS pending_submissions_match ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,created);
            PRAGMA user_version=1;")?;
        if version < JOURNAL_VERSION {
            db.execute_batch(
                "BEGIN IMMEDIATE;
                DROP INDEX IF EXISTS pending_submissions_match;
                ALTER TABLE pending_submissions RENAME TO pending_submissions_v1;
                CREATE TABLE pending_submissions (
                    submission_id TEXT PRIMARY KEY,
                    terminal_id TEXT NOT NULL,
                    agent_kind TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    text_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('prepared','accepted','rejected','uncertain')),
                    correlated INTEGER NOT NULL DEFAULT 0 CHECK(correlated IN (0,1)),
                    created INTEGER NOT NULL,
                    updated INTEGER NOT NULL
                );
                INSERT INTO pending_submissions(
                    submission_id,terminal_id,agent_kind,session_id,text_digest,state,correlated,created,updated
                )
                SELECT submission_id,terminal_id,agent_kind,session_id,text_digest,
                    CASE state WHEN 'consumed' THEN 'accepted' ELSE 'prepared' END,
                    CASE state WHEN 'consumed' THEN 1 ELSE 0 END,
                    created,created
                FROM pending_submissions_v1;
                DROP TABLE pending_submissions_v1;
                CREATE INDEX pending_submissions_match
                    ON pending_submissions(terminal_id,agent_kind,session_id,text_digest,state,correlated,created);
                PRAGMA user_version=2;
                COMMIT;",
            )?;
        }
        db.execute(
            "UPDATE pending_submissions SET state='uncertain',updated=?
             WHERE state='prepared'",
            [now()],
        )?;
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
            let existing_source: RegisteredSource = serde_json::from_str(&existing_definition)?;
            if existing_source.terminal_id != source.terminal_id
                || existing_source.kind != source.kind
                || existing_source.session_id != source.session_id
                || existing_source.header_hash != source.header_hash
                || existing_source.file_identity != source.file_identity
            {
                return Err(EventError("source_identity_conflict"));
            }
            self.db.execute(
                "UPDATE sources SET definition=?,state='active',error=NULL WHERE id=?",
                params![definition, source.id],
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
            TranscriptKind::Traex => "traex",
            TranscriptKind::Pi => "pi",
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

    pub(crate) fn submission_receipt(
        &self,
        submission_id: &str,
    ) -> Result<AgentEventsSubmissionReceipt> {
        if submission_id.is_empty() || submission_id.len() > 256 {
            return Err(EventError("invalid_submission_id"));
        }
        self.db
            .query_row(
                "SELECT terminal_id,agent_kind,session_id,state,created,updated
                 FROM pending_submissions WHERE submission_id=?",
                [submission_id],
                |row| {
                    let kind: String = row.get(1)?;
                    let state: String = row.get(3)?;
                    Ok(AgentEventsSubmissionReceipt {
                        submission_id: submission_id.to_owned(),
                        terminal_id: row.get(0)?,
                        agent_kind: match kind.as_str() {
                            "traex" => TranscriptKind::Traex,
                            "pi" => TranscriptKind::Pi,
                            _ => return Err(rusqlite::Error::InvalidQuery),
                        },
                        session_id: row.get(2)?,
                        state: match state.as_str() {
                            "prepared" => AgentEventsSubmissionState::Prepared,
                            "accepted" => AgentEventsSubmissionState::Accepted,
                            "rejected" => AgentEventsSubmissionState::Rejected,
                            "uncertain" => AgentEventsSubmissionState::Uncertain,
                            _ => return Err(rusqlite::Error::InvalidQuery),
                        },
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )
            .optional()?
            .ok_or(EventError("submission_not_found"))
    }

    pub(crate) fn mark_submission_accepted(
        &mut self,
        submission_id: &str,
    ) -> Result<AgentEventsSubmissionReceipt> {
        self.transition_prepared_submission(
            submission_id,
            "accepted",
            AgentEventsSubmissionState::Accepted,
        )
    }

    pub(crate) fn mark_submission_rejected(
        &mut self,
        submission_id: &str,
    ) -> Result<AgentEventsSubmissionReceipt> {
        self.transition_prepared_submission(
            submission_id,
            "rejected",
            AgentEventsSubmissionState::Rejected,
        )
    }

    pub(crate) fn mark_submission_uncertain(
        &mut self,
        submission_id: &str,
    ) -> Result<AgentEventsSubmissionReceipt> {
        self.transition_prepared_submission(
            submission_id,
            "uncertain",
            AgentEventsSubmissionState::Uncertain,
        )
    }

    fn transition_prepared_submission(
        &mut self,
        submission_id: &str,
        stored_state: &str,
        expected_state: AgentEventsSubmissionState,
    ) -> Result<AgentEventsSubmissionReceipt> {
        self.db.execute(
            "UPDATE pending_submissions SET state=?,updated=?
             WHERE submission_id=? AND state='prepared'",
            params![stored_state, now(), submission_id],
        )?;
        let receipt = self.submission_receipt(submission_id)?;
        if receipt.state != expected_state {
            return Err(EventError("submission_state_conflict"));
        }
        Ok(receipt)
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
        for mut event in events {
            if let ReplyPayload::HumanMessage {
                text,
                submission_id,
                ..
            } = &mut event.payload
            {
                if submission_id.is_none() {
                    let kind = match source.kind {
                        TranscriptKind::Traex => "traex",
                        TranscriptKind::Pi => "pi",
                    };
                    let text_digest = super::codec::human_message_digest(text);
                    let matched: Option<String> = tx
                        .query_row(
                            "SELECT submission_id FROM pending_submissions WHERE terminal_id=? AND agent_kind=? AND session_id=? AND text_digest=? AND state IN ('prepared','accepted','uncertain') AND correlated=0 ORDER BY created,rowid LIMIT 1",
                            params![source.terminal_id, kind, source.session_id, text_digest],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if let Some(matched) = matched {
                        tx.execute(
                            "UPDATE pending_submissions SET correlated=1,updated=? WHERE submission_id=? AND correlated=0",
                            params![now(), &matched],
                        )?;
                        *submission_id = Some(matched);
                    }
                }
            }
            let key = format!("{}:{}", event.turn.as_deref().unwrap_or(""), event.key);
            let body = AgentReplyEvent {
                schema_version: 1,
                event_id: digest(format!("{}:{}:{key}", self.id, source.id).as_bytes()),
                cursor: String::new(),
                source_id: source.id.clone(),
                agent_kind: source.kind,
                session_id: source.session_id.clone(),
                turn_id: event.turn,
                occurred_at: event.time,
                payload: event.payload,
            };
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO events(source,key,body,created) VALUES (?,?,?,?)",
                params![source.id, key, serde_json::to_string(&body)?, now()],
            )?;
            if inserted > 0 {
                tx.execute(
                    "UPDATE sources SET latest=? WHERE id=?",
                    params![tx.last_insert_rowid(), source.id],
                )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::agent_events::{AgentEventsSubmissionState, TranscriptKind};

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
    fn reattaching_the_same_session_epoch_refreshes_ephemeral_source_details() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-session-epoch-{}-{}.db",
            std::process::id(),
            now()
        ));
        let source = RegisteredSource {
            id: "epoch-1".into(),
            terminal_id: "term".into(),
            kind: TranscriptKind::Traex,
            session_id: "session".into(),
            path: "/session.jsonl".into(),
            foreground_pid: 10,
            header_hash: "header".into(),
            file_identity: "file".into(),
        };
        let mut journal = Journal::open(&path).unwrap();
        journal.attach(&source, &Checkpoint::default()).unwrap();

        let mut after_restart = source.clone();
        after_restart.foreground_pid = 20;
        journal
            .attach(&after_restart, &Checkpoint::default())
            .unwrap();

        let active = journal.active().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].0.foreground_pid, 20);

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
        journal.mark_submission_accepted("prompt-1").unwrap();
        let mut next = before.clone();
        next.offset = 10;
        journal
            .commit(
                &source,
                &before,
                &next,
                vec![Decoded {
                    key: "human:user-1".into(),
                    turn: Some("turn-1".into()),
                    time: "2026-09-28T00:00:00Z".into(),
                    payload: ReplyPayload::HumanMessage {
                        message_id: "user-1".into(),
                        text: "continue".into(),
                        truncated: false,
                        submission_id: None,
                    },
                }],
            )
            .unwrap();

        let batch = journal.read("source", "start", 64).unwrap();
        assert!(matches!(
            &batch.events[0].payload,
            ReplyPayload::HumanMessage { submission_id: Some(id), .. } if id == "prompt-1"
        ));
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
    fn accepted_submission_receipt_survives_reopen_without_transcript_evidence() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-submission-receipt-{}-{}.db",
            std::process::id(),
            now()
        ));
        let mut journal = Journal::open(&path).unwrap();
        assert_eq!(
            journal
                .prepare_submission(
                    "prompt-receipt",
                    "term",
                    TranscriptKind::Traex,
                    "session",
                    "continue"
                )
                .unwrap(),
            SubmissionPrepareResult::Prepared
        );
        assert_eq!(
            journal.submission_receipt("prompt-receipt").unwrap().state,
            AgentEventsSubmissionState::Prepared
        );

        let accepted = journal.mark_submission_accepted("prompt-receipt").unwrap();
        assert_eq!(accepted.state, AgentEventsSubmissionState::Accepted);
        drop(journal);

        let mut reopened = Journal::open(&path).unwrap();
        assert_eq!(
            reopened.submission_receipt("prompt-receipt").unwrap().state,
            AgentEventsSubmissionState::Accepted
        );
        assert_eq!(
            reopened
                .prepare_submission(
                    "prompt-receipt",
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
    fn prepared_submission_becomes_uncertain_when_the_journal_reopens() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-submission-uncertain-{}-{}.db",
            std::process::id(),
            now()
        ));
        let mut journal = Journal::open(&path).unwrap();
        journal
            .prepare_submission(
                "prompt-uncertain",
                "term",
                TranscriptKind::Traex,
                "session",
                "continue",
            )
            .unwrap();
        drop(journal);

        let reopened = Journal::open(&path).unwrap();
        assert_eq!(
            reopened
                .submission_receipt("prompt-uncertain")
                .unwrap()
                .state,
            AgentEventsSubmissionState::Uncertain
        );
        drop(reopened);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn submission_receipts_record_rejected_and_uncertain_terminal_outcomes() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-submission-outcomes-{}-{}.db",
            std::process::id(),
            now()
        ));
        let mut journal = Journal::open(&path).unwrap();
        for submission_id in ["prompt-rejected", "prompt-uncertain"] {
            journal
                .prepare_submission(
                    submission_id,
                    "term",
                    TranscriptKind::Traex,
                    "session",
                    submission_id,
                )
                .unwrap();
        }

        assert_eq!(
            journal
                .mark_submission_rejected("prompt-rejected")
                .unwrap()
                .state,
            AgentEventsSubmissionState::Rejected
        );
        assert_eq!(
            journal
                .mark_submission_uncertain("prompt-uncertain")
                .unwrap()
                .state,
            AgentEventsSubmissionState::Uncertain
        );
        assert_eq!(
            journal
                .mark_submission_accepted("prompt-rejected")
                .unwrap_err()
                .0,
            "submission_state_conflict"
        );

        drop(journal);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn version_one_submission_rows_migrate_without_replay() {
        let path = std::env::temp_dir().join(format!(
            "herdr-journal-submission-migration-{}-{}.db",
            std::process::id(),
            now()
        ));
        let db = Connection::open(&path).unwrap();
        db.execute_batch(
            "CREATE TABLE pending_submissions (
                submission_id TEXT PRIMARY KEY,
                terminal_id TEXT NOT NULL,
                agent_kind TEXT NOT NULL,
                session_id TEXT NOT NULL,
                text_digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('prepared','consumed')),
                created INTEGER NOT NULL
            );
            INSERT INTO pending_submissions VALUES
                ('prepared','term','traex','session','a','prepared',1),
                ('consumed','term','traex','session','b','consumed',2);
            PRAGMA user_version=1;",
        )
        .unwrap();
        drop(db);

        let journal = Journal::open(&path).unwrap();
        assert_eq!(
            journal.submission_receipt("prepared").unwrap().state,
            AgentEventsSubmissionState::Uncertain
        );
        assert_eq!(
            journal.submission_receipt("consumed").unwrap().state,
            AgentEventsSubmissionState::Accepted
        );
        drop(journal);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
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

        drop(journal);
        std::fs::remove_file(path.with_extension("lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
