use super::codec::Decoded;
use super::source::Checkpoint;
use super::{digest, now, EventError, RegisteredSource, Result};
use crate::api::schema::agent_events::{
    AgentEventSource, AgentEventsBatch, AgentReplyEvent, ReplyPayload,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

const MAX_SOURCES: i64 = 256;
const RETENTION_SECONDS: i64 = 7 * 24 * 3600;
const MAX_EVENT_BYTES: i64 = 192 * 1024 * 1024;

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
        if version > 1 {
            return Err(EventError("unsupported_journal_version"));
        }
        db.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA max_page_count=65536;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT OR IGNORE INTO metadata VALUES ('id', lower(hex(randomblob(16))));
            CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY, definition TEXT NOT NULL, checkpoint TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'active', error TEXT, floor INTEGER NOT NULL DEFAULT 0, latest INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL, created INTEGER NOT NULL, UNIQUE(source,key));
            CREATE INDEX IF NOT EXISTS events_source_seq ON events(source,seq);
            PRAGMA user_version=1;")?;
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
        for event in events {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::agent_events::TranscriptKind;
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
}
