use super::codec::{Decoded, Decoder};
use super::{digest, EventError, Result};
use crate::api::schema::agent_events::TranscriptKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub(crate) const MAX_RECORD: usize = 1024 * 1024;
const MAX_BATCH: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RegisteredSource {
    pub id: String,
    pub terminal_id: String,
    pub kind: TranscriptKind,
    pub session_id: String,
    pub path: PathBuf,
    pub foreground_pid: u32,
    pub header_hash: String,
    pub file_identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub offset: u64,
    pub anchor: String,
    pub decoder: Decoder,
    #[serde(default)]
    oversized_tail_scan: u64,
}
pub(crate) struct SourceReader;

impl SourceReader {
    pub(crate) fn register_path_session(
        terminal_id: String,
        kind: TranscriptKind,
        path: &Path,
        foreground_pid: u32,
        roots: &[PathBuf],
    ) -> Result<RegisteredSource> {
        let path = path.canonicalize()?;
        if !roots
            .iter()
            .filter_map(|candidate| candidate.canonicalize().ok())
            .any(|root| path.starts_with(root))
        {
            return Err(EventError("source_path_not_allowed"));
        }
        let mut file = open_regular(&path)?;
        let line = match read_line(&mut file)? {
            ReadLine::Complete(line) => line,
            ReadLine::Incomplete => return Err(EventError("incomplete_session_header")),
            ReadLine::OversizedComplete { .. } => return Err(EventError("record_too_large")),
        };
        let header: Value = serde_json::from_slice(&line)?;
        let session_id = match kind {
            TranscriptKind::Traex if header["type"] == "session_meta" => {
                header["payload"]["id"].as_str()
            }
            TranscriptKind::Pi if header["type"] == "session" => header["id"].as_str(),
            _ => None,
        }
        .filter(|value| !value.is_empty() && value.len() <= 512)
        .ok_or(EventError("session_identity_missing"))?
        .to_owned();
        Self::register(terminal_id, kind, session_id, &path, foreground_pid, roots)
    }

    pub(crate) fn checkpoint_at_end(source: &RegisteredSource) -> Result<Checkpoint> {
        let mut file = open_regular(&source.path)?;
        let length = file.metadata()?.len();
        let offset = if length == 0 {
            0
        } else {
            file.seek(SeekFrom::End(-1))?;
            let mut byte = [0_u8; 1];
            file.read_exact(&mut byte)?;
            if byte[0] == b'\n' {
                length
            } else {
                trailing_record_start(&mut file, length)?
            }
        };
        Ok(Checkpoint {
            offset,
            anchor: anchor(&mut file, offset)?,
            decoder: Decoder::awaiting_turn_boundary(),
            oversized_tail_scan: 0,
        })
    }

    pub fn register(
        terminal_id: String,
        kind: TranscriptKind,
        session_id: String,
        path: &Path,
        foreground_pid: u32,
        roots: &[PathBuf],
    ) -> Result<RegisteredSource> {
        if session_id.is_empty() || session_id.len() > 512 || !path.is_absolute() {
            return Err(EventError("invalid_source"));
        }
        let path = path.canonicalize()?;
        if !roots
            .iter()
            .filter_map(|p| p.canonicalize().ok())
            .any(|root| path.starts_with(root))
        {
            return Err(EventError("source_path_not_allowed"));
        }
        let mut file = open_regular(&path)?;
        let line = match read_line(&mut file)? {
            ReadLine::Complete(line) => line,
            ReadLine::Incomplete => return Err(EventError("incomplete_session_header")),
            ReadLine::OversizedComplete { .. } => return Err(EventError("record_too_large")),
        };
        let header: Value = serde_json::from_slice(&line)?;
        let actual = match kind {
            TranscriptKind::Traex if header["type"] == "session_meta" => {
                header["payload"]["id"].as_str()
            }
            TranscriptKind::Pi if header["type"] == "session" => header["id"].as_str(),
            _ => None,
        };
        if actual != Some(session_id.as_str()) {
            return Err(EventError("session_identity_mismatch"));
        }
        let header_hash = digest(&line);
        let file_identity = crate::platform::agent_event_file_identity(&file)?;
        let id = digest(
            serde_json::to_string(&(
                &terminal_id,
                kind,
                &session_id,
                &header_hash,
                &file_identity,
            ))?
            .as_bytes(),
        );
        Ok(RegisteredSource {
            id,
            terminal_id,
            kind,
            session_id,
            path,
            foreground_pid,
            header_hash,
            file_identity,
        })
    }

    pub(crate) fn read(
        source: &RegisteredSource,
        checkpoint: &Checkpoint,
    ) -> Result<(Checkpoint, Vec<Decoded>)> {
        let mut file = open_regular(&source.path)?;
        let header = match read_line(&mut file)? {
            ReadLine::Complete(line) => line,
            ReadLine::Incomplete => return Err(EventError("source_truncated")),
            ReadLine::OversizedComplete { .. } => return Err(EventError("source_replaced")),
        };
        if crate::platform::agent_event_file_identity(&file)? != source.file_identity
            || digest(&header) != source.header_hash
            || file.metadata()?.len() < checkpoint.offset
        {
            return Err(EventError("source_replaced"));
        }
        if checkpoint.offset > 0 && anchor(&mut file, checkpoint.offset)? != checkpoint.anchor {
            return Err(EventError("source_replaced"));
        }
        file.seek(SeekFrom::Start(checkpoint.offset))?;
        let mut next = checkpoint.clone();
        let mut events = Vec::new();
        let mut budget = 0;
        if next.oversized_tail_scan > next.offset {
            let start = next.offset;
            file.seek(SeekFrom::Start(next.oversized_tail_scan))?;
            match scan_to_newline(&mut file)? {
                Some(end) => {
                    next.offset = end;
                    next.oversized_tail_scan = 0;
                    events.push(next.decoder.record_skipped(
                        format!("record-skipped:{start}:{end}"),
                        "record_too_large",
                    ));
                    budget += MAX_RECORD;
                }
                None => {
                    next.oversized_tail_scan = file.stream_position()?;
                    return Ok((next, events));
                }
            }
        }
        for _ in 0..MAX_BATCH {
            let start = next.offset;
            match read_line(&mut file)? {
                ReadLine::Incomplete => {
                    let end = file.metadata()?.len();
                    if end.saturating_sub(start) > MAX_RECORD as u64 {
                        next.oversized_tail_scan = end;
                    }
                    break;
                }
                ReadLine::Complete(line) => {
                    budget += line.len();
                    let value: Value = serde_json::from_slice(&line)?;
                    let mut candidate = next.decoder.clone();
                    match candidate.decode(source.kind, &value) {
                        Ok(decoded) => {
                            next.decoder = candidate;
                            events.extend(decoded);
                        }
                        Err(error) if error.is_skippable_record() => {
                            events.push(next.decoder.record_skipped(
                                format!("record-skipped:{start}:{}", start + line.len() as u64),
                                error.0,
                            ));
                        }
                        Err(error) => return Err(error),
                    }
                    next.offset += line.len() as u64;
                }
                ReadLine::OversizedComplete { length } => {
                    budget += MAX_RECORD;
                    next.offset += length;
                    events.push(next.decoder.record_skipped(
                        format!("record-skipped:{start}:{}", next.offset),
                        "record_too_large",
                    ));
                }
            }
            if events.len() >= MAX_BATCH || budget >= MAX_RECORD {
                break;
            }
        }
        if next.offset != checkpoint.offset {
            next.anchor = anchor(&mut file, next.offset)?;
        }
        Ok((next, events))
    }
}

fn scan_to_newline(file: &mut File) -> Result<Option<u64>> {
    let mut chunk = [0_u8; 8192];
    loop {
        let start = file.stream_position()?;
        let read = file.read(&mut chunk)?;
        if read == 0 {
            return Ok(None);
        }
        if let Some(index) = chunk[..read].iter().position(|byte| *byte == b'\n') {
            let end = start + index as u64 + 1;
            file.seek(SeekFrom::Start(end))?;
            return Ok(Some(end));
        }
    }
}

fn trailing_record_start(file: &mut File, length: u64) -> Result<u64> {
    let mut end = length;
    let mut chunk = [0_u8; 8192];
    while end > 0 {
        let start = end.saturating_sub(chunk.len() as u64);
        let size = (end - start) as usize;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut chunk[..size])?;
        if let Some(index) = chunk[..size].iter().rposition(|byte| *byte == b'\n') {
            return Ok(start + index as u64 + 1);
        }
        end = start;
    }
    Ok(0)
}

fn open_regular(path: &Path) -> Result<File> {
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_file() {
        return Err(EventError("source_not_regular_file"));
    }
    let file = crate::platform::open_agent_event_source(path)?;
    if !file.metadata()?.is_file() {
        return Err(EventError("source_not_regular_file"));
    }
    Ok(file)
}
enum ReadLine {
    Complete(Vec<u8>),
    Incomplete,
    OversizedComplete { length: u64 },
}

fn read_line(file: &mut File) -> Result<ReadLine> {
    let start = file.stream_position()?;
    let mut line = Vec::with_capacity(8192);
    let mut length = 0_u64;
    let mut chunk = [0_u8; 8192];
    loop {
        let read = file.read(&mut chunk)?;
        if read == 0 {
            file.seek(SeekFrom::Start(start))?;
            return Ok(ReadLine::Incomplete);
        }
        let consumed = chunk[..read]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(read, |index| index + 1);
        length += consumed as u64;
        if line.len() <= MAX_RECORD && line.len() + consumed <= MAX_RECORD {
            line.extend_from_slice(&chunk[..consumed]);
        } else {
            line.clear();
        }
        if chunk[consumed - 1] == b'\n' {
            file.seek(SeekFrom::Start(start + length))?;
            return Ok(if length > MAX_RECORD as u64 {
                ReadLine::OversizedComplete { length }
            } else {
                ReadLine::Complete(line)
            });
        }
    }
}
fn anchor(file: &mut File, offset: u64) -> Result<String> {
    let len = offset.min(4096);
    file.seek(SeekFrom::Start(offset - len))?;
    let mut bytes = vec![0; len as usize];
    file.read_exact(&mut bytes)?;
    Ok(digest(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::agent_events::ReplyPayload;
    use std::io::Write;

    #[test]
    fn session_epoch_survives_core_restart_and_changes_when_the_source_is_replaced() {
        let root = std::env::temp_dir().join(format!(
            "herdr-source-epoch-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("session.jsonl");
        std::fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session-1\"}}\n",
        )
        .unwrap();

        let before = SourceReader::register(
            "terminal-1".into(),
            TranscriptKind::Traex,
            "session-1".into(),
            &path,
            10,
            std::slice::from_ref(&root),
        )
        .unwrap();
        let after_core_restart = SourceReader::register(
            "terminal-1".into(),
            TranscriptKind::Traex,
            "session-1".into(),
            &path,
            20,
            std::slice::from_ref(&root),
        )
        .unwrap();
        assert_eq!(before.id, after_core_restart.id);

        let replacement = root.join("replacement.jsonl");
        std::fs::write(
            &replacement,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session-1\"}}\n",
        )
        .unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        let after_source_replacement = SourceReader::register(
            "terminal-1".into(),
            TranscriptKind::Traex,
            "session-1".into(),
            &path,
            20,
            std::slice::from_ref(&root),
        )
        .unwrap();
        assert_ne!(before.id, after_source_replacement.id);

        let next_session_path = root.join("next-session.jsonl");
        std::fs::write(
            &next_session_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session-2\"}}\n",
        )
        .unwrap();
        let next_session = SourceReader::register(
            "terminal-1".into(),
            TranscriptKind::Traex,
            "session-2".into(),
            &next_session_path,
            20,
            std::slice::from_ref(&root),
        )
        .unwrap();
        assert_ne!(after_source_replacement.id, next_session.id);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn incremental_capture_scales_with_one_and_fifteen_sources() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-scale-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for count in [1, 15] {
            let start = std::time::Instant::now();
            let mut captured = 0;
            for index in 0..count {
                let path = dir.join(format!("{count}-{index}.jsonl"));
                let mut data =
                    String::from("{\"type\":\"session_meta\",\"payload\":{\"id\":\"test\"}}\n");
                for turn in 0..100 {
                    data.push_str(&format!("{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\",\"turn_id\":\"t{turn}\"}}}}\n"));
                    data.push_str(&format!("{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_complete\",\"turn_id\":\"t{turn}\",\"last_agent_message\":\"answer\"}}}}\n"));
                }
                std::fs::write(&path, &data).unwrap();
                let source = SourceReader::register(
                    format!("term{index}"),
                    TranscriptKind::Traex,
                    "test".into(),
                    &path,
                    1,
                    std::slice::from_ref(&dir),
                )
                .unwrap();
                let mut cp = Checkpoint::default();
                while cp.offset < data.len() as u64 {
                    let (next, events) = SourceReader::read(&source, &cp).unwrap();
                    assert!(next.offset > cp.offset);
                    assert!(events.len() <= MAX_BATCH + 1);
                    captured += events.len();
                    cp = next;
                }
                assert!(SourceReader::read(&source, &cp).unwrap().1.is_empty());
            }
            assert_eq!(captured, count * 300);
            eprintln!(
                "agent-events sources={count} events={captured} elapsed_ms={}",
                start.elapsed().as_millis()
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn source_boundaries_reject_wrong_root_and_session() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-bounds-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let header = "{\"type\":\"session\",\"id\":\"test\"}\n";
        std::fs::write(&path, header).unwrap();
        assert_eq!(
            SourceReader::register("t".into(), TranscriptKind::Pi, "test".into(), &path, 1, &[])
                .unwrap_err()
                .0,
            "source_path_not_allowed"
        );
        assert_eq!(
            SourceReader::register(
                "t".into(),
                TranscriptKind::Pi,
                "other".into(),
                &path,
                1,
                std::slice::from_ref(&dir)
            )
            .unwrap_err()
            .0,
            "session_identity_mismatch"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn skips_a_complete_oversized_record_and_reads_the_next_record() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-oversized-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let header = "{\"type\":\"session\",\"id\":\"test\"}\n";
        let next = "{\"type\":\"message\",\"id\":\"u\",\"parentId\":null,\"message\":{\"role\":\"user\"}}\n";
        let mut data = header.as_bytes().to_vec();
        data.extend(vec![b'x'; MAX_RECORD + 1]);
        data.push(b'\n');
        data.extend(next.as_bytes());
        std::fs::write(&path, &data).unwrap();
        let source = SourceReader::register(
            "t".into(),
            TranscriptKind::Pi,
            "test".into(),
            &path,
            1,
            std::slice::from_ref(&dir),
        )
        .unwrap();

        let (checkpoint, events) = SourceReader::read(&source, &Checkpoint::default()).unwrap();

        assert_eq!(checkpoint.offset, (header.len() + MAX_RECORD + 2) as u64);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].payload,
            ReplyPayload::RecordSkipped {
                code: "record_too_large".into()
            }
        );
        let (checkpoint, events) = SourceReader::read(&source, &checkpoint).unwrap();
        assert_eq!(checkpoint.offset, data.len() as u64);
        assert_eq!(events.len(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn incomplete_oversized_tail_does_not_advance_checkpoint() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-oversized-tail-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let header = "{\"type\":\"session\",\"id\":\"test\"}\n";
        let mut data = header.as_bytes().to_vec();
        data.extend(vec![b'x'; MAX_RECORD + 1]);
        std::fs::write(&path, &data).unwrap();
        let source = SourceReader::register(
            "t".into(),
            TranscriptKind::Pi,
            "test".into(),
            &path,
            1,
            std::slice::from_ref(&dir),
        )
        .unwrap();
        let (header_checkpoint, _) = SourceReader::read(&source, &Checkpoint::default()).unwrap();

        let (checkpoint, events) = SourceReader::read(&source, &header_checkpoint).unwrap();

        assert_eq!(checkpoint.offset, header_checkpoint.offset);
        assert_eq!(checkpoint.oversized_tail_scan, data.len() as u64);
        assert!(events.is_empty());

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"more").unwrap();
        let (continued, events) = SourceReader::read(&source, &checkpoint).unwrap();
        assert_eq!(continued.offset, header_checkpoint.offset);
        assert_eq!(continued.oversized_tail_scan, data.len() as u64 + 4);
        assert!(events.is_empty());

        file.write_all(b"\n{\"type\":\"message\",\"id\":\"u\",\"parentId\":null,\"message\":{\"role\":\"user\"}}\n").unwrap();
        let (completed, events) = SourceReader::read(&source, &continued).unwrap();
        assert!(completed.offset > continued.offset);
        assert_eq!(completed.oversized_tail_scan, 0);
        assert!(matches!(
            events[0].payload,
            ReplyPayload::RecordSkipped { .. }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn checkpoint_at_end_waits_at_the_start_of_an_oversized_partial_record() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-oversized-end-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let header = "{\"type\":\"session\",\"id\":\"test\"}\n";
        let mut data = header.as_bytes().to_vec();
        data.extend(vec![b'x'; MAX_RECORD + 1]);
        std::fs::write(&path, &data).unwrap();
        let source = SourceReader::register(
            "term".into(),
            TranscriptKind::Pi,
            "test".into(),
            &path,
            1,
            std::slice::from_ref(&dir),
        )
        .unwrap();

        let checkpoint = SourceReader::checkpoint_at_end(&source).unwrap();

        assert_eq!(checkpoint.offset, header.len() as u64);
        let next = "{\"type\":\"message\",\"id\":\"u\",\"parentId\":null,\"message\":{\"role\":\"user\"}}\n";
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"\n").unwrap();
        file.write_all(next.as_bytes()).unwrap();
        let (after_skip, events) = SourceReader::read(&source, &checkpoint).unwrap();
        assert!(matches!(
            events[0].payload,
            ReplyPayload::RecordSkipped { .. }
        ));
        let (after_message, events) = SourceReader::read(&source, &after_skip).unwrap();
        assert_eq!(
            after_message.offset,
            data.len() as u64 + 1 + next.len() as u64
        );
        assert!(matches!(events[0].payload, ReplyPayload::TurnStarted));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn skips_unknown_records_without_poisoning_decoder_state() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-unknown-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let data = [
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"test\"}}",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn\"}}",
            "{\"type\":\"future_record\",\"payload\":{}}",
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"future_item\"}}",
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"id\":\"answer\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"still readable\"}]}}",
        ].join("\n") + "\n";
        std::fs::write(&path, &data).unwrap();
        let source = SourceReader::register(
            "term".into(),
            TranscriptKind::Traex,
            "test".into(),
            &path,
            1,
            std::slice::from_ref(&dir),
        )
        .unwrap();

        let (checkpoint, events) = SourceReader::read(&source, &Checkpoint::default()).unwrap();

        assert_eq!(checkpoint.offset, data.len() as u64);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.payload, ReplyPayload::RecordSkipped { .. }))
                .count(),
            2
        );
        assert!(events.iter().any(|event| {
            matches!(&event.payload, ReplyPayload::Message { text, .. } if text == "still readable")
        }));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn skips_invalid_traex_records_without_failing_the_source() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-invalid-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let data = [
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"test\"}}",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn\"}}",
            "{\"type\":\"event_msg\",\"payload\":{}}",
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"id\":\"answer\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"still readable\"}]}}",
        ].join("\n") + "\n";
        std::fs::write(&path, &data).unwrap();
        let source = SourceReader::register(
            "term".into(),
            TranscriptKind::Traex,
            "test".into(),
            &path,
            1,
            std::slice::from_ref(&dir),
        )
        .unwrap();

        let (checkpoint, events) = SourceReader::read(&source, &Checkpoint::default()).unwrap();

        assert_eq!(checkpoint.offset, data.len() as u64);
        assert!(events.iter().any(|event| {
            matches!(&event.payload, ReplyPayload::RecordSkipped { code } if code == "invalid_record")
        }));
        assert!(events.iter().any(|event| {
            matches!(&event.payload, ReplyPayload::Message { text, .. } if text == "still readable")
        }));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn partial_lines_and_same_size_rewrite_do_not_advance_checkpoint() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let header = "{\"type\":\"session\",\"id\":\"test\"}\n";
        std::fs::write(&path, format!("{header}{{\"type\":")).unwrap();
        let source = SourceReader::register(
            "term".into(),
            TranscriptKind::Pi,
            "test".into(),
            &path,
            1,
            std::slice::from_ref(&dir),
        )
        .unwrap();
        let (cp, events) = SourceReader::read(&source, &Checkpoint::default()).unwrap();
        assert!(events.is_empty());
        assert_eq!(cp.offset, header.len() as u64);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(
            b"\"message\",\"id\":\"u\",\"parentId\":null,\"message\":{\"role\":\"user\"}}\n",
        )
        .unwrap();
        let (cp, events) = SourceReader::read(&source, &cp).unwrap();
        assert_eq!(events.len(), 1);
        let data = std::fs::read_to_string(&path)
            .unwrap()
            .replace("\"u\"", "\"v\"");
        std::fs::write(&path, data).unwrap();
        assert_eq!(
            SourceReader::read(&source, &cp).unwrap_err().0,
            "source_replaced"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn checkpoint_at_end_skips_history_and_stops_before_a_partial_line() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-source-tail-{}-{}",
            std::process::id(),
            super::super::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let complete = "{\"type\":\"session\",\"id\":\"test\"}\n{\"type\":\"message\"}\n";
        std::fs::write(&path, format!("{complete}{{\"type\":")).unwrap();
        let source = SourceReader::register(
            "term".into(),
            TranscriptKind::Pi,
            "test".into(),
            &path,
            1,
            std::slice::from_ref(&dir),
        )
        .unwrap();
        let checkpoint = SourceReader::checkpoint_at_end(&source).unwrap();
        assert_eq!(checkpoint.offset, complete.len() as u64);
        assert!(!checkpoint.anchor.is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
