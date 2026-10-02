mod codec;
mod journal;
mod source;

pub(crate) use journal::{Journal, SubmissionPrepareResult};
pub(crate) use source::{Checkpoint, RegisteredSource, SourceReader};

use std::fmt;

#[derive(Debug)]
pub(crate) struct EventError(pub &'static str);
impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for EventError {}
impl EventError {
    pub(crate) fn is_skippable_record(&self) -> bool {
        matches!(
            self.0,
            "invalid_record"
                | "unsupported_record"
                | "unsupported_response_item"
                | "unsupported_history_mutation"
                | "unsupported_message_role"
                | "unsupported_stop_reason"
        )
    }
}
impl From<rusqlite::Error> for EventError {
    fn from(_: rusqlite::Error) -> Self {
        Self("journal_unavailable")
    }
}
impl From<std::io::Error> for EventError {
    fn from(_: std::io::Error) -> Self {
        Self("source_io_error")
    }
}
impl From<serde_json::Error> for EventError {
    fn from(_: serde_json::Error) -> Self {
        Self("invalid_record")
    }
}
pub(crate) type Result<T> = std::result::Result<T, EventError>;

pub(crate) fn digest(value: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(value))
}

pub(crate) fn timestamp() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

pub(crate) fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}
