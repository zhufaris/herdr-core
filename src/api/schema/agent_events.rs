use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptKind {
    Traex,
    Pi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentEventsSubmissionState {
    Prepared,
    Accepted,
    Rejected,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentEventsSubmissionReceipt {
    pub submission_id: String,
    pub terminal_id: String,
    pub agent_kind: TranscriptKind,
    pub session_id: String,
    pub state: AgentEventsSubmissionState,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentEventsSubmissionParams {
    pub submission_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionEventsOpenParams {
    pub pane_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionEventsReadParams {
    pub stream_id: String,
    #[serde(default = "start_cursor")]
    pub after: String,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionEventStream {
    pub stream_id: String,
    pub terminal_id: String,
    pub agent_kind: TranscriptKind,
    pub session_id: String,
    pub earliest_cursor: String,
    pub latest_cursor: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionEventsBatch {
    pub stream_id: String,
    pub events: Vec<AgentReplyEvent>,
    pub next_cursor: String,
    pub earliest_cursor: String,
    pub latest_cursor: String,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentEventsAttachFrom {
    Start,
    #[default]
    End,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentEventsAttachParams {
    pub pane_id: String,
    pub agent_kind: TranscriptKind,
    pub session_id: String,
    pub path: String,
    #[serde(default)]
    pub from: AgentEventsAttachFrom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentEventsReadParams {
    pub source_id: String,
    pub after: String,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 128))]
    pub limit: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentEventsTurnBoundary {
    Active,
    At,
    After,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentEventsLocateParams {
    pub source_id: String,
    pub boundary: AgentEventsTurnBoundary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentEventsTurnCursor {
    pub source_id: String,
    pub found: bool,
    pub after_cursor: String,
}
fn default_limit() -> u32 {
    64
}
fn start_cursor() -> String {
    "start".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentReplyEvent {
    pub schema_version: u32,
    pub event_id: String,
    pub cursor: String,
    pub source_id: String,
    pub agent_kind: TranscriptKind,
    pub session_id: String,
    pub turn_id: Option<String>,
    pub occurred_at: String,
    pub payload: ReplyPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplyPayload {
    TurnStarted,
    TurnCompleted,
    TurnAborted {
        reason: String,
    },
    HumanMessage {
        message_id: String,
        text: String,
        truncated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        submission_id: Option<String>,
    },
    Message {
        message_id: String,
        channel: String,
        text: String,
        truncated: bool,
    },
    ToolCall {
        call_id: String,
        name: String,
        arguments: String,
        truncated: bool,
    },
    ToolResult {
        call_id: String,
        text: String,
        is_error: bool,
        truncated: bool,
    },
    BranchChanged {
        parent_id: Option<String>,
    },
    SubmissionReceipt {
        submission_id: String,
        state: AgentEventsSubmissionState,
    },
    RuntimeStatusChanged {
        pane_id: String,
        status: super::AgentStatus,
    },
    RuntimeAgentChanged {
        pane_id: String,
        released: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_status: Option<super::AgentStatus>,
    },
    RuntimeEnded {
        pane_id: String,
        reason: String,
    },
    SourceError {
        code: String,
    },
    RecordSkipped {
        code: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentEventsBatch {
    pub events: Vec<AgentReplyEvent>,
    pub next_cursor: String,
    pub earliest_cursor: String,
    pub latest_cursor: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentEventSource {
    pub source_id: String,
    pub terminal_id: String,
    pub agent_kind: TranscriptKind,
    pub session_id: String,
    pub state: String,
    pub error: Option<String>,
    pub checkpoint_offset: u64,
    pub earliest_cursor: String,
    pub latest_cursor: String,
}
