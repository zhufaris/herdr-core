use super::{digest, EventError, Result};
use crate::api::schema::agent_events::{ReplyPayload, TranscriptKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::OnceLock;

const MAX_TEXT: usize = 16 * 1024;
const MAX_NODES: usize = 16_384;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingHumanMessage {
    message_id: String,
    text: String,
    truncated: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Decoder {
    pub turn: Option<String>,
    #[serde(default)]
    awaiting_turn_boundary: bool,
    #[serde(default)]
    traex_turn_active: bool,
    #[serde(default)]
    pending_human_message: Option<PendingHumanMessage>,
    last_node: Option<String>,
    nodes: BTreeMap<String, Option<String>>,
    last_message: Option<(String, String)>,
}
#[derive(Debug)]
pub(crate) struct Decoded {
    pub key: String,
    pub turn: Option<String>,
    pub time: String,
    pub payload: ReplyPayload,
}

fn required<'a>(v: &'a Value, field: &str) -> Result<&'a str> {
    v[field]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 512)
        .ok_or(EventError("invalid_record"))
}
fn text(v: &Value) -> String {
    if let Some(s) = v.as_str() {
        return s.to_owned();
    }
    v.as_array()
        .map(|items| {
            items
                .iter()
                .filter(|v| {
                    matches!(
                        v["type"].as_str(),
                        Some("text" | "input_text" | "output_text")
                    )
                })
                .filter_map(|v| v["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n\n")
        })
        .unwrap_or_default()
}
fn bounded(s: &str) -> (String, bool) {
    let safe = redact(s);
    let end = safe.floor_char_boundary(MAX_TEXT.min(safe.len()));
    let truncated = end < safe.len();
    let mut value = safe[..end].to_owned();
    if truncated {
        value.push_str("\n[truncated]");
    }
    (value, truncated)
}

pub(crate) fn human_message_digest(value: &str) -> String {
    digest(bounded(value).0.as_bytes())
}

fn redact(value: &str) -> String {
    static PATTERNS: OnceLock<Vec<(regex::Regex, &'static str)>> = OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| vec![
        (regex::Regex::new(r"(?is)-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----").expect("private key regex"), "[REDACTED PRIVATE KEY]"),
        (regex::Regex::new(r#"(?i)(["']?(?:access[_-]?token|api[_-]?key|app[_-]?secret|client[_-]?secret|private[_-]?key|token|secret|password|credential)["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;&}]+)"#).expect("secret assignment regex"), "$1[REDACTED]"),
        (regex::Regex::new(r"(?i)(authorization\s*[:=]\s*(?:(?:bearer|basic)\s+)?)[^\r\n,;]+").expect("authorization regex"), "$1[REDACTED]"),
        (regex::Regex::new(r"(?i)(bearer\s+)[a-z0-9._~-]+").expect("bearer regex"), "$1[REDACTED]"),
        (regex::Regex::new(r"(?i)[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+").expect("email regex"), "[REDACTED EMAIL]"),
    ]);
    patterns
        .iter()
        .fold(value.to_owned(), |current, (pattern, replacement)| {
            pattern.replace_all(&current, *replacement).into_owned()
        })
}

impl Decoder {
    pub(crate) fn awaiting_turn_boundary() -> Self {
        Self {
            awaiting_turn_boundary: true,
            ..Self::default()
        }
    }

    pub(crate) fn record_skipped(&self, key: String, code: &str) -> Decoded {
        Decoded {
            key,
            turn: self.turn.clone(),
            time: super::timestamp(),
            payload: ReplyPayload::RecordSkipped { code: code.into() },
        }
    }

    pub fn decode(&mut self, kind: TranscriptKind, v: &Value) -> Result<Vec<Decoded>> {
        let mut payloads: Vec<(String, ReplyPayload)> = Vec::new();
        match kind {
            TranscriptKind::Traex => self.traex(v, &mut payloads)?,
            TranscriptKind::Pi => self.pi(v, &mut payloads)?,
        }
        if payloads.len() > 256 {
            return Err(EventError("record_event_limit"));
        }
        let time = match v.get("timestamp") {
            Some(Value::String(value)) => {
                time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
                    .map_err(|_| EventError("invalid_timestamp"))?;
                value.clone()
            }
            None => super::timestamp(),
            _ => return Err(EventError("invalid_timestamp")),
        };
        Ok(payloads
            .into_iter()
            .map(|(key, payload)| Decoded {
                key,
                turn: self.turn.clone(),
                time: time.clone(),
                payload,
            })
            .collect())
    }

    fn message(
        &mut self,
        id: &str,
        channel: &str,
        body: &str,
        out: &mut Vec<(String, ReplyPayload)>,
    ) {
        if body.is_empty() {
            return;
        }
        let (body, truncated) = bounded(body);
        self.last_message = Some((id.to_owned(), digest(body.as_bytes())));
        out.push((
            format!("message:{id}:{}", digest(body.as_bytes())),
            ReplyPayload::Message {
                message_id: id.to_owned(),
                channel: channel.to_owned(),
                text: body,
                truncated,
            },
        ));
    }

    fn human_message(id: &str, body: &str, out: &mut Vec<(String, ReplyPayload)>) {
        if body.is_empty() {
            return;
        }
        let (text, truncated) = bounded(body);
        out.push((
            format!("human:{id}:{}", digest(text.as_bytes())),
            ReplyPayload::HumanMessage {
                message_id: id.to_owned(),
                text,
                truncated,
                submission_id: None,
            },
        ));
    }

    fn traex(&mut self, v: &Value, out: &mut Vec<(String, ReplyPayload)>) -> Result<()> {
        let p = &v["payload"];
        match v["type"].as_str() {
            Some("event_msg") => match p["type"].as_str() {
                Some("task_started") => {
                    let id = required(p, "turn_id")?.to_owned();
                    self.turn = Some(id.clone());
                    self.awaiting_turn_boundary = false;
                    self.traex_turn_active = true;
                    self.last_message = None;
                    out.push((format!("start:{id}"), ReplyPayload::TurnStarted));
                    if let Some(message) = self.pending_human_message.take() {
                        out.push((
                            format!(
                                "human:{}:{}",
                                message.message_id,
                                digest(message.text.as_bytes())
                            ),
                            ReplyPayload::HumanMessage {
                                message_id: message.message_id,
                                text: message.text,
                                truncated: message.truncated,
                                submission_id: None,
                            },
                        ));
                    }
                }
                Some("task_complete" | "turn_aborted") => {
                    let id = required(p, "turn_id")?;
                    if self.awaiting_turn_boundary {
                        return Ok(());
                    }
                    if self.turn.as_deref() != Some(id) {
                        return Err(EventError("turn_identity_mismatch"));
                    }
                    if p["type"] == "task_complete" {
                        if let Some(body) =
                            p["last_agent_message"].as_str().filter(|s| !s.is_empty())
                        {
                            let hash = digest(bounded(body).0.as_bytes());
                            let message_id = self
                                .last_message
                                .as_ref()
                                .filter(|(_, h)| h == &hash)
                                .map(|(id, _)| id.clone())
                                .unwrap_or_else(|| format!("final:{id}"));
                            let (body, truncated) = bounded(body);
                            out.push((
                                format!("final:{id}"),
                                ReplyPayload::Message {
                                    message_id,
                                    channel: "final".into(),
                                    text: body,
                                    truncated,
                                },
                            ));
                        }
                        out.push((format!("complete:{id}"), ReplyPayload::TurnCompleted));
                    } else {
                        out.push((
                            format!("abort:{id}"),
                            ReplyPayload::TurnAborted {
                                reason: bounded(p["reason"].as_str().unwrap_or("aborted")).0,
                            },
                        ));
                    }
                    self.traex_turn_active = false;
                }
                Some(_) => {}
                None => return Err(EventError("invalid_record")),
            },
            Some("history_mutation") => match p["operation"].as_str() {
                Some("append") => {
                    for item in p["items"].as_array().ok_or(EventError("invalid_record"))? {
                        self.traex_item(item, out)?;
                    }
                }
                // These mutate TraeX's retained conversation context; their
                // contents are historical replacements, not new reply events.
                Some("replace" | "rollback") => {
                    self.last_message = None;
                }
                _ => return Err(EventError("unsupported_history_mutation")),
            },
            Some("response_item") => self.traex_item(p, out)?,
            // Runtime context and agent-to-agent coordination records are not
            // user-visible assistant output. Keep them out of the reply stream.
            Some(
                "session_meta"
                | "turn_context"
                | "compacted"
                | "world_state"
                | "inter_agent_communication",
            ) => {}
            _ => return Err(EventError("unsupported_record")),
        }
        Ok(())
    }

    fn traex_item(&mut self, item: &Value, out: &mut Vec<(String, ReplyPayload)>) -> Result<()> {
        match item["type"].as_str() {
            Some("message") if item["role"] == "user" => {
                if self.awaiting_turn_boundary {
                    return Ok(());
                }
                let message_id = required(item, "id")?;
                let body = text(&item["content"]);
                if body.is_empty() {
                    return Ok(());
                }
                if self.traex_turn_active {
                    Self::human_message(message_id, &body, out);
                } else {
                    let (text, truncated) = bounded(&body);
                    self.pending_human_message = Some(PendingHumanMessage {
                        message_id: message_id.to_owned(),
                        text,
                        truncated,
                    });
                }
            }
            Some("message") if item["role"] == "assistant" => {
                if matches!(item["channel"].as_str(), Some("analysis" | "reasoning")) {
                    return Ok(());
                }
                if self.turn.is_none() {
                    if self.awaiting_turn_boundary {
                        return Ok(());
                    }
                    return Err(EventError("missing_turn_identity"));
                }
                let id = required(item, "id")?;
                let channel = if item["channel"] == "final" {
                    "final"
                } else {
                    "commentary"
                };
                self.message(id, channel, &text(&item["content"]), out);
            }
            Some("function_call" | "custom_tool_call") => {
                if self.turn.is_none() {
                    if self.awaiting_turn_boundary {
                        return Ok(());
                    }
                    return Err(EventError("missing_turn_identity"));
                }
                let call = required(item, "call_id")?;
                let raw = item["arguments"]
                    .as_str()
                    .or_else(|| item["input"].as_str())
                    .ok_or(EventError("invalid_record"))?;
                let (arguments, truncated) = bounded(raw);
                out.push((
                    format!("call:{call}"),
                    ReplyPayload::ToolCall {
                        call_id: call.into(),
                        name: required(item, "name")?.into(),
                        arguments,
                        truncated,
                    },
                ));
            }
            Some("function_call_output" | "custom_tool_call_output") => {
                if self.turn.is_none() {
                    if self.awaiting_turn_boundary {
                        return Ok(());
                    }
                    return Err(EventError("missing_turn_identity"));
                }
                let call = required(item, "call_id")?;
                let raw = if let Some(s) = item["output"].as_str() {
                    s.to_owned()
                } else if item["output"].is_array() {
                    text(&item["output"])
                } else {
                    serde_json::to_string(&item["output"])?
                };
                let (body, truncated) = bounded(&raw);
                out.push((
                    format!("result:{call}"),
                    ReplyPayload::ToolResult {
                        call_id: call.into(),
                        text: body,
                        is_error: false,
                        truncated,
                    },
                ));
            }
            Some("message" | "reasoning" | "ghost_snapshot" | "trae_extra_info") => {}
            _ => return Err(EventError("unsupported_response_item")),
        }
        Ok(())
    }

    fn pi(&mut self, v: &Value, out: &mut Vec<(String, ReplyPayload)>) -> Result<()> {
        if v["type"] == "session" {
            return Ok(());
        }
        let id = required(v, "id")?.to_owned();
        let parent = match &v["parentId"] {
            Value::Null => None,
            Value::String(s) if !s.is_empty() && s.len() <= 512 => Some(s.clone()),
            _ => return Err(EventError("invalid_record")),
        };
        if self.nodes.contains_key(&id) {
            return Ok(());
        }
        if self.nodes.len() >= MAX_NODES {
            return Err(EventError("source_node_limit"));
        }
        if parent != self.last_node {
            self.turn = match &parent {
                Some(p) => self
                    .nodes
                    .get(p)
                    .cloned()
                    .ok_or(EventError("missing_branch_parent"))?,
                None => None,
            };
            self.last_message = None;
            out.push((
                format!("branch:{id}"),
                ReplyPayload::BranchChanged { parent_id: parent },
            ));
        }
        match v["type"].as_str() {
            Some("message") => {
                let m = &v["message"];
                match m["role"].as_str() {
                    Some("user") => {
                        self.turn = Some(id.clone());
                        out.push((format!("start:{id}"), ReplyPayload::TurnStarted));
                        Self::human_message(&id, &text(&m["content"]), out);
                    }
                    Some("assistant") => {
                        if self.turn.is_none() {
                            return Err(EventError("missing_turn_identity"));
                        }
                        self.message(
                            &id,
                            if m["stopReason"] == "stop" {
                                "final"
                            } else {
                                "commentary"
                            },
                            &text(&m["content"]),
                            out,
                        );
                        for block in m["content"]
                            .as_array()
                            .ok_or(EventError("invalid_record"))?
                        {
                            if block["type"] == "toolCall" {
                                let call = required(block, "id")?;
                                let (arguments, truncated) =
                                    bounded(&serde_json::to_string(&block["arguments"])?);
                                out.push((
                                    format!("call:{call}"),
                                    ReplyPayload::ToolCall {
                                        call_id: call.into(),
                                        name: required(block, "name")?.into(),
                                        arguments,
                                        truncated,
                                    },
                                ));
                            }
                        }
                        match m["stopReason"].as_str() {
                            Some("stop") => {
                                out.push((format!("complete:{id}"), ReplyPayload::TurnCompleted))
                            }
                            Some("aborted" | "error" | "length") => out.push((
                                format!("abort:{id}"),
                                ReplyPayload::TurnAborted {
                                    reason: m["stopReason"].as_str().unwrap_or("aborted").into(),
                                },
                            )),
                            Some("toolUse") | None => {}
                            _ => return Err(EventError("unsupported_stop_reason")),
                        }
                    }
                    Some("toolResult") => {
                        let call = required(m, "toolCallId")?;
                        let (body, truncated) = bounded(&text(&m["content"]));
                        out.push((
                            format!("result:{id}"),
                            ReplyPayload::ToolResult {
                                call_id: call.into(),
                                text: body,
                                is_error: m["isError"].as_bool().unwrap_or(false),
                                truncated,
                            },
                        ));
                    }
                    Some("system") => {}
                    _ => return Err(EventError("unsupported_message_role")),
                }
            }
            Some(
                "model_change"
                | "thinking_level_change"
                | "custom"
                | "custom_message"
                | "label"
                | "session_info"
                | "compaction"
                | "branch_summary",
            ) => {}
            _ => return Err(EventError("unsupported_record")),
        }
        self.nodes.insert(id.clone(), self.turn.clone());
        self.last_node = Some(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn traex_projects_user_message_at_the_native_turn_boundary() {
        let mut decoder = Decoder::default();
        let before_start = decoder
            .decode(
                TranscriptKind::Traex,
                &json!({
                    "type":"response_item",
                    "payload":{
                        "type":"message",
                        "id":"user-1",
                        "role":"user",
                        "content":[{"type":"input_text","text":"continue"}]
                    }
                }),
            )
            .unwrap();
        assert!(before_start.is_empty());

        let events = decoder
            .decode(
                TranscriptKind::Traex,
                &json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
            )
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].payload, ReplyPayload::TurnStarted));
        assert!(matches!(
            &events[1].payload,
            ReplyPayload::HumanMessage {
                message_id,
                text,
                truncated: false,
                submission_id: None,
            } if message_id == "user-1" && text == "continue"
        ));
        assert!(events
            .iter()
            .all(|event| event.turn.as_deref() == Some("turn-1")));
    }

    #[test]
    fn pi_projects_user_message_with_its_native_turn_identity() {
        let mut decoder = Decoder::default();
        let events = decoder
            .decode(
                TranscriptKind::Pi,
                &json!({
                    "type":"message",
                    "id":"user-1",
                    "parentId":null,
                    "message":{
                        "role":"user",
                        "content":[{"type":"text","text":"continue"}]
                    }
                }),
            )
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].payload, ReplyPayload::TurnStarted));
        assert!(matches!(
            &events[1].payload,
            ReplyPayload::HumanMessage {
                message_id,
                text,
                truncated: false,
                submission_id: None,
            } if message_id == "user-1" && text == "continue"
        ));
        assert!(events
            .iter()
            .all(|event| event.turn.as_deref() == Some("user-1")));
    }
    #[test]
    fn traex_filters_reasoning_and_updates_final_message_identity() {
        let mut d = Decoder::default();
        d.decode(
            TranscriptKind::Traex,
            &json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"t"}}),
        )
        .unwrap();
        let items = json!({"type":"history_mutation","payload":{"operation":"append","items":[{"type":"message","id":"secret","role":"assistant","channel":"analysis","content":[{"type":"output_text","text":"private"}]},{"type":"message","id":"m","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}});
        let events = d.decode(TranscriptKind::Traex, &items).unwrap();
        assert_eq!(events.len(), 1);
        let final_events = d.decode(TranscriptKind::Traex, &json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"t","last_agent_message":"answer"}})).unwrap();
        assert!(
            matches!(&final_events[0].payload, ReplyPayload::Message { message_id, channel, .. } if message_id == "m" && channel == "final")
        );
        assert!(matches!(
            final_events[1].payload,
            ReplyPayload::TurnCompleted
        ));
    }
    #[test]
    fn traex_projects_custom_tool_records_and_ignores_extra_info() {
        let mut decoder = Decoder::default();
        decoder
            .decode(
                TranscriptKind::Traex,
                &json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"t"}}),
            )
            .unwrap();
        let append = json!({"type":"history_mutation","payload":{"operation":"append","items":[
            {"type":"custom_tool_call","id":"tool","call_id":"call-1","name":"exec","input":"npm test"},
            {"type":"trae_extra_info","id":"extra","extra_info":{"model":"test"}},
            {"type":"custom_tool_call_output","id":"output","call_id":"call-1","output":[{"type":"input_text","text":"passed"}]}
        ]}});

        let events = decoder.decode(TranscriptKind::Traex, &append).unwrap();

        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0].payload, ReplyPayload::ToolCall { call_id, name, arguments, .. } if call_id == "call-1" && name == "exec" && arguments == "npm test")
        );
        assert!(
            matches!(&events[1].payload, ReplyPayload::ToolResult { call_id, text, .. } if call_id == "call-1" && text == "passed")
        );
    }
    #[test]
    fn traex_ignores_runtime_context_and_inter_agent_records() {
        let mut decoder = Decoder::default();
        for record in [
            json!({"type":"world_state","payload":{"full":true,"state":{}}}),
            json!({"type":"inter_agent_communication","payload":{"kind":"message","content":"private coordination"}}),
        ] {
            assert!(decoder
                .decode(TranscriptKind::Traex, &record)
                .unwrap()
                .is_empty());
        }
    }
    #[test]
    fn pi_branches_restore_turn_and_tools_do_not_complete_it() {
        let mut d = Decoder::default();
        d.decode(
            TranscriptKind::Pi,
            &json!({"type":"message","id":"u","parentId":null,"message":{"role":"user"}}),
        )
        .unwrap();
        let tool = d.decode(TranscriptKind::Pi, &json!({"type":"message","id":"m","parentId":"u","message":{"role":"assistant","content":[{"type":"thinking","thinking":"private"},{"type":"toolCall","id":"c","name":"read","arguments":{}}],"stopReason":"toolUse"}})).unwrap();
        assert_eq!(tool.len(), 1);
        assert!(matches!(tool[0].payload, ReplyPayload::ToolCall { .. }));
        let branch = d.decode(TranscriptKind::Pi, &json!({"type":"message","id":"m2","parentId":"u","message":{"role":"assistant","content":[{"type":"text","text":"new"}],"stopReason":"stop"}})).unwrap();
        assert!(matches!(
            branch[0].payload,
            ReplyPayload::BranchChanged { .. }
        ));
        assert!(branch.iter().all(|e| e.turn.as_deref() == Some("u")));
    }
    #[test]
    fn rejects_unbounded_metadata_and_unknown_history_mutations() {
        let mut decoder = Decoder::default();
        assert_eq!(decoder.decode(TranscriptKind::Pi, &json!({"type":"message","id":"u","parentId":"x".repeat(513),"message":{"role":"user"}})).unwrap_err().0, "invalid_record");
        assert_eq!(
            decoder
                .decode(
                    TranscriptKind::Traex,
                    &json!({"type":"history_mutation","payload":{"operation":"unknown","items":[]}})
                )
                .unwrap_err()
                .0,
            "unsupported_history_mutation"
        );
        assert_eq!(
            decoder
                .decode(
                    TranscriptKind::Traex,
                    &json!({"type":"session_meta","timestamp":"invalid"})
                )
                .unwrap_err()
                .0,
            "invalid_timestamp"
        );
    }

    #[test]
    fn traex_ignores_known_history_replacement_operations() {
        let mut decoder = Decoder::default();
        for operation in ["replace", "rollback"] {
            let events = decoder
                .decode(
                    TranscriptKind::Traex,
                    &json!({"type":"history_mutation","payload":{"operation":operation,"items":[{"content":"historical"}]}}),
                )
                .unwrap();
            assert!(events.is_empty());
        }
    }

    #[test]
    fn traex_mid_turn_attach_waits_for_the_next_turn_boundary() {
        let mut decoder = Decoder::awaiting_turn_boundary();
        let partial = json!({"type":"response_item","payload":{"type":"message","id":"old","role":"assistant","content":[{"type":"output_text","text":"old turn tail"}]}});
        assert!(decoder
            .decode(TranscriptKind::Traex, &partial)
            .unwrap()
            .is_empty());
        let completion = json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"old","last_agent_message":"old turn tail"}});
        assert!(decoder
            .decode(TranscriptKind::Traex, &completion)
            .unwrap()
            .is_empty());
        let start = json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"new"}});
        assert_eq!(
            decoder.decode(TranscriptKind::Traex, &start).unwrap().len(),
            1
        );
        let message = json!({"type":"response_item","payload":{"type":"message","id":"answer","role":"assistant","content":[{"type":"output_text","text":"new turn"}]}});
        assert_eq!(
            decoder
                .decode(TranscriptKind::Traex, &message)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn checkpoint_round_trip_preserves_pi_branch_identity() {
        let mut decoder = Decoder::default();
        decoder
            .decode(
                TranscriptKind::Pi,
                &json!({"type":"message","id":"u","parentId":null,"message":{"role":"user"}}),
            )
            .unwrap();
        let mut restored: Decoder =
            serde_json::from_str(&serde_json::to_string(&decoder).unwrap()).unwrap();
        let events = restored.decode(TranscriptKind::Pi, &json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"stopReason":"stop"}})).unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e.turn.as_deref() == Some("u")));
        assert!(time::OffsetDateTime::parse(
            &events[0].time,
            &time::format_description::well_known::Rfc3339
        )
        .is_ok());
    }

    #[test]
    fn truncation_preserves_unicode_and_has_explicit_marker() {
        let (s, cut) = bounded(&"界".repeat(MAX_TEXT));
        assert!(cut);
        assert!(s.ends_with("[truncated]"));
        assert!(s.len() < MAX_TEXT + 20);
    }

    #[test]
    fn redacts_secrets_before_event_payloads_are_persisted() {
        let (safe, _) =
            bounded("authorization: Bearer live-token\napi_key=live-key\nowner@example.com");
        assert!(!safe.contains("live-token"));
        assert!(!safe.contains("live-key"));
        assert!(!safe.contains("owner@example.com"));
        assert!(safe.contains("[REDACTED]"));
        assert!(safe.contains("[REDACTED EMAIL]"));
    }
}
