use crate::api::schema::agent_events::{
    AgentEventsAttachFrom, AgentEventsAttachParams, AgentEventsListTurnsParams,
    AgentEventsLocateParams, AgentEventsReadParams, AgentEventsRecoverTurnParams,
    AgentEventsSubmissionParams, AgentEventsTurnBoundary, TranscriptKind,
};
use crate::api::schema::{EmptyParams, Method, Request};
use std::io::{BufRead, BufReader, Read, Write};

pub(super) fn run(args: &[String]) -> std::io::Result<i32> {
    let Some(action) = args.first().map(String::as_str) else {
        return help();
    };
    let options = &args[1..];
    let mut values = std::collections::BTreeMap::new();
    for pair in options.chunks(2) {
        if pair.len() != 2
            || !pair[0].starts_with("--")
            || values.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return help();
        }
    }
    let get = |key: &str| values.get(key).copied();
    let method = match action {
        "sources" if options.is_empty() => Method::AgentEventsSources(EmptyParams {}),
        "capabilities" if options.is_empty() => Method::AgentEventsCapabilities(EmptyParams {}),
        "submission" => {
            if values.keys().any(|key| *key != "--submission-id") {
                return help();
            }
            let Some(submission_id) = get("--submission-id") else {
                return help();
            };
            Method::AgentEventsSubmission(AgentEventsSubmissionParams {
                submission_id: submission_id.into(),
            })
        }
        "recover-turn" => {
            if values.keys().any(|key| {
                ![
                    "--kind",
                    "--session-id",
                    "--turn-id",
                    "--started-at",
                    "--after",
                    "--limit",
                ]
                .contains(key)
            }) {
                return help();
            }
            let (Some(kind), Some(session_id), Some(turn_id), Some(started_at)) = (
                get("--kind"),
                get("--session-id"),
                get("--turn-id"),
                get("--started-at"),
            ) else {
                return help();
            };
            let agent_kind = match kind {
                "traex" => TranscriptKind::Traex,
                "pi" => TranscriptKind::Pi,
                _ => return help(),
            };
            let limit = match get("--limit").unwrap_or("64").parse::<u32>() {
                Ok(n) if (1..=128).contains(&n) => n,
                _ => return help(),
            };
            Method::AgentEventsRecoverTurn(AgentEventsRecoverTurnParams {
                agent_kind,
                session_id: session_id.into(),
                turn_id: turn_id.into(),
                started_at: started_at.into(),
                after: get("--after").map(str::to_owned),
                limit,
            })
        }
        "turns" => {
            if values
                .keys()
                .any(|key| !["--kind", "--session-id", "--after", "--limit"].contains(key))
            {
                return help();
            }
            let (Some(kind), Some(session_id)) = (get("--kind"), get("--session-id")) else {
                return help();
            };
            let agent_kind = match kind {
                "traex" => TranscriptKind::Traex,
                "pi" => TranscriptKind::Pi,
                _ => return help(),
            };
            let limit = match get("--limit").unwrap_or("64").parse::<u32>() {
                Ok(n) if (1..=128).contains(&n) => n,
                _ => return help(),
            };
            Method::AgentEventsTurns(AgentEventsListTurnsParams {
                agent_kind,
                session_id: session_id.into(),
                after: get("--after").map(str::to_owned),
                limit,
            })
        }
        "attach" => {
            if values
                .keys()
                .any(|k| !["--pane", "--kind", "--session-id", "--path", "--from"].contains(k))
            {
                return help();
            }
            let (Some(pane_id), Some(kind), Some(session_id), Some(path)) = (
                get("--pane"),
                get("--kind"),
                get("--session-id"),
                get("--path"),
            ) else {
                return help();
            };
            let agent_kind = match kind {
                "traex" => TranscriptKind::Traex,
                "pi" => TranscriptKind::Pi,
                _ => return help(),
            };
            let from = match get("--from").unwrap_or("end") {
                "start" => AgentEventsAttachFrom::Start,
                "end" => AgentEventsAttachFrom::End,
                _ => return help(),
            };
            Method::AgentEventsAttach(AgentEventsAttachParams {
                pane_id: pane_id.into(),
                agent_kind,
                session_id: session_id.into(),
                path: path.into(),
                from,
            })
        }
        "read" | "subscribe" => {
            if values
                .keys()
                .any(|k| !["--source", "--after", "--limit"].contains(k))
            {
                return help();
            }
            let Some(source) = get("--source") else {
                return help();
            };
            let limit = match get("--limit").unwrap_or("64").parse::<u32>() {
                Ok(n) if (1..=128).contains(&n) => n,
                _ => return help(),
            };
            let params = AgentEventsReadParams {
                source_id: source.into(),
                after: get("--after").unwrap_or("start").into(),
                limit,
            };
            if action == "read" {
                Method::AgentEventsRead(params)
            } else {
                Method::AgentEventsSubscribe(params)
            }
        }
        "locate" => {
            if values
                .keys()
                .any(|k| !["--source", "--boundary", "--turn-id", "--started-at"].contains(k))
            {
                return help();
            }
            let (Some(source), Some(boundary)) = (get("--source"), get("--boundary")) else {
                return help();
            };
            let boundary = match boundary {
                "active" => AgentEventsTurnBoundary::Active,
                "at" => AgentEventsTurnBoundary::At,
                "after" => AgentEventsTurnBoundary::After,
                _ => return help(),
            };
            Method::AgentEventsLocate(AgentEventsLocateParams {
                source_id: source.into(),
                boundary,
                turn_id: get("--turn-id").map(str::to_owned),
                started_at: get("--started-at").map(str::to_owned),
            })
        }
        _ => return help(),
    };
    let request = Request {
        id: format!("cli:agent:events:{action}"),
        method,
    };
    if action != "subscribe" {
        return super::print_response(&super::send_request(&request)?);
    }
    let mut stream = crate::ipc::connect_local_stream(&crate::api::socket_path())?;
    writeln!(stream, "{}", serde_json::to_string(&request)?)?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        let n = reader.by_ref().take(2 * 1024 * 1024).read_line(&mut line)?;
        if n == 0 {
            return Ok(0);
        }
        if !line.ends_with('\n') {
            return Err(std::io::Error::other("oversized event frame"));
        }
        let value: serde_json::Value = serde_json::from_str(&line)?;
        println!("{value}");
        if value.get("error").is_some() {
            return Ok(1);
        }
    }
}
fn help() -> std::io::Result<i32> {
    eprintln!("herdr agent events sources\nherdr agent events capabilities\nherdr agent events submission --submission-id ID\nherdr agent events turns --kind traex|pi --session-id ID [--after CURSOR] [--limit 1..128]\nherdr agent events recover-turn --kind traex|pi --session-id ID --turn-id ID --started-at RFC3339 [--after CURSOR] [--limit 1..128]\nherdr agent events attach --pane ID --kind traex|pi --session-id ID --path PATH [--from start|end]\nherdr agent events locate --source ID --boundary active|at|after [--turn-id ID --started-at RFC3339]\nherdr agent events read|subscribe --source ID [--after start|latest|CURSOR] [--limit 1..128]");
    Ok(2)
}
