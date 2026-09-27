use crate::api::schema::agent_events::{
    AgentEventsAttachFrom, AgentEventsAttachParams, AgentEventsLocateParams, AgentEventsReadParams,
    AgentEventsTurnBoundary, TranscriptKind,
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
    eprintln!("herdr agent events sources\nherdr agent events attach --pane ID --kind traex|pi --session-id ID --path PATH [--from start|end]\nherdr agent events locate --source ID --boundary active|at|after [--turn-id ID --started-at RFC3339]\nherdr agent events read|subscribe --source ID [--after start|latest|CURSOR] [--limit 1..128]");
    Ok(2)
}
