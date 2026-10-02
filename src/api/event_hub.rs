type SessionEventRecorder = dyn Fn(&str, &crate::api::schema::EventEnvelope) -> Option<crate::api::schema::EventEnvelope>
    + Send
    + Sync;

#[derive(Clone)]
pub struct EventHub {
    inner: std::sync::Arc<std::sync::Mutex<EventHubState>>,
    session_event_recorder:
        std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<SessionEventRecorder>>>>,
    runtime_epoch: std::sync::Arc<str>,
    next_runtime_event: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Default)]
struct EventHubState {
    next_sequence: u64,
    events: Vec<(u64, crate::api::schema::EventEnvelope)>,
}

impl Default for EventHub {
    fn default() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            inner: Default::default(),
            session_event_recorder: Default::default(),
            runtime_epoch: format!("{}-{nanos}", std::process::id()).into(),
            next_runtime_event: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EventHistoryError {
    Lost,
    Unavailable,
}

impl EventHub {
    pub(crate) const MAX_EVENTS: usize = 512;

    pub(crate) fn set_session_event_recorder(
        &self,
        recorder: std::sync::Arc<SessionEventRecorder>,
    ) {
        if let Ok(mut current) = self.session_event_recorder.lock() {
            *current = Some(recorder);
        }
    }

    pub fn push(&self, event: crate::api::schema::EventEnvelope) {
        let recorder = self
            .session_event_recorder
            .lock()
            .ok()
            .and_then(|recorder| recorder.clone());
        if let Some(recorder) = recorder {
            let sequence = self
                .next_runtime_event
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let key = format!("{}:{sequence}", self.runtime_epoch);
            if let Some(wake) = recorder(&key, &event) {
                self.push_ring(wake);
            }
        }
        self.push_ring(event);
    }

    fn push_ring(&self, event: crate::api::schema::EventEnvelope) {
        let Ok(mut state) = self.inner.lock() else {
            return;
        };
        state.next_sequence += 1;
        let sequence = state.next_sequence;
        state.events.push((sequence, event));
        let overflow = state.events.len().saturating_sub(Self::MAX_EVENTS);
        if overflow > 0 {
            state.events.drain(0..overflow);
        }
    }

    pub fn events_after(&self, sequence: u64) -> Vec<(u64, crate::api::schema::EventEnvelope)> {
        let Ok(state) = self.inner.lock() else {
            return Vec::new();
        };
        state
            .events
            .iter()
            .filter(|(event_sequence, _)| *event_sequence > sequence)
            .cloned()
            .collect()
    }

    pub(super) fn events_after_checked(
        &self,
        sequence: u64,
    ) -> Result<Vec<(u64, crate::api::schema::EventEnvelope)>, EventHistoryError> {
        let state = self
            .inner
            .lock()
            .map_err(|_| EventHistoryError::Unavailable)?;
        if state
            .events
            .first()
            .is_some_and(|(first, _)| sequence < first.saturating_sub(1))
        {
            return Err(EventHistoryError::Lost);
        }
        Ok(state
            .events
            .iter()
            .filter(|(event_sequence, _)| *event_sequence > sequence)
            .cloned()
            .collect())
    }

    pub fn current_sequence(&self) -> u64 {
        let Ok(state) = self.inner.lock() else {
            return 0;
        };
        state.next_sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{EventData, EventEnvelope, EventKind};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn durable_session_commit_precedes_wake_and_ring_remains_bounded() {
        let hub = EventHub::default();
        let committed = Arc::new(AtomicBool::new(false));
        let observed = committed.clone();
        hub.set_session_event_recorder(Arc::new(move |_key, event| {
            if matches!(event.event, EventKind::PaneExited) {
                observed.store(true, Ordering::Release);
                Some(EventEnvelope {
                    event: EventKind::SessionEventsAvailable,
                    data: EventData::SessionEventsAvailable {
                        stream_id: "stream-1".into(),
                        latest_cursor: "cursor-1".into(),
                    },
                })
            } else {
                None
            }
        }));

        hub.push(EventEnvelope {
            event: EventKind::PaneExited,
            data: EventData::PaneExited {
                pane_id: "pane-1".into(),
                workspace_id: "workspace-1".into(),
            },
        });

        let events = hub.events_after(0);
        let wake = events
            .iter()
            .find(|(_, event)| event.event == EventKind::SessionEventsAvailable)
            .expect("durable stream wake");
        assert!(committed.load(Ordering::Acquire));
        assert!(matches!(
            &wake.1.data,
            EventData::SessionEventsAvailable { stream_id, latest_cursor }
                if stream_id == "stream-1" && latest_cursor == "cursor-1"
        ));

        for revision in 0..600 {
            hub.push(EventEnvelope {
                event: EventKind::PaneOutputChanged,
                data: EventData::PaneOutputChanged {
                    pane_id: "pane-1".into(),
                    workspace_id: "workspace-1".into(),
                    revision,
                },
            });
        }
        assert_eq!(hub.events_after(0).len(), EventHub::MAX_EVENTS);
    }

    fn event() -> EventEnvelope {
        EventEnvelope {
            event: EventKind::WorkspaceFocused,
            data: EventData::WorkspaceFocused {
                workspace_id: "workspace_1".into(),
            },
        }
    }

    #[test]
    fn checked_history_distinguishes_retained_boundary_from_lost_events() {
        let hub = EventHub::default();
        assert!(hub.events_after_checked(0).unwrap().is_empty());
        for _ in 0..EventHub::MAX_EVENTS {
            hub.push(event());
        }
        assert_eq!(
            hub.events_after_checked(0).unwrap().len(),
            EventHub::MAX_EVENTS
        );
        hub.push(event());
        assert_eq!(hub.events_after_checked(0), Err(EventHistoryError::Lost));
        let retained = hub.events_after_checked(1).unwrap();
        assert_eq!(retained.len(), EventHub::MAX_EVENTS);
        assert_eq!(retained.first().unwrap().0, 2);
        assert_eq!(retained.last().unwrap().0, hub.current_sequence());
        assert!(hub
            .events_after_checked(hub.current_sequence())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn checked_history_reports_unavailable_instead_of_empty_after_poison() {
        let hub = EventHub::default();
        assert!(std::panic::catch_unwind(|| {
            let _guard = hub.inner.lock().unwrap();
            panic!("poison the test event history");
        })
        .is_err());
        assert_eq!(
            hub.events_after_checked(0),
            Err(EventHistoryError::Unavailable)
        );
    }
}
