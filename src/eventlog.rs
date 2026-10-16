use crate::engine::now_ms;
use crate::protocol::{HistorySnapshot, RecordedEvent};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

pub const MAX_EVENTS: usize = 512;
pub const MAX_RETAINED_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub struct BoundedEventLog {
    session_id: String,
    run_id: String,
    raw_recording_enabled: bool,
    next_sequence: u64,
    retained_bytes: usize,
    events: VecDeque<RecordedEvent>,
    truncated: bool,
    truncated_before_sequence: Option<u64>,
}

impl BoundedEventLog {
    pub fn new(
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        raw_recording_enabled: bool,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            run_id: run_id.into(),
            raw_recording_enabled,
            next_sequence: 1,
            retained_bytes: 0,
            events: VecDeque::new(),
            truncated: false,
            truncated_before_sequence: None,
        }
    }

    pub fn push_output(&mut self, bytes: &[u8]) {
        self.push("pty_output", bytes, None)
    }

    pub fn push_input(&mut self, operation: &str, bytes: &[u8]) {
        self.push("input", bytes, Some(operation.to_string()))
    }

    pub fn push_control_event(&mut self, kind: &str, detail: &str) {
        self.push(kind, &[], Some(detail.to_string()))
    }

    pub fn push_resize(&mut self, cols: u16, rows: u16) {
        self.push("resize", &[], Some(format!("{cols}x{rows}")))
    }

    pub fn push_process_exit(&mut self, code: Option<i32>, signal: Option<&str>) {
        self.push(
            "process_exit",
            &[],
            Some(format!("code={code:?} signal={signal:?}")),
        )
    }

    fn push(&mut self, kind: &str, bytes: &[u8], detail: Option<String>) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let raw_base64 = self.raw_recording_enabled.then(|| BASE64.encode(bytes));
        let digest = Sha256::digest(bytes);
        let sha256 = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let event = RecordedEvent {
            sequence,
            at_ms: now_ms(),
            kind: kind.to_string(),
            byte_count: bytes.len(),
            sha256,
            raw_base64,
            detail,
        };
        let event_size = serde_json::to_vec(&event).map_or(bytes.len(), |encoded| encoded.len());
        self.retained_bytes = self.retained_bytes.saturating_add(event_size);
        self.events.push_back(event);
        while self.events.len() > MAX_EVENTS || self.retained_bytes > MAX_RETAINED_BYTES {
            if let Some(removed) = self.events.pop_front() {
                self.retained_bytes = self.retained_bytes.saturating_sub(
                    serde_json::to_vec(&removed).map_or(0, |encoded| encoded.len()),
                );
                self.truncated = true;
                self.truncated_before_sequence = Some(removed.sequence.saturating_add(1));
            } else {
                break;
            }
        }
    }

    pub fn snapshot(&self) -> HistorySnapshot {
        HistorySnapshot {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            events: self.events.iter().cloned().collect(),
            event_count: self.events.len(),
            retained_bytes: self.retained_bytes,
            max_bytes: MAX_RETAINED_BYTES,
            raw_recording_enabled: self.raw_recording_enabled,
            truncated: self.truncated,
            truncated_before_sequence: self.truncated_before_sequence,
            replayable: self.raw_recording_enabled && !self.truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_is_bounded_and_reports_truncation() {
        let mut log = BoundedEventLog::new("s", "r", true);
        for _ in 0..(MAX_EVENTS + 4) {
            log.push_output(b"chunk");
        }
        let history = log.snapshot();
        assert_eq!(history.event_count, MAX_EVENTS);
        assert!(history.truncated);
        assert!(!history.replayable);
        assert_eq!(history.truncated_before_sequence, Some(5));
    }

    #[test]
    fn raw_recording_is_opt_in() {
        let mut log = BoundedEventLog::new("s", "r", false);
        log.push_output(b"private terminal text");
        let history = log.snapshot();
        assert!(!history.raw_recording_enabled);
        assert!(history.events[0].raw_base64.is_none());
        assert!(!history.replayable);
    }
}
