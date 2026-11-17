use crate::{error::ApiError, ffmpeg, supervisor::Control};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Queued,
    Ready,
    Running,
    Draining,
    Completed,
    Failed,
    Cancelled,
    Expired,
}
impl State {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Expired
        )
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Failure {
    pub code: &'static str,
    pub message: String,
}
impl Failure {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Stop {
    pub state: State,
    pub error: Option<Failure>,
}

#[derive(Default, Serialize)]
pub struct InputStatus {
    pub connected: bool,
    pub received_bytes: u64,
}
#[derive(Default, Serialize)]
pub struct OutputStatus {
    pub connected: bool,
    pub written_bytes: u64,
    pub produced_bytes: u64,
}
#[derive(Default, Serialize)]
pub struct ProcessStatus {
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
}
#[derive(Default, Serialize)]
pub struct Timings {
    pub queue_ms: Option<u64>,
    pub startup_ms: Option<u64>,
    pub first_input_ms: Option<u64>,
    pub first_output_ms: Option<u64>,
    pub total_ms: Option<u64>,
}

pub struct Metadata {
    pub state: State,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created: Instant,
    pub finished: Option<Instant>,
    pub ready: Option<Instant>,
    pub input: InputStatus,
    pub output: OutputStatus,
    pub ffmpeg: ProcessStatus,
    pub output_finishing: bool,
    pub timings: Timings,
    pub error: Option<Failure>,
    pub stop: Option<Stop>,
    pub input_attached: bool,
    pub output_attached: bool,
    pub output_body_complete: bool,
    pub pending_output: usize,
    pub pending_since: Option<Instant>,
    pub stderr: VecDeque<u8>,
}

pub struct Session {
    pub id: Uuid,
    pub profile: String,
    pub meta: Mutex<Metadata>,
    pub commands: mpsc::Sender<Control>,
    pub stop: CancellationToken,
    pub changed: Notify,
    // The supervisor can reclaim queued media even when Hyper has stopped polling
    // a response body. The response itself never owns a child pipe or task.
    pub output: Mutex<Option<mpsc::Receiver<Bytes>>>,
}

#[derive(Serialize)]
pub struct Snapshot<'a> {
    pub id: Uuid,
    pub state: State,
    pub profile: &'a str,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub input: &'a InputStatus,
    pub output: &'a OutputStatus,
    pub ffmpeg: &'a ProcessStatus,
    pub error: &'a Option<Failure>,
    pub stderr_tail: String,
    pub timings: &'a Timings,
}

impl Session {
    pub fn new(profile: String) -> (Arc<Self>, mpsc::Receiver<Control>) {
        let (commands, receiver) = mpsc::channel(4);
        let now = Utc::now();
        let session = Self {
            id: Uuid::new_v4(),
            profile,
            meta: Mutex::new(Metadata {
                state: State::Queued,
                created_at: now,
                updated_at: now,
                created: Instant::now(),
                finished: None,
                ready: None,
                input: InputStatus::default(),
                output: OutputStatus::default(),
                ffmpeg: ProcessStatus::default(),
                timings: Timings::default(),
                error: None,
                stop: None,
                input_attached: false,
                output_attached: false,
                output_body_complete: false,
                output_finishing: false,
                pending_output: 0,
                pending_since: None,
                stderr: VecDeque::new(),
            }),
            commands,
            stop: CancellationToken::new(),
            changed: Notify::new(),
            output: Mutex::new(None),
        };
        (Arc::new(session), receiver)
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let meta = self.meta.lock().unwrap();
        serde_json::to_value(Snapshot {
            id: self.id,
            state: meta.state,
            profile: &self.profile,
            created_at: meta.created_at,
            updated_at: meta.updated_at,
            input: &meta.input,
            output: &meta.output,
            ffmpeg: &meta.ffmpeg,
            error: &meta.error,
            stderr_tail: ffmpeg::sanitized_tail(&meta.stderr),
            timings: &meta.timings,
        })
        .expect("session metadata is JSON serializable")
    }

    pub fn request_stop(&self, state: State, error: Option<Failure>) {
        let mut meta = self.meta.lock().unwrap();
        if !meta.state.terminal() && meta.stop.is_none() {
            meta.stop = Some(Stop { state, error });
            self.stop.cancel();
        }
    }

    pub fn fail(&self, error: Failure) {
        self.request_stop(State::Failed, Some(error));
    }

    pub fn attachment_allowed(&self, input: bool) -> Result<(), ApiError> {
        let meta = self.meta.lock().unwrap();
        check_attachment(&meta, input).map_err(|mut error| {
            if error.error.code == "not_ready" {
                error.error.message = format!(
                    "session is waiting for capacity; inspect /v1/sessions/{}",
                    self.id
                );
            }
            error
        })
    }

    pub fn produced(&self, count: usize) -> Result<(), Failure> {
        let mut meta = self.meta.lock().unwrap();
        meta.output.produced_bytes = meta
            .output
            .produced_bytes
            .checked_add(count as u64)
            .ok_or_else(|| Failure::new("output_stream_failed", "output counter overflow"))?;
        meta.pending_output = meta.pending_output.checked_add(count).ok_or_else(|| {
            Failure::new("output_stream_failed", "pending output counter overflow")
        })?;
        if meta.pending_since.is_none() {
            meta.pending_since = Some(Instant::now());
        }
        Ok(())
    }

    pub fn delivered(&self, count: usize) -> Result<(), Failure> {
        let mut meta = self.meta.lock().unwrap();
        meta.output.written_bytes = meta
            .output
            .written_bytes
            .checked_add(count as u64)
            .ok_or_else(|| Failure::new("output_stream_failed", "output counter overflow"))?;
        meta.pending_output = meta.pending_output.saturating_sub(count);
        meta.pending_since = (meta.pending_output > 0 || meta.output_finishing).then(Instant::now);
        if meta.timings.first_output_ms.is_none() {
            meta.timings.first_output_ms = Some(elapsed_ms(meta.created));
        }
        meta.updated_at = Utc::now();
        Ok(())
    }

    pub fn received(&self, count: usize) -> Result<(), Failure> {
        let mut meta = self.meta.lock().unwrap();
        meta.input.received_bytes = meta
            .input
            .received_bytes
            .checked_add(count as u64)
            .ok_or_else(|| Failure::new("input_stream_failed", "input counter overflow"))?;
        if meta.timings.first_input_ms.is_none() {
            meta.timings.first_input_ms = Some(elapsed_ms(meta.created));
        }
        meta.updated_at = Utc::now();
        Ok(())
    }
}

pub fn check_attachment(meta: &Metadata, input: bool) -> Result<(), ApiError> {
    if meta.state.terminal() || meta.stop.is_some() {
        return Err(ApiError::gone(
            "session_terminal",
            "session is terminating or terminal",
        ));
    }
    if meta.state == State::Queued {
        return Err(ApiError::conflict(
            "not_ready",
            "session is waiting for capacity; poll its status URL",
        ));
    }
    if input {
        if meta.input_attached {
            return Err(ApiError::conflict(
                "input_already_attached",
                "input can be attached only once",
            ));
        }
        if !meta.output_attached {
            return Err(ApiError::conflict(
                "output_not_attached",
                "attach and consume output before uploading",
            ));
        }
        if !matches!(meta.state, State::Ready | State::Running) {
            return Err(ApiError::gone(
                "session_terminal",
                "input is no longer available",
            ));
        }
    } else if meta.output_attached {
        return Err(ApiError::conflict(
            "output_already_attached",
            "output can be attached only once",
        ));
    }
    Ok(())
}

pub fn elapsed_ms(since: Instant) -> u64 {
    since.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnected_streams_cannot_reattach() {
        let (session, _receiver) = Session::new("h264-cpu-mkv".into());
        {
            let mut meta = session.meta.lock().unwrap();
            meta.state = State::Running;
            meta.input_attached = true;
            meta.output_attached = true;
            meta.input.connected = false;
            meta.output.connected = false;
        }
        assert_eq!(
            session.attachment_allowed(true).unwrap_err().error.code,
            "input_already_attached"
        );
        assert_eq!(
            session.attachment_allowed(false).unwrap_err().error.code,
            "output_already_attached"
        );
        session.request_stop(State::Cancelled, None);
        assert_eq!(
            session.attachment_allowed(false).unwrap_err().status,
            axum::http::StatusCode::GONE
        );
    }

    #[test]
    fn upload_before_output_does_not_consume_attachment() {
        let (session, _receiver) = Session::new("h264-cpu-mkv".into());
        session.meta.lock().unwrap().state = State::Ready;
        assert_eq!(
            session.attachment_allowed(true).unwrap_err().error.code,
            "output_not_attached"
        );
        assert!(!session.meta.lock().unwrap().input_attached);
        session.meta.lock().unwrap().output_attached = true;
        assert!(session.attachment_allowed(true).is_ok());
    }
}
