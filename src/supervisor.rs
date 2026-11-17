use crate::{
    config::Config,
    error::ApiError,
    ffmpeg,
    session::{Failure, Session, State, check_attachment, elapsed_ms},
    stream::{self, OutputStream},
};
use axum::body::Body;
use bytes::Bytes;
use chrono::Utc;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStderr},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

type UploadResult = Result<(), Failure>;
pub enum Control {
    Input {
        body: Body,
        length: u64,
        request_cancel: CancellationToken,
        reply: oneshot::Sender<Result<oneshot::Receiver<UploadResult>, ApiError>>,
    },
    Output {
        reply: oneshot::Sender<Result<Body, ApiError>>,
    },
}

enum Event {
    Input(UploadResult),
    Output(Result<(), Failure>),
    Stderr(Result<(), Failure>),
}

async fn stderr(
    mut stderr: ChildStderr,
    session: Arc<Session>,
    limit: usize,
    cancel: CancellationToken,
) -> Result<(), Failure> {
    let mut buffer = [0; 4096];
    loop {
        let count = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            count = stderr.read(&mut buffer) => count.map_err(|e| Failure::new("ffmpeg_failed", format!("stderr read failed: {e}")))?,
        };
        if count == 0 {
            return Ok(());
        }
        ffmpeg::append_tail(
            &mut session.meta.lock().unwrap().stderr,
            &buffer[..count],
            limit,
        );
    }
}

pub async fn run(
    session: Arc<Session>,
    mut commands: mpsc::Receiver<Control>,
    config: Arc<Config>,
) {
    if session.stop.is_cancelled() {
        finish(&session);
        return;
    }
    let starting = Instant::now();
    let mut child = match ffmpeg::command(&config, &session.profile).spawn() {
        Ok(child) => child,
        Err(error) => {
            session.fail(Failure::new(
                "ffmpeg_failed",
                format!("could not spawn FFmpeg: {error}"),
            ));
            finish(&session);
            return;
        }
    };
    let pid = child.id();
    let mut stdin = child.stdin.take();
    let mut stdout = child.stdout.take();
    let stderr_pipe = child.stderr.take().expect("FFmpeg stderr is piped");
    let stderr_cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let stderr_session = session.clone();
    let stderr_limit = config.ffmpeg.max_stderr_tail_bytes;
    let stderr_stop = stderr_cancel.clone();
    tasks.spawn(async move {
        Event::Stderr(stderr(stderr_pipe, stderr_session, stderr_limit, stderr_stop).await)
    });
    {
        let mut meta = session.meta.lock().unwrap();
        meta.state = State::Ready;
        meta.ready = Some(Instant::now());
        meta.updated_at = Utc::now();
        meta.ffmpeg.pid = pid;
        meta.timings.queue_ms = Some(
            starting
                .duration_since(meta.created)
                .as_millis()
                .min(u64::MAX as u128) as u64,
        );
        meta.timings.startup_ms = Some(elapsed_ms(starting));
    }
    tracing::debug!(session_id = %session.id, profile = %session.profile, pid, "session ready");
    let mut exit = None;
    let mut input_complete = false;
    let mut output_complete = false;
    let mut output_gate: Option<mpsc::Sender<Bytes>> = None;
    let mut upload_reply: Option<oneshot::Sender<UploadResult>> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        if session.stop.is_cancelled() {
            break;
        }
        if input_complete
            && output_complete
            && tasks.is_empty()
            && exit.as_ref().is_some_and(std::process::ExitStatus::success)
        {
            if output_gate.is_some() {
                {
                    let mut meta = session.meta.lock().unwrap();
                    meta.output_finishing = true;
                    meta.pending_since.get_or_insert_with(Instant::now);
                }
                output_gate.take();
            }
            if session.meta.lock().unwrap().output_body_complete {
                break;
            }
        }
        tokio::select! {
            biased;
            _ = session.stop.cancelled() => break,
            status = child.wait(), if exit.is_none() => {
                match status {
                    Ok(status) => {
                        session.meta.lock().unwrap().ffmpeg.exit_code = status.code();
                        if !status.success() {
                            session.fail(Failure::new("ffmpeg_failed", format!("FFmpeg exited {status}; see stderr_tail. Input must support non-seekable decoding; use MPEG-TS or streaming MKV, not tail-metadata MP4.")));
                        }
                        exit = Some(status);
                    }
                    Err(error) => session.fail(Failure::new("ffmpeg_failed", format!("could not wait for FFmpeg: {error}"))),
                }
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                match result.expect("nonempty task set") {
                    Ok(Event::Input(result)) => {
                        {
                            let mut meta = session.meta.lock().unwrap();
                            meta.input.connected = false;
                            if result.is_ok() {
                                meta.state = State::Draining;
                                meta.updated_at = Utc::now();
                            }
                        }
                        input_complete = result.is_ok();
                        if let Err(error) = &result {
                            session.fail(error.clone());
                        }
                        if let Some(reply) = upload_reply.take() {
                            let _ = reply.send(result);
                        }
                    }
                    Ok(Event::Output(result)) => {
                        output_complete = result.is_ok();
                        if let Err(error) = result {
                            session.fail(error);
                        }
                    }
                    Ok(Event::Stderr(result)) => {
                        if let Err(error) = result {
                            session.fail(error);
                        }
                    }
                    Err(error) => session.fail(Failure::new("internal_error", format!("I/O worker failed: {error}"))),
                }
            }
            command = commands.recv() => {
                match command {
                    Some(Control::Input { body, length, request_cancel, reply }) => {
                        let result = {
                            let mut meta = session.meta.lock().unwrap();
                            check_attachment(&meta, true).map(|()| {
                                meta.input_attached = true;
                                meta.input.connected = true;
                                meta.state = State::Running;
                                meta.updated_at = Utc::now();
                            })
                        };
                        if let Err(error) = result {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                        let input_pipe = stdin.take().expect("input can only be attached once");
                        let (finished, completion) = oneshot::channel();
                        upload_reply = Some(finished);
                        let input_session = session.clone();
                        let idle = Duration::from_secs(config.timeouts.input_idle_secs);
                        tasks.spawn(async move {
                            Event::Input(stream::input(input_pipe, body, length, input_session, request_cancel, idle).await)
                        });
                        if reply.send(Ok(completion)).is_err() {
                            session.fail(Failure::new("input_stream_failed", "upload handler disconnected during attachment"));
                        }
                    }
                    Some(Control::Output { reply }) => {
                        let result = {
                            let mut meta = session.meta.lock().unwrap();
                            check_attachment(&meta, false).map(|()| {
                                meta.output_attached = true;
                                meta.output.connected = true;
                                meta.updated_at = Utc::now();
                            })
                        };
                        if let Err(error) = result {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                        let (sender, receiver) = mpsc::channel(stream::OUTPUT_CHUNKS);
                        *session.output.lock().unwrap() = Some(receiver);
                        output_gate = Some(sender.clone());
                        let output_pipe = stdout.take().expect("output can only be attached once");
                        let output_session = session.clone();
                        tasks.spawn(async move {
                            Event::Output(stream::output(output_pipe, sender, output_session).await)
                        });
                        let body = Body::from_stream(OutputStream::new(session.clone()));
                        // A rejected oneshot drops the body, whose guard reports disconnect.
                        let _ = reply.send(Ok(body));
                    }
                    None => session.request_stop(State::Cancelled, None),
                }
            }
            _ = session.changed.notified() => {},
            _ = tick.tick() => {
                if let Some((state, failure)) = timeout(&session, &config) {
                    session.request_stop(state, Some(failure));
                }
            }
        }
    }

    // No terminal state or freed scheduler slot is visible until the direct child
    // has been reaped, all pipe tasks have joined, and buffered media is reclaimed.
    commands.close();
    drop(stdin);
    drop(stdout);
    if session.stop.is_cancelled() {
        terminate(&mut child, pid, &mut exit, config.shutdown_grace()).await;
        // Closing the read side reclaims its bounded queue even if Hyper is not polling.
        session.output.lock().unwrap().take();
        stderr_cancel.cancel();
    }
    drop(output_gate);
    while let Some(result) = tasks.join_next().await {
        if let Ok(Event::Input(result)) = result {
            if let Some(reply) = upload_reply.take() {
                let _ = reply.send(result);
            }
        }
    }
    if let Some(reply) = upload_reply.take() {
        let _ = reply.send(Err(Failure::new(
            "input_stream_failed",
            "session terminated during upload",
        )));
    }
    if let Some(status) = exit {
        session.meta.lock().unwrap().ffmpeg.exit_code = status.code();
    }
    session.output.lock().unwrap().take();
    finish(&session);
}

pub fn timeout(session: &Session, config: &Config) -> Option<(State, Failure)> {
    let meta = session.meta.lock().unwrap();
    let ready = meta.ready?;
    if config.timeouts.max_session_secs > 0
        && ready.elapsed() >= Duration::from_secs(config.timeouts.max_session_secs)
    {
        return Some((
            State::Expired,
            Failure::new("session_timeout", "maximum session duration exceeded"),
        ));
    }
    if (!meta.input_attached || !meta.output_attached)
        && ready.elapsed() >= Duration::from_secs(config.timeouts.ready_attach_secs)
    {
        return Some((
            State::Expired,
            Failure::new(
                "idle_timeout",
                "both input and output must attach before the ready deadline",
            ),
        ));
    }
    if meta.pending_since.is_some_and(|since| {
        since.elapsed() >= Duration::from_secs(config.timeouts.output_stall_secs)
    }) {
        return Some((
            State::Failed,
            Failure::new(
                "idle_timeout",
                "output consumer made no progress before output stall timeout",
            ),
        ));
    }
    None
}

fn signal_group(pid: Option<u32>, signal: i32) {
    if let Some(pid) = pid {
        // Each child is spawned into its own process group. Never signal our own
        // group, and never derive this identifier from a client-supplied value.
        unsafe {
            libc::kill(-(pid as i32), signal);
        }
    }
}

async fn terminate(
    child: &mut Child,
    pid: Option<u32>,
    exit: &mut Option<std::process::ExitStatus>,
    grace: Duration,
) {
    signal_group(pid, libc::SIGTERM);
    if exit.is_none() {
        match tokio::time::timeout(grace, child.wait()).await {
            Ok(Ok(status)) => *exit = Some(status),
            _ => {
                signal_group(pid, libc::SIGKILL);
                let _ = child.start_kill();
                match child.wait().await {
                    Ok(status) => *exit = Some(status),
                    Err(error) => tracing::error!(%error, "FFmpeg reap failed"),
                }
            }
        }
    }
    // Descendants must not retain inherited pipes after the direct child exits.
    signal_group(pid, libc::SIGKILL);
}

pub fn finish(session: &Session) {
    let mut meta = session.meta.lock().unwrap();
    if meta.state.terminal() {
        return;
    }
    if let Some(stop) = meta.stop.take() {
        meta.state = stop.state;
        meta.error = stop.error;
    } else {
        meta.state = State::Completed;
    }
    meta.input.connected = false;
    meta.output.connected = false;
    meta.pending_output = 0;
    meta.pending_since = None;
    meta.updated_at = Utc::now();
    meta.finished = Some(Instant::now());
    meta.timings.total_ms = Some(elapsed_ms(meta.created));
    tracing::info!(state = ?meta.state, profile = %session.profile, input_bytes = meta.input.received_bytes, output_bytes = meta.output.written_bytes, duration_ms = meta.timings.total_ms, error_code = meta.error.as_ref().map(|error| error.code), "session finished");
    tracing::debug!(session_id = %session.id, "session metadata retained");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failure_wins_and_terminal_state_cannot_be_overwritten() {
        let (session, _commands) = Session::new("h264-cpu-mkv".into());
        session.fail(Failure::new("output_disconnected", "first failure"));
        session.fail(Failure::new("ffmpeg_failed", "late child exit"));
        assert_eq!(session.meta.lock().unwrap().state, State::Queued);
        finish(&session);
        session.request_stop(State::Cancelled, None);
        finish(&session);
        let meta = session.meta.lock().unwrap();
        assert_eq!(meta.state, State::Failed);
        assert_eq!(meta.error.as_ref().unwrap().code, "output_disconnected");
    }

    #[test]
    fn output_attachment_does_not_disable_input_attachment_deadline() {
        let (session, _commands) = Session::new("h264-cpu-mkv".into());
        let config = Config::default();
        {
            let mut meta = session.meta.lock().unwrap();
            meta.state = State::Ready;
            meta.ready = Some(Instant::now() - Duration::from_secs(31));
            meta.output_attached = true;
        }
        assert_eq!(timeout(&session, &config).unwrap().1.code, "idle_timeout");
    }

    #[test]
    fn codec_silence_is_not_output_backpressure() {
        let (session, _commands) = Session::new("h264-cpu-mkv".into());
        let config = Config::default();
        {
            let mut meta = session.meta.lock().unwrap();
            meta.state = State::Running;
            meta.ready = Some(Instant::now() - Duration::from_secs(200));
            meta.input_attached = true;
            meta.output_attached = true;
        }
        assert!(timeout(&session, &config).is_none());
        session.meta.lock().unwrap().pending_since =
            Some(Instant::now() - Duration::from_secs(121));
        assert_eq!(timeout(&session, &config).unwrap().1.code, "idle_timeout");
    }
}
