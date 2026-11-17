use crate::session::{Failure, Session};
use axum::body::Body;
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{ChildStdin, ChildStdout},
    sync::mpsc,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub const CHUNK_SIZE: usize = 32 * 1024;
pub const OUTPUT_CHUNKS: usize = 2;

pub struct RequestGuard(Option<CancellationToken>);
impl RequestGuard {
    pub fn new(token: CancellationToken) -> Self {
        Self(Some(token))
    }
    pub fn disarm(&mut self) {
        self.0 = None;
    }
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        if let Some(token) = &self.0 {
            token.cancel();
        }
    }
}

pub async fn input(
    mut stdin: ChildStdin,
    body: Body,
    length: u64,
    session: Arc<Session>,
    request_cancel: CancellationToken,
    idle: Duration,
) -> Result<(), Failure> {
    let mut stream = body.into_data_stream();
    let mut transferred = 0u64;
    let mut deadline = Instant::now() + idle;
    loop {
        let next = tokio::select! {
            biased;
            _ = session.stop.cancelled() => return Err(Failure::new("input_stream_failed", "session stopped during upload")),
            _ = request_cancel.cancelled() => return Err(Failure::new("input_stream_failed", "upload request disconnected")),
            next = tokio::time::timeout_at(deadline, stream.next()) => {
                next.map_err(|_| Failure::new("idle_timeout", "no incoming upload data before input idle timeout"))?
            }
        };
        let Some(chunk) = next else {
            if transferred != length {
                return Err(Failure::new(
                    "input_stream_failed",
                    format!("premature upload EOF: expected {length} bytes, piped {transferred}"),
                ));
            }
            // ChildStdin has no userspace buffering: dropping it delivers EOF.
            drop(stdin);
            return Ok(());
        };
        let chunk = chunk
            .map_err(|e| Failure::new("input_stream_failed", format!("upload body failed: {e}")))?;
        if chunk.is_empty() {
            continue;
        }
        let end = transferred
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| Failure::new("input_stream_failed", "upload length overflow"))?;
        if end > length {
            return Err(Failure::new(
                "input_stream_failed",
                "upload exceeds declared Content-Length",
            ));
        }
        let mut offset = 0;
        while offset < chunk.len() {
            let until = chunk.len().min(offset + CHUNK_SIZE);
            let count = tokio::select! {
                biased;
                _ = session.stop.cancelled() => return Err(Failure::new("input_stream_failed", "session stopped during upload")),
                _ = request_cancel.cancelled() => return Err(Failure::new("input_stream_failed", "upload request disconnected")),
                count = stdin.write(&chunk[offset..until]) => count.map_err(|e| Failure::new("input_stream_failed", format!("FFmpeg stdin write failed: {e}; input must support non-seekable decoding (use MPEG-TS or streaming MKV)")))?,
            };
            if count == 0 {
                return Err(Failure::new(
                    "input_stream_failed",
                    "FFmpeg stdin closed during upload",
                ));
            }
            session.received(count)?;
            offset += count;
            transferred = transferred
                .checked_add(count as u64)
                .ok_or_else(|| Failure::new("input_stream_failed", "upload counter overflow"))?;
        }
        // Time blocked by FFmpeg/HTTP output backpressure is NOT input idleness.
        deadline = Instant::now() + idle;
    }
}

pub async fn output(
    mut stdout: ChildStdout,
    sender: mpsc::Sender<Bytes>,
    session: Arc<Session>,
) -> Result<(), Failure> {
    loop {
        let mut bytes = BytesMut::with_capacity(CHUNK_SIZE);
        let count = tokio::select! {
            biased;
            _ = session.stop.cancelled() => return Ok(()),
            count = stdout.read_buf(&mut bytes) => count.map_err(|e| Failure::new("output_stream_failed", format!("FFmpeg stdout read failed: {e}")))?,
        };
        if count == 0 {
            return Ok(());
        }
        session.produced(count)?;
        tokio::select! {
            biased;
            _ = session.stop.cancelled() => return Ok(()),
            sent = sender.send(bytes.freeze()) => {
                if sent.is_err() {
                    return Err(Failure::new("output_disconnected", "output consumer disconnected"));
                }
            }
        }
    }
}

pub struct OutputStream {
    // A stalled HTTP connection must not extend terminal metadata retention.
    session: Weak<Session>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    done: bool,
}
impl OutputStream {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            cancelled: Box::pin(session.stop.clone().cancelled_owned()),
            session: Arc::downgrade(&session),
            done: false,
        }
    }
}
impl Stream for OutputStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let Some(session) = this.session.upgrade() else {
            this.done = true;
            return Poll::Ready(Some(Err(io::Error::other("session retention expired"))));
        };
        if this.cancelled.as_mut().poll(cx).is_ready() {
            this.done = true;
            session.meta.lock().unwrap().output.connected = false;
            return Poll::Ready(Some(Err(io::Error::other(
                "transcode failed or was cancelled; inspect session status",
            ))));
        }
        let next = {
            let mut output = session.output.lock().unwrap();
            match output.as_mut() {
                Some(receiver) => receiver.poll_recv(cx),
                None => Poll::Ready(None),
            }
        };
        match next {
            Poll::Ready(Some(bytes)) => {
                if let Err(error) = session.delivered(bytes.len()) {
                    session.fail(error);
                    this.done = true;
                    return Poll::Ready(Some(Err(io::Error::other("output counter overflow"))));
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(None) => {
                // Cleanup may have removed the receiver between the cancellation
                // check and poll_recv. Never turn that race into a clean EOF.
                if session.stop.is_cancelled() {
                    this.done = true;
                    return Poll::Ready(Some(Err(io::Error::other(
                        "transcode stopped before response EOF",
                    ))));
                }
                this.done = true;
                let mut meta = session.meta.lock().unwrap();
                meta.output.connected = false;
                meta.output_body_complete = true;
                meta.pending_since = None;
                drop(meta);
                session.changed.notify_one();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for OutputStream {
    fn drop(&mut self) {
        if !self.done {
            if let Some(session) = self.session.upgrade() {
                session.fail(Failure::new(
                    "output_disconnected",
                    "output response dropped before complete EOF; reattachment is unsupported",
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropping_unpolled_response_cancels_session() {
        let (session, _control) = Session::new("h264-cpu-mkv".into());
        let response = OutputStream::new(session.clone());
        drop(response);
        assert!(session.stop.is_cancelled());
        assert_eq!(
            session
                .meta
                .lock()
                .unwrap()
                .stop
                .as_ref()
                .unwrap()
                .error
                .as_ref()
                .unwrap()
                .code,
            "output_disconnected"
        );
    }

    #[tokio::test]
    async fn cancellation_aborts_instead_of_clean_eof_even_with_queued_bytes() {
        let (session, _control) = Session::new("h264-cpu-mkv".into());
        let (sender, receiver) = mpsc::channel(OUTPUT_CHUNKS);
        *session.output.lock().unwrap() = Some(receiver);
        sender
            .send(Bytes::from_static(b"partial media"))
            .await
            .unwrap();
        let mut response = OutputStream::new(session.clone());
        session.fail(Failure::new("ffmpeg_failed", "encoder exited nonzero"));
        assert!(response.next().await.unwrap().is_err());
        assert!(!session.meta.lock().unwrap().output_body_complete);
    }

    #[tokio::test]
    async fn response_eof_waits_for_supervisor_gate() {
        let (session, _control) = Session::new("h264-cpu-mkv".into());
        let (gate, receiver) = mpsc::channel(OUTPUT_CHUNKS);
        *session.output.lock().unwrap() = Some(receiver);
        let mut response = OutputStream::new(session.clone());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), response.next())
                .await
                .is_err()
        );
        assert!(!session.meta.lock().unwrap().output_body_complete);
        drop(gate);
        assert!(response.next().await.is_none());
        assert!(session.meta.lock().unwrap().output_body_complete);
        drop(response);
        assert!(!session.stop.is_cancelled());
    }

    #[tokio::test]
    async fn unpolled_response_does_not_retain_expired_metadata() {
        let (session, _control) = Session::new("h264-cpu-mkv".into());
        let weak = Arc::downgrade(&session);
        let mut response = OutputStream::new(session);
        assert!(weak.upgrade().is_none());
        assert!(response.next().await.unwrap().is_err());
    }
}
