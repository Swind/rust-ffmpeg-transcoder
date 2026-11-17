# Streaming Transcoder — Architecture & Implementation Specification

**Status:** MVP implementation handoff  
**Language:** Rust  
**Target:** Linux / Docker; AMD GPU host (MAX+395) with FFmpeg/VAAPI  
**Version:** 1.0 (2026-11-17)

> **Instructions for implementing AI agent:** Implement the MVP described here rather than a generalized media platform. Respect the explicit non-goals, API contract, concurrency and memory limits, and acceptance criteria. If a design choice is unspecified, prefer the simplest safe, testable implementation and document the decision. Do not silently add persistence, resumable uploads, custom TCP protocols, or a full media library.

## 1. Objective

Build a lightweight, single-node **on-demand real-time video transcoding service**. A client on another computer (e.g. N6000) creates a conversion session, waits for capacity, opens a streaming download, and concurrently uploads input media. The server on the AMD GPU host runs one FFmpeg subprocess per active session, pipes incoming bytes into FFmpeg stdin, and streams stdout back to the client **while input is still arriving**.

The server should support bounded-memory backpressure all the way from the output consumer to the input producer. No full-file upload or download staging on the server is required.

### 1.1 MVP goals

- Session-oriented HTTP API: create, inspect, stream upload, stream download, cancel.
- Per-session FFmpeg child process with **stdin / stdout / stderr** pipes, not UDS or FIFOs for MVP.
- Simple FIFO scheduler with configurable maximum concurrent jobs and queue limit.
- Exactly one input producer and one output consumer per session.
- Concurrent upload and download over independent HTTP requests/connections.
- Bounded buffering, no unbounded channels, no full-body materialization (`Bytes`, `collect`, temp files).
- Structured status, byte counters, timeout/cancellation handling, graceful server shutdown.
- CPU H.264 profile to test everywhere; optional AMD VAAPI H.264 profile for production deployment.
- Docker setup with optional `/dev/dri` mapping, health endpoint, README, automated integration tests.

### 1.2 Explicit non-goals (MVP)

- Resume upload/download, HTTP Range support, random access/seek, persistent sessions.
- Restarting a failed FFmpeg process from an earlier offset; surviving service restart.
- Multi-node scheduling, Redis, SQLite, Kubernetes orchestration, shared storage.
- HLS/DASH, browser adaptive bitrate, a media library, thumbnail extraction, arbitrary FFmpeg CLI injection.
- Arbitrary consumer fan-out, upload-before-ready buffering, HTTP proxy deployment by default.
- Guaranteed compatibility with every source container (notably MP4 requiring seek or tail metadata).
- Benchmark-derived claims about MAX+395 GPU capabilities before actual host testing.

## 2. Architecture

```text
                  CONTROL PLANE (small JSON requests)
Client  ───────── POST /v1/sessions ──────────────┐
Client  ───────── GET  /v1/sessions/{id} ─────────┤
Client  ───────── DELETE /v1/sessions/{id} ───────┤
                                                  ▼
                                       Session Manager
                                       FIFO Scheduler
                                       Concurrency Permits
                                                  │
                                                  ▼
                                   Session Worker / Supervisor

             DATA PLANE (two concurrent HTTP requests)
Client PUT /v1/sessions/{id}/input     Client GET /v1/sessions/{id}/output
               │                                    ▲
               ▼                                    │
    request-body stream                    response-body stream
               │                                    ▲
               ▼                                    │
       bounded chunking                   bounded chunking
               │                                    ▲
               ▼                                    │
        FFmpeg stdin ── decode/encode ──► FFmpeg stdout
                            │
                         FFmpeg stderr
                            │
                     bounded log tail/status
```

Use Tokio async I/O and Axum HTTP. **No application-level byte ring buffer is necessary**: await the source stream and destination write on each direction, letting pipe capacity, HTTP body polling, socket send buffers and TCP flow control provide backpressure. Use small fixed write/read chunks or bounded streaming adapters where necessary; do not spawn detached tasks for each chunk.

**Important:** output backpressure can stall FFmpeg stdout, which stalls FFmpeg stdin, which stalls HTTP upload. This is expected. It becomes a permanent wait if the client uploads to completion before it starts reading output. Document and test the **mandatory concurrent consumer** requirement.

## 3. Technology choices

- Rust stable (edition 2024, or edition 2021 if compatibility requires).
- `tokio` (`process`, `io-util`, `sync`, `time`, `signal`) for runtime, pipes, child lifecycle.
- `axum` + `hyper`/`http-body-util` as required by the chosen Axum version; `Body::into_data_stream()` for incoming body and `Body::from_stream()` for outgoing bytes.
- `tokio-util` for `CancellationToken` and optional ReaderStream.
- `futures-util` for bounded stream transformations, `bytes`, `serde`, `serde_json`, `uuid`, `tracing`, `tracing-subscriber`, `thiserror`.
- Optional `tower-http` for request tracing; **do not** enable upload-body buffering or request decompression.
- FFmpeg installed in host/container. Probe available encoders on startup or via explicit profile validation.

Use compatible current crate versions and commit `Cargo.lock`. Prefer simple owned state (`Arc<AppState>`, `Mutex`/`RwLock` for metadata only). **Never hold a global/session state mutex across network I/O or `.await` of pipe reads/writes.**

## 4. API contract

Prefix: `/v1`. Request and error JSON: `application/json`. Stream upload body: `application/octet-stream` (contains complete container byte stream). Stream response content type: `video/x-matroska` for MKV.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | API/FFmpeg readiness; optional GPU profile availability |
| `GET` | `/v1/profiles` | List enabled known profiles |
| `POST` | `/v1/sessions` | Create queued session |
| `GET` | `/v1/sessions/{id}` | State, counters, errors |
| `PUT` | `/v1/sessions/{id}/input` | **One** streaming upload, only READY/RUNNING |
| `GET` | `/v1/sessions/{id}/output` | **One** streaming output, only READY/RUNNING/DRAINING; attach before upload |
| `DELETE` | `/v1/sessions/{id}` | Cancel session (idempotent) |

### 4.1 Create

Request:

```http
POST /v1/sessions HTTP/1.1
Content-Type: application/json
Authorization: Bearer <token>

{"profile":"h264-cpu-mkv"}
```

Response: `201 Created`

```json
{
  "id": "b7cb111a-61d2-44db-b3f5-71558f354ac5",
  "state": "queued",
  "profile": "h264-cpu-mkv",
  "status_url": "/v1/sessions/b7cb111a-61d2-44db-b3f5-71558f354ac5",
  "input_url": "/v1/sessions/b7cb111a-61d2-44db-b3f5-71558f354ac5/input",
  "output_url": "/v1/sessions/b7cb111a-61d2-44db-b3f5-71558f354ac5/output"
}
```

- An ID is an identifier, **not** sufficient authorization. Require a configured bearer token for all non-health endpoints (constant-time comparison recommended). Local-only testing may explicitly disable auth; refuse to bind on non-loopback interfaces with auth disabled unless explicitly opted in.
- Profiles come from an allowlist. Unknown profile: `422`. Queue full: `429` or `503` with a machine-readable `queue_full` error and optional `Retry-After`.
- IDs should be non-guessable UUIDv4 and should not appear in URL logs by default.
- Queued sessions have no upload facility until READY. The scheduler allocates a permit and spawns FFmpeg when transitioning to READY.

### 4.2 Inspect

Response example:

```json
{
  "id": "b7cb111a-61d2-44db-b3f5-71558f354ac5",
  "state": "running",
  "profile": "h264-cpu-mkv",
  "created_at": "2026-11-17T00:00:00Z",
  "updated_at": "2026-11-17T00:00:03Z",
  "input": {"connected": true, "received_bytes": 8388608},
  "output": {"connected": true, "written_bytes": 4194304},
  "ffmpeg": {"pid": 12345, "exit_code": null},
  "error": null
}
```

- `received_bytes` means bytes **successfully written to child stdin**, not merely bytes received by the HTTP stack. Document this definition; without durable buffering there is no independently acknowledged persistent offset.
- `written_bytes` means bytes successfully handed to HTTP response machinery, **not proof of remote durable receipt**.
- Counters are advisory (monitoring only; no resume).

### 4.3 Upload

- Only one active producer. Second PUT: `409 input_already_attached`.
- Only permitted when `ready`/`running`; `queued`: `409 not_ready` with status URL. Terminal state: `410`.
- **For MVP, require `Content-Length` for the incoming upload**; reject missing length with `411 Length Required` (simplifies premature-EOF semantics and CLI tests). Reject `Content-Encoding` other than identity. No global body-size cap for streaming uploads; per-session size/time limits are configurable.
- Prefer rejecting invalid headers/session before consuming the body. Do not buffer the body to validate it.
- Stream chunks in order to child stdin. On successful end-of-body AND exact expected length, close/drop stdin to signal FFmpeg EOF.
- If connection drops/stream errors/short body, mark session FAILED, cancel output and terminate/reap child (no resume).
- A PUT success response (`204`) means upload bytes reached the FFmpeg stdin pipe and stdin was closed, **not** that the complete transcode has succeeded. Read GET status for final result.
- If upload exceeds declared size, reject/abort session; update counters using checked arithmetic.
- Do **not** implement request body collection or transparently retry a partial upload.

### 4.4 Download

- Client should attach GET before sending PUT. Only one GET at a time; second: `409 output_already_attached`.
- In READY, allow attaching the output request and waiting for FFmpeg bytes. On first headers, send `200 OK`, `Content-Type: video/x-matroska`, `Cache-Control: no-store`, and no `Content-Length`.
- Stream FFmpeg stdout into HTTP response with backpressure. Never retain all output in memory.
- When FFmpeg exits successfully, stdout is EOF, and all produced bytes have been handed to the response, end the stream normally and set session COMPLETED.
- **An error occurring after HTTP 200 response headers cannot be represented as a new JSON error status.** Abort/truncate the stream and mark FAILED; client must validate container via ffprobe and/or query final session state.
- If GET disconnects while still producing, mark FAILED/CANCELLED according to a documented choice (recommend `failed: output_disconnected`) and terminate FFmpeg. Reattaching is unsupported.
- **Avoid false success if stdout reaches EOF before FFmpeg actually exits**: await child exit status as part of the completion condition.
- Output GET can be empty until upload begins; add client read timeout guidance.

### 4.5 Cancellation and errors

`DELETE` is idempotent: transition a nonterminal session to CANCELLED; queued work is removed or skipped; active child is stopped and reaped. Return `202 Accepted` or `204 No Content` consistently. Terminal sessions retain metadata for configured short TTL.

Error envelope:

```json
{"error":{"code":"not_ready","message":"session is waiting for capacity"}}
```

Use stable codes: `unauthorized`, `invalid_profile`, `queue_full`, `not_found`, `not_ready`, `input_already_attached`, `output_already_attached`, `session_terminal`, `missing_content_length`, `input_stream_failed`, `output_disconnected`, `ffmpeg_failed`, `idle_timeout`, `session_timeout`.

## 5. Session lifecycle and invariants

States:

```text
CREATED (internal, brief) → QUEUED → READY → RUNNING → DRAINING → COMPLETED
                           \         \        \          \
                            └─────────┴────────┴──────────→ FAILED / CANCELLED / EXPIRED
```

- `queued`: waiting for a permit; no FFmpeg process and no active upload.
- `ready`: permit assigned and FFmpeg running; awaiting initial client attachment (output first recommended).
- `running`: upload active and/or bytes are flowing.
- `draining`: valid upload EOF reached, stdin closed; FFmpeg is flushing codecs/muxer and HTTP is draining output.
- `completed`: upload accepted in full; FFmpeg exit code 0; stdout EOF; HTTP body stream has fully completed without observed delivery error. **Cannot guarantee that a client wrote the bytes to durable storage.**
- `failed`, `cancelled`, `expired`: terminal; no child process and no permit held.

Key invariants:

1. Every active session holds exactly one scheduler permit and at most one child FFmpeg process.
2. One input owner and one output owner, chosen atomically; attach once only even after disconnect (no resume).
3. Each terminal transition happens once; failed state must never later become completed.
4. No child process may outlive terminal cleanup or application shutdown.
5. No unbounded queue, no stream data persisted to disk by this service.
6. API handlers never wait on FFmpeg while holding session locks.
7. The scheduler permit is released **only after child termination/reaping and I/O task cleanup**.

Implement transitions in one supervisor component; do not let independent upload, output, and waiter tasks overwrite one another's terminal state.

## 6. Scheduler

- FIFO queue in memory. Defaults: `max_concurrent_jobs=1`, `max_queued_jobs=16` (configurable).
- Session creation enqueues work and returns immediately; scheduler assigns permit, starts child, then transitions READY.
- `ready_attach_timeout=30s`: terminal if no output or input started within the window; recommend requiring output attach before input begins for MVP (enforced or clearly documented).
- `input_idle_timeout=120s` after input attaches; do **not** treat backpressure-induced blocking on child stdin as a disconnected client. Idle timeout should reflect lack of input progress with care, and configurable.
- `output_stall_timeout=120s` for no output write progress once output would otherwise be produced; distinguish slow legitimate codecs vs stuck consumers.
- `max_session_duration=2h` by default (configurable, disableable).
- `terminal_retention=10m`, then metadata removed.
- On shutdown: stop accepting new sessions, cancel queued sessions, signal all workers, grace period, then kill remaining process groups/reap.

Avoid eager FFmpeg process startup for queued sessions; start only when assigned a slot. Do not treat mere HTTP connection presence as progress indefinitely.

## 7. FFmpeg profiles

Profiles are **server-owned allowlisted command argument arrays**, never a single shell interpolated string. Spawn FFmpeg directly through Tokio `Command` (no `sh -c`).

### 7.1 CPU baseline (must implement first)

```bash
ffmpeg -hide_banner -nostdin -loglevel warning \
  -i pipe:0 \
  -map 0:v:0 -map 0:a? \
  -c:v libx264 -preset veryfast -pix_fmt yuv420p \
  -c:a aac -b:a 128k \
  -f matroska pipe:1
```

Why AAC instead of audio copy in baseline: unknown audio codecs may be incompatible with some output containers/clients; a predictable baseline avoids confusing test results. Define video-only input handling and optional audio mapping. MKV output is streamable to stdout without seeking.

### 7.2 AMD VAAPI profile (validate on target host)

**This is an illustrative profile, not a verified MAX+395 command:**

```bash
ffmpeg -hide_banner -nostdin -loglevel warning \
  -vaapi_device /dev/dri/renderD128 \
  -i pipe:0 \
  -map 0:v:0 -map 0:a? \
  -vf 'format=nv12,hwupload' \
  -c:v h264_vaapi -qp 23 \
  -c:a aac -b:a 128k \
  -f matroska pipe:1
```

- Configure VAAPI render node (`/dev/dri/renderD128` only an example), do not assume its index.
- Start with software decode → `format=nv12,hwupload` → hardware encode to reduce initialization complexity. Hardware decoding can be optimized later.
- Validate encoder existence (`ffmpeg -encoders`), render node, `vainfo`, running encode against synthetic fixture, device permissions and performance. Fail readiness for VAAPI profile if prerequisites missing; CPU profile remains available.
- Use explicit FFmpeg options for quality, video scale, bit rate as **fixed per-profile parameters**. Any parameter exposed to users must be validated against numeric bounds.
- Ensure stderr is always concurrently drained, with bounded recent log tail. Avoid filling stderr pipe and freezing process.
- Optionally add `-progress pipe:2 -nostats` in a later iteration after ensuring progress lines and stderr warnings are parsed safely.

### 7.3 Input caveat

`pipe:0` is non-seekable. Some MP4 files (e.g. metadata at the end) cannot be decoded in one forward pass. Do **not** advertise general MP4 streaming support. MVP acceptance fixtures: MPEG-TS and stream-compatible MKV. MP4 test cases should include success/failure examples and helpful failure messages; future option: allow FFmpeg to pull via HTTP Range URL instead of PUSH pipes.

### 7.4 Optional fMP4 (post-MVP)

```bash
-movflags +frag_keyframe+empty_moov -f mp4 pipe:1
```

Only enable via its own profile and automated compatibility tests. MP4 fragmentation is not a drop-in replacement for a conventional seekable MP4 file.

## 8. Streaming implementation details

**Upload:**

1. Atomically reserve input ownership and validate session/profile/length.
2. Get an owned handle to child stdin from supervisor. Avoid locking the entire session during writes.
3. Read `axum::body::Body::into_data_stream()` incrementally; write each chunk with cancellation-aware async write.
4. Update `received_bytes` after successful write, not on receipt of bytes in HTTP buffers.
5. On correct length and graceful body EOF, drop/close stdin and notify supervisor `InputCompleted`.
6. On any stream error, premature EOF, excessive length, client disconnect, timeout, notify supervisor `InputFailed` and terminate.

**Download:**

1. Reserve output ownership, get child stdout as a single owner.
2. Create an HTTP response using `Body::from_stream` around an async stream that repeatedly reads a bounded byte chunk and yields it.
3. A consumer disconnect normally causes the body/stream to be dropped: include a cancellation/drop guard that notifies the supervisor. Distinguish fully completed EOF from premature drop.
4. Supervisor waits for child termination and output pump completion; avoid deadlock caused by waiting for FFmpeg exit before draining stdout.
5. There is no `Content-Length`; response finishes when output stream EOF occurs.

**Concurrency rules:**

- Input pump, output pump, stderr pump and child waiter run concurrently with one supervisor coordinating terminal conditions.
- Use cancellation tokens and owned handles to avoid uncollectable background tasks.
- Ensure backpressure by **not** polling the next body chunk while blocked on writing the previous chunk; likewise do not keep reading child stdout while response consumer is stalled.
- Avoid unbounded `mpsc` and `broadcast` carrying media bytes. A bounded channel of capacity 1–4 chunks is acceptable if required by framework architecture, but not needed by default.
- FFmpeg may internally buffer substantial data; bounded application buffers do not imply constant end-to-end latency, so measure latency under real inputs.
- Avoid blocking mutexes around I/O, nested Tokio runtimes, blocking `std::process::Command`, `wait_with_output` (collects entire output), or `read_to_end` for media.

## 9. Client sequencing and examples

**Required order:**

1. `POST /v1/sessions` with profile.
2. Poll `GET /v1/sessions/{id}` until READY (or terminal).
3. Start `GET /v1/sessions/{id}/output` **in the background**, and ensure response reader is active.
4. Start `PUT /v1/sessions/{id}/input` in parallel; do not wait for it before reading output.
5. Wait for upload success, download EOF, and final session state `completed`.
6. If any stage fails, delete/cancel session. Remove partial client output unless user opts to retain it for diagnostics.

Example (`TOKEN` supplied securely; example assumes a generated READY session ID):

```bash
BASE=http://max395:8080
ID=replace-with-real-id
TOKEN=replace-with-token

# Terminal 1: START THIS FIRST. The local result is only a client-side file.
curl --fail-with-body -N \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/sessions/$ID/output" \
  --output converted.mkv

# Terminal 2: run concurrently after output GET has attached.
curl --fail-with-body --http1.1 \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/octet-stream' \
  --upload-file source.ts \
  "$BASE/v1/sessions/$ID/input"

# After both complete:
curl -H "Authorization: Bearer $TOKEN" "$BASE/v1/sessions/$ID"
ffprobe -v error -show_streams converted.mkv
```

Note: connection buffering by reverse proxies/load balancers can defeat low-latency streaming. MVP should be tested **directly** against the service, not behind a proxy. If later deployed behind nginx, turn off request/response buffering for streaming routes and set appropriate idle timeouts. Authenticate all routes and secure transport via TLS or a trusted tunnel/VPN; avoid plaintext bearer tokens over untrusted networks.

Provide a small **Rust CLI client** (preferred) or test harness that performs the entire sequence concurrently and returns a nonzero exit status on any failure. This avoids fragile shell process orchestration and demonstrates intended SDK integration.

## 10. Configuration

Example `config.toml`:

```toml
[server]
bind = "127.0.0.1:8080"
# For container deployment override bind with 0.0.0.0:8080.

[security]
# Supply token from environment, not checked-in config.
token_env = "TRANSCODER_API_TOKEN"
allow_insecure_no_auth = false

[scheduler]
max_concurrent_jobs = 1
max_queued_jobs = 16

[timeouts]
ready_attach_secs = 30
input_idle_secs = 120
output_stall_secs = 120
max_session_secs = 7200
terminal_retention_secs = 600
shutdown_grace_secs = 10

[ffmpeg]
binary = "/usr/bin/ffmpeg"
vaapi_device = "/dev/dri/renderD128"
max_stderr_tail_bytes = 65536

[profiles]
enabled = ["h264-cpu-mkv"]
# Enable "h264-vaapi-mkv" only after host capability probe passes.
```

Enforce sane limits for JSON request size, authentication attempts (where appropriate), session count, PID count, and per-session resources. Do not rely on untrusted HTTP `Content-Length` as a resource allocation hint.

## 11. Security & deployment

- Restrict endpoints to a trusted network or VPN. Prefer TLS termination or private overlay network when bearer tokens are used.
- Do not accept arbitrary input URLs, file paths, codec names, FFmpeg arguments, output paths or executable paths from clients. This avoids SSRF, filesystem access and option injection.
- Generate argument vectors with validated server-side profiles and fixed `pipe:0`/`pipe:1` endpoints; invoke without a shell.
- Run container as non-root where practical; grant only `/dev/dri/renderD*` device permissions required by VAAPI. Actual container user must belong to the right render/video group IDs.
- Never mount the host Docker socket. Avoid privileged mode. Use `no-new-privileges`, dropped caps, read-only root FS if supported, seccomp/default resource limits.
- Monitor per-process memory and GPU usage; set Docker CPU/RAM/PID limits where practical.
- Set Docker stop signals and termination grace. A parent-only process kill may leave children if launching a wrapper; spawn FFmpeg directly and consider process-group cleanup for future expansion.
- Sanitize or bound all stderr/log output (clients control media metadata and may indirectly affect logs).

### Compose sketch

```yaml
services:
  transcoder:
    build: .
    ports:
      - "8080:8080"
    environment:
      TRANSCODER_API_TOKEN: ${TRANSCODER_API_TOKEN:?required}
    volumes:
      - ./config.toml:/etc/transcoder/config.toml:ro
    # Uncomment only for verified AMD VAAPI environment:
    # devices:
    #   - /dev/dri:/dev/dri
    # group_add:
    #   - "<host-render-group-gid>"
    security_opt:
      - no-new-privileges:true
    init: true
    restart: unless-stopped
```

For CPU-only local tests use default `127.0.0.1` binding or docker binding as appropriate; ensure the server config and compose published port agree. For VAAPI, verify Mesa/VA driver support and encoder capabilities inside the actual image before enabling profile. Keep a dev Dockerfile and a GPU-capable Dockerfile if necessary; do not assume all distro packages expose the same encoder support.

## 12. Suggested module structure

```text
streaming-transcoder/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── config.example.toml
├── Dockerfile
├── compose.yaml
├── src/
│   ├── main.rs
│   ├── config.rs
│   ├── api.rs
│   ├── auth.rs
│   ├── error.rs
│   ├── session.rs          # state machine, metadata, ownership flags
│   ├── scheduler.rs        # FIFO, permits, queues
│   ├── supervisor.rs       # FFmpeg child and task lifecycle
│   ├── ffmpeg.rs           # trusted profile -> Command args, probe
│   └── stream.rs           # upload/download pump and cancellation
├── tests/
│   ├── api_contract.rs
│   ├── streaming_e2e.rs
│   ├── backpressure.rs
│   └── cancellation.rs
└── scripts/
    ├── generate-fixtures.sh
    └── smoke-test.sh
```

Internal suggested types:

```rust
struct AppState { /* sessions, scheduler, config */ }
struct Session { /* immutable ID/profile, metadata, owned worker handles */ }
enum SessionState { Queued, Ready, Running, Draining, Completed, Failed, Cancelled, Expired }
struct Worker { /* child, stdin/stdout ownership, join handles, cancel token */ }
struct SessionMetrics { /* received bytes, produced/delivered bytes, timestamps */ }
```

Use a single worker supervisor to own the process and coordinate child reaping; handlers should communicate via channels/one-shot handoff rather than both mutating child handles. Exactly how pipe ownership is handed to handlers is an internal choice, but should preserve invariants and be tested under races.

## 13. Implementation phases (do in order)

### Phase 0 — Repo bootstrap & validation

- Bootstrap Rust workspace, config, tracing, HTTP health endpoints, unit test infrastructure.
- Probe `ffmpeg -version`, `ffmpeg -encoders`, `ffprobe` availability.
- Make a standalone shell proof of concept: `pipe:0` → CPU libx264 → `pipe:1` using MKV/TS fixture.
- Document known input-container limitations.

**Exit:** `cargo test` and local FFmpeg pipeline succeed.

### Phase 1 — Single-session streaming, no queue

- Create session, mark READY; implement one FFmpeg subprocess.
- Start output GET reader first; allow input PUT with Content-Length.
- Concurrent input/output/stderr tasks, clean input EOF, detect exit status, accurate terminal success/failure.
- Cancellation on client disconnect and child cleanup; no file spooling.

**Exit:** output bytes arrive **before** entire input has been uploaded in a throttled-input test; `ffprobe` validates completed output.

### Phase 2 — Scheduler and resilience

- Implement queue and max concurrency, session timeouts, explicit cancel, attach race protection.
- Add resource cleanup, test denied concurrent upload/download attachments, test queue full.
- Add structured logs and status counters. Handle shutdown/termination and PID reaping.

**Exit:** run multiple simultaneous clients without exceeding `max_concurrent_jobs`; injected failures and disconnects leave no FFmpeg processes.

### Phase 3 — AMD GPU and deployment

- Implement optional VAAPI profile, encoder probe, configure render node, GPU Docker image + compose.
- Test in actual MAX+395 Docker environment, then add measured performance statistics in docs.
- Provide example CLI, operational README and failure diagnostics.

**Exit:** `h264-vaapi-mkv` produces valid H.264 MKV via streamed upload/download on target machine; CPU fallback works when GPU profile disabled.

## 14. Automated acceptance tests

Every important test should be repeatable locally without GPU; GPU tests are opt-in / CI tagged.

| # | Test | Expected result |
|---|---|---|
| T01 | Create session / inspect | 201 + ID, queued/ready status |
| T02 | Invalid profile / auth | 422 / 401, no child launched |
| T03 | GET output first, slow PUT input | output bytes received before input finishes |
| T04 | CPU video-only MPEG-TS input | valid nonempty MKV and H.264 video via ffprobe |
| T05 | Input with audio | valid video+AAC output, timestamps plausible |
| T06 | Missing Content-Length | 411 without FFmpeg stdin corruption |
| T07 | Two PUTs / two GETs | second attachment rejected 409 |
| T08 | Client input disconnect midway | terminal failure; child killed/reaped; permit freed |
| T09 | Client output disconnect midway | terminal failure; producer stopped; child reaped |
| T10 | FFmpeg nonexistent/crash/nonzero exit | session failed; useful bounded stderr diagnostics |
| T11 | Slow download reader | memory remains bounded; upload eventually throttles; no deadlock if consumer keeps reading |
| T12 | Client never attaches to READY job | times out, permit reclaimed |
| T13 | max_concurrent_jobs=1, create three sessions | only one FFmpeg process active at any moment; FIFO fairness |
| T14 | Cancel queued and active jobs | terminal states correct, no leaked tasks/PIDs |
| T15 | Server SIGTERM | graceful drain or forced cleanup within configured timeout |
| T16 | `ffprobe` output / final GET status | exit 0 and complete output before COMPLETED |
| T17 | MP4 non-seekable unsupported case | deterministic failure with clear diagnostic; no hang |
| T18 | GPU-only smoke test | VAAPI render node and encoder work in Docker |

**Streaming proof test:** generate a multi-minute MPEG-TS sample, throttle client PUT to e.g. 1 MiB/s and assert the response's first bytes arrive **before** the upload completes. Instrument timings rather than relying only on output file existence. Use an output reader that continuously consumes data. For backpressure tests, deliberately delay the output reader and observe stable app memory usage and stalled producer; later resume reader and assert completion. Include timeouts so CI never hangs indefinitely.

## 15. Operational observability

- Per-request tracing with session ID (avoid logging bearer tokens or full media metadata).
- Per-session timings: queue wait, startup, first-input-byte, first-output-byte, time-to-complete.
- Counters: input bytes piped, output bytes produced/delivered, active/queued jobs, failures by category.
- FFmpeg PID and exit code, bounded recent stderr (do not collect unbounded logs).
- Optional Prometheus `/metrics` after MVP; not required for first success.

## 16. Important correctness pitfalls

1. **Do not assume one HTTP request can safely upload and download through a generic curl call.** Use two concurrent independent requests.
2. **Do not buffer the entire HTTP body.** Many body helpers collect; use streaming extraction.
3. **Do not call `wait_with_output()`** for FFmpeg when stdout contains media: it gathers stdout in memory.
4. **Do not call `child.wait()` before draining stdout/stderr.** Deadlock risk.
5. **Dropping Tokio `Child` does not kill it by default.** Set `kill_on_drop(true)` as a safeguard, but still explicitly kill/wait/reap and release permits in controlled cleanup.
6. **HTTP success headers can precede FFmpeg failure.** Clients must verify final session status; a streaming HTTP 200 alone is insufficient.
7. **A stalled download can stop upload via backpressure**; that's intended. Require simultaneous consumers and define stall timeout.
8. **Non-seekable MP4 is not universally supported.** First test with TS/MKV inputs.
9. **Do not trust arbitrary user-provided FFmpeg args or URL paths.** Use profile allowlist and fixed pipes.
10. **Do not mark completed when upload merely ends.** Wait for successful encoder exit and completed output pipeline.
11. **Do not mistake successful `write()` to HTTP framework for durable client receipt.** Download durability requires an explicit client acknowledgement, intentionally outside MVP.
12. **Do not silently retry sessions after disconnect.** Failure is terminal; client creates a fresh session.

## 17. Definition of Done

Deliver a buildable Rust repo with:

- All MVP API endpoints, documented OpenAPI or concise API documentation.
- Session lifecycle + FIFO queue, bounded data flow, correct cancellation/reaping.
- CPU profile passing automated streaming, backpressure and failure tests.
- Optional VAAPI profile and Docker device configuration, clearly marking what was actually tested on MAX+395.
- `README.md` with quick start, two-terminal curl example, recommended single CLI wrapper and known limitations.
- Dockerfile, compose file, example config, health checks.
- No full-file buffering or server-side media persistence.
- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` passing (or documented toolchain-specific exceptions).

## 18. References

- FFmpeg protocols (pipes, nonseekable input): https://ffmpeg.org/ffmpeg-protocols.html
- FFmpeg container formats and fragmented MP4: https://ffmpeg.org/ffmpeg-formats.html
- Tokio process cancellation/reaping: https://docs.rs/tokio/latest/tokio/process/
- Axum streaming request and response body: https://docs.rs/axum/latest/axum/body/struct.Body.html
- FFmpeg VAAPI example: https://github.com/FFmpeg/FFmpeg/blob/master/doc/examples/vaapi_encode.c

---

**Agent execution directive:** Start implementing Phase 0 and Phase 1 immediately. Make code changes and run tests rather than returning only a plan. Proceed to Phase 2 after Phase 1 passes. Phase 3 AMD validation requires a real host with accessible `/dev/dri`; do not claim it is verified on MAX+395 unless it is actually tested there. Report completed files, commands used, tests passed/failed, remaining limitations, and next concrete actions.
