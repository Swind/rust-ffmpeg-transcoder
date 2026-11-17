# HTTP streaming transcoder

A single-node Rust/Tokio service which pipes a streamed HTTP upload through one FFmpeg process and returns Matroska over a **separate, concurrent HTTP download**. The baseline profile produces H.264 video and optional AAC audio; an opt-in VAAPI profile uses software decoding and hardware H.264 encoding.

The server uses Axum for HTTP routing and Tokio for asynchronous I/O and process supervision. The Rust CLI uses Reqwest to upload and download concurrently.

There is no server-side media staging, database, resumability, arbitrary FFmpeg arguments, or media library. Sessions and their FIFO queue exist only in memory. Clients may write their own output files.

## Quick start (Linux, CPU)

Install a Rust toolchain supporting edition 2024, Python 3, and FFmpeg/ffprobe with `libx264` and `aac` encoders. For example on Debian/Ubuntu:

```sh
sudo apt-get install ffmpeg python3
cargo build --bins
cp config.example.toml config.toml
export TRANSCODER_API_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
./target/debug/transcoder-server --config config.toml
```

Keep the server terminal open. In another terminal, export the **same** `TRANSCODER_API_TOKEN` (do not generate a new one), then:

```sh
scripts/generate-fixtures.sh /tmp/transcoder-fixtures --without-backpressure
./target/debug/transcoder-client \
  --base-url http://127.0.0.1:8080 --profile h264-cpu-mkv \
  --input /tmp/transcoder-fixtures/audio.mkv --output converted.mkv
ffprobe -v error -show_streams converted.mkv
```

`converted.mkv` must not already exist. The client streams the input file in bounded chunks, waits for READY, attaches output and receives its headers **before** uploading, then uploads and downloads concurrently. Success requires upload HTTP 204, normal output EOF, and final `completed`. Failures, Ctrl-C, SIGTERM, or the overall deadline trigger best-effort DELETE (up to five additional seconds) and partial-output removal. It never intentionally overwrites an existing output and never retries a partial upload. SIGKILL or power loss cannot run cleanup; delete the client partial file manually in that case.

`--deadline-secs N` defaults to 7500 seconds and covers queueing, attachment, both streams, and final status. Increase it for long queues/jobs. All five main options are required; `--help` lists them. Input must be a regular file because the protocol requires a known `Content-Length`. The client token comes from `TRANSCODER_API_TOKEN`; no token flag is placed in process arguments.

## Architecture and semantics

```text
POST /sessions -> bounded FIFO -> permit -> per-session supervisor + FFmpeg
PUT /input     -> bounded HTTP chunks -> FFmpeg stdin
GET /output    <- bounded HTTP chunks <- FFmpeg stdout
                                       FFmpeg stderr -> bounded diagnostic tail
```

One supervisor owns child termination/reaping, terminal state, and its scheduler permit. Upload, output, and stderr drain concurrently. A terminal state is published only after child/resource cleanup. FFmpeg exit zero alone is insufficient: accepted full upload, stdout EOF, and a successfully finished HTTP body are also required. `completed` does **not** prove durable remote storage.

Media chunks are 32 KiB, with a bounded two-chunk output channel. Fixed profiles use two FFmpeg decoder/encoder threads and a pipe-only input protocol allowlist; these bounds do not limit FFmpeg's internal codec buffers.

States are `queued`, `ready`, `running`, `draining`, then `completed`, `failed`, `cancelled`, or `expired`. A queued job has no FFmpeg child. One input and one output can attach, once only; disconnects cannot resume. A failed session does not later become completed. DELETE is idempotent while retained metadata exists.

Output disconnect becomes `failed` with `output_disconnected`. READY-attachment and overall session deadlines become `expired`; input-idle and output-stall failures become `failed` with `idle_timeout`.

Backpressure propagates from the output socket through stdout, FFmpeg, stdin, and the upload socket. **Never upload to completion before starting the downloader.** FFmpeg and operating-system sockets have their own buffers, so a paused consumer may initially allow substantial input progress. The application does not promise a codec-independent fixed memory size or constant latency. Input-idle timing excludes time blocked writing to FFmpeg; output-stall timing applies when output is waiting for a consumer, not while a codec has produced nothing.

Counters are advisory, not resumable offsets:

- `input.received_bytes`: bytes successfully written to FFmpeg stdin, not simply read by HTTP.
- `output.written_bytes`: bytes handed to HTTP response machinery, not acknowledged durable client receipt.
- `input.connected` / `output.connected`: current attachment state; false after terminal cleanup.
- `ffmpeg.pid`, `ffmpeg.exit_code`, `error`, and bounded diagnostic stderr help distinguish process, stream, and timeout failures.

SIGTERM stops accepting new work, cancels queued/active sessions, then terminates and reaps children within the configured shutdown grace (with forced cleanup if needed).

## HTTP API

All `/v1` routes require `Authorization: Bearer TOKEN`. Session IDs are identifiers, **not credentials**. `/healthz` and `/readyz` are unauthenticated. Use TLS termination or a trusted VPN/tunnel on untrusted networks; the service itself exposes HTTP.

| Method | Route | Result |
|---|---|---|
| GET | `/healthz` | Liveness |
| GET | `/readyz` | Readiness and configured-profile availability |
| GET | `/v1/profiles` | `{"profiles":[{"id":"h264-cpu-mkv","available":true}]}`; unavailable entries include a reason |
| POST | `/v1/sessions` | JSON `{"profile":"h264-cpu-mkv"}`; 201 with `id`, `state`, `profile`, `status_url`, `input_url`, `output_url` |
| GET | `/v1/sessions/{id}` | State, timestamps, counters, child metadata, error |
| GET | `/v1/sessions/{id}/output` | 200 streaming `video/x-matroska`, `Cache-Control: no-store`, no Content-Length |
| PUT | `/v1/sessions/{id}/input` | `application/octet-stream`, **Content-Length required**, identity encoding only; 204 when all bytes reach stdin and stdin closes |
| DELETE | `/v1/sessions/{id}` | 204; terminal metadata remains for retention period; unknown/expired ID is 404 |

PUT is allowed only after the output has attached. A READY output GET returns headers even before any media exists; do not use a short read timeout expecting immediate body bytes. A second PUT/GET is rejected with 409. Queued data requests return 409 `not_ready`; terminal attachments return 410. Invalid/unavailable profiles return 422; exhausted capacity returns `queue_full` (429/503).

Errors use `{"error":{"code":"not_ready","message":"..."}}`. Codes include `unauthorized`, `invalid_profile`, `queue_full`, `not_found`, `not_ready`, `output_not_attached`, `input_already_attached`, `output_already_attached`, `session_terminal`, `missing_content_length`, `input_stream_failed`, `output_disconnected`, `ffmpeg_failed`, `idle_timeout`, and `session_timeout`.

**HTTP 200 is not proof of success.** An error after output headers aborts the body; it cannot be replaced by a JSON error response. Always inspect the final status and validate the output. HTTP 204 for PUT only acknowledges the stdin stream, not a successful conversion.

### Two-terminal curl demonstration

The CLI is safer for routine use. This example requires `curl` and `jq`, and an input file whose byte length is known. Set these in both terminals:

```sh
BASE=http://127.0.0.1:8080
TOKEN=$TRANSCODER_API_TOKEN
```

Create a session and wait for `ready` (stop if a terminal failure is returned):

```sh
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"profile":"h264-cpu-mkv"}' \
  "$BASE/v1/sessions"
ID=replace-with-returned-id
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/sessions/$ID" | jq
```

**Terminal 1, first:** use a new output filename. `-D -` prints response headers; wait for HTTP 200 before starting Terminal 2.

```sh
curl --fail-with-body --http1.1 -N -D - --max-time 7200 \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/sessions/$ID/output" --output converted.mkv
```

**Terminal 2, concurrently:** set the same `ID` and run:

```sh
curl --fail-with-body --http1.1 --max-time 7200 \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/octet-stream' \
  --upload-file /tmp/transcoder-fixtures/video.ts \
  "$BASE/v1/sessions/$ID/input"
# After both requests finish, require state == completed:
curl --fail-with-body -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/sessions/$ID" | jq
ffprobe -v error -show_streams converted.mkv
```

If either request fails, DELETE the session and remove only your partial output:

```sh
curl --fail-with-body -X DELETE -H "Authorization: Bearer $TOKEN" "$BASE/v1/sessions/$ID"
```

## Configuration

`transcoder-server --config PATH` loads TOML; omitting the argument uses defaults. Unknown options, fields, or invalid capacities/timeouts are errors. Start from `config.example.toml`.

| Section/key | Default / meaning |
|---|---|
| `server.bind` | `127.0.0.1:8080`; container must use `0.0.0.0:8080` |
| `security.token_env` | `TRANSCODER_API_TOKEN`; keep the secret out of checked-in TOML |
| `security.allow_insecure_no_auth` | `false`; explicit loopback-only no-token testing mode |
| `security.allow_insecure_non_loopback` | `false`; an additional explicit opt-in for unauthenticated non-loopback binding, not recommended |
| `scheduler.max_concurrent_jobs` | `1`; active FFmpeg slots |
| `scheduler.max_queued_jobs` | `16`; waiting queue limit |
| `scheduler.max_sessions` | `1024`; all metadata, including retained terminal sessions |
| `timeouts.ready_attach_secs` | `30`; both streams must attach, not merely output |
| `timeouts.input_idle_secs` | `120`; waiting for another upload chunk, excluding pipe backpressure |
| `timeouts.output_stall_secs` | `120`; output available but downstream not progressing |
| `timeouts.max_session_secs` | `7200`, from READY through draining; `0` disables |
| `timeouts.terminal_retention_secs` | `600`; then status becomes 404 |
| `timeouts.shutdown_grace_secs` | `10` |
| `ffmpeg.binary` | `/usr/bin/ffmpeg` |
| `ffmpeg.vaapi_device` | `/dev/dri/renderD128`; select the actual render node |
| `ffmpeg.max_stderr_tail_bytes` | `65536`; bounded recent diagnostics |
| `ffmpeg.max_input_bytes` | `0` means unlimited; otherwise per-session upload limit |
| `profiles.enabled` | `["h264-cpu-mkv"]`; optional `"h264-vaapi-mkv"` |

Startup probes encoder availability and real synthetic encoding. A configured unavailable GPU profile fails readiness and rejects GPU creation without disabling an available CPU profile. Profiles are fixed server-owned argument vectors; client data cannot supply paths, URLs, executable names, or codec arguments. Input protocols are restricted so media cannot freely fetch external references.

## Docker

```sh
cp config.example.toml config.toml
# Edit config.toml: [server] bind = "0.0.0.0:8080"
export TRANSCODER_API_TOKEN='replace-with-a-long-random-secret'
docker compose up --build -d
docker compose logs -f transcoder
```

The Compose service publishes port 8080 and mounts `./config.toml` read-only. Use the same client against `http://127.0.0.1:8080`. Review the published address before exposing it to a LAN. Container binding and published port must agree. The image runs non-root with FFmpeg installed; Compose restricts capabilities/resources, uses a read-only filesystem, and gives shutdown time for reaping. Do not mount the Docker socket, use privileged mode, or write media into the server container.

The multi-stage image builds with `rust:1.93-bookworm` and runs on `debian:trixie-slim`, using the newer distribution's FFmpeg/Mesa VAAPI packages for recent AMD hardware. Package availability alone is not evidence that a GPU encode works.

```sh
curl --fail http://127.0.0.1:8080/healthz
docker compose stop
```

### Published images (GHCR)

The `Publish container image` GitHub Actions workflow builds `linux/amd64` images from the Dockerfile and publishes them to `ghcr.io/swind/rust-ffmpeg-transcoder`. It runs on pushes to `main`, tags matching `v*`, or manual dispatch from the Actions tab. Authentication uses the repository's `GITHUB_TOKEN` with `packages: write`; no registry secret is required.

- `main` and `latest`: builds from the `main` branch.
- `1.2.3`: a semantic version tag such as `v1.2.3`.
- `sha-<short-commit>`: identifies the source commit.

After configuring `config.toml` with `server.bind = "0.0.0.0:8080"` and exporting `TRANSCODER_API_TOKEN`, run a published image instead of building locally:

```sh
docker pull ghcr.io/swind/rust-ffmpeg-transcoder:latest
docker run --rm --init --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true --pids-limit 256 --memory 2g --cpus 2 \
  -p 127.0.0.1:8080:8080 -e TRANSCODER_API_TOKEN \
  -v "$(pwd)/config.toml:/etc/transcoder/config.toml:ro" \
  ghcr.io/swind/rust-ffmpeg-transcoder:latest
```

GHCR package visibility is separate from repository visibility. For anonymous pulls, make the package public in its GitHub package settings. For private packages, authenticate with `docker login ghcr.io` using credentials with package read access. Publishing an image does not deploy or restart an existing service.

### Optional AMD VAAPI

On the actual GPU host, identify the render node and permissions; do not infer capabilities from a model name:

```sh
ls -l /dev/dri/renderD*
stat -c '%g' /dev/dri/renderD128
# Host tools, when testing outside Docker:
sudo apt-get install vainfo mesa-va-drivers
vainfo --display drm --device /dev/dri/renderD128
ffmpeg -hide_banner -encoders | grep h264_vaapi
```

Enable both profiles in `config.toml`, set `ffmpeg.vaapi_device`, and give the non-root container the render node plus its numeric group. For example create a local Compose override (replace `993` with `stat`'s actual output):

```yaml
services:
  transcoder:
    devices:
      - /dev/dri/renderD128:/dev/dri/renderD128
    group_add:
      - "993"
```

Then recreate the service and check GPU support:

```sh
docker compose up --build -d --force-recreate
python3 scripts/gpu-smoke.py --base-url http://127.0.0.1:8080 \
  --client target/debug/transcoder-client --device /dev/dri/renderD128 \
  --docker-container "$(docker compose ps -q transcoder)"
```

The GPU check runs device-access checks, `vainfo`, and encoder discovery inside the container, then converts an audio/video fixture through the VAAPI profile. The client host needs FFmpeg/ffprobe to generate and validate the fixture. For a server running directly on the GPU host, omit `--docker-container` and run the script there. CPU operation does not need VAAPI tools.

GPU access and driver support depend on the deployment host, render-node permissions, and container image.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --bins
python3 scripts/acceptance.py \
  --server target/debug/transcoder-server --client target/debug/transcoder-client
```

The integration harness starts isolated servers and uses temporary media files. It requires Linux `/proc`, Python 3, FFmpeg/ffprobe, and approximately 1 GiB of temporary disk space. GPU checks are separate. Use `--only SCENARIO` to run an individual scenario.

## Limitations and troubleshooting

- Prefer MPEG-TS or stream-compatible Matroska input. `pipe:0` is nonseekable: ordinary MP4 with tail metadata can need a backwards seek and fail. Faststart/fragmented MP4 may work, but **not all MP4 is supported**. There is no server disk-spooling workaround or HTTP Range fetch feature.
- Empty output followed by failure: inspect final status and bounded stderr. FFmpeg spawn/protocol/decoder/encoder failures are not successful empty conversions.
- Upload stalls: ensure a downloader is active. If it is paused, backpressure is expected; raising a timeout does not fix incorrect sequential client orchestration.
- HTTP 409: wait for READY, attach output before input, and do not retry an already attached stream. Create a new session after failure.
- HTTP 411: use a regular input file and explicit Content-Length; chunked uploads are not the MVP contract.
- Queue full can also reflect retained metadata capacity. Tune queue/concurrency/retention together; do not launch more jobs than CPU/RAM/PID capacity supports.
- `/readyz` fails only with GPU enabled: inspect `/v1/profiles` reasons, device mapping, numeric render group, `vainfo`, Mesa driver, and a real encode. CPU profile can still be used if available.
- Test directly against the server. Reverse-proxy request/response buffering defeats streaming and can deadlock the two-stream workflow. Any later proxy needs buffering disabled and suitable idle limits.
- Media is untrusted input to FFmpeg. Keep FFmpeg patched, use a trusted network, authenticate, run non-root with container resource limits, and treat bounded stderr as untrusted text.
- Sessions disappear on restart. There is no resume, fan-out, persistent acknowledgement, multi-node scheduling, arbitrary codec configuration, or guaranteed durable receipt.
