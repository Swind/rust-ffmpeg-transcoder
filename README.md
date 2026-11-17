# HTTP streaming transcoder

A single-node Rust/Tokio service which pipes a streamed HTTP upload through one FFmpeg process and returns Matroska over a **separate, concurrent HTTP download**. File-based profiles configure H.264, AV1, or HEVC video and AAC encoding or strict AAC copying. The default enables only CPU H.264; VAAPI H.264 is opt-in.

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

Media chunks are 32 KiB, with a bounded two-chunk output channel. Profiles configure decoder and encoder resource settings; the shipped profiles use two decoder threads. A pipe-only input protocol allowlist restricts media access. These bounds do not limit FFmpeg's internal codec buffers.

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
| `timeouts.max_session_secs` | `7200`, from READY through draining; `1..604800`, or `0` disables |
| `timeouts.terminal_retention_secs` | `600`; then status becomes 404 |
| `timeouts.shutdown_grace_secs` | `10` |
| `ffmpeg.binary` | `/usr/bin/ffmpeg` |
| `ffmpeg.vaapi_device` | `/dev/dri/renderD128`; select the actual render node |
| `ffmpeg.max_stderr_tail_bytes` | `65536`; bounded recent diagnostics |
| `ffmpeg.max_input_bytes` | `0` means unlimited; otherwise per-session upload limit |
| `profiles.directory` | `"profiles"`; relative to the config file's parent, or the working directory when `--config` is omitted |
| `profiles.enabled` | `["h264-cpu-mkv"]`; only listed file IDs are loaded and exposed |

Startup probes each enabled encoder with a real short synthetic encode using its configured parameters. An unavailable encoder/device makes that profile unavailable, causes `/readyz` to return 503, and rejects creation for that profile without disabling available profiles. Malformed configuration, invalid/duplicate IDs, or missing enabled files fail startup. Disabled files are not parsed or probed.

### File-based profiles

Each ID names one `<profiles.directory>/<id>.toml` file; there is no `id` field inside it. IDs are 1–64 ASCII lowercase letters, digits, or hyphens, beginning with a letter or digit. Paths, dots, and traversal are forbidden. Files are loaded and compiled once at startup; **restart after editing**. Missing files never fall back to built-in recipes.

The repository and container ship these four recipes:

| ID | Video | Audio |
|---|---|---|
| `h264-cpu-mkv` | libx264, CRF 23, veryfast, threads 2 | Encode every audio track to AAC 128 kbps |
| `h264-vaapi-mkv` | h264_vaapi, QP 23, threads 2 | Encode every audio track to AAC 128 kbps |
| `av1-crf30-preset6-mkv` | libsvtav1, CRF 30, preset 6, `lp=12` | Copy first audio track, AAC only |
| `hevc-crf26-slow-mkv` | libx265, CRF 26, slow, `pools=12:frame-threads=3` | Copy first audio track, AAC only |

To expose AV1 and HEVC in **`/v1/profiles`**, replace the CPU-only enabled list in your main config, then restart:

```toml
[profiles]
directory = "profiles"
enabled = ["h264-cpu-mkv", "av1-crf30-preset6-mkv", "hevc-crf26-slow-mkv"]
```

Use `--profile av1-crf30-preset6-mkv` or `--profile hevc-crf26-slow-mkv` with the client. Your FFmpeg build must support `libsvtav1` or `libx265`, respectively. Enable `h264-vaapi-mkv` separately when the render node and driver are ready.

To add a custom profile, copy a recipe beside your config's other profile files, edit its typed settings, and add its filename stem to `profiles.enabled`:

```sh
cp profiles/hevc-crf26-slow-mkv.toml profiles/archive-hevc.toml
# Edit profiles/archive-hevc.toml, for example crf = 28.
# Add "archive-hevc" to profiles.enabled in config.toml, then restart.
# Select it with: transcoder-client ... --profile archive-hevc
```

For example, the shipped HEVC file contains:

```toml
decoder_threads = 2

[video]
encoder = "libx265"
crf = 26
preset = "slow"
pools = 12
frame_threads = 3

[audio]
mode = "copy_aac"
streams = "first"
```

All recipe fields are explicit. Unknown fields/tables, unsupported encoder/audio variants, and fields belonging to another variant are rejected:

| Location / variant | Required fields and valid values |
|---|---|
| Root | `decoder_threads`: integer `1..64`; `[video]` and `[audio]` tables |
| `video.encoder = "libx264"` | `crf`: integer `0..51`; `preset`: name below; `threads`: integer `1..64` |
| `video.encoder = "h264_vaapi"` | `qp`: integer `0..51`; `threads`: integer `1..64` |
| `video.encoder = "libsvtav1"` | `crf`: integer `0..63`; `preset`: integer `0..13`; `logical_processors`: integer `1..64` |
| `video.encoder = "libx265"` | `crf`: integer `0..51`; `preset`: name below; `pools`: integer `1..64`; `frame_threads`: integer `1..16` |
| `audio.mode = "aac"` | `bitrate_kbps`: integer `8..512`; `streams`: `"all"` or `"first"` |
| `audio.mode = "copy_aac"` | `streams`: `"all"` or `"first"`; no bitrate setting or re-encoding |

x264/x265 preset names: `ultrafast`, `superfast`, `veryfast`, `faster`, `fast`, `medium`, `slow`, `slower`, `veryslow`, `placebo`. `streams = "all"` selects optional `0:a?`; `"first"` selects optional `0:a:0?`. Every profile selects the first video stream.

**Strict AAC copy:** the AV1/HEVC recipes copy only the first audio track, not the first *AAC* track. A missing audio track is valid. If the first track is MP3, Opus, or another non-AAC codec, the job fails, the output body is aborted rather than ending normally, and final status/bounded stderr reports the AAC-only filter failure. Later tracks are ignored, even if they are AAC. AAC is not decoded/re-encoded: `-c:a copy -bsf:a aac_adtstoasc` preserves packet payloads where packaging already matches, and may convert ADTS framing to AudioSpecificConfig.

The fixed pipeline reads `pipe:0`, maps video/audio, and writes `-f matroska pipe:1`. Software video uses `yuv420p`; VAAPI uses `format=nv12,hwupload`. Source dimensions, frame rate, and timing are retained: no resizing, `-r`, frame-rate resampling, frame interpolation, or `-t` time truncation is added. `-enc_time_base:v -1` preserves the source video time base rather than rounding video timestamps to whole frame intervals, keeping copied AAC aligned. There are no MP4 `faststart`, `movflags`, or `hvc1` flags; MP4 remuxing/tagging belongs downstream. The session deadline can fail an overlong job, never silently turn it into a successful shortened encode. Clients cannot supply paths, URLs, executable names, or raw codec arguments; input protocols remain pipe-only.

**CPU resources and long jobs:** `decoder_threads` controls input decoding, not the encoder's workers. AV1 `logical_processors` compiles to `-svtav1-params lp=12` in the shipped recipe; HEVC `pools` and `frame_threads` compile to `-x265-params pools=12:frame-threads=3`. These are not low-resource recipes and are not replaced by generic `-threads 2`. Start with `scheduler.max_concurrent_jobs = 1`. Docker/Compose's two-CPU quota still restricts scheduling despite those worker counts; change quotas deliberately after considering RAM/PID limits. Long CPU encodes can exceed `timeouts.max_session_secs = 7200`: increase it up to `604800`, or use `0` to disable, and also increase the client's `--deadline-secs` (default `7500`) for encoding plus queueing and transfer time.

## Docker

```sh
cp config.example.toml config.toml
# Edit config.toml: [server] bind = "0.0.0.0:8080"
export TRANSCODER_API_TOKEN='replace-with-a-long-random-secret'
docker compose up --build -d
docker compose logs -f transcoder
```

The Compose service publishes port 8080 and mounts both `./config.toml` and `./profiles` read-only. Custom profile files therefore remain host-managed. The image also ships all four recipes at `/etc/transcoder/profiles`; `directory = "profiles"` in `/etc/transcoder/config.toml` resolves there regardless of the container working directory. A directory bind mount replaces that image directory, so include every enabled recipe in it. Use the same client against `http://127.0.0.1:8080`. Review the published address before exposing it to a LAN. Container binding and published port must agree. The image runs non-root with FFmpeg installed; Compose restricts capabilities/resources, uses a read-only filesystem, and gives shutdown time for reaping. Do not mount the Docker socket, use privileged mode, or write media into the server container.

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
  -v "$(pwd)/profiles:/etc/transcoder/profiles:ro" \
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

Set `profiles.enabled = ["h264-cpu-mkv", "h264-vaapi-mkv"]` in `config.toml` (retain AV1/HEVC IDs too if wanted), set `ffmpeg.vaapi_device`, and give the non-root container the render node plus its numeric group. For example create a local Compose override (replace `993` with `stat`'s actual output):

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

The integration harness starts isolated servers and uses temporary client-side media files. It requires Linux `/proc`, Python 3, FFmpeg/ffprobe, and approximately 1 GiB of temporary disk space. GPU checks are separate. Use `--only SCENARIO` to run an individual group. `--only profiles` performs real short AV1/HEVC encodes, checks dimensions/frame count/fractional frame rate/timestamps, AAC packet hashes and first-track selection, absent audio, non-AAC failure with aborted output, cleanup/permit reuse, and an arbitrary filename profile whose lossless setting is verified against decoded source pixels. This group requires `libsvtav1`, `libx265`, and `libmp3lame` alongside the baseline codecs. On hosts lacking an optional encoder, `--allow-unavailable-profiles` explicitly permits availability/rejection checks instead of that encoder's media cases, and reports the skip; without it, missing capability fails the group. It is not a substitute for running both real encoders on a capable host.

## Limitations and troubleshooting

- Prefer MPEG-TS or stream-compatible Matroska input. `pipe:0` is nonseekable: ordinary MP4 with tail metadata can need a backwards seek and fail. Faststart/fragmented MP4 may work, but **not all MP4 is supported**. There is no server disk-spooling workaround or HTTP Range fetch feature.
- Empty output followed by failure: inspect final status and bounded stderr. FFmpeg spawn/protocol/decoder/encoder failures are not successful empty conversions.
- Upload stalls: ensure a downloader is active. If it is paused, backpressure is expected; raising a timeout does not fix incorrect sequential client orchestration.
- HTTP 409: wait for READY, attach output before input, and do not retry an already attached stream. Create a new session after failure.
- HTTP 411: use a regular input file and explicit Content-Length; chunked uploads are not the MVP contract.
- Queue full can also reflect retained metadata capacity. Tune queue/concurrency/retention together; do not launch more jobs than CPU/RAM/PID capacity supports.
- `/readyz` fails with an optional profile enabled: inspect `/v1/profiles` reasons and FFmpeg encoder support. For VAAPI, also check device mapping, numeric render group, `vainfo`, Mesa driver, and a real encode. Available profiles remain usable.
- Test directly against the server. Reverse-proxy request/response buffering defeats streaming and can deadlock the two-stream workflow. Any later proxy needs buffering disabled and suitable idle limits.
- Media is untrusted input to FFmpeg. Keep FFmpeg patched, use a trusted network, authenticate, run non-root with container resource limits, and treat bounded stderr as untrusted text.
- Sessions disappear on restart. There is no resume, fan-out, persistent acknowledgement, multi-node scheduling, arbitrary FFmpeg argument configuration, or guaranteed durable receipt.
