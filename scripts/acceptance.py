#!/usr/bin/env python3
"""Black-box acceptance against real HTTP, subprocesses, FFmpeg and ffprobe (Linux)."""

import argparse
import concurrent.futures
import select
import contextlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import traceback

from fixtures import generate

TOKEN = "isolated-acceptance-token"
TERMINAL = {"completed", "failed", "cancelled", "expired"}


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def until(predicate, timeout=15, description="condition"):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"deadline waiting for {description}")


def proc_exists(pid):
    return pid is not None and Path(f"/proc/{pid}").exists()


def rss(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except FileNotFoundError:
        pass
    return 0


def children(pid):
    found = set()
    for path in Path(f"/proc/{pid}/task").glob("*/children"):
        try:
            found.update(int(child) for child in path.read_text().split())
        except FileNotFoundError:
            # A worker may exit between listing its task and reading its children.
            continue
    return sorted(found)


def probe(path, audio=False, duration=None):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", str(path)],
        capture_output=True, text=True, timeout=30, check=True,
    )
    check(not result.stderr.strip(), f"ffprobe reported damage: {result.stderr}")
    data = json.loads(result.stdout)
    streams = data["streams"]
    video = [item for item in streams if item["codec_type"] == "video"]
    sound = [item for item in streams if item["codec_type"] == "audio"]
    check(len(video) == 1 and video[0]["codec_name"] == "h264", f"wrong video: {streams}")
    check(bool(sound) == audio, f"wrong optional audio mapping: {streams}")
    if audio:
        check(all(item["codec_name"] == "aac" for item in sound), "audio is not AAC")
    check(path.stat().st_size > 1024, "empty output container")
    # Streaming MKV may omit a container duration. Inspect decoded packet timestamps instead.
    packets = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "packet=pts_time,duration_time",
         "-of", "csv=p=0", str(path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30, check=True,
    )
    check(not packets.stderr.strip(), f"packet probe failed: {packets.stderr}")
    first, last, count = None, None, 0
    for line in packets.stdout.splitlines():
        fields = line.split(",")
        if not fields or fields[0] in ("N/A", ""):
            continue
        stamp = float(fields[0])
        first = stamp if first is None else min(first, stamp)
        last = stamp if last is None else max(last, stamp)
        count += 1
    check(count > 10 and first is not None and first >= -0.2, "missing/invalid timestamps")
    if duration is not None:
        check(duration - 1.0 <= last - first <= duration + 1.0,
              f"truncated or implausible duration: {last - first:.3f}, expected {duration}")
    return data


class Server:
    def __init__(self, executable, root, **overrides):
        self.executable = executable
        self.root = Path(tempfile.mkdtemp(prefix="server-", dir=root))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.settings = {
            "server": {"bind": f"127.0.0.1:{self.port}"},
            "security": {"token_env": "TRANSCODER_API_TOKEN", "allow_insecure_no_auth": False},
            "scheduler": {"max_concurrent_jobs": 1, "max_queued_jobs": 2, "max_sessions": 128},
            "timeouts": {"ready_attach_secs": 30, "input_idle_secs": 15, "output_stall_secs": 90,
                         "max_session_secs": 300, "terminal_retention_secs": 60,
                         "shutdown_grace_secs": 3},
            "ffmpeg": {"binary": shutil.which("ffmpeg"), "max_stderr_tail_bytes": 4096,
                       "max_input_bytes": 0, "vaapi_device": "/dev/dri/renderD128"},
            "profiles": {"enabled": ["h264-cpu-mkv"]},
        }
        for section, values in overrides.items():
            self.settings[section].update(values)
        config = []
        for section, values in self.settings.items():
            config.append(f"[{section}]")
            for key, value in values.items():
                config.append(f"{key} = {json.dumps(value)}")
        self.config = self.root / "config.toml"
        self.config.write_text("\n".join(config) + "\n")
        self.log = (self.root / "server.log").open("wb")
        self.process = None
        self.observed_pids = {}

    def __enter__(self):
        env = dict(os.environ, TRANSCODER_API_TOKEN=TOKEN, RUST_LOG="info")
        self.process = subprocess.Popen(
            [str(self.executable), "--config", str(self.config)], env=env,
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            def ready():
                check(self.process.poll() is None, "server exited during startup")
                try:
                    return self.request("GET", "/readyz", token=None)[0] == 200
                except (OSError, http.client.HTTPException):
                    return False
            until(ready, 30, "server readiness")
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise
        return self

    def __exit__(self, kind, value, tb):
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=3)
        if self.process:
            # An assertion must not leak a test-owned encoder even if the server is broken.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
        self.log.close()
        if kind is not None:
            print(f"--- server log {self.root} ---", flush=True)
            print((self.root / "server.log").read_text(errors="replace")[-24000:], flush=True)

    def connection(self, timeout=20, receive_buffer=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        conn.connect()
        conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32768)
        if receive_buffer:
            conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        return conn

    def request(self, method, path, body=None, headers=None, token=TOKEN):
        conn = self.connection()
        headers = dict(headers or {})
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if isinstance(body, dict):
            body = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        try:
            conn.request(method, path, body=body, headers=headers)
            response = conn.getresponse()
            payload = response.read(1024 * 1024)
            check(len(payload) < 1024 * 1024, "unbounded control response")
            data = json.loads(payload) if payload else None
            return response.status, data
        finally:
            conn.close()

    def create(self, profile="h264-cpu-mkv"):
        code, data = self.request("POST", "/v1/sessions", {"profile": profile})
        check(code == 201, f"create: {code} {data}")
        check(data["state"] in ("queued", "ready"), f"bad initial state: {data}")
        sid = data["id"]
        for key, suffix in (("status_url", ""), ("input_url", "/input"), ("output_url", "/output")):
            check(data[key] == f"/v1/sessions/{sid}{suffix}", f"wrong {key}: {data}")
        return sid

    def status(self, sid):
        code, data = self.request("GET", f"/v1/sessions/{sid}")
        check(code == 200, f"status: {code} {data}")
        check(data["id"] == sid and "created_at" in data and "updated_at" in data,
              f"incomplete status: {data}")
        if data["ffmpeg"]["pid"]:
            self.observed_pids[sid] = data["ffmpeg"]["pid"]
        return data

    def state(self, sid, expected, timeout=20):
        expected = {expected} if isinstance(expected, str) else set(expected)
        def reached():
            data = self.status(sid)
            if data["state"] in expected:
                return data
            check(data["state"] not in TERMINAL,
                  f"unexpected terminal state, expected {expected}: {data}")
            return None
        return until(reached, timeout, f"{sid} state {expected}")

    def terminal(self, sid, expected=None, timeout=20):
        data = self.state(sid, TERMINAL, timeout)
        if expected:
            check(data["state"] in set(expected), f"wrong terminal state: {data}")
        check(not data["input"]["connected"] and not data["output"]["connected"],
              f"terminal session still connected: {data}")
        pid = self.observed_pids.get(sid, data["ffmpeg"]["pid"])
        check(not proc_exists(pid), f"child {pid} still exists at terminal publication")
        time.sleep(0.1)
        check(self.status(sid)["state"] == data["state"], "terminal state overwritten")
        return data

    def cancel(self, sid):
        code, data = self.request("DELETE", f"/v1/sessions/{sid}")
        check(code == 204, f"cancel: {code} {data}")
        return self.terminal(sid, {"cancelled"})

    def reuse(self):
        sid = self.create()
        self.state(sid, "ready")
        self.cancel(sid)

    def output(self, sid, receive_buffer=None):
        conn = self.connection(timeout=300, receive_buffer=receive_buffer)
        conn.request("GET", f"/v1/sessions/{sid}/output", headers={"Authorization": f"Bearer {TOKEN}"})
        response = conn.getresponse()
        check(response.status == 200, f"output attach: {response.status}")
        check(response.getheader("Content-Type", "").split(";")[0] == "video/x-matroska",
              "wrong output media type")
        check(response.getheader("Cache-Control") == "no-store", "output must not be cached")
        check(response.getheader("Content-Length") is None, "stream output declared a length")
        return conn, response

    def raw_input(self, sid, length=None, encoding=None):
        conn = self.connection(timeout=300)
        conn.putrequest("PUT", f"/v1/sessions/{sid}/input")
        conn.putheader("Authorization", f"Bearer {TOKEN}")
        conn.putheader("Content-Type", "application/octet-stream")
        if length is not None:
            conn.putheader("Content-Length", str(length))
        if encoding:
            conn.putheader("Content-Encoding", encoding)
        conn.endheaders()
        return conn


class Transfer:
    """Two actual concurrent sockets, bounded chunks, observable client-side timings."""

    def __init__(self, server, sid, source, destination, rate=None, paused=False):
        self.server, self.sid, self.source, self.destination = server, sid, source, destination
        self.rate = rate
        self.sent = self.received = 0
        self.first_output = self.upload_finished = None
        self.upload_status = None
        self.errors = []
        self.stop = threading.Event()
        self.upload_done, self.download_done = threading.Event(), threading.Event()
        self.output_conn, self.response = server.output(sid, receive_buffer=32768 if paused else None)
        self.input_conn = None
        self.reader = None
        self.writer = threading.Thread(target=self._upload, daemon=True)
        self.writer.start()
        if not paused:
            self.resume()

    def _upload(self):
        try:
            self.input_conn = self.server.raw_input(self.sid, self.source.stat().st_size)
            started = time.monotonic()
            with self.source.open("rb") as source:
                while not self.stop.is_set():
                    chunk = source.read(32768)
                    if not chunk:
                        break
                    self.input_conn.send(chunk)
                    self.sent += len(chunk)
                    if self.rate:
                        delay = self.sent / self.rate - (time.monotonic() - started)
                        if delay > 0:
                            self.stop.wait(delay)
            self.upload_finished = time.monotonic()
            if not self.stop.is_set():
                response = self.input_conn.getresponse()
                self.upload_status = response.status
                response.read(8192)
        except (OSError, http.client.HTTPException) as error:
            self.errors.append(("upload", repr(error)))
        finally:
            if self.input_conn:
                self.input_conn.close()
            self.upload_done.set()

    def _download(self):
        try:
            with self.destination.open("wb") as output:
                while not self.stop.is_set():
                    chunk = self.response.read1(65536)
                    if not chunk:
                        break
                    if self.first_output is None:
                        self.first_output = time.monotonic()
                    output.write(chunk)
                    self.received += len(chunk)
        except (OSError, http.client.HTTPException) as error:
            self.errors.append(("download", repr(error)))
        finally:
            self.response.close()
            self.output_conn.close()
            self.download_done.set()

    def resume(self):
        check(self.reader is None, "reader already started")
        self.reader = threading.Thread(target=self._download, daemon=True)
        self.reader.start()

    def finish(self, timeout=240, success=True):
        end = time.monotonic() + timeout
        check(self.upload_done.wait(max(0, end - time.monotonic())), "upload thread deadline")
        check(self.download_done.wait(max(0, end - time.monotonic())), "download thread deadline")
        data = self.server.terminal(self.sid)
        if success:
            check(not self.errors, f"transfer errors: {self.errors}")
            check(self.upload_status == 204, f"upload HTTP {self.upload_status}")
            check(data["state"] == "completed", f"transcode failed: {data}")
            check(data["ffmpeg"]["exit_code"] == 0, f"wrong exit: {data}")
            check(data["input"]["received_bytes"] == self.source.stat().st_size, "input byte count mismatch")
            check(data["output"]["written_bytes"] == self.received, "output byte count mismatch")
        else:
            check(data["state"] != "completed", f"false success: {data}")
            check(any(side == "download" for side, _ in self.errors),
                  f"failed session ended HTTP 200 body normally: {self.errors}")
        return data

    def close(self):
        self.stop.set()
        with contextlib.suppress(AttributeError, OSError):
            self.response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
        for conn in (self.input_conn, self.output_conn):
            if conn:
                if conn.sock:
                    with contextlib.suppress(OSError):
                        conn.sock.shutdown(socket.SHUT_RDWR)
                conn.close()
        self.response.close()
        self.writer.join(timeout=3)
        if self.reader:
            self.reader.join(timeout=3)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def error_code(reply, expected_status, expected_code=None):
    status, data = reply
    allowed = {expected_status} if isinstance(expected_status, int) else set(expected_status)
    check(status in allowed, f"expected HTTP {allowed}, got {status}: {data}")
    check(isinstance(data, dict) and isinstance(data.get("error"), dict), f"missing error envelope: {data}")
    check(isinstance(data["error"].get("message"), str) and data["error"]["message"], f"empty error: {data}")
    if expected_code:
        check(data["error"]["code"] == expected_code, f"wrong error code: {data}")


class Suite:
    def __init__(self, args, root, fixtures):
        self.args, self.root, self.fixtures = args, root, fixtures

    def server(self, **overrides):
        return Server(self.args.server, self.root, **overrides)

    def transfer(self, server, source="video", **kwargs):
        sid = server.create()
        server.state(sid, "ready")
        return Transfer(server, sid, self.fixtures[source], self.root / f"{sid}.mkv", **kwargs)

    def api(self):
        with self.server() as server:
            check(server.request("GET", "/healthz", token=None)[0] == 200, "health requires auth")
            check(server.request("GET", "/readyz", token=None)[0] == 200, "ready requires auth")
            error_code(server.request("GET", "/v1/profiles", token=None), 401, "unauthorized")
            error_code(server.request("POST", "/v1/sessions", {"profile": "h264-cpu-mkv"}, token="wrong"),
                       401, "unauthorized")
            code, profiles = server.request("GET", "/v1/profiles")
            check(code == 200 and any(p["id"] == "h264-cpu-mkv" and p["available"]
                                      for p in profiles["profiles"]), f"CPU unavailable: {profiles}")
            error_code(server.request("POST", "/v1/sessions", {"profile": "-injected"}), 422, "invalid_profile")
            check(not children(server.process.pid), "rejected create spawned a child")
            error_code(server.request("GET", "/v1/sessions/00000000-0000-4000-8000-000000000000"), 404, "not_found")
            sid = server.create()
            server.state(sid, "ready")
            path = f"/v1/sessions/{sid}"
            for method, suffix in (("GET", ""), ("GET", "/output"), ("PUT", "/input"), ("DELETE", "")):
                error_code(server.request(method, path + suffix, token=None), 401, "unauthorized")
            error_code(server.request("PUT", path + "/input", body=b""), 409, "output_not_attached")
            output_conn, output = server.output(sid)
            try:
                error_code(server.request("GET", path + "/output"), 409, "output_already_attached")
                conn = server.raw_input(sid)
                try:
                    response = conn.getresponse()
                    error_code((response.status, json.loads(response.read())), 411, "missing_content_length")
                finally:
                    conn.close()
                conn = server.raw_input(sid, 0, "gzip")
                try:
                    response = conn.getresponse()
                    error_code((response.status, json.loads(response.read())), {400, 415})
                finally:
                    conn.close()
                # Rejected requests must not acquire the input or write anything to stdin.
                data = server.status(sid)
                check(data["input"]["received_bytes"] == 0 and not data["input"]["connected"],
                      f"invalid headers poisoned input: {data}")
                conn = server.raw_input(sid, self.fixtures["video"].stat().st_size)
                try:
                    with self.fixtures["video"].open("rb") as source:
                        conn.send(source.read(32768))
                    until(lambda: server.status(sid)["input"]["connected"], description="input attachment")
                    error_code(server.request("PUT", path + "/input", body=b""), 409, "input_already_attached")
                    server.cancel(sid)
                finally:
                    conn.close()
            finally:
                output.close()
                output_conn.close()
            check(server.request("DELETE", path)[0] == 204, "terminal DELETE is not idempotent")
            error_code(server.request("GET", path + "/output"), 410, "session_terminal")
            error_code(server.request("PUT", path + "/input", body=b""), 410, "session_terminal")
            server.reuse()
            raced = server.create()
            server.state(raced, "ready")
            barrier = threading.Barrier(2)

            def attach_output():
                conn = server.connection(timeout=10)
                try:
                    barrier.wait(timeout=5)
                    conn.request("GET", f"/v1/sessions/{raced}/output",
                                 headers={"Authorization": f"Bearer {TOKEN}"})
                    return conn, conn.getresponse()
                except BaseException:
                    conn.close()
                    raise

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                attempts = [pool.submit(attach_output) for _ in range(2)]
                attached = [attempt.result(timeout=15) for attempt in attempts]
            try:
                check(sorted(response.status for _, response in attached) == [200, 409],
                      "simultaneous GETs both acquired output or both failed")
                server.cancel(raced)
            finally:
                for conn, response in attached:
                    response.close()
                    conn.close()
            server.reuse()

    def media(self):
        with self.server() as server:
            started = time.monotonic()
            with self.transfer(server, rate=1024 * 1024) as transfer:
                transfer.finish()
                check(transfer.first_output is not None and transfer.first_output < transfer.upload_finished,
                      "not streaming: first output did not precede upload completion")
                print(f"  first output={transfer.first_output - started:.3f}s; "
                      f"upload finished={transfer.upload_finished - started:.3f}s", flush=True)
                probe(transfer.destination, duration=120)
            with self.transfer(server, "audio") as transfer:
                transfer.finish()
                probe(transfer.destination, audio=True, duration=12)
            with self.transfer(server, "fast") as transfer:
                transfer.finish()
                probe(transfer.destination, duration=30)

    def scheduler(self):
        with self.server() as server:
            first = server.create()
            ready = server.state(first, "ready")
            second, third = server.create(), server.create()
            check(server.status(second)["state"] == "queued" and server.status(third)["state"] == "queued",
                  "scheduler exceeded concurrency")
            check(server.status(second)["ffmpeg"]["pid"] is None and server.status(third)["ffmpeg"]["pid"] is None,
                  "queued job spawned FFmpeg")
            check(children(server.process.pid) == [ready["ffmpeg"]["pid"]], "more than one child at concurrency=1")
            error_code(server.request("POST", "/v1/sessions", {"profile": "h264-cpu-mkv"}), {429, 503}, "queue_full")
            for method, suffix in (("PUT", "/input"), ("GET", "/output")):
                error_code(server.request(method, f"/v1/sessions/{second}{suffix}", body=b"" if method == "PUT" else None),
                           409, "not_ready")
            server.cancel(first)
            ready2 = server.state(second, "ready")
            check(server.status(third)["state"] == "queued", "FIFO bypassed oldest queued job")
            check(children(server.process.pid) == [ready2["ffmpeg"]["pid"]], "child/permit leaked across FIFO handoff")
            server.cancel(second)
            server.state(third, "ready")
            queued = server.create()
            server.cancel(queued)
            server.cancel(third)
            server.reuse()
        with self.server(scheduler={"max_concurrent_jobs": 2}) as server:
            first, second = server.create(), server.create()
            server.state(first, "ready")
            server.state(second, "ready")
            third = server.create()
            check(server.status(third)["state"] == "queued", "concurrency cap of two not enforced")
            check(len(children(server.process.pid)) == 2, "two jobs did not run concurrently")
            server.cancel(first)
            server.state(third, "ready")
            server.cancel(second)
            server.cancel(third)

    def disconnects(self):
        with self.server() as server:
            sid = server.create()
            pid = server.state(sid, "ready")["ffmpeg"]["pid"]
            conn, response = server.output(sid)
            upload = server.raw_input(sid, self.fixtures["video"].stat().st_size)
            with self.fixtures["video"].open("rb") as source:
                upload.send(source.read(65536))
            upload.sock.shutdown(socket.SHUT_RDWR)
            upload.close()
            try:
                data = server.terminal(sid, {"failed"})
                check(data["error"]["code"] == "input_stream_failed", f"wrong disconnect cause: {data}")
                check(not proc_exists(pid), "input-disconnected child not reaped")
                try:
                    while response.read1(65536):
                        pass
                except (OSError, http.client.HTTPException):
                    pass
                else:
                    raise AssertionError("failed input produced a normally terminated output body")
            finally:
                response.close()
                conn.close()
            server.reuse()
            with self.transfer(server, rate=256 * 1024) as transfer:
                until(lambda: transfer.first_output, 30, "output before forced disconnect")
                pid = server.status(transfer.sid)["ffmpeg"]["pid"]
                # Close the actual socket, not just a wrapper which may retain response ownership.
                sock = transfer.response.fp.raw._sock
                sock.shutdown(socket.SHUT_RDWR)
                data = server.terminal(transfer.sid, {"failed"})
                check(data["error"]["code"] == "output_disconnected", f"wrong output cause: {data}")
                check(not proc_exists(pid), "output-disconnected child not reaped")
                check(transfer.upload_done.wait(10), "producer did not stop after consumer disconnect")
                check(transfer.sent < transfer.source.stat().st_size, "disconnected producer sent entire fixture")
            server.reuse()

    def failures(self):
        bad = self.root / "invalid-media.bin"
        bad.write_bytes(b"not a media container\n" * 20000)
        with self.server() as server:
            sid = server.create()
            server.state(sid, "ready")
            with Transfer(server, sid, bad, self.root / "invalid-output.mkv") as transfer:
                data = transfer.finish(success=False)
                check(data["error"] and data["error"]["message"], f"missing useful diagnostic: {data}")
                tail = data.get("stderr_tail", data["ffmpeg"].get("stderr_tail", ""))
                check(len(str(tail).encode()) <= 4096 + 4, "stderr tail exceeded configured bound")
            server.reuse()
            sid = server.create()
            pid = server.state(sid, "ready")["ffmpeg"]["pid"]
            os.kill(pid, signal.SIGKILL)
            data = server.terminal(sid, {"failed"})
            check(data["error"]["code"] == "ffmpeg_failed", f"encoder crash not reported: {data}")
            check(not proc_exists(pid), "crashed encoder was not reaped")
            server.reuse()
        executable = self.root / "temporary-ffmpeg"
        executable.symlink_to(shutil.which("ffmpeg"))
        with self.server(ffmpeg={"binary": str(executable)}) as server:
            executable.unlink()  # Real OS spawn failure, after the real startup probe passed.
            sid = server.create()
            data = server.terminal(sid, {"failed"})
            check(data["error"] and data["error"]["message"], "spawn failure lacks diagnostic")
            executable.symlink_to(shutil.which("ffmpeg"))
            server.reuse()

    def timeouts(self):
        for attach_output in (False, True):
            with self.server(timeouts={"ready_attach_secs": 2}) as server:
                sid = server.create()
                pid = server.state(sid, "ready")["ffmpeg"]["pid"]
                output = server.output(sid) if attach_output else None
                try:
                    server.terminal(sid, {"failed", "expired"}, timeout=8)
                    check(not proc_exists(pid), "attach timeout child not reaped")
                finally:
                    if output:
                        output[1].close()
                        output[0].close()
                server.reuse()
        with self.server(timeouts={"input_idle_secs": 2}) as server:
            sid = server.create()
            server.state(sid, "ready")
            conn, response = server.output(sid)
            upload = server.raw_input(sid, 10000000)
            try:
                upload.send(b"\x47" * 188)
                data = server.terminal(sid, {"failed", "expired"}, timeout=8)
                check(data["error"]["code"] == "idle_timeout", f"wrong input idle cause: {data}")
            finally:
                upload.close()
                response.close()
                conn.close()
            server.reuse()
        with self.server(timeouts={"max_session_secs": 2, "ready_attach_secs": 20}) as server:
            sid = server.create()
            server.state(sid, "ready")
            data = server.terminal(sid, {"failed", "expired"}, timeout=8)
            check(data["error"]["code"] == "session_timeout", f"wrong total timeout: {data}")
            server.reuse()
        with self.server(timeouts={"terminal_retention_secs": 1}) as server:
            sid = server.create()
            server.state(sid, "ready")
            server.cancel(sid)
            until(lambda: server.request("GET", f"/v1/sessions/{sid}")[0] == 404,
                  8, "terminal metadata retention expiry")

    def backpressure(self):
        # Enough high-entropy media to exceed socket, pipe, codec and HTTP buffers.
        with self.server(timeouts={"input_idle_secs": 2, "output_stall_secs": 90}) as server:
            with self.transfer(server, "backpressure", paused=True) as transfer:
                until(lambda: transfer.sent > 1024 * 1024, 30, "initial input progress")
                samples, plateau = [], None
                end = time.monotonic() + 60
                while time.monotonic() < end:
                    data = server.status(transfer.sid)
                    check(data["state"] not in TERMINAL, f"backpressure misclassified as idle/disconnect: {data}")
                    sample = (time.monotonic(), transfer.sent, data["input"]["received_bytes"],
                              rss(server.process.pid), rss(data["ffmpeg"]["pid"]))
                    samples.append(sample)
                    if len(samples) >= 7:
                        recent = samples[-7:]
                        if recent[-1][1] == recent[0][1] and recent[-1][2] == recent[0][2]:
                            plateau = recent
                            break
                    check(not transfer.upload_done.is_set(), "entire upload buffered while output paused")
                    time.sleep(0.5)
                check(plateau is not None, f"upload did not throttle: {samples[-8:]}")
                check(transfer.sent < transfer.source.stat().st_size, "no producer backpressure")
                memory = [sample[3] for sample in samples]
                check(max(memory) - min(memory) < 48 * 1024 * 1024,
                      f"server RSS grew with paused media: {memory}")
                print(f"  stalled input={plateau[-1][2]} bytes; server RSS={max(memory)}; "
                      f"FFmpeg RSS={max(sample[4] for sample in samples)}; "
                      f"plateau={plateau[-1][0] - plateau[0][0]:.2f}s", flush=True)
                sent = transfer.sent
                transfer.resume()
                until(lambda: transfer.sent > sent, 30, "upload resuming after consumer resumes")
                transfer.finish(timeout=240)
                probe(transfer.destination, duration=180)
        with self.server(timeouts={"output_stall_secs": 2, "input_idle_secs": 30}) as server:
            with self.transfer(server, "backpressure", paused=True) as transfer:
                data = server.terminal(transfer.sid, {"failed", "expired"}, timeout=45)
                check(data["error"]["code"] == "idle_timeout", f"wrong stalled output cause: {data}")
            server.reuse()

    def mp4(self):
        source = self.fixtures["tail"]
        # Prove this exact fixture is valid when seekable and fails on an actual input pipe.
        subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", str(source)],
                       stdout=subprocess.DEVNULL, check=True, timeout=20)
        # A feeder thread keeps the standalone pipe proof bounded (no read-all input).
        diagnostic_file = tempfile.TemporaryFile()
        process = subprocess.Popen(
            ["ffmpeg", "-hide_banner", "-v", "error", "-i", "pipe:0", "-map", "0:v:0", "-f", "null", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=diagnostic_file,
        )
        def feed():
            try:
                with source.open("rb") as media:
                    shutil.copyfileobj(media, process.stdin, 65536)
            except BrokenPipeError:
                pass
            finally:
                with contextlib.suppress(BrokenPipeError):
                    process.stdin.close()
        feeder = threading.Thread(target=feed, daemon=True)
        feeder.start()
        try:
            process.wait(timeout=30)
            feeder.join(timeout=3)
            diagnostic_file.seek(0)
            diagnostic = diagnostic_file.read(65536)
            check(process.returncode != 0 and diagnostic, "fixture unexpectedly supports nonseekable MP4")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            diagnostic_file.close()
        with self.server() as server:
            with self.transfer(server, "tail") as transfer:
                data = transfer.finish(timeout=45, success=False)
                check(data["error"] and data["error"]["message"], f"MP4 failure lacks diagnostic: {data}")
            server.reuse()

    def shutdown(self):
        with self.server(scheduler={"max_concurrent_jobs": 2}) as server:
            with self.transfer(server, rate=512 * 1024) as transfer:
                until(lambda: transfer.first_output, 30, "running stream before SIGTERM")
                second = server.create()
                pids = [server.status(transfer.sid)["ffmpeg"]["pid"],
                        server.state(second, "ready")["ffmpeg"]["pid"]]
                queued = server.create()
                check(server.status(queued)["state"] == "queued", "shutdown fixture did not queue")
                started = time.monotonic()
                server.process.send_signal(signal.SIGTERM)
                server.process.wait(timeout=8)
                check(server.process.returncode == 0, f"unclean server shutdown: {server.process.returncode}")
                check(time.monotonic() - started < 8, "shutdown exceeded grace + cleanup deadline")
                check(all(not proc_exists(pid) for pid in pids), f"shutdown leaked children: {pids}")
                check(transfer.upload_done.wait(5) and transfer.download_done.wait(5),
                      "SIGTERM left an HTTP producer or consumer hanging")
                check(transfer.sent < transfer.source.stat().st_size,
                      "shutdown scenario did not interrupt an active upload")


    def cli(self):
        with self.server() as server:
            output = self.root / "cli-output.mkv"
            def command(source, target, deadline=90):
                return [str(self.args.client), "--base-url", server.base, "--profile", "h264-cpu-mkv",
                        "--input", str(source), "--output", str(target), "--deadline-secs", str(deadline)]
            env = dict(os.environ, TRANSCODER_API_TOKEN=TOKEN)
            result = subprocess.run(command(self.fixtures["audio"], output), env=env,
                                    capture_output=True, text=True, timeout=100)
            check(result.returncode == 0, f"CLI failed: {result.stderr}")
            probe(output, audio=True, duration=12)
            sid = next(line.split()[1] for line in result.stderr.splitlines() if line.startswith("session "))
            server.terminal(sid, {"completed"})
            sentinel = self.root / "do-not-overwrite.mkv"
            sentinel.write_bytes(b"user-owned-output")
            result = subprocess.run(command(self.fixtures["audio"], sentinel), env=env,
                                    capture_output=True, text=True, timeout=10)
            check(result.returncode != 0 and sentinel.read_bytes() == b"user-owned-output",
                  "CLI overwrote or deleted existing output")
            failed_output = self.root / "cli-failed.mkv"
            result = subprocess.run(command(self.fixtures["tail"], failed_output), env=env,
                                    capture_output=True, text=True, timeout=100)
            check(result.returncode != 0 and not failed_output.exists(),
                  f"CLI accepted failed transcode or retained partial: {result.stderr}")
            sid = next(line.split()[1] for line in result.stderr.splitlines() if line.startswith("session "))
            server.terminal(sid, {"failed", "cancelled"})
            blocker = server.create()
            server.state(blocker, "ready")
            deadline_output = self.root / "cli-deadline.mkv"
            result = subprocess.run(command(self.fixtures["audio"], deadline_output, 1), env=env,
                                    capture_output=True, text=True, timeout=10)
            check(result.returncode != 0 and not deadline_output.exists(), "CLI deadline/partial cleanup failed")
            sid = next(line.split()[1] for line in result.stderr.splitlines() if line.startswith("session "))
            server.terminal(sid, {"cancelled"})
            interrupt_output = self.root / "cli-interrupt.mkv"
            process = subprocess.Popen(command(self.fixtures["audio"], interrupt_output), env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                until(lambda: interrupt_output.exists(), 5, "CLI output creation")
                # Wait until the queued session exists, using observable CLI stderr.
                readable, _, _ = select.select([process.stderr], [], [], 5)
                check(readable, "CLI did not report its created session before deadline")
                line = process.stderr.readline()
                check(line.startswith("session "), f"CLI did not create session: {line}")
                sid = line.split()[1]
                process.send_signal(signal.SIGINT)
                _, stderr = process.communicate(timeout=10)
                check(process.returncode != 0 and not interrupt_output.exists(),
                      f"CLI interrupt/partial cleanup failed: {stderr}")
                server.terminal(sid, {"cancelled"})
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)
            server.cancel(blocker)
            server.reuse()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--only", choices=["api", "media", "scheduler", "disconnects", "failures",
                                          "timeouts", "backpressure", "mp4", "shutdown", "cli"],
                        help="Run one real scenario while debugging (default: entire matrix)")
    args = parser.parse_args()
    args.server, args.client = args.server.resolve(), args.client.resolve()
    for tool in ("ffmpeg", "ffprobe"):
        check(shutil.which(tool), f"required executable missing: {tool}")
    check(Path("/proc/self/status").exists(), "Linux /proc is required for reaping/RSS assertions")
    check(args.server.is_file() and args.client.is_file(), "build both Rust binaries first")
    names = [args.only] if args.only else ["api", "media", "scheduler", "disconnects", "failures",
                                         "timeouts", "backpressure", "mp4", "shutdown", "cli"]
    with tempfile.TemporaryDirectory(prefix="transcoder-acceptance-") as directory:
        root = Path(directory)
        print(f"Generating real FFmpeg fixtures in {root}", flush=True)
        fixtures = generate(root / "fixtures", backpressure="backpressure" in names)
        suite = Suite(args, root, fixtures)
        for name in names:
            started = time.monotonic()
            print(f"RUN {name}", flush=True)
            getattr(suite, name)()
            print(f"PASS {name} ({time.monotonic() - started:.2f}s)", flush=True)
    print("CPU acceptance passed. GPU and Docker are separate opt-in real checks.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt):
        traceback.print_exc()
        raise SystemExit(1)
