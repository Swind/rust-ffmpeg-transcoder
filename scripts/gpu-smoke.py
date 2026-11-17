#!/usr/bin/env python3
"""Real VAAPI encode through a running server; optionally verify inside its Docker container."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import urllib.request

from acceptance import check, probe
from fixtures import ffmpeg


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--device", default="/dev/dri/renderD128")
    parser.add_argument("--docker-container", help="Actual running transcoder container name/ID")
    args = parser.parse_args()
    token = os.environ.get("TRANSCODER_API_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    prefix = ["docker", "exec", args.docker_container] if args.docker_container else []
    for permission in ("-c", "-r", "-w"):
        subprocess.run([*prefix, "test", permission, args.device], check=True, timeout=10)
    subprocess.run([*prefix, "vainfo", "--display", "drm", "--device", args.device],
                   check=True, timeout=30)
    encoders = subprocess.run([*prefix, "ffmpeg", "-hide_banner", "-encoders"],
                              check=True, capture_output=True, text=True, timeout=20)
    check("h264_vaapi" in encoders.stdout, "FFmpeg lacks h264_vaapi")
    request = urllib.request.Request(args.base_url.rstrip("/") + "/v1/profiles", headers=headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        profiles = json.load(response)["profiles"]
    gpu = next((profile for profile in profiles if profile["id"] == "h264-vaapi-mkv"), None)
    check(gpu and gpu["available"], f"VAAPI profile unavailable: {gpu}")
    check(shutil.which("ffmpeg") and shutil.which("ffprobe"), "client host needs FFmpeg and ffprobe")
    with tempfile.TemporaryDirectory(prefix="transcoder-vaapi-smoke-") as directory:
        source, output = Path(directory) / "input.ts", Path(directory) / "output.mkv"
        ffmpeg(source, ["-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30",
                        "-f", "lavfi", "-i", "sine=frequency=997:sample_rate=48000"],
               ["-t", "12", "-c:v", "libx264", "-threads", "2", "-preset", "ultrafast",
                "-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "mpegts"])
        subprocess.run([str(args.client.resolve()), "--base-url", args.base_url,
                        "--profile", "h264-vaapi-mkv", "--input", str(source), "--output", str(output),
                        "--deadline-secs", "120"], check=True, timeout=130)
        probe(output, audio=True, duration=12)
    location = f"Docker container {args.docker_container}" if args.docker_container else "host"
    print(f"PASS real VAAPI HTTP encode + ffprobe ({location}, {args.device}); no performance claim")


if __name__ == "__main__":
    main()
