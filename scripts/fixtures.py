#!/usr/bin/env python3
"""Generate real, deterministic-shape media fixtures; files live on the client only."""

import argparse
from pathlib import Path
import shutil
import subprocess


def ffmpeg(output, inputs, options, timeout=240):
    command = [shutil.which("ffmpeg") or "ffmpeg", "-hide_banner", "-loglevel", "error",
               "-nostdin", "-y", *inputs, *options, str(output)]
    subprocess.run(command, check=True, timeout=timeout)


def generate(directory, backpressure=True):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    video = directory / "video.ts"
    ffmpeg(video, ["-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30"],
           ["-t", "120", "-an", "-c:v", "libx264", "-threads", "2", "-preset", "ultrafast",
            "-crf", "18", "-pix_fmt", "yuv420p", "-g", "30", "-f", "mpegts"])
    audio = directory / "audio.mkv"
    ffmpeg(audio, ["-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25",
                   "-f", "lavfi", "-i", "sine=frequency=997:sample_rate=48000"],
           ["-t", "12", "-c:v", "libx264", "-threads", "2", "-preset", "ultrafast",
            "-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "matroska"])
    tail = directory / "tail-moov.mp4"
    ffmpeg(tail, ["-i", str(video)], ["-t", "30", "-c", "copy", "-f", "mp4"])
    fast = directory / "faststart.mp4"
    ffmpeg(fast, ["-i", str(tail)], ["-c", "copy", "-movflags", "+faststart", "-f", "mp4"])
    files = {"video": video, "audio": audio, "tail": tail, "fast": fast}
    if backpressure:
        noise = directory / "backpressure.ts"
        ffmpeg(noise, ["-f", "lavfi", "-i",
                       "nullsrc=size=512x288:rate=30,noise=alls=100:allf=t+u:all_seed=42"],
               ["-t", "180", "-an", "-c:v", "libx264", "-threads", "2", "-preset", "ultrafast",
                "-crf", "0", "-pix_fmt", "yuv420p", "-g", "30", "-f", "mpegts"])
        files["backpressure"] = noise
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--without-backpressure", action="store_true")
    args = parser.parse_args()
    for name, path in generate(args.directory, not args.without_backpressure).items():
        print(f"{name}: {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
