use crate::config::Config;
use serde::Serialize;
use std::{collections::VecDeque, os::unix::fs::FileTypeExt, process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};

pub const CPU_PROFILE: &str = "h264-cpu-mkv";
pub const VAAPI_PROFILE: &str = "h264-vaapi-mkv";

#[derive(Clone, Debug, Serialize)]
pub struct Profile {
    pub id: String,
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

pub fn arguments(profile: &str, device: &str) -> Vec<String> {
    let mut args = vec![
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "warning",
        "-nostats",
        "-filter_threads",
        "1",
    ];
    if profile == VAAPI_PROFILE {
        args.extend(["-vaapi_device", device]);
    }
    // Demuxers can otherwise open URLs or local files referenced by hostile media.
    // Only stdin/stdout pipes are allowed, including nested demuxer protocol opens.
    args.extend([
        "-protocol_whitelist",
        "pipe",
        "-threads",
        "2",
        "-i",
        "pipe:0",
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
    ]);
    if profile == VAAPI_PROFILE {
        args.extend([
            "-vf",
            "format=nv12,hwupload",
            "-c:v",
            "h264_vaapi",
            "-qp",
            "23",
        ]);
    } else {
        args.extend([
            "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
        ]);
    }
    args.extend([
        "-threads", "2", "-c:a", "aac", "-b:a", "128k", "-f", "matroska", "pipe:1",
    ]);
    args.into_iter().map(str::to_owned).collect()
}

pub fn command(config: &Config, profile: &str) -> Command {
    let mut command = Command::new(&config.ffmpeg.binary);
    command
        .args(arguments(profile, &config.ffmpeg.vaapi_device))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0);
    command
}

pub fn append_tail(tail: &mut VecDeque<u8>, bytes: &[u8], limit: usize) {
    let bytes = if bytes.len() > limit {
        &bytes[bytes.len() - limit..]
    } else {
        bytes
    };
    let excess = tail.len().saturating_add(bytes.len()).saturating_sub(limit);
    tail.drain(..excess);
    tail.extend(bytes.iter().copied());
}

pub fn sanitized_tail(tail: &VecDeque<u8>) -> String {
    let contiguous: Vec<u8> = tail.iter().copied().collect();
    String::from_utf8_lossy(&contiguous)
        .chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
        .collect()
}

async fn bounded_text<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
) -> std::io::Result<VecDeque<u8>> {
    let mut tail = VecDeque::new();
    let mut buffer = [0; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(tail);
        }
        append_tail(&mut tail, &buffer[..count], 262_144);
    }
}

async fn probe(binary: &str, args: &[&str]) -> Result<String, String> {
    let mut child = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("cannot launch {binary}: {e}"))?;
    let stdout = tokio::spawn(bounded_text(child.stdout.take().unwrap()));
    let stderr = tokio::spawn(bounded_text(child.stderr.take().unwrap()));
    let status = match tokio::time::timeout(Duration::from_secs(20), child.wait()).await {
        Ok(status) => status.map_err(|e| e.to_string()),
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(format!("{binary} capability probe timed out"))
        }
    };
    let out = stdout
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    let err = stderr
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    let status = status?;
    if !status.success() {
        return Err(format!(
            "{binary} probe exited {status}: {}",
            sanitized_tail(&err)
        ));
    }
    Ok(sanitized_tail(&out))
}

pub async fn profiles(config: &Config) -> Vec<Profile> {
    let baseline = async {
        probe(&config.ffmpeg.binary, &["-version"]).await?;
        probe("ffprobe", &["-version"]).await?;
        probe(&config.ffmpeg.binary, &["-hide_banner", "-encoders"]).await
    }
    .await;
    let mut profiles = Vec::with_capacity(config.profiles.enabled.len());
    for id in &config.profiles.enabled {
        let availability = match &baseline {
            Err(error) => Err(error.clone()),
            Ok(encoders) => validate_profile(config, id, encoders).await,
        };
        let reason = availability.err();
        profiles.push(Profile {
            id: id.clone(),
            available: reason.is_none(),
            reason,
        });
    }
    profiles
}

async fn validate_profile(config: &Config, profile: &str, encoders: &str) -> Result<(), String> {
    let encoder = if profile == VAAPI_PROFILE {
        "h264_vaapi"
    } else {
        "libx264"
    };
    for required in [encoder, "aac"] {
        if !encoders
            .lines()
            .any(|line| line.split_whitespace().nth(1) == Some(required))
        {
            return Err(format!("FFmpeg encoder {required} is unavailable"));
        }
    }
    if profile == CPU_PROFILE {
        return Ok(());
    }
    let device = &config.ffmpeg.vaapi_device;
    let metadata = std::fs::metadata(device).map_err(|e| format!("VAAPI device {device}: {e}"))?;
    if !metadata.file_type().is_char_device() {
        return Err(format!("VAAPI device {device} is not a character device"));
    }
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(device)
        .map_err(|e| format!("VAAPI device permissions {device}: {e}"))?;
    probe("vainfo", &["--display", "drm", "--device", device]).await?;
    probe(
        &config.ffmpeg.binary,
        &[
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-vaapi_device",
            device,
            "-filter_threads",
            "1",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x128:rate=10",
            "-frames:v",
            "2",
            "-vf",
            "format=nv12,hwupload",
            "-c:v",
            "h264_vaapi",
            "-qp",
            "23",
            "-an",
            "-f",
            "null",
            "-",
        ],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_tail_is_bounded_and_sanitized() {
        let mut tail = VecDeque::new();
        append_tail(&mut tail, b"discard", 8);
        append_tail(&mut tail, b"\x1bwarning\n", 8);
        assert_eq!(tail.len(), 8);
        assert_eq!(sanitized_tail(&tail), "warning\n");
    }

    #[test]
    fn protocols_are_restricted_for_both_profiles() {
        for profile in [CPU_PROFILE, VAAPI_PROFILE] {
            let args = arguments(profile, "/dev/dri/renderD129");
            let whitelist = args
                .iter()
                .position(|arg| arg == "-protocol_whitelist")
                .unwrap();
            let input = args.iter().position(|arg| arg == "-i").unwrap();
            assert!(whitelist < input);
            assert_eq!(args[whitelist + 1], "pipe");
            assert_eq!(args[input + 1], "pipe:0");
            assert_eq!(args.last().unwrap(), "pipe:1");
        }
    }
}
