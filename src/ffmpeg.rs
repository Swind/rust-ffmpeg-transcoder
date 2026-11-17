use crate::{
    config::Config,
    profile::{self, Profile},
};
use std::{
    collections::VecDeque, ffi::OsStr, os::unix::fs::FileTypeExt, process::Stdio, time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};

pub fn command(config: &Config, arguments: &[String]) -> Command {
    let mut command = Command::new(&config.ffmpeg.binary);
    command
        .args(arguments)
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

async fn probe<S: AsRef<OsStr>>(binary: &str, args: &[S]) -> Result<String, String> {
    let mut child = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0)
        .spawn()
        .map_err(|e| format!("cannot launch {binary}: {e}"))?;
    let pid = child.id();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    // Include draining in the deadline: descendants must not keep probe pipes open.
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let (status, out, err) =
            tokio::join!(child.wait(), bounded_text(stdout), bounded_text(stderr));
        let status = status.map_err(|e| e.to_string())?;
        let out = out.map_err(|e| e.to_string())?;
        let err = err.map_err(|e| e.to_string())?;
        if !status.success() {
            return Err(format!(
                "{binary} probe exited {status}: {}",
                sanitized_tail(&err)
            ));
        }
        Ok(sanitized_tail(&out))
    })
    .await;
    // Reap the direct child and kill any descendants on every exit path.
    if let Some(pid) = pid {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
    result.unwrap_or_else(|_| Err(format!("{binary} capability probe timed out")))
}

pub async fn profiles(config: &Config) -> Result<Vec<Profile>, String> {
    // Parse all enabled recipes before launching probes or binding the listener.
    let mut profiles = profile::load(config)?;
    let baseline = probe("ffprobe", &["-version"]).await;
    let copy_guard = if profiles.iter().any(|profile| profile.copy_aac) {
        probe(&config.ffmpeg.binary, &["-hide_banner", "-bsfs"])
            .await
            .and_then(|filters| {
                if filters.lines().any(|line| line.trim() == "aac_adtstoasc") {
                    Ok(())
                } else {
                    Err("FFmpeg AAC-copy guard aac_adtstoasc is unavailable".into())
                }
            })
    } else {
        Ok(())
    };
    for profile in &mut profiles {
        let availability = match &baseline {
            Err(error) => Err(error.clone()),
            Ok(_) if profile.copy_aac && copy_guard.is_err() => copy_guard.clone(),
            Ok(_) => validate_profile(config, profile).await,
        };
        profile.reason = availability.err();
        profile.available = profile.reason.is_none();
    }
    Ok(profiles)
}

async fn validate_profile(config: &Config, profile: &Profile) -> Result<(), String> {
    if profile.vaapi {
        let device = &config.ffmpeg.vaapi_device;
        let metadata =
            std::fs::metadata(device).map_err(|e| format!("VAAPI device {device}: {e}"))?;
        if !metadata.file_type().is_char_device() {
            return Err(format!("VAAPI device {device} is not a character device"));
        }
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(device)
            .map_err(|e| format!("VAAPI device permissions {device}: {e}"))?;
        probe("vainfo", &["--display", "drm", "--device", device]).await?;
    }
    probe(&config.ffmpeg.binary, &profile.probe_arguments).await?;
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
}
