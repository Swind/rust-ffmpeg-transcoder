use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: Server,
    pub security: Security,
    pub scheduler: Scheduler,
    pub timeouts: Timeouts,
    pub ffmpeg: Ffmpeg,
    pub profiles: Profiles,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
    pub bind: SocketAddr,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".parse().unwrap(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Security {
    pub token_env: String,
    pub allow_insecure_no_auth: bool,
    pub allow_insecure_non_loopback: bool,
}
impl Default for Security {
    fn default() -> Self {
        Self {
            token_env: "TRANSCODER_API_TOKEN".into(),
            allow_insecure_no_auth: false,
            allow_insecure_non_loopback: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Scheduler {
    pub max_concurrent_jobs: usize,
    pub max_queued_jobs: usize,
    pub max_sessions: usize,
}
impl Default for Scheduler {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: 1,
            max_queued_jobs: 16,
            max_sessions: 1024,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Timeouts {
    pub ready_attach_secs: u64,
    pub input_idle_secs: u64,
    pub output_stall_secs: u64,
    pub max_session_secs: u64,
    pub terminal_retention_secs: u64,
    pub shutdown_grace_secs: u64,
}
impl Default for Timeouts {
    fn default() -> Self {
        Self {
            ready_attach_secs: 30,
            input_idle_secs: 120,
            output_stall_secs: 120,
            max_session_secs: 7200,
            terminal_retention_secs: 600,
            shutdown_grace_secs: 10,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Ffmpeg {
    pub binary: String,
    pub vaapi_device: String,
    pub max_stderr_tail_bytes: usize,
    pub max_input_bytes: u64,
}
impl Default for Ffmpeg {
    fn default() -> Self {
        Self {
            binary: "/usr/bin/ffmpeg".into(),
            vaapi_device: "/dev/dri/renderD128".into(),
            max_stderr_tail_bytes: 65536,
            max_input_bytes: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Profiles {
    pub directory: PathBuf,
    pub enabled: Vec<String>,
}
impl Default for Profiles {
    fn default() -> Self {
        Self {
            directory: "profiles".into(),
            enabled: vec!["h264-cpu-mkv".into()],
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config: Self = match path {
            Some(path) => {
                let source = std::fs::read_to_string(path)
                    .map_err(|error| format!("config {}: {error}", path.display()))?;
                toml::from_str(&source)
                    .map_err(|error| format!("config {}: {error}", path.display()))?
            }
            None => Self::default(),
        };
        config.validate().map_err(|error| match path {
            Some(path) => format!("config {}: {error}", path.display()),
            None => error,
        })?;
        if config.profiles.directory.is_relative() {
            let base = match path {
                Some(path) => path.parent().unwrap_or_else(|| Path::new("")).to_path_buf(),
                None => std::env::current_dir()?,
            };
            config.profiles.directory = base.join(&config.profiles.directory);
        }
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        let scheduler = &self.scheduler;
        if !(1..=128).contains(&scheduler.max_concurrent_jobs)
            || !(1..=100_000).contains(&scheduler.max_sessions)
            || scheduler.max_queued_jobs > scheduler.max_sessions
            || scheduler.max_concurrent_jobs > scheduler.max_sessions
        {
            return Err(
                "invalid scheduler capacities (concurrency 1..128, total sessions 1..100000)"
                    .into(),
            );
        }
        for (name, value) in [
            ("ready_attach_secs", self.timeouts.ready_attach_secs),
            ("input_idle_secs", self.timeouts.input_idle_secs),
            ("output_stall_secs", self.timeouts.output_stall_secs),
            (
                "terminal_retention_secs",
                self.timeouts.terminal_retention_secs,
            ),
            ("shutdown_grace_secs", self.timeouts.shutdown_grace_secs),
        ] {
            if value == 0 || value > 604800 {
                return Err(format!("{name} must be between 1 and 604800 seconds"));
            }
        }
        if self.timeouts.max_session_secs > 604800 {
            return Err("max_session_secs must be 0 (disabled) or at most 604800".into());
        }
        if !(1024..=1_048_576).contains(&self.ffmpeg.max_stderr_tail_bytes) {
            return Err("max_stderr_tail_bytes must be between 1024 and 1048576".into());
        }
        if self.ffmpeg.binary.is_empty() || self.security.token_env.is_empty() {
            return Err("ffmpeg binary and security token_env must not be empty".into());
        }
        if self.profiles.directory.as_os_str().is_empty() {
            return Err("profiles.directory must not be empty".into());
        }
        if self.profiles.enabled.is_empty() {
            return Err("at least one profile must be enabled".into());
        }
        for (index, profile) in self.profiles.enabled.iter().enumerate() {
            if !crate::profile::valid_id(profile) {
                return Err(format!(
                    "invalid profile ID {profile:?} in {}: use 1..64 lowercase ASCII letters, digits or hyphens, beginning with a letter or digit",
                    self.profiles.directory.display()
                ));
            }
            if self.profiles.enabled[..index].contains(profile) {
                return Err(format!(
                    "duplicate configured profile {profile:?} in {}",
                    self.profiles.directory.display()
                ));
            }
        }
        Ok(())
    }

    pub fn token(&self) -> Result<Option<String>, String> {
        let token = match std::env::var(&self.security.token_env) {
            Ok(token) if !token.is_empty() => Some(token),
            Ok(_) | Err(std::env::VarError::NotPresent) => None,
            Err(_) => return Err("API token environment variable is not UTF-8".into()),
        };
        if let Some(token) = &token {
            if token.len() > 4096 || token.bytes().any(|b| b.is_ascii_control()) {
                return Err(
                    "API token must be at most 4096 bytes without control characters".into(),
                );
            }
        } else if !self.security.allow_insecure_no_auth {
            return Err(format!(
                "{} is required; explicit local insecure mode is available for testing",
                self.security.token_env
            ));
        } else if !self.server.bind.ip().is_loopback() && !self.security.allow_insecure_non_loopback
        {
            return Err(
                "unauthenticated non-loopback binding requires allow_insecure_non_loopback=true"
                    .into(),
            );
        }
        Ok(token)
    }

    pub fn shutdown_grace(&self) -> Duration {
        Duration::from_secs(self.timeouts.shutdown_grace_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unauthenticated_binding_requires_both_explicit_opt_ins() {
        let mut config = Config::default();
        config.security.token_env =
            format!("TRANSCODER_TEST_UNSET_{}", uuid::Uuid::new_v4().simple());
        assert!(config.token().is_err());
        config.security.allow_insecure_no_auth = true;
        assert!(config.token().unwrap().is_none());
        config.server.bind = "0.0.0.0:8080".parse().unwrap();
        assert!(config.token().is_err());
        config.security.allow_insecure_non_loopback = true;
        assert!(config.token().unwrap().is_none());
    }

    #[test]
    fn zero_total_deadline_is_allowed_but_zero_idle_deadline_is_not() {
        let mut config = Config::default();
        config.timeouts.max_session_secs = 0;
        assert!(config.validate().is_ok());
        config.timeouts.input_idle_secs = 0;
        assert!(config.validate().is_err());
    }
}
