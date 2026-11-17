use std::{path::PathBuf, process::ExitCode};
use streaming_transcoder::config::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match start().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "server failed");
            ExitCode::FAILURE
        }
    }
}

async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let mut config = None;
    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("--config") if config.is_none() => {
                config = Some(PathBuf::from(
                    args.next().ok_or("--config requires a path")?,
                ));
            }
            Some("--help" | "-h") => {
                println!(
                    "Usage: transcoder-server [--config PATH]\nBearer token: TRANSCODER_API_TOKEN (or configured token_env).\nWithout --config, secure loopback defaults are used."
                );
                return Ok(());
            }
            _ => {
                return Err(format!(
                    "unknown or duplicate argument: {}",
                    argument.to_string_lossy()
                )
                .into());
            }
        }
    }
    streaming_transcoder::run(Config::load(config.as_deref())?).await
}
