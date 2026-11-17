pub mod api;
pub mod config;
pub mod error;
pub mod ffmpeg;
pub mod scheduler;
pub mod session;
pub mod stream;
pub mod supervisor;

use config::Config;
use scheduler::AppState;
use std::{error::Error, time::Duration};

pub async fn run(config: Config) -> Result<(), Box<dyn Error>> {
    config.validate()?;
    let token = config.token()?;
    let profiles = ffmpeg::profiles(&config).await;
    for profile in &profiles {
        if let Some(reason) = &profile.reason {
            tracing::warn!(profile = %profile.id, %reason, "profile unavailable");
        } else {
            tracing::info!(profile = %profile.id, "profile available");
        }
    }
    let listener = tokio::net::TcpListener::bind(config.server.bind).await?;
    let address = listener.local_addr()?;
    let grace = config.shutdown_grace();
    let (app, scheduler) = AppState::start(config, profiles, token);
    let routes = api::router(app.clone());
    let stopped = app.shutdown.clone();
    let mut http = tokio::spawn(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(stopped.cancelled_owned())
            .await
    });
    tracing::info!(bind = %address, "transcoder server listening");
    let mut http_finished = false;
    let mut http_result: Result<(), Box<dyn Error>> = Ok(());
    tokio::select! {
        signal = shutdown_signal() => {
            http_result = signal.map_err(Into::into);
        }
        _ = app.shutdown.cancelled() => {},
        result = &mut http => {
            http_finished = true;
            http_result = match result {
                Ok(result) => result.map_err(Into::into),
                Err(error) => Err(error.into()),
            };
        }
    }
    let deadline = tokio::time::Instant::now() + grace + Duration::from_secs(1);
    app.stop();
    // Reaping is mandatory, even if an HTTP peer never reads its cancelled body.
    scheduler.await?;
    if !http_finished {
        match tokio::time::timeout_at(deadline, &mut http).await {
            Ok(result) => {
                http_result = match result {
                    Ok(result) => result.map_err(Into::into),
                    Err(error) => Err(error.into()),
                };
            }
            Err(_) => {
                http.abort();
                let _ = http.await;
                tracing::warn!("closed HTTP connections after shutdown grace");
            }
        }
    }
    http_result?;
    tracing::info!("all session processes reaped; shutdown complete");
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}
