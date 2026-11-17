use crate::{
    config::Config,
    error::ApiError,
    ffmpeg::Profile,
    session::{Session, State},
    supervisor::{self, Control},
};
use axum::http::StatusCode;
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct Entry {
    session: Arc<Session>,
    commands: Option<mpsc::Receiver<Control>>,
}
struct Registry {
    sessions: HashMap<Uuid, Entry>,
    queue: VecDeque<Uuid>,
    active: usize,
    accepting: bool,
    failures: BTreeMap<&'static str, u64>,
}

#[derive(Serialize)]
pub struct Counts {
    pub active: usize,
    pub queued: usize,
    pub retained: usize,
    pub failures: BTreeMap<&'static str, u64>,
}

pub struct AppState {
    pub config: Arc<Config>,
    pub profiles: Vec<Profile>,
    pub token: Option<String>,
    registry: Mutex<Registry>,
    wake: Notify,
    pub shutdown: CancellationToken,
}

impl AppState {
    pub fn start(
        config: Config,
        profiles: Vec<Profile>,
        token: Option<String>,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let app = Arc::new(Self {
            config: Arc::new(config),
            profiles,
            token,
            registry: Mutex::new(Registry {
                sessions: HashMap::new(),
                queue: VecDeque::new(),
                active: 0,
                accepting: true,
                failures: BTreeMap::new(),
            }),
            wake: Notify::new(),
            shutdown: CancellationToken::new(),
        });
        let scheduler = tokio::spawn(schedule(app.clone()));
        (app, scheduler)
    }

    pub fn create(&self, profile: &str) -> Result<Arc<Session>, ApiError> {
        if !self
            .profiles
            .iter()
            .any(|known| known.id == profile && known.available)
        {
            return Err(ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_profile",
                "profile is unknown, disabled, or unavailable; inspect /v1/profiles",
            ));
        }
        let mut registry = self.registry.lock().unwrap();
        if !registry.accepting || self.shutdown.is_cancelled() {
            return Err(ApiError::unavailable());
        }
        let free_slots = self
            .config
            .scheduler
            .max_concurrent_jobs
            .saturating_sub(registry.active);
        if registry.sessions.len() >= self.config.scheduler.max_sessions
            || registry.queue.len() >= self.config.scheduler.max_queued_jobs + free_slots
        {
            return Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "queue_full",
                "session or queue capacity has been reached",
            ));
        }
        let (session, commands) = Session::new(profile.to_owned());
        registry.queue.push_back(session.id);
        registry.sessions.insert(
            session.id,
            Entry {
                session: session.clone(),
                commands: Some(commands),
            },
        );
        drop(registry);
        self.wake.notify_one();
        Ok(session)
    }

    pub fn get(&self, id: Uuid) -> Result<Arc<Session>, ApiError> {
        self.registry
            .lock()
            .unwrap()
            .sessions
            .get(&id)
            .map(|entry| entry.session.clone())
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "session does not exist or retention expired",
                )
            })
    }

    pub fn cancel(&self, id: Uuid) -> Result<(), ApiError> {
        self.get(id)?.request_stop(State::Cancelled, None);
        self.wake.notify_one();
        Ok(())
    }

    pub fn counts(&self) -> Counts {
        let registry = self.registry.lock().unwrap();
        Counts {
            active: registry.active,
            queued: registry.queue.len(),
            retained: registry.sessions.len(),
            failures: registry.failures.clone(),
        }
    }

    pub fn stop(&self) {
        self.registry.lock().unwrap().accepting = false;
        self.shutdown.cancel();
    }
}

async fn schedule(app: Arc<AppState>) {
    let mut workers = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        {
            let mut registry = app.registry.lock().unwrap();
            let shutting_down = app.shutdown.is_cancelled();
            if shutting_down {
                registry.accepting = false;
                for entry in registry.sessions.values() {
                    entry.session.request_stop(State::Cancelled, None);
                }
            }
            // Cancelled queued jobs must be reclaimed even while every slot is busy.
            let mut index = 0;
            while index < registry.queue.len() {
                let id = registry.queue[index];
                let entry = registry
                    .sessions
                    .get_mut(&id)
                    .expect("queued session exists");
                if entry.session.stop.is_cancelled() {
                    entry.commands.take();
                    supervisor::finish(&entry.session);
                    registry.queue.remove(index);
                } else {
                    index += 1;
                }
            }
            while !shutting_down && registry.active < app.config.scheduler.max_concurrent_jobs {
                let Some(id) = registry.queue.pop_front() else {
                    break;
                };
                let entry = registry
                    .sessions
                    .get_mut(&id)
                    .expect("queued session exists");
                let commands = entry
                    .commands
                    .take()
                    .expect("queued worker has not started");
                let session = entry.session.clone();
                let config = app.config.clone();
                registry.active += 1;
                workers.spawn(async move {
                    supervisor::run(session.clone(), commands, config).await;
                    session
                });
            }
            let retention = Duration::from_secs(app.config.timeouts.terminal_retention_secs);
            registry.sessions.retain(|_, entry| {
                entry
                    .session
                    .meta
                    .lock()
                    .unwrap()
                    .finished
                    .is_none_or(|finished| finished.elapsed() < retention)
            });
            if shutting_down && workers.is_empty() {
                break;
            }
        }
        tokio::select! {
            result = workers.join_next(), if !workers.is_empty() => {
                let mut registry = app.registry.lock().unwrap();
                registry.active -= 1;
                match result.expect("nonempty worker set") {
                    Ok(session) => {
                        let meta = session.meta.lock().unwrap();
                        if let Some(error) = &meta.error {
                            let count = registry.failures.entry(error.code).or_default();
                            *count = count.saturating_add(1);
                        }
                        tracing::info!(active = registry.active, queued = registry.queue.len(), "scheduler capacity released");
                    }
                    Err(error) => {
                        // Supervisors contain no detached child/I/O tasks. A panic is
                        // nevertheless fatal: do not keep accepting work in unknown state.
                        tracing::error!(%error, "session supervisor panicked; shutting down");
                        app.shutdown.cancel();
                    }
                }
            }
            _ = app.wake.notified() => {},
            _ = app.shutdown.cancelled(), if !app.shutdown.is_cancelled() => {},
            _ = tick.tick() => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queue_is_bounded_and_fifo_before_scheduler_runs() {
        let mut config = Config::default();
        config.scheduler.max_queued_jobs = 1;
        let profiles = vec![Profile {
            id: "h264-cpu-mkv".into(),
            available: true,
            reason: None,
        }];
        let (app, worker) = AppState::start(config, profiles, None);
        let first = app.create("h264-cpu-mkv").unwrap();
        let second = app.create("h264-cpu-mkv").unwrap();
        assert!(
            matches!(app.create("h264-cpu-mkv"), Err(error) if error.error.code == "queue_full")
        );
        assert_eq!(
            app.registry
                .lock()
                .unwrap()
                .queue
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![first.id, second.id]
        );
        app.stop();
        worker.await.unwrap();
        assert_eq!(first.meta.lock().unwrap().state, State::Cancelled);
        assert_eq!(second.meta.lock().unwrap().state, State::Cancelled);
        assert_eq!(app.counts().active, 0);
    }

    #[tokio::test]
    async fn retained_terminal_metadata_counts_toward_session_limit() {
        let mut config = Config::default();
        config.scheduler.max_sessions = 1;
        config.scheduler.max_queued_jobs = 1;
        config.ffmpeg.binary = "/nonexistent/transcoder-regression-ffmpeg".into();
        let profiles = vec![Profile {
            id: "h264-cpu-mkv".into(),
            available: true,
            reason: None,
        }];
        let (app, worker) = AppState::start(config, profiles, None);
        let session = app.create("h264-cpu-mkv").unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if session.meta.lock().unwrap().state.terminal() && app.counts().active == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(app.counts().queued, 0);
        assert_eq!(app.counts().retained, 1);
        assert!(
            matches!(app.create("h264-cpu-mkv"), Err(error) if error.error.code == "queue_full")
        );
        app.stop();
        worker.await.unwrap();
    }
}
