use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use streaming_transcoder::{
    api, config::Config, ffmpeg::Profile, scheduler::AppState, session::State,
};
use tower::ServiceExt;

fn app(config: Config) -> (Arc<AppState>, tokio::task::JoinHandle<()>) {
    AppState::start(
        config,
        vec![Profile {
            id: "h264-cpu-mkv".into(),
            available: true,
            reason: None,
        }],
        Some("test-secret".into()),
    )
}

fn request(method: &str, path: &str, token: Option<&str>, body: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    request.body(Body::from(body.to_owned())).unwrap()
}

async fn json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap()
}

#[tokio::test]
async fn auth_and_profile_rejections_do_not_launch_work() {
    let (app, scheduler) = app(Config::default());
    let router = api::router(app.clone());
    let denied = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/sessions",
            None,
            r#"{"profile":"h264-cpu-mkv"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json(denied).await["error"]["code"], "unauthorized");
    let invalid = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/sessions",
            Some("test-secret"),
            r#"{"profile":"arbitrary-ffmpeg-args"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json(invalid).await["error"]["code"], "invalid_profile");
    assert_eq!(app.counts().active, 0);
    assert_eq!(app.counts().retained, 0);
    let health = router
        .oneshot(request("GET", "/healthz", None, ""))
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    app.stop();
    scheduler.await.unwrap();
}

#[tokio::test]
async fn invalid_json_and_unknown_fields_have_json_errors() {
    let (app, scheduler) = app(Config::default());
    let router = api::router(app.clone());
    for payload in [
        "{",
        r#"{"profile":"h264-cpu-mkv","args":["-i","file:///etc/passwd"]}"#,
    ] {
        let response = router
            .clone()
            .oneshot(request(
                "POST",
                "/v1/sessions",
                Some("test-secret"),
                payload,
            ))
            .await
            .unwrap();
        assert!(response.status().is_client_error());
        assert_eq!(json(response).await["error"]["code"], "invalid_request");
    }
    assert_eq!(app.counts().retained, 0);
    app.stop();
    scheduler.await.unwrap();
}

#[tokio::test]
async fn spawn_failure_releases_slot_and_retains_stable_failure() {
    let mut config = Config::default();
    config.ffmpeg.binary = "/nonexistent/transcoder-regression-ffmpeg".into();
    let (app, scheduler) = app(config);
    let first = app.create("h264-cpu-mkv").unwrap();
    let second = app.create("h264-cpu-mkv").unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if first.meta.lock().unwrap().state == State::Failed
                && second.meta.lock().unwrap().state == State::Failed
                && app.counts().active == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for session in [first, second] {
        let path = format!("/v1/sessions/{}", session.id);
        let cancelled = api::router(app.clone())
            .oneshot(request("DELETE", &path, Some("test-secret"), ""))
            .await
            .unwrap();
        assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);
        let status = session.snapshot();
        assert_eq!(status["state"], "failed");
        assert_eq!(status["error"]["code"], "ffmpeg_failed");
        assert!(status["ffmpeg"]["pid"].is_null());
        assert_eq!(status["input"]["connected"], false);
        assert_eq!(status["output"]["connected"], false);
    }
    app.stop();
    scheduler.await.unwrap();
}

#[tokio::test]
async fn unavailable_gpu_is_reported_without_disabling_cpu() {
    let profiles = vec![
        Profile {
            id: "h264-cpu-mkv".into(),
            available: true,
            reason: None,
        },
        Profile {
            id: "h264-vaapi-mkv".into(),
            available: false,
            reason: Some("VAAPI prerequisite missing".into()),
        },
    ];
    let (app, scheduler) = AppState::start(Config::default(), profiles, Some("test-secret".into()));
    let router = api::router(app.clone());
    let ready = router
        .clone()
        .oneshot(request("GET", "/readyz", None, ""))
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let status = json(ready).await;
    assert_eq!(status["profiles"][0]["available"], true);
    assert_eq!(status["profiles"][1]["available"], false);
    let invalid = router
        .oneshot(request(
            "POST",
            "/v1/sessions",
            Some("test-secret"),
            r#"{"profile":"h264-vaapi-mkv"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(app.counts().retained, 0);
    app.stop();
    scheduler.await.unwrap();
}
