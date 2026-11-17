use crate::{error::ApiError, scheduler::AppState, stream::RequestGuard, supervisor::Control};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, MatchedPath, Path, Request, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub fn router(app: Arc<AppState>) -> Router {
    let protected = Router::new()
        .route("/v1/profiles", get(profiles))
        .route(
            "/v1/sessions",
            post(create).layer(DefaultBodyLimit::max(4096)),
        )
        .route("/v1/sessions/{id}", get(inspect).delete(cancel))
        .route(
            "/v1/sessions/{id}/input",
            put(upload).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/v1/sessions/{id}/output",
            get(download).head(|| async {
                ApiError::new(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "output requires GET; HEAD never reserves a consumer",
                )
            }),
        )
        .route_layer(middleware::from_fn_with_state(app.clone(), authenticate));
    Router::new()
        .merge(protected)
        .route("/healthz", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/readyz", get(ready))
        .fallback(|| async {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "endpoint does not exist",
            )
        })
        .method_not_allowed_fallback(|| async {
            ApiError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "HTTP method not supported",
            )
        })
        .layer(middleware::from_fn(trace_request))
        .with_state(app)
}

async fn trace_request(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    // Matched route templates, not raw paths, keep session identifiers out of
    // default request logs. Authorization and media metadata are never logged.
    let route = request.extensions().get::<MatchedPath>().cloned();
    let response = next.run(request).await;
    let route = route
        .as_ref()
        .map(MatchedPath::as_str)
        .unwrap_or("unmatched");
    tracing::info!(%method, %route, status = response.status().as_u16(), elapsed_ms = started.elapsed().as_millis() as u64, "request");
    response
}

fn same_token(expected: &[u8], supplied: &[u8]) -> bool {
    let mut difference = expected.len() ^ supplied.len();
    for (index, byte) in expected.iter().enumerate() {
        difference |= (*byte ^ supplied.get(index).copied().unwrap_or(0)) as usize;
    }
    difference == 0
}

async fn authenticate(
    State(app): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if let Some(expected) = &app.token {
        let authorization = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !authorization
            .is_some_and(|supplied| same_token(expected.as_bytes(), supplied.as_bytes()))
        {
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "valid bearer authorization is required",
            ));
        }
    }
    Ok(next.run(request).await)
}

async fn ready(State(app): State<Arc<AppState>>) -> Response {
    let ready =
        !app.shutdown.is_cancelled() && app.profiles.iter().all(|profile| profile.available);
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "ready": ready,
            "profiles": app.profiles,
            "scheduler": app.counts(),
        })),
    )
        .into_response()
}

async fn profiles(State(app): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({"profiles": app.profiles}))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    profile: String,
}

async fn create(
    State(app): State<Arc<AppState>>,
    request: Result<Json<Create>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) = request
        .map_err(|error| ApiError::new(error.status(), "invalid_request", error.body_text()))?;
    let session = app.create(&request.profile)?;
    let status_url = format!("/v1/sessions/{}", session.id);
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": session.id,
            "state": "queued",
            "profile": session.profile,
            "status_url": status_url,
            "input_url": format!("{status_url}/input"),
            "output_url": format!("{status_url}/output"),
        })),
    )
        .into_response())
}

fn session_id(id: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(id)
        .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "not_found", "session does not exist"))
}

async fn inspect(
    State(app): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(app.get(session_id(&id)?)?.snapshot()))
}

async fn cancel(
    State(app): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    app.cancel(session_id(&id)?)?;
    Ok(StatusCode::NO_CONTENT)
}

fn upload_length(headers: &HeaderMap, limit: u64) -> Result<u64, ApiError> {
    if headers.contains_key(header::TRANSFER_ENCODING) {
        return Err(ApiError::new(
            StatusCode::LENGTH_REQUIRED,
            "missing_content_length",
            "streaming uploads require Content-Length, not Transfer-Encoding",
        ));
    }
    for encoding in headers.get_all(header::CONTENT_ENCODING) {
        if !encoding.as_bytes().eq_ignore_ascii_case(b"identity") {
            return Err(ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "invalid_content_encoding",
                "only identity Content-Encoding is supported",
            ));
        }
    }
    if !headers.get(header::CONTENT_TYPE).is_some_and(|value| {
        value
            .as_bytes()
            .eq_ignore_ascii_case(b"application/octet-stream")
    }) {
        return Err(ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "invalid_content_type",
            "upload Content-Type must be application/octet-stream",
        ));
    }
    let length = headers
        .get(header::CONTENT_LENGTH)
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::LENGTH_REQUIRED,
                "missing_content_length",
                "upload requires Content-Length",
            )
        })?
        .to_str()
        .ok()
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_content_length",
                "Content-Length must be an unsigned decimal integer",
            )
        })?;
    if limit > 0 && length > limit {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "input_too_large",
            "upload exceeds the configured per-session size limit",
        ));
    }
    Ok(length)
}

fn control_error(error: tokio::sync::mpsc::error::TrySendError<Control>) -> ApiError {
    match error {
        tokio::sync::mpsc::error::TrySendError::Closed(_) => worker_unavailable(),
        tokio::sync::mpsc::error::TrySendError::Full(_) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "session_busy",
            "session control queue is full",
        ),
    }
}

fn worker_unavailable() -> ApiError {
    ApiError::gone("session_terminal", "session worker is no longer available")
}

async fn upload(
    State(app): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, ApiError> {
    let session = app.get(session_id(&id)?)?;
    session.attachment_allowed(true)?;
    let length = upload_length(&headers, app.config.ffmpeg.max_input_bytes)?;
    let request_cancel = CancellationToken::new();
    let mut guard = RequestGuard::new(request_cancel.clone());
    let (reply, accepted) = oneshot::channel();
    session
        .commands
        .try_send(Control::Input {
            body,
            length,
            request_cancel,
            reply,
        })
        .map_err(control_error)?;
    let completion = accepted.await.map_err(|_| worker_unavailable())??;
    let result = completion.await.map_err(|_| worker_unavailable())?;
    guard.disarm();
    result.map_err(|error| ApiError {
        status: StatusCode::BAD_REQUEST,
        error,
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn download(
    State(app): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let session = app.get(session_id(&id)?)?;
    if headers.contains_key(header::RANGE) {
        return Err(ApiError::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "range_not_supported",
            "output is not seekable or resumable",
        ));
    }
    session.attachment_allowed(false)?;
    let (reply, accepted) = oneshot::channel();
    session
        .commands
        .try_send(Control::Output { reply })
        .map_err(control_error)?;
    let body = accepted.await.map_err(|_| worker_unavailable())??;
    Ok((
        [
            (header::CONTENT_TYPE, "video/x-matroska"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_rejects_length_and_content_differences() {
        assert!(same_token(b"long-secret", b"long-secret"));
        assert!(!same_token(b"long-secret", b"long-secret-extra"));
        assert!(!same_token(b"long-secret", b"long"));
        assert!(!same_token(b"long-secret", b"long-secrex"));
    }

    #[test]
    fn upload_headers_never_reserve_ownership_or_allocate_by_length() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/octet-stream".parse().unwrap(),
        );
        assert_eq!(
            upload_length(&headers, 0).unwrap_err().status,
            StatusCode::LENGTH_REQUIRED
        );
        headers.insert(
            header::CONTENT_LENGTH,
            u64::MAX.to_string().parse().unwrap(),
        );
        assert_eq!(upload_length(&headers, 0).unwrap(), u64::MAX);
        assert_eq!(
            upload_length(&headers, 100).unwrap_err().status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
        assert_eq!(
            upload_length(&headers, 0).unwrap_err().status,
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }
}
