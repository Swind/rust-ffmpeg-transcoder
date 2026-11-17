use crate::session::Failure;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub error: Failure,
}
impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            error: Failure::new(code, message),
        }
    }
    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }
    pub fn gone(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::GONE, code, message)
    }
    pub fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_stopping",
            "server is shutting down",
        )
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response =
            (self.status, Json(serde_json::json!({"error": self.error}))).into_response();
        if self.error.code == "queue_full" {
            response
                .headers_mut()
                .insert("retry-after", "1".parse().unwrap());
        }
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert("www-authenticate", "Bearer".parse().unwrap());
        }
        response
    }
}
