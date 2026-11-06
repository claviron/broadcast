//! Prepared Pusher REST publication without a router framework.

use std::collections::HashMap;
use std::sync::Arc;

use claviron_http::{RouteBody, RouteResponse, ServiceResult, bytes_body};
use claviron_pusher::{
    PusherAppKey, PusherAppSecret, TriggerEvent, TriggerEventError,
    authenticate,
};
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Limited};
use tracing::debug;

use crate::BroadcastService;

const MAX_PUBLISH_BODY_BYTES: usize = 64 * 1024;

// This endpoint maps body failures to one status; keep Limited's input error concrete.
#[derive(Debug, thiserror::Error)]
#[error("publication body could not be read")]
struct PublishBodyError;

fn publication_body_error<'a>(
    _: Box<dyn std::error::Error + Send + Sync + 'a>
) -> PublishBodyError {
    PublishBodyError
}

pub(crate) struct PublishBinding {
    service: Arc<BroadcastService>,
    application: Arc<str>,
    key: PusherAppKey,
    secret: Option<PusherAppSecret>,
}

/// Exact publication endpoints prepared from the service's application catalog.
/// Unrelated paths return their original, unread request.
pub struct PublishHandler {
    bindings: HashMap<Box<str>, Arc<PublishBinding>>,
}

impl PublishHandler {
    pub fn new(service: Arc<BroadcastService>) -> Self {
        let bindings = service
            .applications()
            .applications()
            .iter()
            .map(|(name, application)| {
                (
                    format!("/apps/{}/events", application.id).into_boxed_str(),
                    Arc::new(PublishBinding {
                        service: Arc::clone(&service),
                        application: Arc::from(name.as_str()),
                        key: application.key.clone(),
                        secret: application.secret.clone(),
                    }),
                )
            })
            .collect();
        Self { bindings }
    }

    pub(crate) fn binding(
        &self,
        path: &str,
    ) -> Option<&Arc<PublishBinding>> {
        self.bindings.get(path)
    }

    pub async fn try_handle(
        &self,
        request: Request<RouteBody>,
    ) -> ServiceResult<RouteResponse, Request<RouteBody>> {
        match self.binding(request.uri().path()) {
            Some(binding) => {
                ServiceResult::Handled(binding.handle(request).await)
            }
            None => ServiceResult::Unhandled(request),
        }
    }
}

impl PublishBinding {
    pub(crate) async fn handle(
        &self,
        request: Request<RouteBody>,
    ) -> RouteResponse {
        if request.method() != Method::POST {
            let mut response = error(
                StatusCode::METHOD_NOT_ALLOWED,
                "Pusher publication requires POST",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, http::HeaderValue::from_static("POST"));
            return response;
        }
        let Some(secret) = self.secret.as_ref() else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Pusher publishing is not configured",
            );
        };
        let (parts, body) = request.into_parts();
        let body = match Limited::new(
            body.map_err(publication_body_error),
            MAX_PUBLISH_BODY_BYTES,
        )
        .collect()
        .await
        {
            Ok(body) => body.to_bytes(),
            Err(_) => {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Pusher request body is too large",
                );
            }
        };
        if let Err(authentication) = authenticate(
            parts.method.as_str(),
            parts.uri.path(),
            parts.uri.query(),
            &body,
            &self.key,
            secret,
        ) {
            debug!(application = %self.application, error = %authentication, "Pusher publish authentication rejected");
            return error(
                StatusCode::UNAUTHORIZED,
                "Pusher authentication failed",
            );
        }
        let event = match serde_json::from_slice::<TriggerEvent>(&body) {
            Ok(event) => match event.validate() {
                Ok(event) => event,
                Err(TriggerEventError::DataTooLarge) => {
                    return error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "Pusher event data is too large",
                    );
                }
                Err(event) => {
                    return error(StatusCode::BAD_REQUEST, event.to_string());
                }
            },
            Err(event) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    format!("invalid Pusher event: {event}"),
                );
            }
        };
        match self.service.publish(&self.application, &event) {
            Ok(delivered) => {
                debug!(application = %self.application, delivered, event = event.name, "Pusher event published locally");
                Response::builder()
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(bytes_body(bytes::Bytes::from_static(b"{}")))
                    .expect("static response")
            }
            Err(crate::Error::Unavailable) => error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Broadcast service is not running",
            ),
            Err(publish) => {
                error(StatusCode::INTERNAL_SERVER_ERROR, publish.to_string())
            }
        }
    }
}

fn error(
    status: StatusCode,
    message: impl Into<String>,
) -> RouteResponse {
    let payload =
        serde_json::to_vec(&serde_json::json!({ "error": message.into() }))
            .expect("JSON error response");
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(bytes_body(payload.into()))
        .expect("static response")
}

#[cfg(test)]
#[path = "../tests/http.rs"]
mod tests;
