//! A native WebTransport session is the one door whose session is not an offer a
//! backend can take, so it keeps its binding, prepared with the host.

use std::sync::Arc;

use async_trait::async_trait;
use claviron_edge::{
    AnyAuthenticated, AuthContext, H3WebTransportSession, Host,
    WebTransportAccept, WebTransportHandler, WebTransportRegistry,
    WebTransportReject, WebTransportResult,
};
use claviron_pipeline::Site;
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use http::StatusCode;

use crate::{BroadcastService, BroadcastSession};

pub(super) fn declares_fleet(host: &Site) -> bool {
    is_fleet(host) || host.routes.iter().any(|(_, route)| is_fleet(route))
}

pub(super) fn bind(
    state: &AppState,
    canonical: &Host,
    host: &Site,
) -> Result<()> {
    if !declares_fleet(host) {
        return Ok(());
    }
    let service = state
        .registry_ref()
        .resolve::<BroadcastService>()
        .ok_or_else(|| {
            Error::msg(
                "fleet Pusher target requires the registered BroadcastService",
            )
        })?;
    let sessions =
        state.registry_ref().resolve::<WebTransportRegistry>().ok_or_else(
            || Error::msg("broadcast requires the WebTransport registry"),
        )?;
    for (name, application) in service.applications().applications() {
        let path = Arc::<str>::from(format!("/app/{}", application.key));
        if !is_fleet_route(host, &path) {
            continue;
        }
        // What the binding was prepared from, for its admission to compare.
        let Some(source) = directive(selected(host, &path)).cloned() else {
            continue;
        };
        sessions.bind_handler(
            canonical.clone(),
            path.clone(),
            Arc::new(BroadcastWebTransport {
                service: service.clone(),
                application: Arc::from(name.as_str()),
                path,
                source: Arc::new(source),
            }),
        )?;
    }
    Ok(())
}

/// A scope's broadcast declaration as written. `pusher` is its other name;
/// the edge refuses a scope that declares both.
fn directive(scope: &Site) -> Option<&serde_yaml::Value> {
    crate::backend::directive(scope).ok().flatten()
}

use crate::backend::declared;

/// The scope that owns `path`: its most specific route, or the host itself.
fn selected<'a>(
    host: &'a Site,
    path: &str,
) -> &'a Site {
    host.routes.find(path).map(|route| route.as_ref()).unwrap_or(host)
}

/// Whether the scope targets this fleet. A declaration that does not decode
/// selects nothing here; the backend's preparation of the same scope reports
/// it.
fn is_fleet(scope: &Site) -> bool {
    declared(directive(scope)).is_ok_and(|config| {
        config.is_some_and(|config| {
            matches!(config.target, claviron_pusher::PusherTarget::Fleet)
        })
    })
}

fn is_fleet_route(
    host: &Site,
    path: &str,
) -> bool {
    is_fleet(selected(host, path))
}

/// Whether the scope that owns `path` still declares exactly `source`, the
/// declaration a binding was prepared from.
fn still_declared(
    host: &Site,
    path: &str,
    source: &serde_yaml::Value,
) -> bool {
    directive(selected(host, path)) == Some(source)
}

struct BroadcastWebTransport {
    service: Arc<BroadcastService>,
    application: Arc<str>,
    path: Arc<str>,
    /// The fleet declaration this binding was prepared from, as its scope
    /// wrote it. Admission compares against it and decodes nothing.
    source: Arc<serde_yaml::Value>,
}

impl BroadcastWebTransport {
    fn admit(
        &self,
        ctx: &AuthContext,
    ) -> std::result::Result<BroadcastSession, StatusCode> {
        // A removed fleet binding must not be revived by a retained registry
        // entry: the scope that owns the path must still declare what this
        // binding was prepared from. An edited declaration has been prepared
        // again by then, and its binding replaced this one.
        // Public/private/presence admission remains session-owned.
        if ctx.path != self.path
            || !still_declared(&ctx.host_config, &ctx.path, &self.source)
        {
            return Err(StatusCode::NOT_FOUND);
        }
        let route = ctx.host_config.routes.find(&ctx.path);
        let cors = route
            .map(|route| &**route)
            .unwrap_or(&ctx.host_config)
            .cors
            .as_ref();
        if let Some(origin) = ctx.headers.get(http::header::ORIGIN)
            && let Some(cors) = cors
            && !origin.to_str().is_ok_and(|origin| {
                cors.enabled() && cors.allows_origin(origin)
            })
        {
            return Err(StatusCode::FORBIDDEN);
        }
        self.service
            .accept_session(self.application.clone())
            .map_err(|refusal| refusal.status())
    }
}

#[async_trait]
impl WebTransportHandler for BroadcastWebTransport {
    fn name(&self) -> &'static str {
        "broadcast_webtransport"
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &AuthContext,
    ) -> WebTransportResult {
        if ctx.admission.is_none() {
            return Err(WebTransportReject::new(
                StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        self.admit(ctx)
            .map(WebTransportAccept::authenticated)
            .map_err(WebTransportReject::new)
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        accepted: Option<AnyAuthenticated>,
        transport: H3WebTransportSession,
    ) -> Result<()> {
        let mut session = accepted
            .ok_or_else(|| Error::msg("broadcast session missing"))?
            .take::<BroadcastSession>()?;
        ctx.admission.as_ref().ok_or_else(|| {
            Error::msg("broadcast WebTransport admission missing")
        })?;
        let shutdown = async {
            tokio::select! { _ = ctx.wait_graceful_shutdown() => {}, _ = state.on_shutdown() => {} }
        };
        tokio::pin!(shutdown);
        let stream = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            _ = session.wait_shutdown() => return Ok(()),
            stream = transport.accept_bi() => stream,
        };
        let Some(stream) = stream else { return Ok(()) };
        let mut stream = crate::stream::WebTransportMessageStream::new(stream);
        if !session
            .authenticate_stream(
                &ctx.host.to_string(),
                &mut stream,
                &mut shutdown,
            )
            .await?
        {
            return Ok(());
        }
        let result = session
            .run_with_shutdown(stream, &mut shutdown)
            .await
            .map_err(|error| Error::recoverable(error.to_string()));
        drop(transport);
        result
    }
}

#[cfg(test)]
#[path = "../../tests/edge.rs"]
mod tests;
