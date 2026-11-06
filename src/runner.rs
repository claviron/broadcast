//! Broadcasting in front of a language runner's owner, on every door: the
//! Pusher REST publication routes and each application's WebSocket at
//! `/app/{key}` on the sockets, and the same session over Orbit, where the edge
//! speaks the protocol and whole messages cross the slot.
//!
//! The protocol, the sessions, channel authorization and the fleet's fanout are
//! the service's; this only places them. What is not theirs goes on to the owner.

use std::convert::Infallible;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use claviron_http::session::SessionSpawner;
use claviron_http::upgrade::{H1UpgradeRegistry, try_h1_upgrade};
use claviron_http::{ServiceResult, empty_body};
use claviron_orbit::link::application::Endpoint;
use claviron_runner::orbit::{Handshake, OrbitSession};
use claviron_runner::routing::Upgrades;
use claviron_runner::{AppRequest, AppResponse, Owner, RequestBody};
use claviron_runtime::registry::Bootable;
use claviron_runtime::{AppState, Component, registry};
use http::{Method, Response, StatusCode, Version};
use tower::{ServiceExt, service_fn};
use tracing::debug;

use crate::stream::OrbitMessageStream;
use crate::{BroadcastService, BroadcastSession, PublishHandler};

/// `owner` with the registered service's endpoints in front of it. Without the
/// service the owner is returned as it was. The endpoints are read from the
/// catalog at boot, once its configuration has been applied.
pub fn in_front(
    state: &AppState,
    owner: Owner,
) -> registry::Result<Owner> {
    if state.registry_ref().resolve::<BroadcastService>().is_none() {
        return Ok(owner);
    }
    let (owner, placement) = place(state, owner);
    state.registry_ref().register(placement)?;
    Ok(owner)
}

fn place(
    state: &AppState,
    mut owner: Owner,
) -> (Owner, Arc<Placement>) {
    let upgrades = owner.take_upgrades();
    let sessions = owner.take_orbit_sessions();
    let placement = Arc::new(Placement::default());
    let prepared = Arc::clone(&placement.prepared);
    let answering = service_fn(move |request: AppRequest| {
        let (prepared, owner) = (Arc::clone(&prepared), owner.clone());
        async move {
            let request = request.map(RequestBody::into_inner);
            let Some(prepared) = prepared.get() else {
                return Ok::<_, Infallible>(owner.handle(request).await);
            };
            Ok(match prepared.publish.try_handle(request).await {
                ServiceResult::Handled(response) => response,
                ServiceResult::Unhandled(request) => {
                    owner.handle(request).await
                }
            })
        }
    });
    let sockets = Sockets {
        state: state.clone(),
        prepared: Arc::clone(&placement.prepared),
        owner: upgrades,
    };
    let on_lanes =
        OnLanes { prepared: Arc::clone(&placement.prepared), sessions };
    let owner = Owner::new(answering)
        .upgrades(service_fn(move |request: AppRequest| {
            let sockets = sockets.clone();
            async move { Ok::<_, Infallible>(sockets.answer(request).await) }
        }))
        .orbit_sessions(move |request, endpoint, handshake| {
            let on_lanes = on_lanes.clone();
            async move { on_lanes.serve(request, endpoint, handshake).await }
        });
    (owner, placement)
}

/// What the catalog names, prepared once it has been applied.
struct Prepared {
    service: Arc<BroadcastService>,
    publish: PublishHandler,
    sockets: H1UpgradeRegistry,
    host: claviron_types::Host,
}

impl Prepared {
    fn new(service: Arc<BroadcastService>) -> registry::Result<Self> {
        // The listener's own identity in the upgrade registry, not a host: a
        // runner has none, and whatever stands in front owns public hosts.
        let host: claviron_types::Host =
            "runner".parse().expect("a valid listener name");
        let sockets = H1UpgradeRegistry::default();
        for (name, application) in service.applications().applications() {
            sockets.register_handler(
                host.clone(),
                format!("/app/{}", application.key),
                service.upgrade_handler(name.as_str()),
            )?;
        }
        Ok(Self {
            publish: PublishHandler::new(Arc::clone(&service)),
            service,
            sockets,
            host,
        })
    }
}

/// Prepares the endpoints during boot, after the service's configuration.
#[derive(Default)]
struct Placement {
    prepared: Arc<OnceLock<Prepared>>,
}

impl Placement {
    fn prepare(
        &self,
        service: Arc<BroadcastService>,
    ) -> registry::Result<()> {
        self.prepared
            .set(Prepared::new(service)?)
            .map_err(|_| registry::Error::msg("already prepared"))
    }
}

impl Component<AppState> for Placement {
    type Deps = Arc<BroadcastService>;

    fn name(&self) -> &'static str {
        "BroadcastPlacement"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, Arc<BroadcastService>> for Placement {
    async fn boot(
        &self,
        service: Arc<BroadcastService>,
        _state: &AppState,
    ) -> registry::Result<()> {
        self.prepare(service)
    }
}

/// Each application's WebSocket on the sockets.
#[derive(Clone)]
struct Sockets {
    state: AppState,
    prepared: Arc<OnceLock<Prepared>>,
    owner: Option<Upgrades>,
}

impl Sockets {
    async fn answer(
        &self,
        request: AppRequest,
    ) -> AppResponse {
        // An HTTP/1.1 GET to a registered path, on a connection that can hold
        // the session it starts. Any other upgrade is the owner's to claim.
        let binding = self.prepared.get().filter(|_| {
            request.method() == Method::GET
                && request.version() == Version::HTTP_11
                && request.extensions().get::<SessionSpawner>().is_some()
        });
        let binding = binding.and_then(|prepared| {
            let binding = prepared
                .sockets
                .resolve(&prepared.host, request.uri().path())?;
            Some((prepared.host.clone(), binding))
        });
        let request = if let Some((host, binding)) = binding {
            match try_h1_upgrade(
                self.state.clone(),
                host,
                binding,
                request.map(RequestBody::into_inner),
            )
            .await
            {
                ServiceResult::Handled(response) => return response,
                ServiceResult::Unhandled(request) => {
                    request.map(RequestBody::new)
                }
            }
        } else {
            request
        };
        match self.owner.clone() {
            Some(owner) => {
                let Ok(response) = owner.oneshot(request).await;
                response
            }
            None => status(StatusCode::NOT_IMPLEMENTED),
        }
    }
}

fn status(code: StatusCode) -> AppResponse {
    let mut response = Response::new(empty_body());
    *response.status_mut() = code;
    response
}

/// The same sessions over Orbit. The edge owns the client's handshake and waits
/// for this admission before accepting.
#[derive(Clone)]
struct OnLanes {
    prepared: Arc<OnceLock<Prepared>>,
    sessions: Option<OrbitSession>,
}

impl OnLanes {
    async fn serve(
        self,
        request: AppRequest,
        mut endpoint: Endpoint,
        handshake: Handshake,
    ) {
        let found = self.prepared.get().and_then(|prepared| {
            let key = request.uri().path().strip_prefix("/app/")?;
            let catalog = prepared.service.applications();
            let (name, _) = catalog.application_by_key(key)?;
            Some((Arc::clone(&prepared.service), Arc::<str>::from(name)))
        });
        let Some((service, application)) = found else {
            // Not one of theirs: the owner's own WebSocket.
            match self.sessions {
                Some(sessions) => sessions(request, endpoint, handshake).await,
                None => {
                    refuse(handshake, endpoint, StatusCode::NOT_IMPLEMENTED)
                }
            }
            return;
        };
        let webtransport = request.version() == Version::HTTP_3
            && request.method() == Method::CONNECT;
        let host = request
            .headers()
            .get(http::header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let mut session = match service.accept_session(application) {
            Ok(session) => session,
            Err(refusal) => {
                return refuse(handshake, endpoint, refusal.status());
            }
        };
        if !webtransport
            && let Err(refused) =
                session.authenticate_headers(&host, request.headers()).await
        {
            return refuse(handshake, endpoint, refused);
        }
        // Held before it is acknowledged, so a 101 never announces a session
        // that could not start.
        if !handshake.try_answer(101, Vec::new()) {
            endpoint.reset();
            return;
        }
        if let Err(error) = run(session, host, webtransport, endpoint).await {
            debug!("broadcast session ended: {error}");
        }
    }
}

fn refuse(
    handshake: Handshake,
    mut endpoint: Endpoint,
    status: StatusCode,
) {
    if !handshake.try_answer(status.as_u16(), Vec::new()) {
        endpoint.reset();
    }
}

/// A WebTransport stream uses the same message records over Link as it does on
/// a native H3 listener, including its optional first authentication record.
async fn run(
    mut session: BroadcastSession,
    host: String,
    webtransport: bool,
    endpoint: Endpoint,
) -> crate::Result<()> {
    let mut stream = OrbitMessageStream::new(endpoint);
    if webtransport
        && !session
            .authenticate_stream(&host, &mut stream, std::future::pending())
            .await?
    {
        return Ok(());
    }
    session.run(stream).await
}

#[cfg(test)]
#[path = "../tests/runner.rs"]
mod tests;
