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
use claviron_http::{ServiceResult, err_status};
use claviron_runner::routing::Upgrades;
use claviron_runner::{AppRequest, AppResponse, Owner, RequestBody};
use claviron_runtime::registry::Bootable;
use claviron_runtime::{AppState, Component, registry};
use http::StatusCode;
use tower::{ServiceExt, service_fn};

use crate::{BroadcastService, PublishHandler};

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
    let (owner, placement) = place(owner);
    state.registry_ref().register(placement)?;
    Ok(owner)
}

fn place(mut owner: Owner) -> (Owner, Arc<Placement>) {
    let execution = owner.execution();
    let upgrades = owner.take_upgrades();
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
    let sockets =
        Sockets { prepared: Arc::clone(&placement.prepared), owner: upgrades };
    let owner = Owner::new(execution, answering).upgrades(service_fn(
        move |request: AppRequest| {
            let sockets = sockets.clone();
            async move { Ok::<_, Infallible>(sockets.answer(request).await) }
        },
    ));
    (owner, placement)
}

/// What the catalog names, prepared once it has been applied.
struct Prepared {
    service: Arc<BroadcastService>,
    publish: PublishHandler,
}

impl Prepared {
    fn new(service: Arc<BroadcastService>) -> Self {
        Self { publish: PublishHandler::new(Arc::clone(&service)), service }
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
            .set(Prepared::new(service))
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

/// Each application's WebSocket, on every door. Any other attempt is the owner's.
#[derive(Clone)]
struct Sockets {
    prepared: Arc<OnceLock<Prepared>>,
    owner: Option<Upgrades>,
}

impl Sockets {
    async fn answer(
        &self,
        request: AppRequest,
    ) -> AppResponse {
        let found = self.prepared.get().and_then(|prepared| {
            let path = request.uri().path();
            Some((
                Arc::clone(&prepared.service),
                prepared.service.socket_application(path)?,
            ))
        });
        if let Some((service, application)) = found {
            let (request, _) = request.into_parts();
            return service.open_socket(application, None, &request).await;
        }
        match self.owner.clone() {
            Some(owner) => {
                let Ok(response) = owner.oneshot(request).await;
                response
            }
            None => err_status(StatusCode::NOT_IMPLEMENTED),
        }
    }
}

#[cfg(test)]
#[path = "../tests/runner.rs"]
mod tests;
