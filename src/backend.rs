//! Configured Pusher publication owners for a host-independent site registry.

use std::sync::Arc;

use claviron_http::{RouteBody, ServiceResult};
use claviron_pipeline::factory::{
    BackendContext, BackendFactory, BackendPlacement,
};
use claviron_pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep, HostContext, Site, SiteRegistry,
};
use claviron_pusher::{PusherConfig, PusherTarget};
use claviron_runtime::registry::{Error, Result};
use http::Request;

use crate::PublishHandler;

/// Install the configured publication factory; the caller owns the site registry.
pub fn register(registry: &SiteRegistry) -> Result<()> {
    for name in ["broadcast", "pusher"] {
        registry.register_backend(Arc::new(Factory(name)))?;
    }
    Ok(())
}

pub(crate) fn directive(scope: &Site) -> Result<Option<&serde_yaml::Value>> {
    match (scope.directive("broadcast"), scope.directive("pusher")) {
        (Some(_), Some(_)) => {
            Err(Error::msg("a scope cannot declare both broadcast and pusher"))
        }
        (broadcast, pusher) => Ok(broadcast.or(pusher)),
    }
}

pub(crate) fn declared(
    source: Option<&serde_yaml::Value>
) -> std::result::Result<Option<PusherConfig>, serde_yaml::Error> {
    match source {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(source) => serde_yaml::from_value(source.clone()).map(Some),
    }
}

struct Factory(&'static str);
impl BackendFactory for Factory {
    fn name(&self) -> &'static str {
        self.0
    }
    fn placement(&self) -> BackendPlacement {
        BackendPlacement::BeforeFiles
    }
    fn prepare(
        &self,
        context: &BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn BackendHandler>>> {
        // Validate aliases in this scope even when the factory's own name is absent.
        directive(context.route)?;
        let config =
            declared(context.route.directive(self.0)).map_err(|error| {
                Error::msg(format!(
                    "backend '{}' at '{}': {error}",
                    self.0, context.prefix
                ))
            })?;
        let Some(config) = config else { return Ok(Vec::new()) };
        let owner: Arc<dyn BackendHandler> = match config.target {
            PusherTarget::Fleet => {
                let service = context.state.registry_ref().resolve::<crate::BroadcastService>()
                    .ok_or_else(|| Error::msg("fleet Pusher target requires the registered BroadcastService"))?;
                if service.applications().applications().is_empty() {
                    return Ok(Vec::new());
                }
                Arc::new(Rest::Fleet(Fleet {
                    publish: PublishHandler::new(Arc::clone(&service)),
                    #[cfg(feature = "websocket")]
                    sockets: Sockets::new(context, service),
                }))
            }
            #[cfg(feature = "pusher")]
            PusherTarget::Http(config) => Arc::new(Rest::Http(
                claviron_pusher::PusherAdapter::new(Arc::new(config)),
            )),
            #[cfg(not(feature = "pusher"))]
            PusherTarget::Http(_) => {
                return Err(Error::msg(
                    "an external Pusher target requires the pusher feature",
                ));
            }
        };
        Ok(vec![owner])
    }
}

/// What a fleet declaration answers: publication, and each application's socket.
struct Fleet {
    publish: PublishHandler,
    #[cfg(feature = "websocket")]
    sockets: Sockets,
}

/// The terms a scope prepared its sockets with.
#[cfg(feature = "websocket")]
struct Sockets {
    service: Arc<crate::BroadcastService>,
    host: Option<Box<str>>,
    /// The transport's `h1.upgrade`: what the connection's terms allow.
    h1_upgrade: bool,
    cors: Option<claviron_http::config::cors::CorsConfig>,
}

#[cfg(feature = "websocket")]
impl Sockets {
    fn new(
        context: &BackendContext<'_>,
        service: Arc<crate::BroadcastService>,
    ) -> Self {
        Self {
            service,
            host: context.host.map(|host| host.to_string().into_boxed_str()),
            h1_upgrade: context.transport.h1.upgrade,
            cors: context.route.cors.clone(),
        }
    }

    fn answer(
        &self,
        request: Request<RouteBody>,
    ) -> Socket {
        use claviron_http::socket::{Carrier, Offer};
        use http::StatusCode;

        let Some(offer) = request.extensions().get::<Offer>() else {
            return Socket::None(request);
        };
        if offer.info().carrier == Carrier::H1 && !self.h1_upgrade {
            return Socket::Answer(refuse(StatusCode::FORBIDDEN));
        }
        let Some(application) =
            self.service.socket_application(request.uri().path())
        else {
            return Socket::None(request);
        };
        // An origin the scope's CORS does not allow opens nothing; channel
        // authorization remains the session's.
        if let Some(origin) = request.headers().get(http::header::ORIGIN)
            && let Some(cors) = &self.cors
            && !origin.to_str().is_ok_and(|origin| {
                cors.enabled() && cors.allows_origin(origin)
            })
        {
            return Socket::Answer(refuse(StatusCode::FORBIDDEN));
        }
        let service = Arc::clone(&self.service);
        let host = self.host.clone();
        let (request, _) = request.into_parts();
        Socket::Answer(DispatchCall::new(async move {
            ServiceResult::Handled(
                service
                    .open_socket(application, host.as_deref(), &request)
                    .await,
            )
        }))
    }
}

/// Whether a request opens one of this scope's sockets.
#[cfg(feature = "websocket")]
enum Socket {
    Answer(DispatchCall),
    None(Request<RouteBody>),
}

#[cfg(feature = "websocket")]
fn refuse(status: http::StatusCode) -> DispatchCall {
    DispatchCall::ready(ServiceResult::Handled(claviron_http::err_status(
        status,
    )))
}

enum Rest {
    Fleet(Fleet),
    #[cfg(feature = "pusher")]
    Http(claviron_pusher::PusherAdapter),
}

impl BackendHandler for Rest {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: match self {
                Self::Fleet(_) => BackendIdentity::None,
                #[cfg(feature = "pusher")]
                Self::Http(_) => BackendIdentity::Request,
            },
            reply: BackendReply::Owner,
        }
    }
}

impl DispatchStep for Rest {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<HostContext<'_>>,
    ) -> DispatchCall {
        match self {
            Self::Fleet(fleet) => {
                #[cfg(feature = "websocket")]
                let request = match fleet.sockets.answer(request) {
                    Socket::Answer(answer) => return answer,
                    Socket::None(request) => request,
                };
                match fleet.publish.binding(request.uri().path()).cloned() {
                    Some(binding) => DispatchCall::new(async move {
                        ServiceResult::Handled(binding.handle(request).await)
                    }),
                    None => {
                        DispatchCall::ready(ServiceResult::Unhandled(request))
                    }
                }
            }
            #[cfg(feature = "pusher")]
            Self::Http(adapter)
                if claviron_pusher::PusherAdapter::accepts_path(
                    request.uri().path(),
                ) =>
            {
                let adapter = adapter.clone();
                DispatchCall::new(async move {
                    let Ok(response) = adapter.handle(request).await;
                    ServiceResult::Handled(response)
                })
            }
            #[cfg(feature = "pusher")]
            Self::Http(_) => {
                DispatchCall::ready(ServiceResult::Unhandled(request))
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/backend.rs"]
mod tests;
