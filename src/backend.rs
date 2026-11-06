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
        registry.register_backend_factory(Arc::new(Factory(name)))?;
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
                Arc::new(Rest::Fleet(PublishHandler::new(service)))
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

enum Rest {
    Fleet(PublishHandler),
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
            Self::Fleet(handler) => match handler
                .binding(request.uri().path())
                .cloned()
            {
                Some(binding) => DispatchCall::new(async move {
                    ServiceResult::Handled(binding.handle(request).await)
                }),
                None => DispatchCall::ready(ServiceResult::Unhandled(request)),
            },
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
