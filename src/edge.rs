//! The directive on a virtual host. A session at `/app/{key}` reaches the same
//! backend as publication, on every door but native WebTransport.

#[cfg(feature = "webtransport")]
use std::sync::Arc;

use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};

#[cfg(feature = "webtransport")]
mod webtransport;

/// Register host integration only when this application's Edge capability is selected.
/// Runner publication and native sessions do not select this feature.
pub fn register(state: &AppState) -> Result<()> {
    let registry = state
        .registry_ref()
        .resolve::<claviron_edge::HostAppRegistry>()
        .ok_or_else(|| {
            Error::msg("Broadcast requires the Edge host registry")
        })?;
    crate::backend::register(registry.site())?;
    #[cfg(feature = "webtransport")]
    claviron_edge::register_routable(
        state,
        Arc::new(Sessions { state: state.clone() }),
    )?;
    Ok(())
}

#[cfg(feature = "webtransport")]
struct Sessions {
    state: AppState,
}

#[cfg(feature = "webtransport")]
impl claviron_runtime::registry::Component<AppState> for Sessions {
    type Deps = ();
    fn name(&self) -> &str {
        "BroadcastEdgeSessions"
    }
}

#[cfg(feature = "webtransport")]
impl claviron_edge::Routable for Sessions {
    fn match_host(
        &self,
        _: &claviron_edge::Host,
        host: &claviron_edge::HostConfig,
    ) -> bool {
        webtransport::declares_fleet(host)
    }

    fn prepare_sessions(
        &self,
        state: &AppState,
        key: &claviron_edge::Host,
        host: &claviron_edge::HostConfig,
    ) -> Result<()> {
        let _ = &self.state;
        webtransport::bind(state, key, host)
    }
}
