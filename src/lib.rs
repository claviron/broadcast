mod admission;
#[cfg(feature = "backend")]
pub mod backend;
mod channels;
mod config;
#[cfg(feature = "edge")]
pub mod edge;
mod error;
mod fleet;
pub mod groups;
pub mod hooks;
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "runner")]
pub mod runner;
mod service;
mod session;
pub mod stream;
#[cfg(feature = "websocket")]
mod upgrade;

pub use admission::SessionAuthenticator;
pub use config::{
    BroadcastApplicationConfig, BroadcastConfig, BroadcastConfigError,
};
pub use error::{Error, Result, SessionRefusal};
#[cfg(feature = "http")]
pub use http::PublishHandler;
pub use service::{
    BroadcastService, GroupLayer, register, register_with_config,
};
pub use session::BroadcastSession;
#[cfg(feature = "websocket")]
pub use upgrade::BroadcastUpgradeHandler;

/// Wire compatibility, independent of the broadcast service's transport.
pub mod protocol {
    pub use claviron_pusher as pusher;
}

#[cfg(all(test, feature = "http"))]
#[path = "../tests/support.rs"]
mod test_support;
