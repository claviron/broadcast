//! A broadcast session at `/app/{key}`, on every door: the offer says what is
//! being opened and carries it, so nothing here knows HTTP versions or Orbit.

use std::sync::Arc;

use claviron_http::session::SessionSpawner;
use claviron_http::socket::{Offer, SocketKind};
use claviron_http::{RouteResponse, err_status};
use claviron_transport::websocket::accept_socket;
use http::StatusCode;
use http::request::Parts;
use tracing::debug;

use crate::BroadcastService;
use crate::stream::{MAX_MESSAGE_SIZE, SocketStream};

impl BroadcastService {
    /// The application whose WebSocket lives at `path`, if one does.
    pub fn socket_application(
        &self,
        path: &str,
    ) -> Option<Arc<str>> {
        let key = path.strip_prefix("/app/")?;
        let catalog = self.applications();
        let (name, _) = catalog.application_by_key(key)?;
        Some(Arc::from(name))
    }

    /// Answer a request attempting to open a session with `application`. `host`
    /// is the site's name; a site without one has the request's.
    pub async fn open_socket(
        &self,
        application: Arc<str>,
        host: Option<&str>,
        request: &Parts,
    ) -> RouteResponse {
        let Some(offer) = request.extensions.get::<Offer>().cloned() else {
            return err_status(StatusCode::BAD_REQUEST);
        };
        // A WebTransport stream an edge carries over Orbit sends the same
        // messages, and may authenticate in its first one.
        let webtransport = match offer.info().kind {
            SocketKind::WebSocket => false,
            SocketKind::WebTransport => true,
            SocketKind::Connect => {
                return err_status(StatusCode::NOT_IMPLEMENTED);
            }
        };
        let host = host
            .or_else(|| request.uri.host())
            .or_else(|| {
                request
                    .headers
                    .get(http::header::HOST)
                    .and_then(|host| host.to_str().ok())
            })
            .unwrap_or_default()
            .to_owned();
        let mut session = match self.accept_session(application) {
            Ok(session) => session,
            Err(refusal) => return err_status(refusal.status()),
        };
        if !webtransport
            && let Err(status) =
                session.authenticate_headers(&host, &request.headers).await
        {
            return err_status(status);
        }
        // Held before it is proposed, so a handshake never announces a session
        // that could not start.
        let listener = request.extensions.get::<SessionSpawner>().cloned();
        accept_socket(&offer, MAX_MESSAGE_SIZE, move |socket| async move {
            // Over Orbit no listener holds the session: retirement arrives as
            // a close.
            let shutdown = async move {
                match listener {
                    Some(listener) => listener.wait_graceful_shutdown().await,
                    None => std::future::pending().await,
                }
            };
            let mut stream = SocketStream(socket);
            let result = async {
                tokio::pin!(shutdown);
                if webtransport
                    && !session
                        .authenticate_stream(&host, &mut stream, &mut shutdown)
                        .await?
                {
                    return Ok(());
                }
                session.run_with_shutdown(stream, shutdown).await
            };
            if let Err(error) = result.await {
                debug!(%error, "broadcast session ended");
            }
        })
    }
}
