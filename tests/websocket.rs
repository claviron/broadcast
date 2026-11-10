use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use claviron_broadcast::{BroadcastConfig, BroadcastService, PublishHandler};
use claviron_http::ext::ConnCtx;
use claviron_http::session::SessionOwner;
use claviron_http::socket::attach;
use claviron_http::{ServiceResult, boxed_body, bytes_body};
use claviron_runtime::registry::Runnable;
use claviron_runtime::{AppState, Gate, RunContext};
use fastwebsockets::{FragmentCollector, Frame, Payload, Role, WebSocket};
use http::{Method, Request, StatusCode, header};
use http_body_util::Empty;
use hyper_util::rt::TokioIo;
use std::task::Poll;
use tokio::io::duplex;
use tokio_util::sync::CancellationToken;

const TIMEOUT: Duration = Duration::from_secs(5);

async fn start_service(
    service: Arc<BroadcastService>,
    state: AppState,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<claviron_runtime::registry::Result<()>> {
    let mut generation =
        service.run((None,), state, RunContext::new(cancellation));
    let first = std::future::poll_fn(|context| {
        Poll::Ready(generation.as_mut().poll(context))
    })
    .await;
    assert!(first.is_pending(), "service must publish its generation and park");
    tokio::spawn(generation)
}

fn config() -> BroadcastConfig {
    serde_yaml::from_str::<BroadcastConfig>(
        r#"
applications:
  demo:
    id: "1"
    key: demo-key
    secret: demo-secret
    client_events: true
"#,
    )
    .unwrap()
    .validate()
    .unwrap()
}

fn connection_context() -> Arc<ConnCtx> {
    Arc::new(ConnCtx {
        transport: claviron_http::listener::ListenerTransport::Tcp,
        state: Default::default(),
        listener: "test".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "127.0.0.1:12345".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 443,
        real_ip: None,
        country_code: None,
        tls: None,
    })
}

/// The record format on an accepted Orbit duplex slot, without a door.
struct OrbitMessageStream {
    incoming: orbitive::stream::ReadHalf,
    outgoing: orbitive::stream::WriteHalf,
    records: claviron_orbit::link::application::MessageReader,
}

impl OrbitMessageStream {
    fn new(endpoint: orbitive::stream::Endpoint) -> Self {
        let (incoming, outgoing) = endpoint.split();
        let records =
            claviron_orbit::link::application::MessageReader::with_limit(
                64 * 1024,
            );
        Self { incoming, outgoing, records }
    }
}

impl claviron_broadcast::stream::MessageStream for OrbitMessageStream {
    async fn receive(
        &mut self
    ) -> claviron_broadcast::Result<Option<claviron_broadcast::stream::Message>>
    {
        Ok(self.records.read(&mut self.incoming).await?)
    }

    async fn send(
        &mut self,
        message: claviron_broadcast::stream::Message,
    ) -> claviron_broadcast::Result<()> {
        use tokio::io::AsyncWriteExt;

        claviron_orbit::link::application::write_message(
            &mut self.outgoing,
            &message,
        )
        .await?;
        if matches!(message, claviron_broadcast::stream::Message::Close { .. })
        {
            self.outgoing.shutdown().await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn orbit_messages_share_the_pusher_session_and_service_shutdown() {
    use claviron_broadcast::stream::{Message, MessageStream};
    use claviron_orbit::link::application::ApplicationBodies;

    tokio::time::timeout(TIMEOUT, async {
        let directory = tempfile::tempdir().unwrap();
        let service = Arc::new(BroadcastService::new(config()));
        let cancellation = CancellationToken::new();
        let running = start_service(
            Arc::clone(&service),
            AppState::standalone("broadcast-orbit-test", directory.path()),
            cancellation.clone(),
        )
        .await;

        let fleet = Arc::new(
            orbitive::Fleet::join("broadcast-stream-test", 1).unwrap(),
        );
        let bodies = ApplicationBodies::open(fleet, 1).unwrap();
        let (edge, ticket) = bodies.create().unwrap();
        let application =
            OrbitMessageStream::new(bodies.accept(ticket).unwrap());
        let session =
            service.accept_session(Arc::from("demo")).unwrap_or_else(|_| {
                panic!("running service must admit the configured application")
            });
        let serving = tokio::spawn(session.run(application));
        let mut edge = OrbitMessageStream::new(edge);

        let Some(Message::Text(established)) = edge.receive().await.unwrap()
        else {
            panic!("expected the Pusher establishment message");
        };
        let established: serde_json::Value =
            serde_json::from_str(&established).unwrap();
        assert_eq!(established["event"], "pusher:connection_established");
        edge.send(Message::Text(r#"{"event":"pusher:ping","data":{}}"#.into()))
            .await
            .unwrap();
        let Some(Message::Text(pong)) = edge.receive().await.unwrap() else {
            panic!("expected Pusher pong");
        };
        let pong: serde_json::Value = serde_json::from_str(&pong).unwrap();
        assert_eq!(pong["event"], "pusher:pong");

        cancellation.cancel();
        assert!(matches!(
            edge.receive().await.unwrap(),
            Some(Message::Close { code: 1001, .. })
        ));
        assert_eq!(edge.receive().await.unwrap(), None);
        serving.await.unwrap().unwrap();
        running.await.unwrap().unwrap();
        assert!(service.accept_session(Arc::from("demo")).is_err());
    })
    .await
    .expect("Orbit Pusher lifecycle must finish");
}

#[tokio::test]
async fn websocket_upgrade_speaks_pusher_and_service_restart_drains_the_session()
 {
    tokio::time::timeout(TIMEOUT, async {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::standalone("broadcast-test", directory.path());
        let service = Arc::new(BroadcastService::new(config()));
        let cancellation = CancellationToken::new();
        let running = start_service(Arc::clone(&service), state.clone(), cancellation.clone()).await;

        let owner = SessionOwner::new("test".parse().unwrap());
        let gate = Gate::new(None, TIMEOUT);
        let scope = owner.bind(gate.try_enter().unwrap());
        let sessions = scope.sessions();
        let (client_io, server_io) = duplex(8192);
        let opening = Arc::clone(&service);
        let server = tokio::spawn(async move {
            let answering = Arc::clone(&opening);
            let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                let answering = Arc::clone(&answering);
                let sessions = sessions.clone();
                async move {
                    let mut request = request.map(boxed_body);
                    request.extensions_mut().insert(connection_context());
                    let offer = attach(&mut request, &sessions).unwrap().unwrap();
                    let (parts, _) = request.into_parts();
                    let application =
                        answering.socket_application(parts.uri.path()).unwrap();
                    let response = answering
                        .open_socket(application, Some("socket.test"), &parts)
                        .await;
                    Ok::<_, std::convert::Infallible>(offer.finish(response))
                }
            });
            let _scope = scope;
            hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(server_io), service)
                .with_upgrades()
                .await
                .unwrap();
        });
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(client_io)).await.unwrap();
        let client = tokio::spawn(connection.with_upgrades());
        let request = Request::builder()
            .uri("/app/demo-key?protocol=7&client=js&version=8.4.0")
            .header(header::HOST, "socket.test")
            .header(header::CONNECTION, "Upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let mut response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        let upgraded = hyper::upgrade::on(&mut response).await.unwrap();
        let mut socket = FragmentCollector::new(WebSocket::after_handshake(
            TokioIo::new(upgraded),
            Role::Client
        ));

        let established = socket.read_frame().await.unwrap();
        let established: serde_json::Value = serde_json::from_slice(&established.payload).unwrap();
        assert_eq!(established["event"], "pusher:connection_established");
        let established_data: serde_json::Value =
            serde_json::from_str(established["data"].as_str().unwrap()).unwrap();
        let socket_id = established_data["socket_id"].as_str().unwrap();
        let catalog = service.applications();
        let application = catalog.application_by_id("1").unwrap().1;
        let authorization = claviron_pusher::authorize_channel(
            socket_id,
            "private-news",
            None,
            &application.key,
            application.secret.as_ref().unwrap()
        );

        socket
            .write_frame(Frame::text(Payload::Owned(
                serde_json::to_vec(&serde_json::json!({
                    "event": "pusher:subscribe",
                    "data": {
                        "channel": "private-news",
                        "auth": authorization
                    }
                }))
                .unwrap()
            )))
            .await
            .unwrap();
        let subscribed = socket.read_frame().await.unwrap();
        let subscribed: serde_json::Value = serde_json::from_slice(&subscribed.payload).unwrap();
        assert_eq!(subscribed["event"], "pusher_internal:subscription_succeeded");
        assert_eq!(subscribed["channel"], "private-news");

        let body = br#"{"name":"wallet.updated","channel":"private-news","data":{"amount":5}}"#;
        let query = claviron_pusher::authentication_query(
            Method::POST.as_str(),
            "/apps/1/events",
            body,
            &application.key,
            application.secret.as_ref().unwrap()
        )
        .unwrap();
        let result = PublishHandler::new(Arc::clone(&service))
            .try_handle(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/apps/1/events?{query}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(bytes_body(Bytes::copy_from_slice(body)))
                    .unwrap()
            )
            .await;
        let ServiceResult::Handled(response) = result else { panic!("publication path must be handled") };
        assert_eq!(response.status(), StatusCode::OK);
        let published = socket.read_frame().await.unwrap();
        let published: serde_json::Value = serde_json::from_slice(&published.payload).unwrap();
        assert_eq!(published["event"], "wallet.updated");
        assert_eq!(published["channel"], "private-news");
        assert_eq!(published["data"], r#"{"amount":5}"#);

        cancellation.cancel();
        running.await.unwrap().unwrap();

        server.await.unwrap();
        drop(socket);
        drop(sender);
        client.await.unwrap().unwrap();
        let drain = owner.close();
        gate.graceful_shutdown(None);
        let _ = gate.wait_all_done().await;
        drain.join().await;

        let cancellation = CancellationToken::new();
        let restarted = start_service(Arc::clone(&service),
            AppState::standalone("broadcast-restart-test", directory.path()),
            cancellation.clone()
        ).await;
        cancellation.cancel();
        restarted.await.unwrap().unwrap();
    })
    .await
    .expect("Broadcast WebSocket lifecycle must finish");
}
