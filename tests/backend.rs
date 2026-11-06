use super::*;
use crate::test_support;
use bytes::Bytes;
use claviron_runtime::AppState;
use http::StatusCode;
use http_body_util::BodyExt;
use std::sync::atomic::Ordering;

fn prepare(
    source: &str,
    registered: bool,
) -> Result<Vec<Arc<dyn BackendHandler>>> {
    let state = AppState::standalone("broadcast-preparation", ".");
    if registered {
        state.registry_ref().insert(test_support::service())?;
    }
    let site: Site = serde_yaml::from_str(source).unwrap();
    let prefix = claviron_http::ext::PathPrefix::root();
    let transport =
        claviron_http::config::transport::TransportPolicy::default();
    let context = BackendContext {
        state: &state,
        host: None,
        site: &site,
        route: &site,
        prefix: &prefix,
        policy: claviron_http::policy::HttpPolicy::unlimited(),
        transport: &transport,
        protocol: None,
        listener: None,
    };
    let mut owners = Factory("broadcast").prepare(&context)?;
    owners.extend(Factory("pusher").prepare(&context)?);
    Ok(owners)
}

#[test]
fn inactive_directives_prepare_no_owner_without_a_service() {
    for source in ["{}", "broadcast: null", "pusher: null"] {
        assert!(prepare(source, false).unwrap().is_empty());
    }
}

#[test]
fn missing_fleet_service_and_ambiguous_aliases_fail_preparation() {
    assert!(prepare("broadcast: {target: fleet}", false).is_err());
    assert!(
        prepare("broadcast: {target: fleet}\npusher: {target: fleet}", true)
            .is_err()
    );
    assert_eq!(prepare("pusher: {target: fleet}", true).unwrap().len(), 1);
}

#[tokio::test]
async fn native_owner_returns_unread_fallthrough_and_keeps_handled_errors_terminal()
 {
    let owners = prepare("broadcast: {target: fleet}", true).unwrap();
    assert_eq!(owners[0].contract().identity, BackendIdentity::None);
    for (uri, matched) in
        [("/assets/app.js", false), ("/apps/1/events?x=1", true)]
    {
        let payload = Bytes::from(vec![0, 255, 13, 10]);
        let address = payload.as_ptr();
        let (body, polls) = test_support::body(payload);
        let request = Request::builder().uri(uri).body(body).unwrap();
        let result = match owners[0].call(request, None) {
            DispatchCall::Ready(result) => result,
            DispatchCall::Pending(work) => work.await,
        };
        assert_eq!(polls.load(Ordering::Relaxed), 0);
        match result {
            ServiceResult::Handled(response) => {
                assert!(matched);
                assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
            }
            ServiceResult::Unhandled(request) => {
                assert!(!matched);
                assert_eq!(request.uri(), uri);
                assert_eq!(
                    request
                        .into_body()
                        .collect()
                        .await
                        .unwrap()
                        .to_bytes()
                        .as_ptr(),
                    address
                );
            }
        }
    }
}
