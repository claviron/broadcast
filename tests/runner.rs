use bytes::Bytes;
use http::Request;
use http_body_util::BodyExt;

use super::*;

fn owner() -> Owner {
    Owner::new(service_fn(|_: AppRequest| async {
        Ok::<_, Infallible>(Response::new(claviron_http::bytes_body(
            Bytes::from_static(b"owner"),
        )))
    }))
}

async fn answer(
    owner: &Owner,
    method: Method,
    path: &str,
) -> (StatusCode, Bytes) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .body(claviron_http::empty_body())
        .unwrap();
    let response = owner.handle(request).await;
    let status = response.status();
    (status, response.into_body().collect().await.unwrap().to_bytes())
}

/// Publication is answered in front of the owner, and everything else still
/// reaches it. A process with no service keeps its owner as it was.
#[tokio::test]
async fn publication_stands_in_front_of_the_owner_and_nothing_else_does() {
    let state = AppState::standalone("broadcast-runner", ".");
    let alone = in_front(&state, owner()).unwrap();
    let (status, body) = answer(&alone, Method::POST, "/apps/1/events").await;
    assert_eq!((status, body.as_ref()), (StatusCode::OK, b"owner".as_ref()));

    let (served, placement) = place(&state, owner());
    placement.prepare(crate::test_support::service()).unwrap();
    let (status, body) = answer(&served, Method::POST, "/apps/1/events").await;
    assert_ne!(status, StatusCode::OK);
    assert_ne!(body.as_ref(), b"owner");
    let (status, body) = answer(&served, Method::GET, "/elsewhere").await;
    assert_eq!((status, body.as_ref()), (StatusCode::OK, b"owner".as_ref()));
}
