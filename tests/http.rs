use super::*;
use crate::test_support;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn unrelated_paths_keep_the_original_unpolled_body() {
    let handler = PublishHandler::new(test_support::service());
    for path in
        ["/apps/1", "/apps/1/events/", "/apps/other/events", "/assets/app.js"]
    {
        let payload = bytes::Bytes::from(vec![0, 255, 13, 10]);
        let address = payload.as_ptr();
        let (body, polls) = test_support::body(payload);
        let request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .body(body)
            .unwrap();
        let ServiceResult::Unhandled(request) =
            handler.try_handle(request).await
        else {
            panic!("unrelated path must fall through: {path}");
        };
        assert_eq!(polls.load(Ordering::Relaxed), 0);
        assert_eq!(request.uri(), path);
        assert_eq!(
            request.into_body().collect().await.unwrap().to_bytes().as_ptr(),
            address
        );
    }
}

#[tokio::test]
async fn publication_rejects_wrong_methods_before_reading_the_body() {
    let handler = PublishHandler::new(test_support::service());
    let (body, polls) =
        test_support::body(bytes::Bytes::from_static(b"unread"));
    let request =
        Request::builder().uri("/apps/1/events?x=1").body(body).unwrap();
    let ServiceResult::Handled(response) = handler.try_handle(request).await
    else {
        panic!("configured endpoint must be terminal");
    };
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()[header::ALLOW], "POST");
    assert_eq!(polls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn authentication_precedes_json_decoding_and_service_delivery() {
    let service = test_support::service();
    let handler = PublishHandler::new(Arc::clone(&service));
    let catalog = service.applications();
    let application = &catalog.applications()["example"];
    for (body, signed, expected) in [
        (b"not json".as_slice(), false, StatusCode::UNAUTHORIZED),
        (b"not json".as_slice(), true, StatusCode::BAD_REQUEST),
        (
            br#"{"name":"notice","channel":"news","data":"{}"}"#.as_slice(),
            true,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let query = if signed {
            claviron_pusher::authentication_query(
                "POST",
                "/apps/1/events",
                body,
                &application.key,
                application.secret.as_ref().unwrap(),
            )
            .unwrap()
        } else {
            String::new()
        };
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/apps/1/events?{query}"))
            .body(bytes_body(bytes::Bytes::copy_from_slice(body)))
            .unwrap();
        let ServiceResult::Handled(response) =
            handler.try_handle(request).await
        else {
            panic!("configured endpoint must be handled");
        };
        assert_eq!(response.status(), expected);
    }
}
