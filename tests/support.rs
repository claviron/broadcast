use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_http::RouteBody;
use http_body::{Body, Frame};

pub(crate) fn service() -> Arc<crate::BroadcastService> {
    let config: crate::BroadcastConfig = serde_yaml::from_str(
        "applications:\n  example:\n    id: '1'\n    key: example-key\n    secret: example-secret\n",
    ).unwrap();
    Arc::new(crate::BroadcastService::new(config.validate().unwrap()))
}

pub(crate) fn body(payload: Bytes) -> (RouteBody, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let body =
        ObservedBody { payload: Some(payload), polls: Arc::clone(&polls) };
    (claviron_http::boxed_body(body), polls)
}

struct ObservedBody {
    payload: Option<Bytes>,
    polls: Arc<AtomicUsize>,
}

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.payload.take().map(|payload| Ok(Frame::data(payload))))
    }
}
