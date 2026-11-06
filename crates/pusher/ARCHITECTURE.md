# claviron-pusher

## Sources

- `src/lib.rs`: protocol exports and optional HTTP adapter.
- `src/config.rs`: validated application identities, redacted secret and target configuration.
- `src/protocol.rs`: client/server messages, publication and presence validation.
- `src/auth.rs`: REST signing/verification and private/presence channel signatures.
- `src/http/adapter.rs`: optional native request/response entry and namespace selection.
- `src/http/handler.rs`: outbound HTTP, trusted forwarding and owned WebSocket tunnels.
- `tests/auth.rs`, `protocol.rs`, `http/adapter.rs`: pure protocol and path checks.
- `tests/lifecycle.rs`: isolated external HTTP/upgrade lifecycle integration.

## Boundary

Broadcast depends on these contracts. This crate does not depend on Broadcast,
Pipeline, Edge or Transport. Protocol identities differ from hosts and end users;
channel signatures do not establish a login or resolve permissions.

Default features contain only wire/configuration and signing code. The `http`
feature adds Claviron HTTP bodies, forwarding and session ownership plus an
outbound Hyper client. The caller provides ingress policy and transport context.
There is no Axum router or service lifecycle registered here.

The adapter selects Pusher namespaces before consuming a body. Once handled,
an upstream response is terminal regardless of status. Upgrade admission happens
before 101; accepted tunnels stay with the listener's session owner and release
their connection drivers on refusal, shutdown or cancellation.
