# Broadcast workspace

## Dependencies

```text
claviron-broadcast ── claviron-pusher
        │                   │
        │             wire types + signing
        ├── Orbit / Runtime / App
        ├── HTTP             [http / websocket]
        ├── Pipeline         [backend]
        └── Edge             [edge; webtransport extends it]
```

There is one repository and two publishable crates. Broadcast owns live state;
Pusher owns protocol contracts. Pusher never depends on Broadcast. Both consume
individual Claviron crates; neither requires the root Claviron facade.
No crate in this workspace owns a socket listener or depends on Transport.

## Sources

- `src/lib.rs`: public service, session and protocol exports; capability gates.
- `src/config.rs`: application catalog and identity uniqueness validation.
- `src/service.rs`: configuration registration, generations and native publication.
- `src/session.rs`: Pusher session loop and channel commands.
- `src/admission.rs`: optional existing-session verification for native entrances.
- `src/channels.rs`: local subscribers, delivery and presence projection.
- `src/fleet.rs`: bounded Orbit publication and membership records.
- `src/groups.rs`: addressed mailboxes, expiry and group fanout.
- `src/hooks.rs`, `src/hooks/occupancy/`: callbacks, transition owner and application events.
- `src/stream.rs`: message contract, Orbit/record codecs and optional WebSocket framing.
- `src/upgrade.rs`: optional native H1 handshake and session handoff.
- `src/http.rs`: exact REST bindings and authenticated publication without Axum.
- `src/backend.rs`: optional site-directive factories and native dispatch owners.
- `src/edge.rs`: optional host-selected H1/H2 and WebTransport session bindings.
- `src/error.rs`: service and session-refusal errors.
- `crates/pusher/`: wire contracts, signing and the optional external HTTP adapter.

## Preparation

`BroadcastService` receives one validated application catalog. Configuration
belongs to runtime preparation; file-backed and code-supplied registration use
the same service. Native callers, HTTP publishers and sessions share its identity
and credential catalog. They do not create independent services per host.

`backend::register(&SiteRegistry)` installs the `broadcast` and `pusher` codecs.
A scope uses one spelling. Absent/null declarations install no owner; a missing
fleet service is a preparation error. External HTTP targets require the explicit
`pusher` feature. Prepared owners run BeforeFiles, after shared admission and
Rust contributions. Unrelated paths return the original body before polling it.

REST bindings retain exact catalog paths and credentials prepared once. There
is one path lookup, no Axum state layer and no request-time config decoding.
`edge::register` adds these factories to Edge's site registry and prepares native
session bindings only for selected fleet declarations. H2/WebTransport admission
checks the retained declaration against the selected scope so removed or masked
bindings cannot revive an old entrance.

## Lifecycle and streams

The service owns a replaceable generation. Each accepted session retains its
permit; stop seals admission, signals sessions, drains their cleanup and then
releases the generation. The listener remains the separate transport owner.
Neither boundary authorizes terminating work owned by another service.

`MessageStream` uses concrete futures rather than a boxed future per message.
Orbit and WebTransport share the existing bounded record codec; WebSocket
framing is feature-selected. Subscription, authentication, presence and fleet
fanout remain in the same session implementation.

Native publication and protocol-only consumers do not select Edge, Pipeline,
WebSocket framing or an external HTTP client. Application/framework adapters
belong to their consuming repositories.

`webtransport` extends an explicitly selected Edge integration; it does not
enable Edge on its own. Generic record streams remain transport-independent.

## Tests

- `tests/channels.rs`, `session.rs`: local delivery, membership and session commands.
- `tests/fleet.rs`, `groups.rs`: fanout records and mailbox behavior.
- `tests/hooks/occupancy.rs`, `occupancy.rs`: callbacks and transition state.
- `tests/http.rs`, `backend.rs`, `support.rs`: authentication order and unread body handoff.
- `tests/edge.rs`: selected scope and retained-binding admission.
- `tests/websocket.rs`: explicit native H1/Orbit session integration over isolated resources.
- `crates/pusher/tests/`: signatures, wire validation, path selection and HTTP lifecycle.
