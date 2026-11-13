# claviron-broadcast

Realtime sessions, channel publication, and same-host fleet fanout for Claviron.

| Package | Responsibility |
| --- | --- |
| `claviron-broadcast` | Live sessions, channels, presence, groups and fleet publication. |
| `claviron-pusher` | Pusher wire messages, validated identities, signatures and an optional external HTTP adapter. |

Broadcast uses Pusher's protocol contracts. Pusher has no dependency on the live
service. Native publication reaches other serving workers through Orbit;
it does not make loopback HTTP calls or require Redis.

## Register the service

`register(state)` loads `broadcast.yaml` through the runtime's configuration
environment. `register_with_config(state, config)` accepts the same validated
catalog from code. Neither opens a listener.

```yaml
applications:
  dashboard:
    id: "1"
    key: example-key
    secret: ${BROADCAST_APP_SECRET}
    client_events: false
```

The map key is the application name used by native callers. IDs, keys and
secrets are Pusher wire identities. Duplicate IDs or keys fail validation.
Call `BroadcastService::publish` with a validated `PublishedEvent`, or
`publish_by_key` when a trusted native caller has the application key.
The returned count describes local delivery, not remote acknowledgements.
This bounded in-memory service does not provide durable history or replay.

## HTTP and session integration

Capabilities are selected explicitly; defaults install no HTTP adapter.

| Feature | Adds |
| --- | --- |
| `http` | Native `PublishHandler` for authenticated `POST /apps/{id}/events`. |
| `websocket` | HTTP/1 upgrade handler and WebSocket message framing. |
| `backend` | Configured REST owners for `claviron_pipeline::SiteRegistry`. |
| `pusher` | External Pusher HTTP/WebSocket adapter from `claviron-pusher`. |
| `edge` | Host-selected H1/H2 session bindings and configured REST owners. |
| `webtransport` | WebTransport session bindings when `edge` is also selected. |

The REST handler accepts `Request<RouteBody>` directly. It authenticates the
request before decoding event JSON and returns the original unread request
for unrelated paths. It requires no Axum router.

For configured sites, register the service and factory separately:

```rust,ignore
claviron_broadcast::register(state)?;
claviron_broadcast::backend::register(site_registry)?;
```

Then select the local fleet in the serving scope:

```yaml
broadcast:
  target: fleet
```

`pusher:` is an alternative spelling; a scope cannot declare both. An absent
or null directive creates no backend owner. Local fleet preparation requires
the registered service. An external `target: {http: ...}` requires `pusher`;
its connect, request and idle budgets belong to `HttpTargetConfig`.

For host-selected native sessions, call `edge::register(state)` after Edge
registration. It installs these same REST factories and exact `/app/{key}`
upgrade bindings from the catalog. Choose that entry point instead of separately
registering the same factories. Listener transport and host policy still apply.
The serving application owns listeners; this repository creates none.

## Sessions and authorization

`accept_session` admits a `BroadcastSession`; its owner runs it over
`MessageStream`. Orbit and reliable WebTransport record streams share the
session loop with optional WebSocket framing. WebTransport uses a bounded
Text/Binary/Close record codec; an ordinary Pusher WebSocket client cannot
connect without a compatible stream adapter.

Private/presence signatures authorize channel membership. Optional
`require_session(application, authenticator)` verifies an existing application
session before native admission. Broadcast creates neither users nor logins.
Service stop seals admission and drains its sessions. Accepted transport work
also remains under its listener's lifecycle.

`groups()` exposes addressed byte mailboxes and group fanout. Local occupancy
hooks report fleet zero-to-one and one-to-zero channel transitions. These are
best-effort signals; retain a hook's subscription while it should receive work.

See [ARCHITECTURE.md](ARCHITECTURE.md) for ownership and source locations, and
[claviron-pusher](crates/pusher/README.md) for the protocol and external adapter.
