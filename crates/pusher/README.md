# claviron-pusher

Pusher-compatible wire messages, request signing and target configuration.

This crate is maintained in the Broadcast workspace. It has no dependency on
`claviron-broadcast`; its default features create no HTTP client or live service.
The independent workspace's standalone release is not published yet.

## Protocol

Validated `PusherAppId` and `PusherAppKey` identify protocol applications.
`PusherAppSecret` redacts its Debug output. The crate exposes client/server
messages, publication validation, subscription commands and presence snapshots.
`TriggerEvent::validate` enforces the supported channel and event-data bounds.

`authentication_query` signs a REST request; `authenticate` verifies its query,
body digest, timestamp and HMAC signature. Their `_at` variants take the clock
value explicitly. `authorize_channel` and `verify_channel_authorization` handle
private/presence signatures. The application still decides who may subscribe;
these primitives do not read users, sessions or roles.

Broadcast exposes the same contracts through
`claviron_broadcast::protocol::pusher`. Applications needing only signatures or
wire types may depend on this crate directly.

## External HTTP adapter

The optional `http` feature exposes `PusherAdapter`. It forwards `/app/{key}`
WebSocket upgrades and `/apps/{id}` REST paths to an external HTTP/1.1 server.
`accepts_path` selects the namespace before request ownership is transferred.

The adapter preserves public paths and queries, applies the shared forwarding
header rules, and keeps upgraded tunnels under `SessionSpawner` ownership.
Connect, response-head and tunnel-idle budgets come from `HttpTargetConfig`.
It opens outbound connections; it owns no listener or local subscription registry.

A `PusherTarget::Fleet` is handled by Broadcast composition, never by this
external HTTP adapter. Neither the protocol crate nor the adapter selects Edge.

See [ARCHITECTURE.md](ARCHITECTURE.md) and the [Broadcast API](../../README.md).
