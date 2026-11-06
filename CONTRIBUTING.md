# Contributing

Read [ARCHITECTURE.md](ARCHITECTURE.md) and the affected public contract first.
Broadcast changes live in the root crate; protocol/signing and the external
HTTP adapter live in `crates/pusher`. Keep the dependency direction one-way.

Use stable Rust. For Rust changes, run:

```sh
cargo fmt --package claviron-broadcast --package claviron-pusher
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features --no-fail-fast
```

Use small synthetic fixtures for pure tests. Keep native protocol/lifecycle
integration in its isolated target; it must not use deployment configuration
or require a running server. Configuration preparation must not start a service.

Report the behavior changed and what was verified. Update feature/ownership
documentation when those contracts change. Contributions use the repository's
MIT or Apache-2.0 license terms.
