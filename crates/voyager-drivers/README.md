# voyager-drivers

[![crates.io](https://img.shields.io/crates/v/voyager-drivers.svg)](https://crates.io/crates/voyager-drivers)
[![docs.rs](https://img.shields.io/docsrs/voyager-drivers.svg)](https://docs.rs/voyager-drivers)
[![CI](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml/badge.svg)](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.87-blue.svg)](https://github.com/tanishq-kumar/voyager/blob/main/Cargo.toml)

**Native asynchronous driver layer for Voyager OGM: connection pooling, wire protocols, and Arrow results.**

`voyager-drivers` is the pure safe Rust driver layer for Voyager OGM. It opens asynchronous Bolt (Neo4j, Memgraph), PostgreSQL wire (Apache AGE, PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)) and Redis RESP (FalkorDB) connections over TCP or TLS, pools them, runs native transactions, and returns Apache Arrow record batches. It builds on [Tokio](https://tokio.rs) and contains no unsafe code.

## Install

Install with the default features (`bolt`, `postgres`, `redis`, `arrow`, `tls`):

```bash
cargo add voyager-drivers
```

For a Bolt-only build:

```bash
cargo add voyager-drivers --no-default-features --features bolt
```

| Feature | Default | Enables |
| :--- | :--- | :--- |
| `bolt` | yes | Bolt client (Neo4j, Memgraph) |
| `postgres` | yes | PostgreSQL wire client (Apache AGE, PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)) |
| `redis` | yes | Redis RESP2/RESP3 client (FalkorDB) |
| `arrow` | yes | Arrow result batches (`rows_to_record_batch`) |
| `tls` | yes | TLS for every protocol through [rustls](https://docs.rs/rustls) (ring provider, system root store) |
| `testkit` | no | Mock engine and `BoltStubServer` for tests |

A URI or client that targets a protocol whose feature is disabled returns a `NetError` naming the missing feature. The same holds for TLS: without the `tls` feature a TLS URI fails with `NetError::TlsError` naming the feature, and is never downgraded to plaintext. The Bolt-only example above keeps TLS off; add `--features bolt,tls` to keep it.

## Example

The snippet below is a `no_run` doctest: it compiles but needs a live database to execute.

```rust,no_run
# use std::collections::HashMap;
# use voyager_drivers::NativeClient;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = NativeClient::new("bolt://127.0.0.1:7687", None)?;
client.ping().await?;

let mut params = HashMap::new();
params.insert("name".to_string(), serde_json::json!("Alice").into());
let result = client.execute(
    "MATCH (p:Person {name: $name}) RETURN p.name AS name",
    &params,
).await?;

println!("{} column(s), {} record batch(es)", result.columns.len(), result.batches.len());
client.close().await;
# Ok(())
# }
```

For a single connection with credentials, use `ConnectionConfig::from_uri(uri).with_auth(user, pass)` and `BoltConnection::connect(&config).await`. A runnable version is in [`crates/voyager-drivers/examples/live_connect_and_query.rs`](https://github.com/tanishq-kumar/voyager/blob/main/crates/voyager-drivers/examples/live_connect_and_query.rs).

## Protocols and URI schemes

| Protocol | URI schemes | Default port | Versions |
| :--- | :--- | :--- | :--- |
| Bolt | `bolt://`, `neo4j://`, `memgraph://` | 7687 | Bolt v4.2, v4.4, v5.0, v5.4 (proposed in the handshake) |
| PostgreSQL wire | `postgresql://`, `postgres://`, `age://` | 5432 | Frontend/Backend v3.0 |
| Redis RESP | `redis://`, `falkordb://`, `valkey://` | 6379 | RESP2 / RESP3 |

`duckdb://` is recognized as an embedded URI, but this crate is a TCP driver layer and rejects it; embedded DuckDB/DuckPGQ execution lives in Voyager's Python package (`DuckDbBridge`), not here.

## TLS

With the `tls` feature (on by default) every protocol can connect over TLS. The scheme or a
URI parameter asks for TLS, and a TLS request never falls back to plaintext, except for
PostgreSQL `sslmode=prefer` and `allow` as described below.

| URI | Transport | Server certificate |
| :--- | :--- | :--- |
| `bolt+s://`, `neo4j+s://`, `memgraph+s://` | Bolt over TLS | verified: trusted CA and hostname |
| `bolt+ssc://`, `neo4j+ssc://`, `memgraph+ssc://` | Bolt over TLS | not verified (any certificate, as the Neo4j drivers do) |
| `rediss://`, `falkordbs://`, `valkeys://` | RESP over TLS | verified: trusted CA and hostname |
| `postgresql://…?sslmode=…` (also `postgres://`, `age://`) | SSLRequest, then TLS | per `sslmode`, see below |
| `postgresql+s://`, `age+s://` | as `sslmode=verify-full` | verified |
| `postgresql+ssc://`, `age+ssc://` | as `sslmode=require` | not verified |
| any scheme with `?tls=true` / `?ssl=true` | TLS | verified |
| any scheme with `?tls=insecure` | TLS | not verified |

Trusted roots are the operating system's store (through
[rustls-native-certs](https://docs.rs/rustls-native-certs), which also honours `SSL_CERT_FILE`
and `SSL_CERT_DIR`) unless a CA bundle is given; a CA bundle replaces the system roots. A
`+ssc` / `insecure` connection is encrypted but does not authenticate the server: use it only
for development.

### TLS parameters

These URI parameters apply to every protocol; the libpq names are accepted everywhere too.

| Parameter | Alias | Meaning |
| :--- | :--- | :--- |
| `tls_ca_file` | `sslrootcert` | PEM file with the trusted CA certificates (`system`: the system store, and implies verified TLS) |
| `tls_cert_file` | `sslcert` | PEM client certificate chain, for mutual TLS |
| `tls_key_file` | `sslkey` | PEM private key for the client certificate (PKCS#8, PKCS#1 or SEC1) |
| `tls_server_name` | | name sent as SNI and verified against the certificate, instead of the URI host |

A parameter can strengthen the TLS that the scheme asks for (`bolt+ssc://…?tls=true`,
`postgresql://…?ssl=true`) but never weaken it: `bolt+s://…?tls=false`,
`rediss://…?tls=insecure`, `postgresql+s://…?sslmode=require` and
`postgresql://…?sslmode=require&ssl=false` are rejected with `NetError::InvalidUri` before
anything is sent. Two TLS parameters that disagree are rejected too: the same parameter
given twice with different values, two aliases with different values (`tls_ca_file` and
`sslrootcert`), or `ssl` / `tls` / `sslmode` / `sslrootcert=system` asking for different
levels. TLS parameter names are case-insensitive (`SSLMODE=require` works); their values are
not (`sslmode=REQUIRE` is an error, as in libpq).

```text
bolt+s://neo4j:secret@db.example.com:7687
bolt+s://10.0.0.5:7687?tls_ca_file=/etc/voyager/ca.pem&tls_server_name=db.internal
rediss://default:secret@cache.example.com:6380?tls_ca_file=/etc/voyager/ca.pem
postgresql://app:secret@pg.example.com/graph?sslmode=verify-full&sslrootcert=/etc/voyager/ca.pem&sslcert=/etc/voyager/client.pem&sslkey=/etc/voyager/client.key
```

In Rust the same settings can be set on a `ConnectionConfig` (they win over the URI
parameters), or passed to `PostgresConnection::connect_with_tls` and
`RedisConnection::connect_with_tls`:

```rust
use voyager_drivers::{ConnectionConfig, TlsMode, TlsOptions};

let config = ConnectionConfig::from_uri("bolt://db.internal:7687")
    .with_auth("neo4j", "secret")
    .with_tls(TlsMode::Required)
    .with_tls_options(
        TlsOptions::new()
            .with_ca_file("/etc/voyager/ca.pem")
            .with_client_cert("/etc/voyager/client.pem", "/etc/voyager/client.key")
            .with_server_name("db.internal"),
    );
assert_eq!(config.tls, TlsMode::Required);
```

`BoltConnection::is_tls()`, `PostgresConnection::is_tls()` and `RedisConnection::is_tls()`
report whether a connection is encrypted.

### PostgreSQL `sslmode`

PostgreSQL and Apache AGE follow libpq. Without `sslmode` (and without a TLS scheme or
`?ssl=`) the connection is plaintext, which differs from libpq's default `prefer` and keeps
existing URIs unchanged.

| `sslmode` | First attempt | Server answers `N` to the SSLRequest | Certificate check |
| :--- | :--- | :--- | :--- |
| `disable` | plaintext | (no SSLRequest) | none |
| `allow` | plaintext; TLS only if the server rejects it | error | none, or CA chain with a root certificate |
| `prefer` | TLS | continues in plaintext | none, or CA chain with a root certificate |
| `require` | TLS | error | none, or CA chain with a root certificate |
| `verify-ca` | TLS | error | CA chain |
| `verify-full` | TLS | error | CA chain and hostname |

As in libpq, giving `sslrootcert` makes `require` (and `allow`/`prefer`) verify the chain like
`verify-ca`, and `sslrootcert=system` (or `tls_ca_file=system`) defaults to `verify-full` and
rejects weaker modes. `allow` retries with TLS after any failure of the plaintext attempt, and
data the server sends after its `S` / `N` reply, before the TLS handshake, is rejected. Unlike
libpq, a TLS handshake or certificate failure under `prefer` is an error; only the server's
`N` reply leads to plaintext. `verify-ca` / `verify-full` without `sslrootcert` use the system
roots (libpq would read `~/.postgresql/root.crt`; default files under `~/.postgresql/` are not
read).

`sslnegotiation` chooses how TLS starts, as in libpq 17:

| `sslnegotiation` | Behaviour |
| :--- | :--- |
| `postgres` (default) | send an SSLRequest, start TLS after the server answers `S` (every PostgreSQL version) |
| `direct` | start TLS immediately, without an SSLRequest, with the ALPN protocol `postgresql`; PostgreSQL 17 or later |

`direct` saves a round trip and needs `sslmode=require`, `verify-ca` or `verify-full` (a
weaker `sslmode` is rejected, as in libpq). A server older than PostgreSQL 17 does not
understand it and the connection fails with a TLS error; a server that completes the
handshake without selecting the `postgresql` ALPN protocol is refused.

```text
postgresql://app:secret@pg.example.com/graph?sslmode=verify-full&sslrootcert=/etc/voyager/ca.pem&sslnegotiation=direct
```

## Related projects

`voyager-drivers` executes queries compiled by [`voyager-ogm`](https://crates.io/crates/voyager-ogm). [`voyager-cli`](https://crates.io/crates/voyager-cli) installs the `voyager` diagnostic binary. Python users get this driver layer inside the [`voyager-ogm`](https://pypi.org/project/voyager-ogm/) package on PyPI.

## MSRV

Rust **1.87.0** (the workspace `rust-version`).

## License

MIT OR Apache-2.0. See [LICENSE-MIT](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-MIT) and [LICENSE-APACHE](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-APACHE).

## Links

- Repository: <https://github.com/tanishq-kumar/voyager>
- API docs: <https://docs.rs/voyager-drivers>
- Changelog: <https://github.com/tanishq-kumar/voyager/blob/main/CHANGELOG.md>
