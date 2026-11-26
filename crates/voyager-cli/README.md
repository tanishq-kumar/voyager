# voyager-cli

[![crates.io](https://img.shields.io/crates/v/voyager-cli.svg)](https://crates.io/crates/voyager-cli)
[![crates.io downloads](https://img.shields.io/crates/d/voyager-cli.svg)](https://crates.io/crates/voyager-cli)
[![CI](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml/badge.svg)](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.87-blue.svg)](https://github.com/tanishq-kumar/voyager/blob/main/Cargo.toml)

The command-line interface for Voyager OGM. It is a single binary, `voyager`
(the release archives also include the short alias `voy`), and provides four
subcommands:

- `doctor` reports build facts, or checks a database connection with `--uri`.
- `introspect` prints a live database's graph schema.
- `compile` compiles a Query Spec v1 JSON document to a dialect.
- `validate` checks a Query Spec v1 document without emitting a statement.

Query input is always Query Spec v1 JSON. The CLI does not parse openCypher,
ISO GQL or SQL/PGQ text; use the Rust or Python query builders for that.

## Install

### Prebuilt binaries (GitHub Releases)

Archives for Linux (`x86_64-unknown-linux-gnu`, static `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-gnu`), macOS (`aarch64-apple-darwin`, `x86_64-apple-darwin`), and Windows (`x86_64-pc-windows-msvc`) are attached to every [GitHub Release](https://github.com/tanishq-kumar/voyager/releases) as `voyager-cli-v<version>-<target>.tar.gz` (or `.zip` on Windows), alongside `SHA256SUMS` and GitHub build provenance attestations. macOS and Windows binaries are not OS code-signed yet. The Linux `-gnu` binaries need glibc 2.28 or newer (Debian 10+, Ubuntu 20.04+, RHEL 8+); the static `musl` binary runs on any x86_64 Linux, including Alpine.

Verify the checksum and attestation before unpacking:

```bash
# 1. Verify the SHA-256 checksum
grep "voyager-cli-v<version>-x86_64-unknown-linux-musl.tar.gz" SHA256SUMS | sha256sum -c -

# 2. Verify GitHub build provenance (requires GitHub CLI)
gh attestation verify voyager-cli-v<version>-x86_64-unknown-linux-musl.tar.gz --repo tanishq-kumar/voyager

# 3. Unpack and put the directory (with `voyager` and its `voy` alias) on your PATH
tar -xzf voyager-cli-v<version>-x86_64-unknown-linux-musl.tar.gz
```

### cargo-binstall

```bash
cargo binstall voyager-cli
```

### Cargo (from source)

```bash
cargo install voyager-cli
```

### Python (`pip` / `uv`)

Install the standalone CLI wheel or pull it in via the `cli` extra of `voyager-ogm`:

```bash
pip install "voyager-ogm[cli]"
# or standalone without the Python SDK:
pip install voyager-cli
```

Check the installed version:

```bash
voyager --version
```

### The `voy` alias

`voy` is a short alias for `voyager`. The release archives include it: a symlink
on Linux and macOS, and a `voy.cmd` shim on Windows. `cargo install`,
`cargo binstall` and `pip` install only `voyager`; add the alias yourself if you
want it:

```bash
alias voy=voyager                # bash / zsh
```

```powershell
Set-Alias voy voyager            # PowerShell
```

## Exit codes

Every subcommand documents the same exit codes in `--help`:

| Code | Meaning |
| :--- | :--- |
| `0` | Success |
| `1` | Invalid Query Spec document, malformed JSON, or a compilation the dialect rejects (`compile`, `validate`) |
| `2` | Usage error, including unknown flags, a missing input, or an unknown dialect or format |
| `3` | Connection, check or catalog query failure in `doctor` or `introspect` (invalid URI, unsupported protocol, connect/auth/ping failure, missing property graph) |
| `4` | Input/output error, such as an unreadable spec file |
| `5` | `introspect` does not support the engine (Apache AGE, DuckDB, PostgreSQL without SQL/PGQ) |

`validate` succeeds silently and exits `0`, so it can run in CI pipelines
without extra output. A validation failure prints one line to stderr in the
form `<json-pointer>: <message> (<ERROR_CODE>)`, and exits `1`:

```text
/bogus: Invalid query spec at '/bogus': Unknown root key 'bogus' in query spec dictionary (INVALID_SPEC)
```

A failure at the document root uses `<root>` in place of the JSON pointer.

## `voyager compile`

```bash
voyager compile query.spec.json --dialect iso_gql
cat query.spec.json | voyager compile - --dialect sql_pgq --format json
```

The input is a Query Spec v1 JSON document (the same document accepted by
`compile_query_from_spec`), from a file or `-` for stdin.

Dialects are `cypher` (default), `age`, `iso_gql`, `sql_pgq` and `duck_pgq`.

### `--format text` (default)

Prints only the statement to stdout, so the output can be piped to a database
client. Warnings go to stderr. Parameters are omitted by default; pass
`--show-params` to print them as one JSON object on stderr:

```bash
voyager compile query.spec.json --dialect cypher
voyager compile query.spec.json --dialect cypher --show-params
```

### `--format json`

Prints one JSON object to stdout:

```json
{
  "statement": "MATCH (p:Person) RETURN p.name AS user_name",
  "parameters": {},
  "columns": [{ "name": "user_name", "alias": "user_name" }]
}
```

`warnings` is included as an array when the consumer returns non-fatal
warnings. `columns` is always present; each column carries its `name` and an
optional `alias`.

`--graph-name <NAME>` sets the graph name used by the SQL/PGQ and Apache AGE
emitters. `--optimize` and `--optimization-level <minimal|standard|aggressive>`
control the AST optimiser.

## `voyager validate`

```bash
voyager validate query.spec.json
cat query.spec.json | voyager validate -
```

Validation builds the query without emitting a statement. It exits `0` when the
document is valid and prints nothing. When it is invalid it prints the JSON
Pointer and message for the first structural error (with its Voyager error code)
to stderr and exits `1`.

## `voyager doctor`

### Build facts

Without `--uri`, `doctor` prints only facts it can check in the current build:

```text
$ voyager doctor
voyager 0.4.0
voyager-ogm 0.4.0
driver protocol features: bolt, postgres, redis
TLS: enabled (rustls; bolt+s, rediss, PostgreSQL sslmode)
supported dialects: cypher, age, iso_gql, sql_pgq, duck_pgq
connection checks: run `voyager doctor --uri <URI>`
```

`driver protocol features` lists the driver protocols compiled into this
binary. Builds can opt out with Cargo features (`--no-default-features` and then
`bolt`, `postgres`, `redis` and/or `tls`). A URI for a protocol that is not compiled in
fails the check with a message naming the missing feature; so does a TLS URI in a
build without `tls`.

### Connection check

```bash
voyager doctor --uri bolt://localhost:7687 --username neo4j --password-env NEO4J_PASSWORD
voyager doctor --uri age://postgres:secret@localhost:5455/voyager_graph
voyager doctor --uri bolt://localhost:7688
voyager doctor --uri redis://localhost:6379
voyager doctor --uri 'bolt+s://neo4j@db.example.com:7687' --password-env NEO4J_PASSWORD
voyager doctor --uri 'rediss://cache.internal:6380?tls_ca_file=/etc/voyager/ca.pem'
voyager doctor --uri 'postgresql://app@pg.internal/graph?sslmode=verify-full&sslrootcert=/etc/voyager/ca.pem' --password-env PGPASSWORD
```

TLS URIs (`bolt+s://`, `bolt+ssc://`, `rediss://`, PostgreSQL `sslmode`) work as in
`voyager-drivers`; a private CA, a client certificate or a server name override are
URI parameters (`tls_ca_file` / `sslrootcert`, `tls_cert_file` / `sslcert`,
`tls_key_file` / `sslkey`, `tls_server_name`). The reported protocol ends in
`over TLS` when the connection is encrypted.

With `--uri`, `doctor` parses the URI, connects through `voyager-drivers`,
sends a `ping` (a trivial round trip), and reports:

- the endpoint and database;
- the protocol, including the negotiated Bolt version or RESP version;
- the server version when the driver exposes it (Bolt server agent,
  PostgreSQL `server_version`, or Redis/FalkorDB version);
- the matching dialect (`cypher`, `age` or `sql_pgq`);
- the ping round-trip latency in milliseconds.

### Credentials

Credentials are optional. A URI can carry them in its userinfo, `--username`
overrides the URI username, and `--password-env VAR` reads the password from an
environment variable. Passwords are never accepted on the command line and the
CLI does not prompt for one; an unset environment variable is a check failure
(exit `3`).

With no username and no password, `doctor` connects without authentication.
Each protocol does this differently:

| Protocol | No credentials |
| :--- | :--- |
| Bolt (`bolt://`, `neo4j://`, `memgraph://`) | Sends the Bolt `none` authentication scheme. Servers with authentication enabled reject this. |
| Redis/FalkorDB (`redis://`, `falkordb://`) | Sends `HELLO 3` without `AUTH`. Servers with a password reject this with `WRONGPASS`/`NOAUTH`. |
| PostgreSQL/AGE (`postgresql://`, `age://`) | Not possible: the startup message must name a role, so a username is required. |

So `voyager doctor --uri bolt://localhost:7688` checks a Memgraph instance that
has authentication disabled, and `voyager doctor --uri redis://localhost:6379`
checks a FalkorDB instance that has no password. PostgreSQL URIs must name a
user, either in the URI (`postgresql://postgres@localhost:5432/voyager_graph`)
or with `--username`. A password without a username is an error. For servers
that do require authentication, keep the credentials in the URI or pass
`--username` plus `--password-env`.

DuckDB is embedded and has no network endpoint, so `doctor --uri duckdb://...`
reports that it cannot be checked. For PostgreSQL URIs, `doctor` reports
`age` for `age://` URIs or PostgreSQL URIs carrying an AGE graph parameter;
otherwise it checks whether the server exposes SQL/PGQ property graph catalogs
and reports `sql_pgq` when present, or fails with a version-specific error if
SQL/PGQ is unavailable (experimental: PostgreSQL 19 Beta 1-3 and 20
development builds).

## `voyager introspect`

`introspect` reads a live database's labels, relationship types with their
endpoints, properties, constraints and indexes, and prints them:

```bash
voyager introspect --uri bolt://localhost:7687 --username neo4j --password-env NEO4J_PASSWORD
voyager introspect --uri bolt://localhost:7688 --format json
voyager introspect --uri "falkordb://localhost:6379?graph=social"
voyager introspect --uri "postgresql://postgres@localhost:5456/voyager_graph?graph=social" --password-env PGPASSWORD
```

Credentials work as for `doctor`. `--format text` (the default) prints a
summary; `--format json` prints the same JSON as the library call
(`voyager_drivers::introspect` in Rust, `voyager_ogm.introspect_schema` in
Python). `nodes` and `relationships` have the shape of the Python
`export_schema(models)` result, so a model export and a live database can be
compared key by key. `constraints`, `indexes` and `source` (engine, version,
edition, database, sample size) are added. Keys a database cannot know are
`null`: `model`, `description`, `attribute`, `has_default`, `constraints`
(the `Field` value constraints) and a node's `relationship_attributes`.

How properties are filled in:

- `unique`: a single-property uniqueness or key constraint.
- `nullable`: `false` only with an existence (`NOT NULL`) or key constraint.
- `primary_key`: `true` only for a key constraint (Neo4j Enterprise node key,
  PostgreSQL primary key), otherwise `null`.
- `indexed` / `index_type`: a single-property index that does not back a
  constraint.
- `type`: a type constraint or declared column type if there is one, else the
  value types of at most `--sample-size` (default 1000) entities per label or
  relationship type: one type gives that type, mixed values give `ANY`, no
  values give `null`. Relationship endpoints are sampled the same way, except
  on PostgreSQL where the property graph declares them.

| Engine | Catalogs read |
| :--- | :--- |
| Neo4j 5.13+ | `db.labels()`, `db.relationshipTypes()`, `SHOW CONSTRAINTS`, `SHOW INDEXES`, `valueType()` |
| Memgraph | `SHOW CONSTRAINT INFO`, `SHOW INDEX INFO`, `valueType()`; labels from `SHOW NODE_LABELS INFO` when schema metadata is enabled, else a label scan |
| FalkorDB | `db.labels()`, `db.relationshipTypes()`, `db.indexes()`, `db.constraints()`, `typeOf()` for the URI's graph |
| PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds) | `pg_propgraph_*` catalogs plus the element tables' keys, `NOT NULL`, unique constraints and indexes; needs `?graph=<name>` |
| Apache AGE, DuckDB | not supported (exit `5`) |

Neo4j Community Edition has no existence or key constraints, so every property
there is reported nullable.

## MSRV

Rust **1.87.0** (the workspace `rust-version`).

## License

MIT OR Apache-2.0. See [LICENSE-MIT](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-MIT) and [LICENSE-APACHE](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-APACHE).

## Links

- Repository: <https://github.com/tanishq-kumar/voyager>
- Changelog: <https://github.com/tanishq-kumar/voyager/blob/main/CHANGELOG.md>
