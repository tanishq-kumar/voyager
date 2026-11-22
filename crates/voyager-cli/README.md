# voyager-cli

[![crates.io](https://img.shields.io/crates/v/voyager-cli.svg)](https://crates.io/crates/voyager-cli)
[![crates.io downloads](https://img.shields.io/crates/d/voyager-cli.svg)](https://crates.io/crates/voyager-cli)
[![CI](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml/badge.svg)](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.87-blue.svg)](https://github.com/tanishq-kumar/voyager/blob/main/Cargo.toml)

The command-line interface for Voyager OGM. It installs two identical binaries,
`voyager` and the short alias `voy`, and provides three subcommands:

- `doctor` reports build facts, or checks a database connection with `--uri`.
- `compile` compiles a Query Spec v1 JSON document to a dialect.
- `validate` checks a Query Spec v1 document without emitting a statement.

Query input is always Query Spec v1 JSON. The CLI does not parse openCypher,
ISO GQL or SQL/PGQ text; use the Rust or Python query builders for that.

## Install

```bash
cargo install voyager-cli
```

Both `voyager` and `voy` are installed. Check the installed version with either:

```bash
voyager --version
voy --version
```

## Exit codes

Every subcommand documents the same exit codes in `--help`:

| Code | Meaning |
| :--- | :--- |
| `0` | Success |
| `1` | Invalid Query Spec document, malformed JSON, or a compilation the dialect rejects (`compile`, `validate`) |
| `2` | Usage error, including unknown flags, a missing input, or an unknown dialect or format |
| `3` | `doctor` connection or check failure (invalid URI, unsupported protocol, connect/auth/ping failure) |
| `4` | Input/output error, such as an unreadable spec file |

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
supported dialects: cypher, age, iso_gql, sql_pgq, duck_pgq
connection checks: run `voyager doctor --uri <URI>`
```

`driver protocol features` lists the driver protocols compiled into this
binary. Builds can opt out with Cargo features (`--no-default-features` and then
`bolt`, `postgres` and/or `redis`). A URI for a protocol that is not compiled in
fails the check with a message naming the missing feature.

### Connection check

```bash
voyager doctor --uri bolt://localhost:7687 --username neo4j --password-env NEO4J_PASSWORD
voyager doctor --uri age://postgres:secret@localhost:5455/voyager_graph
voyager doctor --uri redis://localhost:6379
```

With `--uri`, `doctor` parses the URI, connects through `voyager-drivers`,
sends a `ping` (a trivial round trip), and reports:

- the endpoint and database;
- the protocol, including the negotiated Bolt version or RESP version;
- the server version when the driver exposes it (Bolt server agent,
  PostgreSQL `server_version`, or Redis/FalkorDB version);
- the matching dialect (`cypher`, `age` or `sql_pgq`);
- the ping round-trip latency in milliseconds.

Passwords are never accepted on the command line. Take the password from an
environment variable with `--password-env VAR`, or embed credentials in the URI
userinfo. The CLI does not prompt for a password. An unset environment variable
is a check failure (exit `3`). `--username` overrides the URI username.

DuckDB is embedded and has no network endpoint, so `doctor --uri duckdb://...`
reports that it cannot be checked. SQL/PGQ itself is checked only through
compilation; `doctor` connects to PostgreSQL and reports `age` for `age://`
URIs or PostgreSQL URIs carrying an AGE graph parameter, and `sql_pgq`
otherwise.

## MSRV

Rust **1.87.0** (the workspace `rust-version`).

## License

MIT OR Apache-2.0. See [LICENSE-MIT](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-MIT) and [LICENSE-APACHE](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-APACHE).

## Links

- Repository: <https://github.com/tanishq-kumar/voyager>
- Changelog: <https://github.com/tanishq-kumar/voyager/blob/main/CHANGELOG.md>
