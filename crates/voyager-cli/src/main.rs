//! Voyager OGM Command-Line Interface (`voyager`, also installed as `voy`).
//!
//! The CLI compiles and validates Query Spec v1 JSON documents with the Rust
//! consumer in [`voyager_ogm::spec`] and diagnoses database connections through
//! [`voyager_drivers`]. It does not parse query text and does not reimplement
//! the spec consumer.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{self, Read};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use serde_json::{Map, Value};

use voyager_drivers::Auth;
use voyager_drivers::uri::{DatabaseProtocol, ParsedUri};
use voyager_ogm::ast::LiteralValue;
use voyager_ogm::spec::{SpecCompileOptions, build_query, compile};

/// Exit code for a successful command.
const EXIT_SUCCESS: u8 = 0;
/// Exit code for an invalid Query Spec document or a rejected compilation.
const EXIT_INVALID_SPEC: u8 = 1;
/// Exit code for a failed doctor connection or check.
const EXIT_CHECK: u8 = 3;
/// Exit code for an input/output error.
const EXIT_IO: u8 = 4;

const EXIT_HELP: &str = "\
Exit codes:
  0  success
  1  invalid Query Spec document or rejected compilation (compile, validate)
  2  usage error
  3  doctor connection or check failure
  4  input/output error (cannot read the spec file or stdin)";

#[derive(Parser, Debug)]
#[command(
    name = "voyager",
    author = "Voyager OGM Team",
    version = env!("CARGO_PKG_VERSION"),
    about = "Compile and validate Voyager Query Spec documents and check database connections",
    long_about = "The Voyager CLI compiles Query Spec v1 JSON documents with the same consumer as \
                  `compile_query_from_spec`, validates them without emitting a statement, and checks \
                  database connections through voyager-drivers. The same program is installed as both \
                  `voyager` and `voy`.",
    after_help = EXIT_HELP
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show build facts, or check a database connection with --uri
    #[command(after_help = EXIT_HELP)]
    Doctor {
        /// Connection URI to check (bolt://, neo4j://, postgresql://, age://, redis://, falkordb://)
        #[arg(long)]
        uri: Option<String>,

        /// Username for the connection check (overrides the URI userinfo)
        #[arg(long)]
        username: Option<String>,

        /// Environment variable holding the password (never pass a password on the command line)
        #[arg(long, value_name = "VAR")]
        password_env: Option<String>,
    },

    /// Compile a Query Spec v1 document to a dialect
    #[command(after_help = EXIT_HELP)]
    Compile {
        /// Query Spec v1 JSON file, or `-` to read from stdin
        input: String,

        /// Target dialect
        #[arg(short, long, value_enum, default_value_t = Dialect::Cypher)]
        dialect: Dialect,

        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,

        /// Print the parameters to stderr after the statement (text format only)
        #[arg(long)]
        show_params: bool,

        /// Graph name used by the SQL/PGQ and Apache AGE emitters
        #[arg(long)]
        graph_name: Option<String>,

        /// Run the AST optimiser before emission
        #[arg(long)]
        optimize: bool,

        /// Optimiser level: minimal, standard or aggressive
        #[arg(long, default_value = "standard", value_name = "LEVEL")]
        optimization_level: String,
    },

    /// Validate a Query Spec v1 document without compiling output
    #[command(after_help = EXIT_HELP)]
    Validate {
        /// Query Spec v1 JSON file, or `-` to read from stdin
        input: String,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Dialect {
    /// openCypher (Neo4j, Memgraph, FalkorDB)
    #[value(name = "cypher")]
    Cypher,
    /// Apache AGE (PostgreSQL extension)
    #[value(name = "age")]
    Age,
    /// ISO GQL
    #[value(name = "iso_gql")]
    IsoGql,
    /// SQL:2023 PGQ (PostgreSQL 19)
    #[value(name = "sql_pgq")]
    SqlPgq,
    /// DuckPGQ (DuckDB)
    #[value(name = "duck_pgq")]
    DuckPgq,
}

impl Dialect {
    fn api_name(self) -> &'static str {
        match self {
            Self::Cypher => "cypher",
            Self::Age => "age",
            Self::IsoGql => "iso_gql",
            Self::SqlPgq => "sql_pgq",
            Self::DuckPgq => "duckpgq",
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    /// Statement on stdout; parameters only with --show-params
    #[value(name = "text")]
    Text,
    /// JSON object with statement, parameters and columns
    #[value(name = "json")]
    Json,
}

#[derive(Debug)]
struct Failure {
    code: u8,
    message: String,
}

impl Failure {
    fn new(code: u8, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn invalid(error: &voyager_ogm::spec::SpecError) -> Self {
        let path = error
            .path()
            .filter(|pointer| !pointer.is_empty())
            .unwrap_or("<root>");
        Self::new(
            EXIT_INVALID_SPEC,
            format!("{path}: {} ({})", error.message(), error.code()),
        )
    }

    fn invalid_message(message: impl Into<String>) -> Self {
        Self::new(EXIT_INVALID_SPEC, message)
    }

    fn io(message: impl Into<String>) -> Self {
        Self::new(EXIT_IO, message)
    }

    fn check(message: impl Into<String>) -> Self {
        Self::new(EXIT_CHECK, message)
    }
}

#[tokio::main]
pub(crate) async fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli).await {
        Ok(()) => ExitCode::from(EXIT_SUCCESS),
        Err(failure) => {
            eprintln!("{}", failure.message);
            ExitCode::from(failure.code)
        }
    }
}

async fn run(cli: Cli) -> Result<(), Failure> {
    match cli.command {
        Commands::Doctor {
            uri,
            username,
            password_env,
        } => run_doctor(uri, username, password_env).await,
        Commands::Compile {
            input,
            dialect,
            format,
            show_params,
            graph_name,
            optimize,
            optimization_level,
        } => run_compile(
            &input,
            dialect,
            format,
            show_params,
            graph_name,
            optimize,
            optimization_level,
        ),
        Commands::Validate { input } => run_validate(&input),
    }
}

fn run_compile(
    input: &str,
    dialect: Dialect,
    format: OutputFormat,
    show_params: bool,
    graph_name: Option<String>,
    optimize: bool,
    optimization_level: String,
) -> Result<(), Failure> {
    let text = read_input(input)?;
    let spec = parse_spec(&text)?;
    let options = SpecCompileOptions {
        graph_name,
        optimize,
        optimization_level,
        use_cache: true,
    };

    match compile(&spec, dialect.api_name(), &options) {
        Ok(result) => {
            print_warnings(&result.warnings);
            let parameters = parameters_to_json(&result.compiled.parameters);
            match format {
                OutputFormat::Text => {
                    println!("{}", result.compiled.statement);
                    if show_params {
                        eprintln!("Parameters: {}", render_json(&parameters));
                    }
                }
                OutputFormat::Json => {
                    let mut document = Map::new();
                    document.insert(
                        "statement".to_string(),
                        Value::String(result.compiled.statement),
                    );
                    document.insert("parameters".to_string(), parameters);
                    let columns: Vec<Value> = result
                        .compiled
                        .columns
                        .iter()
                        .map(|column| {
                            let mut value = Map::new();
                            value.insert("name".to_string(), Value::String(column.name.clone()));
                            value.insert("alias".to_string(), json_optional(&column.alias));
                            Value::Object(value)
                        })
                        .collect();
                    document.insert("columns".to_string(), Value::Array(columns));
                    if !result.warnings.is_empty() {
                        document.insert(
                            "warnings".to_string(),
                            Value::Array(
                                result
                                    .warnings
                                    .iter()
                                    .map(|warning| Value::String(warning.clone()))
                                    .collect(),
                            ),
                        );
                    }
                    println!("{}", render_json(&Value::Object(document)));
                }
            }
            Ok(())
        }
        Err(error) => {
            print_warnings(error.warnings());
            Err(Failure::invalid(&error))
        }
    }
}

fn run_validate(input: &str) -> Result<(), Failure> {
    let text = read_input(input)?;
    let spec = parse_spec(&text)?;

    let builder = build_query(&spec).map_err(|error| Failure::invalid(&error))?;
    builder.try_build().map_err(|error| {
        Failure::new(
            EXIT_INVALID_SPEC,
            format!("<root>: {error} ({})", error.code()),
        )
    })?;
    Ok(())
}

async fn run_doctor(
    uri: Option<String>,
    username: Option<String>,
    password_env: Option<String>,
) -> Result<(), Failure> {
    match uri {
        None => {
            if username.is_some() || password_env.is_some() {
                eprintln!(
                    "warning: --username and --password-env only apply to doctor --uri; ignoring them"
                );
            }
            print_build_facts();
            Ok(())
        }
        Some(uri) => {
            let mut parsed = ParsedUri::parse(&uri)
                .map_err(|error| Failure::check(format!("doctor: {error}")))?;
            let password = match password_env {
                Some(variable) => Some(env::var(&variable).map_err(|_| {
                    Failure::check(format!(
                        "doctor: environment variable '{variable}' is not set (--password-env)"
                    ))
                })?),
                None => None,
            };
            apply_credentials(&mut parsed, username.as_deref(), password.as_deref())?;
            let report = probe_connection(&parsed).await?;
            print_connection_report(&parsed, &report);
            Ok(())
        }
    }
}

fn print_build_facts() {
    println!("voyager {}", env!("CARGO_PKG_VERSION"));
    println!("voyager-ogm {}", voyager_ogm::VERSION);
    println!("driver protocol features: {}", enabled_driver_features());
    println!("supported dialects: cypher, age, iso_gql, sql_pgq, duck_pgq");
    println!("connection checks: run `voyager doctor --uri <URI>`");
}

fn enabled_driver_features() -> String {
    let mut features = Vec::new();
    if cfg!(feature = "bolt") {
        features.push("bolt");
    }
    if cfg!(feature = "postgres") {
        features.push("postgres");
    }
    if cfg!(feature = "redis") {
        features.push("redis");
    }
    if features.is_empty() {
        "(none)".to_string()
    } else {
        features.join(", ")
    }
}

fn apply_credentials(
    parsed: &mut ParsedUri,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), Failure> {
    if username.is_none() && password.is_none() {
        return Ok(());
    }

    let (mut current_user, mut current_password) = match &parsed.auth {
        Auth::Basic {
            username, password, ..
        } => (username.clone(), password.clone()),
        _ => (String::new(), String::new()),
    };

    if let Some(username) = username {
        current_user = username.to_string();
    }
    if let Some(password) = password {
        current_password = password.to_string();
    }

    if current_user.is_empty() {
        return Err(Failure::check(
            "doctor: a username is required; put it in the URI or pass --username",
        ));
    }

    parsed.auth = Auth::Basic {
        username: current_user,
        password: current_password,
        realm: None,
    };
    Ok(())
}

struct ProbeReport {
    protocol: String,
    server_version: Option<String>,
    latency: Duration,
    dialect: &'static str,
}

async fn probe_connection(parsed: &ParsedUri) -> Result<ProbeReport, Failure> {
    match parsed.protocol {
        DatabaseProtocol::Bolt => {
            #[cfg(feature = "bolt")]
            {
                probe_bolt(parsed).await
            }
            #[cfg(not(feature = "bolt"))]
            {
                Err(missing_feature(parsed))
            }
        }
        DatabaseProtocol::Postgres => {
            #[cfg(feature = "postgres")]
            {
                probe_postgres(parsed).await
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(missing_feature(parsed))
            }
        }
        DatabaseProtocol::Redis => {
            #[cfg(feature = "redis")]
            {
                probe_redis(parsed).await
            }
            #[cfg(not(feature = "redis"))]
            {
                Err(missing_feature(parsed))
            }
        }
        DatabaseProtocol::DuckDb => Err(Failure::check(
            "doctor: DuckDB is embedded and does not use a network connection; doctor --uri cannot check it",
        )),
    }
}

#[cfg(any(
    not(feature = "bolt"),
    not(feature = "postgres"),
    not(feature = "redis")
))]
fn missing_feature(parsed: &ParsedUri) -> Failure {
    Failure::check(format!(
        "doctor: this build has no driver support for {} (enable the matching voyager-cli Cargo feature)",
        parsed.protocol
    ))
}

#[cfg(feature = "bolt")]
async fn probe_bolt(parsed: &ParsedUri) -> Result<ProbeReport, Failure> {
    use voyager_drivers::{AsyncConnection, BoltConnection, ConnectionConfig};

    let mut config = ConnectionConfig::from_uri(parsed.raw.clone());
    config.auth = parsed.auth.clone();
    config.database = parsed.database.clone();
    config.tls = parsed.tls;

    let mut connection = BoltConnection::connect(&config)
        .await
        .map_err(|error| Failure::check(format!("doctor: {error}")))?;
    let protocol_version = connection.version();
    let server_version = connection.server_agent().map(str::to_string);
    let latency = measure_ping(&mut connection).await?;
    let _ = connection.close().await;

    Ok(ProbeReport {
        protocol: format!(
            "Bolt v{}.{}",
            protocol_version.major, protocol_version.minor
        ),
        server_version,
        latency,
        dialect: "cypher",
    })
}

#[cfg(feature = "postgres")]
async fn probe_postgres(parsed: &ParsedUri) -> Result<ProbeReport, Failure> {
    use voyager_drivers::{AsyncConnection, PostgresConnection};

    let mut connection = PostgresConnection::connect(parsed)
        .await
        .map_err(|error| Failure::check(format!("doctor: {error}")))?;
    let server_version = connection
        .server_parameters()
        .get("server_version")
        .cloned();
    let latency = measure_ping(&mut connection).await?;
    let _ = connection.close().await;

    let uses_age = parsed.raw.starts_with("age:")
        || parsed.params.contains_key("graph")
        || parsed.params.contains_key("graph_name")
        || parsed.params.contains_key("age")
        || parsed.params.get("dialect").map(String::as_str) == Some("age");

    Ok(ProbeReport {
        protocol: "PostgreSQL wire".to_string(),
        server_version,
        latency,
        dialect: if uses_age { "age" } else { "sql_pgq" },
    })
}

#[cfg(feature = "redis")]
async fn probe_redis(parsed: &ParsedUri) -> Result<ProbeReport, Failure> {
    use voyager_drivers::{AsyncConnection, RedisConnection};

    let mut connection = RedisConnection::connect(parsed)
        .await
        .map_err(|error| Failure::check(format!("doctor: {error}")))?;
    let resp_label = if connection.is_resp3() {
        "Redis/RESP3"
    } else {
        "Redis/RESP2"
    };
    let server_version = redis_server_version(connection.server_info());
    let latency = measure_ping(&mut connection).await?;
    let _ = connection.close().await;

    Ok(ProbeReport {
        protocol: resp_label.to_string(),
        server_version,
        latency,
        dialect: "cypher",
    })
}

#[cfg(feature = "redis")]
fn redis_server_version(info: &HashMap<String, voyager_drivers::RespValue>) -> Option<String> {
    for key in [
        "falkor_version",
        "redis_version",
        "version",
        "server_version",
    ] {
        if let Some(value) = info.get(key) {
            let value = value.to_string_lossy();
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

#[cfg(any(feature = "bolt", feature = "postgres", feature = "redis"))]
async fn measure_ping(
    connection: &mut impl voyager_drivers::AsyncConnection,
) -> Result<Duration, Failure> {
    let started = std::time::Instant::now();
    connection
        .ping()
        .await
        .map_err(|error| Failure::check(format!("doctor: ping failed: {error}")))?;
    Ok(started.elapsed())
}

fn print_connection_report(parsed: &ParsedUri, report: &ProbeReport) {
    println!("endpoint: {}", parsed.socket_addr());
    if let Some(database) = &parsed.database {
        println!("database: {database}");
    }
    println!("protocol: {}", report.protocol);
    println!(
        "server version: {}",
        report.server_version.as_deref().unwrap_or("unavailable")
    );
    println!("dialect: {}", report.dialect);
    println!(
        "latency: {} (ping round trip)",
        format_latency(report.latency)
    );
    println!("status: ok");
}

fn format_latency(latency: Duration) -> String {
    format!("{:.3} ms", latency.as_secs_f64() * 1000.0)
}

fn read_input(input: &str) -> Result<String, Failure> {
    if input == "-" {
        let mut text = String::new();
        io::stdin()
            .read_to_string(&mut text)
            .map_err(|error| Failure::io(format!("error: cannot read stdin: {error}")))?;
        Ok(text)
    } else {
        fs::read_to_string(input)
            .map_err(|error| Failure::io(format!("error: cannot read '{input}': {error}")))
    }
}

fn parse_spec(text: &str) -> Result<Value, Failure> {
    serde_json::from_str(text).map_err(|error| {
        Failure::invalid_message(format!("<root>: invalid JSON: {error} (INVALID_SPEC)"))
    })
}

fn print_warnings(warnings: &[String]) {
    for warning in warnings {
        eprintln!("warning: {warning}");
    }
}

fn parameters_to_json(parameters: &HashMap<String, LiteralValue>) -> Value {
    let mut sorted: Vec<_> = parameters.iter().collect();
    sorted.sort_by(|left, right| left.0.cmp(right.0));
    let mut object = Map::new();
    for (name, value) in sorted {
        object.insert(name.clone(), literal_to_json(value));
    }
    Value::Object(object)
}

fn literal_to_json(value: &LiteralValue) -> Value {
    match value {
        LiteralValue::Null => Value::Null,
        LiteralValue::Bool(boolean) => Value::Bool(*boolean),
        LiteralValue::Int64(integer) => Value::Number((*integer).into()),
        LiteralValue::Float64(float) => serde_json::Number::from_f64(*float)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        LiteralValue::String(string) => Value::String(string.clone()),
        LiteralValue::ParameterRef(name) => Value::String(name.clone()),
        LiteralValue::List(items) => Value::Array(items.iter().map(literal_to_json).collect()),
        LiteralValue::Map(members) => {
            let mut object = Map::new();
            for (key, value) in members {
                object.insert(key.clone(), literal_to_json(value));
            }
            Value::Object(object)
        }
    }
}

fn json_optional(value: &Option<String>) -> Value {
    value
        .as_ref()
        .map(|value| Value::String(value.clone()))
        .unwrap_or(Value::Null)
}

fn render_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn literal_values_map_to_plain_json() {
        let literal = LiteralValue::Map(vec![
            (
                "name".to_string(),
                LiteralValue::String("Alice".to_string()),
            ),
            ("age".to_string(), LiteralValue::Int64(30)),
            (
                "tags".to_string(),
                LiteralValue::List(vec![
                    LiteralValue::String("a".to_string()),
                    LiteralValue::Bool(true),
                ]),
            ),
        ]);

        assert_eq!(
            literal_to_json(&literal),
            json!({"name": "Alice", "age": 30, "tags": ["a", true]})
        );
    }

    #[test]
    fn doctor_lists_only_compiled_protocol_features() {
        let features = enabled_driver_features();
        assert_eq!(features.contains("bolt"), cfg!(feature = "bolt"));
        assert_eq!(features.contains("postgres"), cfg!(feature = "postgres"));
        assert_eq!(features.contains("redis"), cfg!(feature = "redis"));
    }
}
