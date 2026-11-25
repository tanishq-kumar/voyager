//! Voyager OGM Command-Line Interface (`voyager`, also installed as `voy`).
//!
//! The CLI compiles and validates Query Spec v1 JSON documents with the Rust
//! consumer in [`voyager_ogm::spec`], diagnoses database connections through
//! [`voyager_drivers`], and prints a live database schema with
//! [`voyager_drivers::introspect`]. It does not parse query text and does not
//! reimplement the spec consumer.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{self, Read};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use serde_json::{Map, Value};

use voyager_drivers::Auth;
use voyager_drivers::introspect::{DEFAULT_SAMPLE_SIZE, GraphSchema, IntrospectOptions};
use voyager_drivers::uri::{DatabaseProtocol, ParsedUri};
use voyager_ogm::ast::LiteralValue;
use voyager_ogm::spec::{SpecCompileOptions, build_query, compile};

/// Exit code for a successful command.
const EXIT_SUCCESS: u8 = 0;
/// Exit code for an invalid Query Spec document or a rejected compilation.
const EXIT_INVALID_SPEC: u8 = 1;
/// Exit code for a failed connection, check or catalog query (doctor, introspect).
const EXIT_CHECK: u8 = 3;
/// Exit code for an input/output error.
const EXIT_IO: u8 = 4;
/// Exit code for an engine that `introspect` does not support.
const EXIT_UNSUPPORTED: u8 = 5;

const EXIT_HELP: &str = "\
Exit codes:
  0  success
  1  invalid Query Spec document or rejected compilation (compile, validate)
  2  usage error
  3  connection, check or catalog query failure (doctor, introspect)
  4  input/output error (cannot read the spec file or stdin)
  5  engine not supported by introspect (Apache AGE, DuckDB, PostgreSQL without SQL/PGQ)";

#[derive(Parser, Debug)]
#[command(
    name = "voyager",
    author = "Voyager OGM Team",
    version = env!("CARGO_PKG_VERSION"),
    about = "Compile and validate Voyager Query Spec documents, check database connections and introspect live schemas",
    long_about = "The Voyager CLI compiles Query Spec v1 JSON documents with the same consumer as \
                  `compile_query_from_spec`, validates them without emitting a statement, checks \
                  database connections through voyager-drivers, and prints a live database's graph \
                  schema in the shape of `export_schema`. The same program is installed as both \
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
    #[command(
        long_about = "Show build facts, or check a database connection with --uri.\n\n\
                      Credentials are optional. With no username and no password, doctor connects \
                      without authentication: Bolt uses the `none` authentication scheme and Redis \
                      skips AUTH. PostgreSQL still requires a username. Put the username in the URI \
                      userinfo or pass --username, and read the password from an environment \
                      variable with --password-env. A password without a username is an error.",
        after_help = EXIT_HELP
    )]
    Doctor {
        /// Connection URI to check (bolt://, neo4j://, postgresql://, age://, redis://, falkordb://;
        /// TLS: bolt+s://, bolt+ssc://, rediss://, ?sslmode=..., with ?tls_ca_file=<PEM> for a private CA)
        #[arg(long)]
        uri: Option<String>,

        /// Username for the connection check (overrides the URI userinfo); omit it for servers that do not authenticate
        #[arg(long)]
        username: Option<String>,

        /// Environment variable holding the password (never pass a password on the command line)
        #[arg(long, value_name = "VAR")]
        password_env: Option<String>,
    },

    /// Print a live database's graph schema (labels, relationship types, properties, constraints, indexes)
    #[command(
        long_about = "Print a live database's graph schema: labels, relationship types with their \
                      endpoints, properties, constraints and indexes.\n\n\
                      The JSON format is the result of the library call (Python \
                      `voyager_ogm.introspect_schema`, Rust `voyager_drivers::introspect`) and has the \
                      shape of `export_schema(models)`, plus `constraints`, `indexes` and `source`. \
                      Keys a database cannot know (model, attribute, description, has_default, \
                      constraints) are null. Property value types and relationship endpoints come from \
                      a sample of at most --sample-size entities per label or relationship type, \
                      unless the engine declares them (PostgreSQL).\n\n\
                      Supported: Neo4j 5.13+ and Memgraph (bolt://), FalkorDB (redis:// or \
                      falkordb://, graph from the URI), PostgreSQL SQL/PGQ (experimental: \
                      PostgreSQL 19 Beta 1-3 and 20 development builds) property graphs \
                      (postgresql://...?graph=<name>). Apache AGE, DuckDB, and PostgreSQL servers \
                      without SQL/PGQ exit with code 5.\n\n\
                      Credentials work as for doctor: URI userinfo or --username, and \
                      --password-env for the password.",
        after_help = EXIT_HELP
    )]
    Introspect {
        /// Connection URI (bolt://, neo4j://, redis://, falkordb://, postgresql://...?graph=<name>)
        #[arg(long)]
        uri: String,

        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,

        /// Username (overrides the URI userinfo); omit it for servers that do not authenticate
        #[arg(long)]
        username: Option<String>,

        /// Environment variable holding the password (never pass a password on the command line)
        #[arg(long, value_name = "VAR")]
        password_env: Option<String>,

        /// Maximum entities sampled per label or relationship type for property types and endpoints (0 disables sampling)
        #[arg(long, value_name = "N", default_value_t = DEFAULT_SAMPLE_SIZE)]
        sample_size: usize,
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
    /// SQL:2023 PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)
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
    /// Human-readable text (compile: the statement; introspect: a schema summary)
    #[value(name = "text")]
    Text,
    /// JSON (compile: statement, parameters and columns; introspect: the schema)
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
        Commands::Introspect {
            uri,
            format,
            username,
            password_env,
            sample_size,
        } => run_introspect(&uri, format, username, password_env, sample_size).await,
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
            let parsed = connection_uri("doctor", &uri, username, password_env)?;
            let report = probe_connection(&parsed).await?;
            print_connection_report(&parsed, &report);
            Ok(())
        }
    }
}

/// Parses `uri` and applies --username / --password-env.
fn connection_uri(
    command: &str,
    uri: &str,
    username: Option<String>,
    password_env: Option<String>,
) -> Result<ParsedUri, Failure> {
    let mut parsed =
        ParsedUri::parse(uri).map_err(|error| Failure::check(format!("{command}: {error}")))?;
    let password = match password_env {
        Some(variable) => Some(env::var(&variable).map_err(|_| {
            Failure::check(format!(
                "{command}: environment variable '{variable}' is not set (--password-env)"
            ))
        })?),
        None => None,
    };
    apply_credentials(
        command,
        &mut parsed,
        username.as_deref(),
        password.as_deref(),
    )?;
    Ok(parsed)
}

async fn run_introspect(
    uri: &str,
    format: OutputFormat,
    username: Option<String>,
    password_env: Option<String>,
    sample_size: usize,
) -> Result<(), Failure> {
    let parsed = connection_uri("introspect", uri, username, password_env)?;
    let options = IntrospectOptions { sample_size };
    let schema = voyager_drivers::introspect(&parsed, &options)
        .await
        .map_err(|error| {
            let code = if error.code() == voyager_ogm::ErrorCode::UnsupportedFeature {
                EXIT_UNSUPPORTED
            } else {
                EXIT_CHECK
            };
            Failure::new(
                code,
                format!(
                    "introspect: {}",
                    voyager_drivers::introspect::describe_error(&error)
                ),
            )
        })?;
    match format {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&schema.to_json_value())
                .unwrap_or_else(|_| "{}".to_string())
        ),
        OutputFormat::Text => print!("{}", render_schema_text(&schema)),
    }
    Ok(())
}

/// Renders a schema as an indented text summary.
fn render_schema_text(schema: &GraphSchema) -> String {
    use std::fmt::Write as _;
    use voyager_drivers::introspect::{EntityKind, PropertyEntry};

    fn property_line(out: &mut String, property: &PropertyEntry) {
        let mut facts = vec![
            property
                .type_name
                .clone()
                .unwrap_or_else(|| "?".to_string()),
        ];
        if property.primary_key == Some(true) {
            facts.push("primary key".to_string());
        }
        if property.unique {
            facts.push("unique".to_string());
        }
        if !property.nullable {
            facts.push("not null".to_string());
        }
        if property.indexed {
            facts.push(format!(
                "indexed {}",
                property.index_type.as_deref().unwrap_or("")
            ));
        }
        let _ = writeln!(
            out,
            "    {}: {}",
            property.name,
            facts.join(", ").trim_end()
        );
    }
    fn entity(kind: EntityKind) -> &'static str {
        match kind {
            EntityKind::Node => "node",
            EntityKind::Relationship => "relationship",
        }
    }

    let mut out = String::new();
    let source = &schema.source;
    let mut engine = source.engine.clone();
    if let Some(version) = &source.server_version {
        engine.push(' ');
        engine.push_str(version);
    }
    if let Some(edition) = &source.edition {
        let _ = write!(engine, " ({edition})");
    }
    let _ = writeln!(out, "engine: {engine}");
    if let Some(database) = &source.database {
        let _ = writeln!(out, "database: {database}");
    }
    let _ = writeln!(out, "sample size: {}", source.sample_size);

    let _ = writeln!(out, "\nnodes ({}):", schema.nodes.len());
    for node in &schema.nodes {
        let _ = writeln!(out, "  {}", node.name);
        for property in node.properties.values() {
            property_line(&mut out, property);
        }
    }
    let _ = writeln!(out, "\nrelationships ({}):", schema.relationships.len());
    for rel in &schema.relationships {
        let side = |labels: &[String]| {
            if labels.is_empty() {
                "()".to_string()
            } else {
                format!("(:{})", labels.join("|"))
            }
        };
        let _ = writeln!(
            out,
            "  {}  {}-[:{}]->{}",
            rel.type_name,
            side(&rel.source_labels),
            rel.type_name,
            side(&rel.target_labels)
        );
        for property in rel.properties.values() {
            property_line(&mut out, property);
        }
    }
    let _ = writeln!(out, "\nconstraints ({}):", schema.constraints.len());
    for constraint in &schema.constraints {
        let kind = match constraint.kind {
            voyager_drivers::introspect::ConstraintKind::Unique => "unique",
            voyager_drivers::introspect::ConstraintKind::Exists => "exists",
            voyager_drivers::introspect::ConstraintKind::Key => "key",
            voyager_drivers::introspect::ConstraintKind::Type => "type",
        };
        let mut line = format!(
            "  {kind} {} {}({})",
            entity(constraint.entity),
            constraint.label_or_type,
            constraint.properties.join(", ")
        );
        if let Some(property_type) = &constraint.property_type {
            let _ = write!(line, " :: {property_type}");
        }
        if let Some(name) = &constraint.name {
            let _ = write!(line, " [{name}]");
        }
        let _ = writeln!(out, "{line}");
    }
    let _ = writeln!(out, "\nindexes ({}):", schema.indexes.len());
    for index in &schema.indexes {
        let mut line = format!(
            "  {} {} {}({})",
            index.index_type,
            entity(index.entity),
            if index.labels_or_types.is_empty() {
                "*".to_string()
            } else {
                index.labels_or_types.join("|")
            },
            index.properties.join(", ")
        );
        if let Some(name) = &index.name {
            let _ = write!(line, " [{name}]");
        }
        if let Some(owner) = &index.owning_constraint {
            let _ = write!(line, " backs {owner}");
        }
        let _ = writeln!(out, "{line}");
    }
    out
}

fn print_build_facts() {
    println!("voyager {}", env!("CARGO_PKG_VERSION"));
    println!("voyager-ogm {}", voyager_ogm::VERSION);
    println!("driver protocol features: {}", enabled_driver_features());
    println!(
        "TLS: {}",
        if voyager_drivers::TLS_AVAILABLE {
            "enabled (rustls; bolt+s, rediss, PostgreSQL sslmode)"
        } else {
            "not built (enable the `tls` Cargo feature)"
        }
    );
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
    command: &str,
    parsed: &mut ParsedUri,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), Failure> {
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
        if !current_password.is_empty() {
            return Err(Failure::check(format!(
                "{command}: a password needs a username; put the username in the URI or pass --username"
            )));
        }
        return match parsed.protocol {
            // PostgreSQL has no anonymous login: the startup message must name a role.
            DatabaseProtocol::Postgres => Err(Failure::check(format!(
                "{command}: PostgreSQL requires a username; put it in the URI (for example postgresql://user@host:5432/database) or pass --username"
            ))),
            // Bolt sends the `none` authentication scheme and Redis skips AUTH, which is
            // what servers without authentication expect.
            _ => {
                parsed.auth = Auth::None;
                Ok(())
            }
        };
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
    /// Non-fatal findings printed as `warning:` lines; the connection itself is healthy.
    warnings: Vec<String>,
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
    let tls = tls_label(connection.is_tls());
    let server_version = connection.server_agent().map(str::to_string);
    let latency = measure_ping(&mut connection).await?;
    let _ = connection.close().await;

    Ok(ProbeReport {
        protocol: format!(
            "Bolt v{}.{}{tls}",
            protocol_version.major, protocol_version.minor
        ),
        server_version,
        latency,
        dialect: "cypher",
        warnings: Vec::new(),
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
    let tls = tls_label(connection.is_tls());
    let latency = measure_ping(&mut connection).await?;

    let uses_age = parsed.raw.starts_with("age:")
        || voyager_drivers::introspect::uses_age(parsed)
        || parsed.params.contains_key("graph")
        || parsed.params.contains_key("graph_name")
        || parsed.params.contains_key("age")
        || parsed.params.get("dialect").map(String::as_str) == Some("age");

    // The connection is healthy either way: a server without SQL/PGQ is reported
    // as a warning, since a plain postgresql:// URI does not say SQL/PGQ is wanted.
    let mut warnings = Vec::new();
    if !uses_age {
        match voyager_drivers::introspect::probe_pgq_support(&mut connection).await {
            Ok(voyager_drivers::introspect::PgqSupport::Supported) => {}
            Ok(voyager_drivers::introspect::PgqSupport::Unsupported { message, .. }) => {
                warnings.push(format!(
                    "{message}; SQL/PGQ queries will fail on this server"
                ));
            }
            Err(error) => {
                warnings.push(format!("could not check SQL/PGQ support: {error}"));
            }
        }
    }
    let _ = connection.close().await;

    Ok(ProbeReport {
        protocol: format!("PostgreSQL wire{tls}"),
        server_version,
        latency,
        dialect: if uses_age { "age" } else { "sql_pgq" },
        warnings,
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
    let tls = tls_label(connection.is_tls());
    let latency = measure_ping(&mut connection).await?;
    let _ = connection.close().await;

    Ok(ProbeReport {
        protocol: format!("{resp_label}{tls}"),
        server_version,
        latency,
        dialect: "cypher",
        warnings: Vec::new(),
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
fn tls_label(is_tls: bool) -> &'static str {
    if is_tls { " over TLS" } else { "" }
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
    for warning in &report.warnings {
        eprintln!("warning: {warning}");
    }
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
    fn schema_text_lists_properties_constraints_and_indexes() {
        use voyager_drivers::introspect::{
            ConstraintEntry, ConstraintKind, EntityKind, SchemaBuilder, SourceInfo,
        };
        let mut builder = SchemaBuilder::new();
        builder.add_property_type(EntityKind::Node, "Person", "email", Some("STRING NOT NULL"));
        builder.add_endpoint("KNOWS", Some("Person"), Some("Person"));
        builder.add_constraint(ConstraintEntry {
            entity: EntityKind::Node,
            label_or_type: "Person".to_string(),
            properties: vec!["email".to_string()],
            kind: ConstraintKind::Unique,
            property_type: None,
            name: Some("person_email".to_string()),
            engine_type: "UNIQUENESS".to_string(),
        });
        let schema = builder.finish(SourceInfo {
            engine: "neo4j".to_string(),
            server_version: Some("5.26.0".to_string()),
            edition: Some("community".to_string()),
            database: None,
            sample_size: 1000,
        });
        let text = render_schema_text(&schema);
        assert!(
            text.starts_with("engine: neo4j 5.26.0 (community)\n"),
            "{text}"
        );
        assert!(
            text.contains("  Person\n    email: STRING, unique\n"),
            "{text}"
        );
        assert!(
            text.contains("  KNOWS  (:Person)-[:KNOWS]->(:Person)"),
            "{text}"
        );
        assert!(
            text.contains("  unique node Person(email) [person_email]"),
            "{text}"
        );
    }

    #[test]
    fn doctor_lists_only_compiled_protocol_features() {
        let features = enabled_driver_features();
        assert_eq!(features.contains("bolt"), cfg!(feature = "bolt"));
        assert_eq!(features.contains("postgres"), cfg!(feature = "postgres"));
        assert_eq!(features.contains("redis"), cfg!(feature = "redis"));
    }

    #[test]
    fn username_is_optional_for_bolt_and_redis() {
        for uri in ["bolt://localhost:7687", "redis://localhost:6379"] {
            let mut parsed = ParsedUri::parse(uri).expect("parse URI");
            apply_credentials("doctor", &mut parsed, None, None).expect("no credentials needed");
            assert_eq!(parsed.auth, Auth::None, "{uri}");
        }
    }

    #[test]
    fn postgres_without_a_username_is_rejected() {
        let mut parsed = ParsedUri::parse("postgresql://localhost:5432/graph").expect("parse URI");
        let failure =
            apply_credentials("doctor", &mut parsed, None, None).expect_err("username required");
        assert_eq!(failure.code, EXIT_CHECK);
        assert!(
            failure.message.contains("requires a username"),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("--username"),
            "{}",
            failure.message
        );
    }

    #[test]
    fn password_without_a_username_is_rejected() {
        let mut parsed = ParsedUri::parse("bolt://localhost:7687").expect("parse URI");
        let failure = apply_credentials("doctor", &mut parsed, None, Some("secret"))
            .expect_err("username required");
        assert_eq!(failure.code, EXIT_CHECK);
        assert!(
            failure.message.contains("needs a username"),
            "{}",
            failure.message
        );
    }

    #[test]
    fn username_without_a_password_is_kept() {
        let mut parsed = ParsedUri::parse("bolt://localhost:7687").expect("parse URI");
        apply_credentials("doctor", &mut parsed, Some("memgraph"), None)
            .expect("username is enough");
        assert_eq!(
            parsed.auth,
            Auth::Basic {
                username: "memgraph".to_string(),
                password: String::new(),
                realm: None,
            }
        );
    }
}
