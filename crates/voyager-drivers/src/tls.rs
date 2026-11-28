//! Transport security: the plain-or-TLS socket used by every network driver, the TLS
//! options shared by Bolt, PostgreSQL and Redis, and the PostgreSQL `sslmode` rules.
//!
//! TLS itself is implemented with [rustls](https://docs.rs/rustls) and is compiled only
//! with the `tls` Cargo feature. Without it a TLS URI is still parsed, and connecting
//! fails with [`NetError::TlsError`] naming the missing feature; a TLS request is never
//! downgraded to plaintext.
//!
//! # URI parameters
//!
//! | Parameter | Aliases | Meaning |
//! |-----------|---------|---------|
//! | `tls_ca_file` | `sslrootcert` | PEM bundle of trusted CA certificates (replaces the system roots); `system` selects the system roots and implies verified TLS |
//! | `tls_cert_file` | `sslcert` | PEM client certificate chain for mutual TLS |
//! | `tls_key_file` | `sslkey` | PEM private key (PKCS#8, PKCS#1 or SEC1) for the client certificate |
//! | `tls_server_name` | | Name sent as SNI and checked against the server certificate instead of the URI host |
//! | `sslnegotiation` | | PostgreSQL only: `postgres` (SSLRequest, default) or `direct` (PostgreSQL 17+) |
//!
//! Parameter names are matched case-insensitively. A parameter may strengthen the TLS that the
//! scheme asks for but never weaken it, and contradictory TLS parameters are rejected with
//! [`NetError::InvalidUri`].

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::config::TlsMode;
use crate::error::{NetError, Result};
use crate::uri::{DatabaseProtocol, ParsedUri};

/// Whether this build of voyager-drivers can open TLS connections (the `tls` feature).
pub const TLS_AVAILABLE: bool = cfg!(feature = "tls");

/// Client-side TLS settings shared by every protocol.
///
/// Values usually come from URI parameters (see the [module documentation](self)); a
/// [`ConnectionConfig`](crate::ConnectionConfig) can also set them programmatically,
/// in which case each field set there wins over the URI.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsOptions {
    /// PEM file with the CA certificates that sign the server certificate. When set, only
    /// these CAs are trusted; when unset, the operating system's root store is used.
    pub ca_file: Option<PathBuf>,
    /// PEM client certificate chain presented for mutual TLS (requires `client_key_file`).
    pub client_cert_file: Option<PathBuf>,
    /// PEM private key matching `client_cert_file`.
    pub client_key_file: Option<PathBuf>,
    /// Overrides the name used for SNI and certificate hostname verification
    /// (defaults to the URI host).
    pub server_name: Option<String>,
}

impl TlsOptions {
    /// Creates empty options (system roots, no client certificate, URI host as server name).
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads the TLS parameters of a parsed URI (`tls_ca_file` / `sslrootcert`, ...).
    ///
    /// `sslrootcert=system` is treated as "no custom CA file" (the system roots).
    pub fn from_params(params: &HashMap<String, String>) -> Self {
        let get = |names: &[&str]| {
            names
                .iter()
                .find_map(|name| params.get(*name))
                .filter(|value| !value.is_empty())
                .cloned()
        };
        Self {
            ca_file: get(&["tls_ca_file", "sslrootcert"])
                .filter(|value| value != "system")
                .map(PathBuf::from),
            client_cert_file: get(&["tls_cert_file", "sslcert"]).map(PathBuf::from),
            client_key_file: get(&["tls_key_file", "sslkey"]).map(PathBuf::from),
            server_name: get(&["tls_server_name"]),
        }
    }

    /// Sets the trusted CA bundle (PEM).
    pub fn with_ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_file = Some(path.into());
        self
    }

    /// Sets the client certificate chain and private key (PEM) for mutual TLS.
    pub fn with_client_cert(
        mut self,
        cert_file: impl Into<PathBuf>,
        key_file: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert_file = Some(cert_file.into());
        self.client_key_file = Some(key_file.into());
        self
    }

    /// Sets the server name used for SNI and hostname verification.
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Returns these options with every unset field taken from `fallback`.
    pub fn or(self, fallback: Self) -> Self {
        Self {
            ca_file: self.ca_file.or(fallback.ca_file),
            client_cert_file: self.client_cert_file.or(fallback.client_cert_file),
            client_key_file: self.client_key_file.or(fallback.client_key_file),
            server_name: self.server_name.or(fallback.server_name),
        }
    }
}

/// How the server certificate is checked during the TLS handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVerification {
    /// Verify the chain against the trusted roots and the hostname (or `tls_server_name`).
    Full,
    /// Verify the chain against the trusted roots but not the hostname (PostgreSQL `verify-ca`).
    CaOnly,
    /// Accept any certificate (Bolt `+ssc`, `?tls=insecure`, PostgreSQL `require` without a root
    /// certificate). The connection is encrypted but not authenticated.
    None,
}

impl TlsVerification {
    /// Verification for the Bolt and Redis TLS modes: `Required` verifies fully, `SelfSigned`
    /// accepts any certificate, `Disabled` means no TLS at all (`None` is returned).
    pub fn for_mode(mode: TlsMode) -> Option<Self> {
        match mode {
            TlsMode::Disabled => Option::None,
            TlsMode::Required => Some(Self::Full),
            TlsMode::SelfSigned => Some(Self::None),
        }
    }
}

/// PostgreSQL `sslmode`, with libpq semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SslMode {
    /// Plaintext only.
    Disable,
    /// Plaintext first; TLS only if the server rejects the plaintext connection.
    Allow,
    /// TLS if the server supports it, plaintext if the server answers the SSLRequest with `N`.
    Prefer,
    /// TLS required; the certificate is not verified unless a root certificate is configured,
    /// in which case it behaves like `VerifyCa` (as libpq does).
    Require,
    /// TLS required; the certificate chain is verified, the hostname is not.
    VerifyCa,
    /// TLS required; the certificate chain and the hostname are verified.
    VerifyFull,
}

impl SslMode {
    /// Parses a libpq `sslmode` value.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "disable" => Ok(Self::Disable),
            "allow" => Ok(Self::Allow),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify-ca" => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            other => Err(NetError::InvalidUri(format!(
                "invalid sslmode value '{other}' (expected disable, allow, prefer, require, verify-ca or verify-full)"
            ))),
        }
    }

    /// Returns the libpq spelling of this mode.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Allow => "allow",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }

    /// Resolves the effective `sslmode` of a PostgreSQL / Apache AGE URI.
    ///
    /// An explicit `?sslmode=` wins. Otherwise the scheme and `?ssl=` / `?tls=` decide:
    /// `postgresql+s://` or `?ssl=true` is `verify-full`, `postgresql+ssc://` or
    /// `?ssl=insecure` is `require` (encrypted, unverified), and a plain URI is `disable`.
    /// `sslrootcert=system` (or `tls_ca_file=system`) defaults to `verify-full` and rejects
    /// weaker explicit modes, as libpq does. [`ParsedUri::parse`] already rejects an `sslmode`
    /// weaker than the TLS the scheme or another parameter asks for.
    pub fn from_uri(uri: &ParsedUri) -> Result<Self> {
        let mode = match uri.params.get("sslmode") {
            Some(value) => Self::parse(value)?,
            None => match uri.tls {
                TlsMode::Disabled => Self::Disable,
                TlsMode::Required => Self::VerifyFull,
                TlsMode::SelfSigned => Self::Require,
            },
        };
        if let Some(name) = system_root_parameter(&uri.params) {
            return match uri.params.get("sslmode") {
                None => Ok(Self::VerifyFull),
                Some(_) if mode == Self::VerifyFull => Ok(mode),
                Some(value) => Err(NetError::InvalidUri(format!(
                    "{name}=system requires sslmode=verify-full (got sslmode={value})"
                ))),
            };
        }
        Ok(mode)
    }

    fn strength(self) -> TlsStrength {
        match self {
            Self::Disable => TlsStrength::Plaintext,
            Self::Allow | Self::Prefer => TlsStrength::Opportunistic,
            Self::Require => TlsStrength::Encrypted,
            Self::VerifyCa => TlsStrength::ChainVerified,
            Self::VerifyFull => TlsStrength::Verified,
        }
    }

    /// Whether the first connection attempt sends an SSLRequest.
    pub fn requests_tls_first(self) -> bool {
        !matches!(self, Self::Disable | Self::Allow)
    }

    /// Whether this mode may ever use TLS.
    pub fn may_use_tls(self) -> bool {
        self != Self::Disable
    }

    /// Whether a server that answers the SSLRequest with `N` may be used in plaintext.
    /// Only `prefer` falls back; every other TLS mode fails.
    pub fn accepts_plaintext_fallback(self) -> bool {
        self == Self::Prefer
    }

    /// Certificate verification for a TLS session in this mode. `has_root_cert` is whether a
    /// root certificate (`sslrootcert` / `tls_ca_file`) is configured: as in libpq, a root
    /// certificate makes `allow`, `prefer` and `require` verify the chain.
    pub fn verification(self, has_root_cert: bool) -> TlsVerification {
        match self {
            Self::VerifyFull => TlsVerification::Full,
            Self::VerifyCa => TlsVerification::CaOnly,
            Self::Disable | Self::Allow | Self::Prefer | Self::Require => {
                if has_root_cert {
                    TlsVerification::CaOnly
                } else {
                    TlsVerification::None
                }
            }
        }
    }
}

/// PostgreSQL `sslnegotiation` (libpq 17+): how a TLS session is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SslNegotiation {
    /// Send an SSLRequest and start TLS after the server answers `S` (every PostgreSQL version).
    #[default]
    Postgres,
    /// Start TLS immediately, without an SSLRequest, offering the ALPN protocol `postgresql`
    /// (PostgreSQL 17 and later). Only allowed with `sslmode` `require`, `verify-ca` or
    /// `verify-full`, as in libpq.
    Direct,
}

impl SslNegotiation {
    /// ALPN protocol name PostgreSQL requires for direct TLS negotiation.
    pub const ALPN: &'static [u8] = b"postgresql";

    /// Resolves `?sslnegotiation=` for a URI whose effective `sslmode` is `mode`.
    pub fn from_uri(uri: &ParsedUri, mode: SslMode) -> Result<Self> {
        match uri.params.get("sslnegotiation").map(String::as_str) {
            None | Some("postgres") => Ok(Self::Postgres),
            Some("direct") => match mode {
                SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => Ok(Self::Direct),
                weak => Err(NetError::InvalidUri(format!(
                    "weak sslmode \"{}\" may not be used with sslnegotiation=direct (use \"require\", \"verify-ca\", or \"verify-full\")",
                    weak.as_str()
                ))),
            },
            Some(other) => Err(NetError::InvalidUri(format!(
                "invalid sslnegotiation value '{other}' (expected postgres or direct)"
            ))),
        }
    }
}

/// How much protection a TLS setting asks for; used to reject URI parameters that would
/// weaken the TLS requested by the scheme or by another parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TlsStrength {
    /// No TLS (`ssl=false`, `sslmode=disable`).
    Plaintext,
    /// TLS that may fall back to plaintext (`sslmode=allow` / `prefer`).
    Opportunistic,
    /// TLS without certificate verification (`+ssc`, `ssl=insecure`, `sslmode=require`).
    Encrypted,
    /// TLS with the chain verified but not the hostname (`sslmode=verify-ca`).
    ChainVerified,
    /// TLS with chain and hostname verified (`+s`, `ssl=true`, `sslmode=verify-full`,
    /// `sslrootcert=system`).
    Verified,
}

impl TlsStrength {
    fn of_mode(mode: TlsMode) -> Self {
        match mode {
            TlsMode::Disabled => Self::Plaintext,
            TlsMode::SelfSigned => Self::Encrypted,
            TlsMode::Required => Self::Verified,
        }
    }
}

/// URI parameters that configure TLS. Their names are matched case-insensitively
/// ([`ParsedUri::parse`] stores them lower-cased), and giving one twice with different
/// values is an error.
pub const TLS_PARAMETERS: &[&str] = &[
    "ssl",
    "tls",
    "sslmode",
    "sslnegotiation",
    "sslrootcert",
    "tls_ca_file",
    "sslcert",
    "tls_cert_file",
    "sslkey",
    "tls_key_file",
    "tls_server_name",
];

/// Pairs of parameter names that set the same option.
const TLS_ALIASES: &[(&str, &str)] = &[
    ("tls_ca_file", "sslrootcert"),
    ("tls_cert_file", "sslcert"),
    ("tls_key_file", "sslkey"),
];

/// Returns the name of the root-certificate parameter set to `system`, if any.
fn system_root_parameter(params: &HashMap<String, String>) -> Option<&'static str> {
    ["sslrootcert", "tls_ca_file"]
        .into_iter()
        .find(|name| params.get(*name).map(String::as_str) == Some("system"))
}

/// Parses an `ssl` / `tls` parameter value.
fn parse_ssl_flag(name: &str, value: &str) -> Result<TlsMode> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "require" | "required" => Ok(TlsMode::Required),
        "self-signed" | "ssc" | "allow_invalid" | "insecure" => Ok(TlsMode::SelfSigned),
        "false" | "0" | "disable" | "disabled" => Ok(TlsMode::Disabled),
        _ => Err(NetError::InvalidUri(format!(
            "invalid {name} value '{value}' (expected true, false or insecure)"
        ))),
    }
}

/// Resolves the coarse [`TlsMode`] of a URI from its scheme and its TLS parameters, and
/// rejects parameters that contradict each other or weaken the TLS the scheme asks for.
///
/// `params` must hold the TLS parameter names lower-cased (as [`ParsedUri::parse`] stores
/// them). A TLS scheme (`bolt+s`, `rediss`, `postgresql+ssc`, ...) can be strengthened by a
/// parameter but never weakened: `bolt+s://…?tls=false`, `rediss://…?tls=insecure` and
/// `postgresql+s://…?sslmode=require` are errors. Two parameters that ask for different TLS
/// levels (`?ssl=true&sslmode=require`, `?sslrootcert=system&sslmode=require`) are errors too,
/// as are two aliases (`tls_ca_file` / `sslrootcert`, ...) with different values.
pub(crate) fn resolve_uri_tls(
    scheme: &str,
    scheme_tls: TlsMode,
    params: &HashMap<String, String>,
) -> Result<TlsMode> {
    for (name, alias) in TLS_ALIASES {
        if let (Some(a), Some(b)) = (params.get(*name), params.get(*alias))
            && a != b
        {
            return Err(NetError::InvalidUri(format!(
                "conflicting TLS parameters {name}={a} and {alias}={b} (they set the same option)"
            )));
        }
    }

    let mut explicit: Vec<(String, TlsStrength)> = Vec::new();
    let mut flag_mode = None;
    for name in ["ssl", "tls"] {
        if let Some(value) = params.get(name) {
            let mode = parse_ssl_flag(name, value)?;
            explicit.push((format!("{name}={value}"), TlsStrength::of_mode(mode)));
            if flag_mode.is_some_and(|previous| previous != mode) {
                return Err(NetError::InvalidUri(format!(
                    "conflicting TLS parameters ssl={} and tls={}",
                    params["ssl"], params["tls"]
                )));
            }
            flag_mode = Some(mode);
        }
    }
    let sslmode = match params.get("sslmode") {
        Some(value) => {
            let mode = SslMode::parse(value)?;
            explicit.push((format!("sslmode={value}"), mode.strength()));
            Some(mode)
        }
        None => None,
    };
    let system_root = system_root_parameter(params);
    if let Some(name) = system_root {
        explicit.push((format!("{name}=system"), TlsStrength::Verified));
    }

    if let Some((first, first_strength)) = explicit.first()
        && let Some((other, _)) = explicit
            .iter()
            .find(|(_, strength)| strength != first_strength)
    {
        return Err(NetError::InvalidUri(format!(
            "conflicting TLS parameters {first} and {other} ask for different TLS levels"
        )));
    }

    if scheme_tls != TlsMode::Disabled {
        let required = TlsStrength::of_mode(scheme_tls);
        if let Some((weaker, _)) = explicit.iter().find(|(_, strength)| *strength < required) {
            let what = if scheme_tls == TlsMode::Required {
                "verified TLS"
            } else {
                "TLS"
            };
            return Err(NetError::InvalidUri(format!(
                "{weaker} would weaken the {what} that the {scheme}:// scheme requires; remove the parameter or use a scheme without TLS"
            )));
        }
    }

    Ok(match (flag_mode, sslmode) {
        (Some(mode), _) => mode,
        // Coarse mapping for Bolt and Redis; PostgreSQL resolves the exact libpq behaviour
        // from `sslmode` itself (see `SslMode::from_uri`).
        (None, Some(SslMode::Disable)) => TlsMode::Disabled,
        (None, Some(_)) => TlsMode::Required,
        (None, None) if system_root.is_some() => TlsMode::Required,
        (None, None) => scheme_tls,
    })
}

/// Error returned when a TLS connection is requested from a build without the `tls` feature.
pub fn tls_unavailable(protocol: DatabaseProtocol, host: &str) -> NetError {
    NetError::TlsError(format!(
        "TLS was requested for {protocol} endpoint '{host}', but voyager-drivers was built without the `tls` feature (rebuild with --features tls). Plaintext downgrade rejected."
    ))
}

/// A connected socket: plain TCP, or TCP wrapped in a rustls client session.
#[derive(Debug)]
pub enum NetStream {
    /// Unencrypted TCP.
    Plain(TcpStream),
    /// TLS over TCP.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl NetStream {
    /// Returns whether the connection is encrypted.
    pub fn is_tls(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(_) => true,
        }
    }

    /// Returns the underlying TCP socket.
    pub fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.get_ref().0,
        }
    }

    /// Checks whether the underlying socket appears open and free of unexpected unread data
    /// without blocking or sending any network round-trip.
    pub fn is_locally_alive(&self) -> bool {
        is_stream_locally_alive(self, None)
    }
}

/// Connects a TCP socket to `addr` with `SO_KEEPALIVE` and `TCP_NODELAY` enabled,
/// returning the async Tokio stream alongside a cloned `std::net::TcpStream` probe
/// handle for non-blocking `MSG_PEEK` liveness checks independent of Tokio's readiness cache.
#[cfg(any(feature = "bolt", feature = "postgres", feature = "redis"))]
pub(crate) async fn connect_tcp(addr: &str) -> io::Result<(TcpStream, std::net::TcpStream)> {
    let mut last_err = None;
    let mut resolved = false;
    for sock_addr in tokio::net::lookup_host(addr).await? {
        resolved = true;
        let socket = match sock_addr {
            std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        let _ = socket.set_keepalive(true);
        match socket.connect(sock_addr).await {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let std_stream = stream.into_std()?;
                let probe = std_stream.try_clone()?;
                let tokio_stream = TcpStream::from_std(std_stream)?;
                return Ok((tokio_stream, probe));
            }
            Err(e) => last_err = Some(e),
        }
    }
    if !resolved {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Could not resolve socket address '{addr}'"),
        ));
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("Failed to connect to '{addr}'"),
        )
    }))
}

/// Performs a zero-round-trip local liveness check on `stream` (and its `std::net::TcpStream`
/// probe if available), rejecting peer EOF, socket errors, TLS close/pending-plaintext,
/// and unexpected unread bytes on an idle connection.
pub(crate) fn is_stream_locally_alive(
    stream: &NetStream,
    probe: Option<&std::net::TcpStream>,
) -> bool {
    #[cfg(feature = "tls")]
    if let NetStream::Tls(tls) = stream
        && !tls.get_ref().1.wants_read()
    {
        return false;
    }

    if let Some(std_tcp) = probe {
        if !matches!(std_tcp.take_error(), Ok(None)) {
            return false;
        }
        let mut buf = [0u8; 1];
        return matches!(
            std_tcp.peek(&mut buf),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock
        );
    }

    let tcp = stream.tcp();
    if !matches!(tcp.take_error(), Ok(None)) {
        return false;
    }
    let mut buf = [0u8; 1];
    matches!(
        tcp.try_read(&mut buf),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock
    )
}

impl From<TcpStream> for NetStream {
    fn from(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }
}

impl AsyncRead for NetStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for NetStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Wraps a connected TCP socket in TLS, or refuses when the `tls` feature is off.
///
/// `host` is the URI host (used for SNI and hostname verification unless
/// [`TlsOptions::server_name`] overrides it).
pub async fn upgrade(
    tcp: TcpStream,
    protocol: DatabaseProtocol,
    host: &str,
    verification: TlsVerification,
    options: &TlsOptions,
) -> Result<NetStream> {
    upgrade_with_alpn(tcp, protocol, host, verification, options, None).await
}

/// Like [`upgrade`], offering one ALPN protocol and requiring the server to select it.
pub(crate) async fn upgrade_with_alpn(
    tcp: TcpStream,
    protocol: DatabaseProtocol,
    host: &str,
    verification: TlsVerification,
    options: &TlsOptions,
    alpn: Option<&[u8]>,
) -> Result<NetStream> {
    #[cfg(feature = "tls")]
    {
        rustls_impl::connect(tcp, protocol, host, verification, options, alpn).await
    }
    #[cfg(not(feature = "tls"))]
    {
        let _ = (tcp, verification, options, alpn);
        Err(tls_unavailable(protocol, host))
    }
}

/// Checks the TLS options without connecting: files exist and parse, the client certificate
/// has a key. With the `tls` feature off this returns the feature error.
pub fn validate(
    protocol: DatabaseProtocol,
    host: &str,
    verification: TlsVerification,
    options: &TlsOptions,
) -> Result<()> {
    #[cfg(feature = "tls")]
    {
        let _ = protocol;
        rustls_impl::client_config(host, verification, options, None).map(|_| ())
    }
    #[cfg(not(feature = "tls"))]
    {
        let _ = (verification, options);
        Err(tls_unavailable(protocol, host))
    }
}

/// Splits one DER element off `input`: returns its tag, its contents and the remaining bytes.
/// Only single-byte tags and lengths of up to four bytes are accepted (enough for X.509).
#[cfg(feature = "tls")]
fn der_element(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let count = usize::from(first & 0x7F);
        if count == 0 || count > 4 || rest.len() < count {
            return None;
        }
        let (bytes, rest) = rest.split_at(count);
        let len = bytes
            .iter()
            .fold(0usize, |acc, byte| (acc << 8) | usize::from(*byte));
        (len, rest)
    };
    if rest.len() < len {
        return None;
    }
    let (contents, rest) = rest.split_at(len);
    Some((tag, contents, rest))
}

/// Reads the keyUsage extension of an X.509 certificate (DER). Returns `None` when the
/// certificate has no keyUsage extension (or cannot be walked), otherwise whether the
/// `digitalSignature` bit is set.
#[cfg(feature = "tls")]
fn leaf_key_usage_allows_signing(certificate: &[u8]) -> Option<bool> {
    const SEQUENCE: u8 = 0x30;
    const EXTENSIONS: u8 = 0xA3;
    const KEY_USAGE_OID: [u8; 3] = [0x55, 0x1D, 0x0F];

    let (SEQUENCE, certificate, _) = der_element(certificate)? else {
        return None;
    };
    let (SEQUENCE, mut tbs, _) = der_element(certificate)? else {
        return None;
    };
    while !tbs.is_empty() {
        let (tag, contents, rest) = der_element(tbs)?;
        tbs = rest;
        if tag != EXTENSIONS {
            continue;
        }
        let (SEQUENCE, mut extensions, _) = der_element(contents)? else {
            return None;
        };
        while !extensions.is_empty() {
            let (tag, extension, rest) = der_element(extensions)?;
            extensions = rest;
            if tag != SEQUENCE {
                return None;
            }
            let (0x06, oid, mut fields) = der_element(extension)? else {
                return None;
            };
            if oid != KEY_USAGE_OID {
                continue;
            }
            let (mut tag, mut value, rest) = der_element(fields)?;
            if tag == 0x01 {
                // the optional `critical` BOOLEAN
                fields = rest;
                (tag, value, _) = der_element(fields)?;
            }
            if tag != 0x04 {
                return None;
            }
            let (0x03, bits, _) = der_element(value)? else {
                return None;
            };
            // bits[0] is the count of unused bits; digitalSignature is the first named bit.
            return Some(bits.get(1).is_some_and(|byte| byte & 0x80 != 0));
        }
        return None;
    }
    None
}

#[cfg(feature = "tls")]
mod rustls_impl {
    use std::path::Path;
    use std::sync::{Arc, OnceLock};

    use rustls::client::WebPkiServerVerifier;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
    use rustls::{CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore};
    use tokio::net::TcpStream;
    use tokio_rustls::TlsConnector;

    use super::{NetStream, TlsOptions, TlsVerification};
    use crate::error::{NetError, Result};
    use crate::uri::DatabaseProtocol;

    fn provider() -> Arc<CryptoProvider> {
        static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
        PROVIDER
            .get_or_init(|| Arc::new(rustls::crypto::ring::default_provider()))
            .clone()
    }

    /// The operating system root store, loaded once per process.
    fn system_roots() -> Result<Arc<RootCertStore>> {
        static ROOTS: OnceLock<std::result::Result<Arc<RootCertStore>, String>> = OnceLock::new();
        ROOTS
            .get_or_init(|| {
                let loaded = rustls_native_certs::load_native_certs();
                let mut roots = RootCertStore::empty();
                roots.add_parsable_certificates(loaded.certs);
                if roots.is_empty() {
                    let detail = loaded
                        .errors
                        .first()
                        .map(|error| format!(" ({error})"))
                        .unwrap_or_default();
                    Err(format!(
                        "no trusted root certificates were found in the system store{detail}; set tls_ca_file (or sslrootcert) to a PEM CA bundle"
                    ))
                } else {
                    Ok(Arc::new(roots))
                }
            })
            .clone()
            .map_err(NetError::TlsError)
    }

    fn read_ca_file(path: &Path) -> Result<Arc<RootCertStore>> {
        let certs = read_certs(path, "CA")?;
        let mut roots = RootCertStore::empty();
        let (added, _ignored) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(NetError::TlsError(format!(
                "CA file '{}' contains no usable certificates",
                path.display()
            )));
        }
        Ok(Arc::new(roots))
    }

    fn read_certs(path: &Path, what: &str) -> Result<Vec<CertificateDer<'static>>> {
        let certs = CertificateDer::pem_file_iter(path)
            .and_then(|iter| iter.collect::<std::result::Result<Vec<_>, _>>())
            .map_err(|error| {
                NetError::TlsError(format!(
                    "cannot read {what} certificate file '{}': {error}",
                    path.display()
                ))
            })?;
        if certs.is_empty() {
            return Err(NetError::TlsError(format!(
                "{what} certificate file '{}' contains no PEM certificates",
                path.display()
            )));
        }
        Ok(certs)
    }

    fn roots(options: &TlsOptions) -> Result<Arc<RootCertStore>> {
        match &options.ca_file {
            Some(path) => read_ca_file(path),
            None => system_roots(),
        }
    }

    pub(super) fn client_config(
        host: &str,
        verification: TlsVerification,
        options: &TlsOptions,
        alpn: Option<&[u8]>,
    ) -> Result<(Arc<ClientConfig>, ServerName<'static>)> {
        let provider = provider();
        let builder = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|error| NetError::TlsError(format!("TLS configuration error: {error}")))?;

        let builder = match verification {
            TlsVerification::Full | TlsVerification::CaOnly => {
                let inner = WebPkiServerVerifier::builder_with_provider(roots(options)?, provider)
                    .build()
                    .map_err(|error| {
                        NetError::TlsError(format!("TLS configuration error: {error}"))
                    })?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(CheckedVerifier {
                        inner,
                        check_hostname: verification == TlsVerification::Full,
                    }))
            }
            TlsVerification::None => {
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate {
                        algorithms: provider.signature_verification_algorithms,
                    }))
            }
        };

        let mut config = match (&options.client_cert_file, &options.client_key_file) {
            (Some(cert_file), Some(key_file)) => {
                let chain = read_certs(cert_file, "client")?;
                let key = PrivateKeyDer::from_pem_file(key_file).map_err(|error| {
                    NetError::TlsError(format!(
                        "cannot read client key file '{}': {error}",
                        key_file.display()
                    ))
                })?;
                builder.with_client_auth_cert(chain, key).map_err(|error| {
                    NetError::TlsError(format!("invalid client certificate or key: {error}"))
                })?
            }
            (None, None) => builder.with_no_client_auth(),
            (Some(_), None) => {
                return Err(NetError::TlsError(
                    "a client certificate (tls_cert_file / sslcert) needs a private key (tls_key_file / sslkey)"
                        .to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(NetError::TlsError(
                    "a client key (tls_key_file / sslkey) needs a client certificate (tls_cert_file / sslcert)"
                        .to_string(),
                ));
            }
        };

        let name = options.server_name.as_deref().unwrap_or(host);
        let name = name
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .unwrap_or(name);
        let server_name = ServerName::try_from(name.to_string()).map_err(|error| {
            NetError::TlsError(format!("invalid TLS server name '{name}': {error}"))
        })?;
        if let Some(protocol) = alpn {
            config.alpn_protocols = vec![protocol.to_vec()];
        }
        Ok((Arc::new(config), server_name))
    }

    pub(super) async fn connect(
        tcp: TcpStream,
        protocol: DatabaseProtocol,
        host: &str,
        verification: TlsVerification,
        options: &TlsOptions,
        alpn: Option<&[u8]>,
    ) -> Result<NetStream> {
        let (config, server_name) = client_config(host, verification, options, alpn)?;
        let shown_name = server_name.to_str().into_owned();
        let stream = TlsConnector::from(config)
            .connect(server_name, tcp)
            .await
            .map_err(|error| {
                NetError::TlsError(format!(
                    "TLS handshake with {protocol} endpoint '{host}' (server name '{shown_name}') failed: {error}"
                ))
            })?;
        if let Some(expected) = alpn
            && stream.get_ref().1.alpn_protocol() != Some(expected)
        {
            return Err(NetError::TlsError(format!(
                "{protocol} endpoint '{host}' completed the TLS handshake without selecting the ALPN protocol '{}'",
                String::from_utf8_lossy(expected)
            )));
        }
        Ok(NetStream::Tls(Box::new(stream)))
    }

    /// Verifies the chain against the trusted roots (rustls-webpki), optionally the hostname
    /// (PostgreSQL `verify-ca` skips it), and that a leaf certificate with a keyUsage
    /// extension allows `digitalSignature`, which every TLS key exchange rustls supports needs.
    #[derive(Debug)]
    struct CheckedVerifier {
        inner: Arc<WebPkiServerVerifier>,
        check_hostname: bool,
    }

    impl ServerCertVerifier for CheckedVerifier {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            server_name: &ServerName<'_>,
            ocsp_response: &[u8],
            now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            let verified = match self.inner.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                ocsp_response,
                now,
            ) {
                Err(rustls::Error::InvalidCertificate(
                    CertificateError::NotValidForName
                    | CertificateError::NotValidForNameContext { .. },
                )) if !self.check_hostname => ServerCertVerified::assertion(),
                other => other?,
            };
            if super::leaf_key_usage_allows_signing(end_entity.as_ref()) == Some(false) {
                return Err(rustls::Error::InvalidCertificate(CertificateError::Other(
                    rustls::OtherError(Arc::new(KeyUsageError)),
                )));
            }
            Ok(verified)
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            self.inner.verify_tls12_signature(message, cert, dss)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            self.inner.verify_tls13_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.inner.supported_verify_schemes()
        }
    }

    /// The server certificate's keyUsage extension does not allow `digitalSignature`.
    /// `Debug` prints the message too, because rustls shows this error with `{:?}`.
    struct KeyUsageError;

    impl KeyUsageError {
        const MESSAGE: &'static str = "server certificate keyUsage does not allow digitalSignature, which TLS server authentication needs";
    }

    impl std::fmt::Display for KeyUsageError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(Self::MESSAGE)
        }
    }

    impl std::fmt::Debug for KeyUsageError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(Self::MESSAGE)
        }
    }

    impl std::error::Error for KeyUsageError {}

    /// Accepts any server certificate. The handshake signatures are still checked, so the
    /// session is bound to the certificate the server presented; only its identity is unverified.
    #[derive(Debug)]
    struct AcceptAnyCertificate {
        algorithms: WebPkiSupportedAlgorithms,
    }

    impl ServerCertVerifier for AcceptAnyCertificate {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.algorithms.supported_schemes()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg(uri: &str) -> Result<SslMode> {
        SslMode::from_uri(&ParsedUri::parse(uri)?)
    }

    #[test]
    fn sslmode_resolution_from_uri() {
        let cases = [
            ("postgresql://h/db", SslMode::Disable),
            ("postgresql://h/db?sslmode=disable", SslMode::Disable),
            ("postgresql://h/db?sslmode=allow", SslMode::Allow),
            ("postgresql://h/db?sslmode=prefer", SslMode::Prefer),
            ("postgresql://h/db?sslmode=require", SslMode::Require),
            ("postgresql://h/db?sslmode=verify-ca", SslMode::VerifyCa),
            ("postgresql://h/db?sslmode=verify-full", SslMode::VerifyFull),
            ("postgresql+s://h/db", SslMode::VerifyFull),
            ("postgresql+ssc://h/db", SslMode::Require),
            ("age+s://h/db", SslMode::VerifyFull),
            ("postgresql://h/db?ssl=true", SslMode::VerifyFull),
            ("postgresql://h/db?ssl=insecure", SslMode::Require),
            (
                "postgresql+s://h/db?sslmode=verify-full",
                SslMode::VerifyFull,
            ),
            ("postgresql+ssc://h/db?sslmode=verify-ca", SslMode::VerifyCa),
            ("postgresql://h/db?SSLMODE=require", SslMode::Require),
            ("postgresql://h/db?SslMode=verify-full", SslMode::VerifyFull),
            (
                "postgresql://h/db?sslmode=require&sslmode=require",
                SslMode::Require,
            ),
            (
                "postgresql://h/db?ssl=true&sslmode=verify-full",
                SslMode::VerifyFull,
            ),
            ("postgresql://h/db?sslrootcert=system", SslMode::VerifyFull),
            ("postgresql://h/db?tls_ca_file=system", SslMode::VerifyFull),
            ("postgresql://h/db?TLS_CA_FILE=system", SslMode::VerifyFull),
            (
                "postgresql+ssc://h/db?tls_ca_file=system",
                SslMode::VerifyFull,
            ),
            (
                "postgresql://h/db?sslrootcert=system&sslmode=verify-full",
                SslMode::VerifyFull,
            ),
        ];
        for (uri, expected) in cases {
            assert_eq!(pg(uri).unwrap(), expected, "{uri}");
        }
    }

    #[test]
    fn invalid_sslmode_values_are_rejected() {
        for uri in [
            "postgresql://h/db?sslmode=required",
            "postgresql://h/db?sslmode=VERIFY-FULL",
            "postgresql://h/db?sslrootcert=system&sslmode=require",
            "postgresql://h/db?tls_ca_file=system&sslmode=require",
            "postgresql://h/db?tls_ca_file=system&sslmode=verify-ca",
            "postgresql://h/db?tls_ca_file=system&sslmode=prefer",
            "postgresql://h/db?tls_ca_file=system&sslmode=disable",
            "postgresql://h/db?tls_ca_file=system&ssl=insecure",
        ] {
            assert!(
                matches!(pg(uri), Err(NetError::InvalidUri(_))),
                "{uri} should be rejected"
            );
        }
    }

    /// A TLS scheme, an `sslmode` that requires TLS, or `sslrootcert=system` is never weakened
    /// by another parameter, whatever its spelling or position.
    #[test]
    fn weakening_tls_parameters_are_rejected() {
        let tls_schemes = [
            "bolt+s",
            "neo4j+s",
            "memgraph+s",
            "bolt+ssc",
            "neo4j+ssc",
            "memgraph+ssc",
            "rediss",
            "falkordbs",
            "valkeys",
            "postgresql+s",
            "postgres+s",
            "age+s",
            "postgresql+ssc",
            "age+ssc",
        ];
        let weakening = [
            "tls=false",
            "ssl=false",
            "ssl=0",
            "tls=0",
            "ssl=disable",
            "tls=disabled",
            "TLS=false",
            "Ssl=false",
            "sslmode=disable",
            "SSLMODE=disable",
            "sslmode=allow",
            "sslmode=prefer",
        ];
        for scheme in tls_schemes {
            for query in weakening {
                let uri = format!("{scheme}://u:pw@h:1/db?{query}");
                match ParsedUri::parse(&uri) {
                    Err(NetError::InvalidUri(message)) => {
                        assert!(message.contains("weaken"), "{uri}: {message}");
                        assert!(!message.contains("pw"), "{uri}: {message}");
                    }
                    other => panic!("{uri}: expected InvalidUri, got {other:?}"),
                }
            }
        }
        // A verified scheme is not weakened to "encrypted but unverified" either.
        for uri in [
            "bolt+s://h?tls=insecure",
            "rediss://h?ssl=ssc",
            "neo4j+s://h?sslmode=require",
            "postgresql+s://h/db?sslmode=require",
            "postgresql+s://h/db?sslmode=verify-ca",
            "age+s://h/db?ssl=allow_invalid",
        ] {
            assert!(
                matches!(ParsedUri::parse(uri), Err(NetError::InvalidUri(_))),
                "{uri} should be rejected"
            );
        }
        // An sslmode that requires TLS is not weakened by ssl / tls, in either order.
        for query in [
            "sslmode=require&ssl=false",
            "ssl=false&sslmode=require",
            "sslmode=verify-full&tls=0",
            "sslmode=verify-ca&tls=insecure",
            "ssl=true&sslmode=require",
            "ssl=true&sslmode=prefer",
            "sslrootcert=system&ssl=false",
        ] {
            for scheme in ["postgresql", "bolt", "redis"] {
                let uri = format!("{scheme}://h/db?{query}");
                assert!(
                    matches!(ParsedUri::parse(&uri), Err(NetError::InvalidUri(_))),
                    "{uri} should be rejected"
                );
            }
        }
    }

    #[test]
    fn duplicate_and_conflicting_tls_parameters_are_rejected() {
        for uri in [
            "postgresql://h/db?sslmode=verify-full&sslmode=disable",
            "postgresql://h/db?sslmode=require&sslmode=disable",
            "postgresql://h/db?sslmode=require&SSLMODE=disable",
            "bolt://h?tls=true&tls=false",
            "bolt://h?ssl=true&tls=false",
            "rediss://h?tls_ca_file=/a.pem&tls_ca_file=/b.pem",
            "postgresql://h/db?sslrootcert=/ca.pem&tls_ca_file=system",
            "postgresql://h/db?sslrootcert=/a.pem&tls_ca_file=/b.pem",
            "bolt+s://h?sslcert=/a.pem&tls_cert_file=/b.pem",
            "bolt+s://h?sslkey=/a.key&TLS_KEY_FILE=/b.key",
            "bolt+s://h?tls_server_name=a&tls_server_name=b",
            "postgresql://h/db?sslnegotiation=direct&sslnegotiation=postgres",
            "bolt://h?tls=maybe",
            "bolt://h?sslmode=verify",
        ] {
            assert!(
                matches!(ParsedUri::parse(uri), Err(NetError::InvalidUri(_))),
                "{uri} should be rejected"
            );
        }
        // Repeating a parameter with the same value, or giving both aliases the same value, is fine.
        let uri = ParsedUri::parse(
            "bolt+s://h?tls_ca_file=/ca.pem&sslrootcert=/ca.pem&tls=true&tls=true",
        )
        .unwrap();
        assert_eq!(uri.tls, TlsMode::Required);
        assert_eq!(
            TlsOptions::from_params(&uri.params).ca_file,
            Some(PathBuf::from("/ca.pem"))
        );
    }

    #[test]
    fn strengthening_tls_parameters_keep_working() {
        let cases = [
            ("bolt://h?tls=true", TlsMode::Required),
            ("bolt://h?TLS=true", TlsMode::Required),
            ("bolt://h?ssl=insecure", TlsMode::SelfSigned),
            ("bolt://h?sslmode=require", TlsMode::Required),
            ("bolt://h?tls_ca_file=system", TlsMode::Required),
            ("bolt+ssc://h?tls=true", TlsMode::Required),
            ("bolt+ssc://h?tls=insecure", TlsMode::SelfSigned),
            ("bolt+s://h?tls=true", TlsMode::Required),
            ("bolt+s://h?sslmode=verify-full", TlsMode::Required),
            ("redis://h?ssl=1", TlsMode::Required),
            ("rediss://h?tls_ca_file=system", TlsMode::Required),
            ("postgresql://h/db?sslmode=disable", TlsMode::Disabled),
            ("postgresql://h/db?ssl=false", TlsMode::Disabled),
            (
                "postgresql+ssc://h/db?sslmode=verify-full",
                TlsMode::Required,
            ),
            ("bolt://h?tls_ca_file=/ca.pem", TlsMode::Disabled),
        ];
        for (uri, expected) in cases {
            assert_eq!(ParsedUri::parse(uri).unwrap().tls, expected, "{uri}");
        }
        // Non-TLS parameters keep their case and last-wins behaviour.
        let uri = ParsedUri::parse("bolt://h?Routing=a&Routing=b").unwrap();
        assert_eq!(uri.params.get("Routing").map(String::as_str), Some("b"));
    }

    #[test]
    fn sslnegotiation_follows_libpq() {
        let resolve = |uri: &str| -> Result<SslNegotiation> {
            let uri = ParsedUri::parse(uri)?;
            SslNegotiation::from_uri(&uri, SslMode::from_uri(&uri)?)
        };
        for (uri, expected) in [
            (
                "postgresql://h/db?sslmode=require",
                SslNegotiation::Postgres,
            ),
            (
                "postgresql://h/db?sslmode=require&sslnegotiation=postgres",
                SslNegotiation::Postgres,
            ),
            (
                "postgresql://h/db?sslmode=require&sslnegotiation=direct",
                SslNegotiation::Direct,
            ),
            (
                "postgresql://h/db?sslmode=verify-ca&sslnegotiation=direct",
                SslNegotiation::Direct,
            ),
            (
                "postgresql+s://h/db?sslnegotiation=direct",
                SslNegotiation::Direct,
            ),
            (
                "age+ssc://h/db?SSLNEGOTIATION=direct",
                SslNegotiation::Direct,
            ),
        ] {
            assert_eq!(resolve(uri).unwrap(), expected, "{uri}");
        }
        for uri in [
            "postgresql://h/db?sslnegotiation=direct",
            "postgresql://h/db?sslmode=disable&sslnegotiation=direct",
            "postgresql://h/db?sslmode=allow&sslnegotiation=direct",
            "postgresql://h/db?sslmode=prefer&sslnegotiation=direct",
            "postgresql://h/db?sslmode=require&sslnegotiation=Direct",
            "postgresql://h/db?sslmode=require&sslnegotiation=tls",
        ] {
            assert!(
                matches!(resolve(uri), Err(NetError::InvalidUri(_))),
                "{uri} should be rejected"
            );
        }
        let message = resolve("postgresql://h/db?sslmode=prefer&sslnegotiation=direct")
            .unwrap_err()
            .to_string();
        assert!(message.contains("weak sslmode \"prefer\""), "{message}");
    }

    #[cfg(feature = "tls")]
    #[test]
    fn key_usage_reader() {
        use der_fixtures as der;
        assert_eq!(
            leaf_key_usage_allows_signing(&der::with_key_usage(0x80)),
            Some(true)
        );
        assert_eq!(
            leaf_key_usage_allows_signing(&der::with_key_usage(0xA0)),
            Some(true)
        );
        assert_eq!(
            leaf_key_usage_allows_signing(&der::with_key_usage(0x20)),
            Some(false)
        );
        assert_eq!(
            leaf_key_usage_allows_signing(&der::without_key_usage()),
            None
        );
        assert_eq!(leaf_key_usage_allows_signing(&[0x30, 0x05, 0x01]), None);
        assert_eq!(leaf_key_usage_allows_signing(&[]), None);
    }

    /// Hand-built DER skeletons (only the structure the keyUsage reader walks).
    #[cfg(feature = "tls")]
    mod der_fixtures {
        fn tlv(tag: u8, contents: &[u8]) -> Vec<u8> {
            assert!(contents.len() < 0x80);
            let mut out = vec![tag, contents.len() as u8];
            out.extend_from_slice(contents);
            out
        }

        fn certificate(extensions: Vec<u8>) -> Vec<u8> {
            let mut tbs = tlv(0xA0, &tlv(0x02, &[2])); // version
            tbs.extend(tlv(0x02, &[1])); // serial
            tbs.extend(tlv(0xA3, &tlv(0x30, &extensions)));
            let mut cert = tlv(0x30, &tbs);
            cert.extend(tlv(0x30, &[])); // signature algorithm
            tlv(0x30, &cert)
        }

        fn basic_constraints() -> Vec<u8> {
            let mut ext = tlv(0x06, &[0x55, 0x1D, 0x13]);
            ext.extend(tlv(0x04, &tlv(0x30, &[])));
            tlv(0x30, &ext)
        }

        pub fn with_key_usage(bits: u8) -> Vec<u8> {
            let mut ext = tlv(0x06, &[0x55, 0x1D, 0x0F]);
            ext.extend(tlv(0x01, &[0xFF])); // critical
            ext.extend(tlv(0x04, &tlv(0x03, &[0x05, bits])));
            let mut extensions = basic_constraints();
            extensions.extend(tlv(0x30, &ext));
            certificate(extensions)
        }

        pub fn without_key_usage() -> Vec<u8> {
            certificate(basic_constraints())
        }
    }

    #[test]
    fn sslmode_decision_table() {
        use SslMode::*;
        use TlsVerification as V;
        // (mode, TLS first, may use TLS, N-reply falls back, verification w/o root, with root)
        let table = [
            (Disable, false, false, false, V::None, V::CaOnly),
            (Allow, false, true, false, V::None, V::CaOnly),
            (Prefer, true, true, true, V::None, V::CaOnly),
            (Require, true, true, false, V::None, V::CaOnly),
            (VerifyCa, true, true, false, V::CaOnly, V::CaOnly),
            (VerifyFull, true, true, false, V::Full, V::Full),
        ];
        for (mode, first, may, fallback, without_root, with_root) in table {
            assert_eq!(mode.requests_tls_first(), first, "{mode:?}");
            assert_eq!(mode.may_use_tls(), may, "{mode:?}");
            assert_eq!(mode.accepts_plaintext_fallback(), fallback, "{mode:?}");
            assert_eq!(mode.verification(false), without_root, "{mode:?}");
            assert_eq!(mode.verification(true), with_root, "{mode:?}");
        }
    }

    #[test]
    fn tls_options_read_uri_parameters_and_aliases() {
        let uri = ParsedUri::parse(
            "bolt+s://h:7687?tls_ca_file=/ca.pem&sslcert=/c.pem&tls_key_file=/k.pem&tls_server_name=db.example",
        )
        .unwrap();
        let options = TlsOptions::from_params(&uri.params);
        assert_eq!(options.ca_file, Some(PathBuf::from("/ca.pem")));
        assert_eq!(options.client_cert_file, Some(PathBuf::from("/c.pem")));
        assert_eq!(options.client_key_file, Some(PathBuf::from("/k.pem")));
        assert_eq!(options.server_name.as_deref(), Some("db.example"));

        let system =
            ParsedUri::parse("postgresql://h/db?sslrootcert=system&sslmode=verify-full").unwrap();
        assert_eq!(TlsOptions::from_params(&system.params).ca_file, None);

        let explicit = TlsOptions::new().with_ca_file("/mine.pem");
        let merged = explicit.or(options);
        assert_eq!(merged.ca_file, Some(PathBuf::from("/mine.pem")));
        assert_eq!(merged.server_name.as_deref(), Some("db.example"));
    }

    #[test]
    fn bolt_and_redis_modes_map_to_verification() {
        assert_eq!(TlsVerification::for_mode(TlsMode::Disabled), Option::None);
        assert_eq!(
            TlsVerification::for_mode(TlsMode::Required),
            Some(TlsVerification::Full)
        );
        assert_eq!(
            TlsVerification::for_mode(TlsMode::SelfSigned),
            Some(TlsVerification::None)
        );
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn validate_names_the_missing_feature() {
        let error = validate(
            DatabaseProtocol::Bolt,
            "localhost",
            TlsVerification::Full,
            &TlsOptions::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("`tls` feature"), "{error}");
        assert!(error.to_string().contains("Plaintext downgrade rejected"));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn client_certificate_needs_a_key() {
        let options = TlsOptions {
            client_cert_file: Some(PathBuf::from("/nonexistent.pem")),
            ..TlsOptions::new()
        };
        // Verification None avoids loading roots; the key check fails first or the file read does.
        let error = validate(
            DatabaseProtocol::Redis,
            "localhost",
            TlsVerification::None,
            &options,
        )
        .unwrap_err();
        assert!(matches!(error, NetError::TlsError(_)), "{error}");
    }

    #[cfg(feature = "tls")]
    #[test]
    fn missing_ca_file_is_a_tls_error() {
        let options = TlsOptions::new().with_ca_file("/nonexistent/ca.pem");
        let error = validate(
            DatabaseProtocol::Bolt,
            "localhost",
            TlsVerification::Full,
            &options,
        )
        .unwrap_err();
        assert!(error.to_string().contains("/nonexistent/ca.pem"), "{error}");
    }

    #[cfg(any(feature = "bolt", feature = "postgres", feature = "redis"))]
    #[tokio::test]
    async fn connect_tcp_enables_keepalive_and_local_liveness_detects_eof_and_unread_bytes() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::{TcpListener, TcpSocket};

        let sock = TcpSocket::new_v4().unwrap();
        sock.set_keepalive(true).unwrap();
        assert!(sock.keepalive().unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_task = tokio::spawn(async move {
            let (s1, _) = listener.accept().await.unwrap();
            let (s2, _) = listener.accept().await.unwrap();
            (s1, s2, listener)
        });

        let (stream1, probe1) = connect_tcp(&addr.to_string()).await.unwrap();
        let (stream2, probe2) = connect_tcp(&addr.to_string()).await.unwrap();
        let (mut srv1, mut srv2, _listener) = accept_task.await.unwrap();

        let net1 = NetStream::Plain(stream1);
        let net2 = NetStream::Plain(stream2);
        assert!(is_stream_locally_alive(&net1, Some(&probe1)));
        assert!(is_stream_locally_alive(&net2, Some(&probe2)));

        // 1. Peer closes socket -> local liveness check detects EOF immediately (0 RTT).
        srv1.shutdown().await.unwrap();
        drop(srv1);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!is_stream_locally_alive(&net1, Some(&probe1)));

        // 2. Peer sends unexpected bytes on idle socket -> rejected as unsafe to reuse.
        srv2.write_all(b"UNEXPECTED").await.unwrap();
        srv2.flush().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!is_stream_locally_alive(&net2, Some(&probe2)));
    }
}
