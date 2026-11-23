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
//! | `tls_ca_file` | `sslrootcert` | PEM bundle of trusted CA certificates (replaces the system roots); `system` selects the system roots |
//! | `tls_cert_file` | `sslcert` | PEM client certificate chain for mutual TLS |
//! | `tls_key_file` | `sslkey` | PEM private key (PKCS#8, PKCS#1 or SEC1) for the client certificate |
//! | `tls_server_name` | | Name sent as SNI and checked against the server certificate instead of the URI host |

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
    /// `sslrootcert=system` defaults to `verify-full` and rejects weaker explicit modes, as
    /// libpq does.
    pub fn from_uri(uri: &ParsedUri) -> Result<Self> {
        let mode = match uri.params.get("sslmode") {
            Some(value) => Self::parse(value)?,
            None => match uri.tls {
                TlsMode::Disabled => Self::Disable,
                TlsMode::Required => Self::VerifyFull,
                TlsMode::SelfSigned => Self::Require,
            },
        };
        if uri.params.get("sslrootcert").map(String::as_str) == Some("system") {
            return match uri.params.get("sslmode") {
                None => Ok(Self::VerifyFull),
                Some(_) if mode == Self::VerifyFull => Ok(mode),
                Some(value) => Err(NetError::InvalidUri(format!(
                    "sslrootcert=system requires sslmode=verify-full (got sslmode={value})"
                ))),
            };
        }
        Ok(mode)
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
    #[cfg(feature = "tls")]
    {
        rustls_impl::connect(tcp, protocol, host, verification, options).await
    }
    #[cfg(not(feature = "tls"))]
    {
        let _ = (tcp, verification, options);
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
        rustls_impl::client_config(host, verification, options).map(|_| ())
    }
    #[cfg(not(feature = "tls"))]
    {
        let _ = (verification, options);
        Err(tls_unavailable(protocol, host))
    }
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
    ) -> Result<(Arc<ClientConfig>, ServerName<'static>)> {
        let provider = provider();
        let builder = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|error| NetError::TlsError(format!("TLS configuration error: {error}")))?;

        let builder = match verification {
            TlsVerification::Full => builder.with_root_certificates(roots(options)?),
            TlsVerification::CaOnly => {
                let inner = WebPkiServerVerifier::builder_with_provider(roots(options)?, provider)
                    .build()
                    .map_err(|error| {
                        NetError::TlsError(format!("TLS configuration error: {error}"))
                    })?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(CaOnlyVerifier { inner }))
            }
            TlsVerification::None => {
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate {
                        algorithms: provider.signature_verification_algorithms,
                    }))
            }
        };

        let config = match (&options.client_cert_file, &options.client_key_file) {
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
        Ok((Arc::new(config), server_name))
    }

    pub(super) async fn connect(
        tcp: TcpStream,
        protocol: DatabaseProtocol,
        host: &str,
        verification: TlsVerification,
        options: &TlsOptions,
    ) -> Result<NetStream> {
        let (config, server_name) = client_config(host, verification, options)?;
        let shown_name = server_name.to_str().into_owned();
        let stream = TlsConnector::from(config)
            .connect(server_name, tcp)
            .await
            .map_err(|error| {
                NetError::TlsError(format!(
                    "TLS handshake with {protocol} endpoint '{host}' (server name '{shown_name}') failed: {error}"
                ))
            })?;
        Ok(NetStream::Tls(Box::new(stream)))
    }

    /// Verifies the chain against the trusted roots and ignores a hostname mismatch
    /// (PostgreSQL `verify-ca`).
    #[derive(Debug)]
    struct CaOnlyVerifier {
        inner: Arc<WebPkiServerVerifier>,
    }

    impl ServerCertVerifier for CaOnlyVerifier {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            server_name: &ServerName<'_>,
            ocsp_response: &[u8],
            now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            match self.inner.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                ocsp_response,
                now,
            ) {
                Err(rustls::Error::InvalidCertificate(
                    CertificateError::NotValidForName
                    | CertificateError::NotValidForNameContext { .. },
                )) => Ok(ServerCertVerified::assertion()),
                other => other,
            }
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
            ("postgresql+s://h/db?sslmode=require", SslMode::Require),
            ("postgresql://h/db?sslrootcert=system", SslMode::VerifyFull),
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
        ] {
            assert!(
                matches!(pg(uri), Err(NetError::InvalidUri(_))),
                "{uri} should be rejected"
            );
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
}
