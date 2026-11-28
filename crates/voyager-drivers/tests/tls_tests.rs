//! In-process TLS tests: a throwaway PKI (rcgen) and rustls stub servers speaking just
//! enough Bolt, PostgreSQL and RESP to complete a connection. They cover certificate and
//! hostname verification, mutual TLS, PostgreSQL `sslmode` negotiation (including the
//! server's `N` reply) and, without the `tls` feature, the refusal that names the feature.

use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use voyager_drivers::{NetError, TLS_AVAILABLE, connect_any};

fn assert_tls_error(result: Result<(), NetError>, context: &str) -> String {
    match result {
        Err(NetError::TlsError(message)) => message,
        Err(other) => panic!("{context}: expected a TLS error, got {other:?}"),
        Ok(()) => panic!("{context}: expected a TLS error, got a connection"),
    }
}

// ---------------------------------------------------------------------------
// Feature off: TLS requests are refused and never downgraded
// ---------------------------------------------------------------------------

#[cfg(not(feature = "tls"))]
#[tokio::test]
async fn tls_uris_name_the_missing_feature() {
    for uri in [
        "bolt+s://localhost:7687",
        "neo4j+ssc://localhost:7687",
        "rediss://localhost:6379",
        "falkordbs://localhost:6379",
        "valkeys://localhost:6379",
        "postgresql+s://postgres@localhost:5432/db",
        "postgresql://postgres@localhost:5432/db?sslmode=prefer",
        "postgresql://postgres@localhost:5432/db?sslmode=require",
        "age://postgres@localhost:5455/db?sslmode=verify-full",
    ] {
        let message = assert_tls_error(connect_any(uri).await.map(|_| ()), uri);
        assert!(message.contains("`tls` feature"), "{uri}: {message}");
        assert!(message.contains("Plaintext downgrade rejected"), "{uri}");
    }
}

// ---------------------------------------------------------------------------
// The client never speaks plaintext when TLS was requested
// ---------------------------------------------------------------------------

/// Returns the first bytes the client sent to a bare TCP listener (empty if it never connected).
async fn first_bytes_sent(uri_for_port: impl FnOnce(u16) -> String) -> (Vec<u8>, NetError) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let capture = tokio::spawn(async move {
        match tokio::time::timeout(Duration::from_secs(5), listener.accept()).await {
            Ok(Ok((mut tcp, _))) => {
                let mut buf = vec![0u8; 8];
                let n = tcp.read(&mut buf).await.unwrap_or(0);
                buf.truncate(n);
                buf
            }
            _ => Vec::new(),
        }
    });
    let uri = uri_for_port(port);
    let error = match tokio::time::timeout(Duration::from_secs(5), connect_any(&uri)).await {
        Ok(Ok(_)) => panic!("{uri}: connected to a server that never answered"),
        Ok(Err(error)) => error,
        Err(_) => panic!("{uri}: connect hung"),
    };
    let bytes = if TLS_AVAILABLE {
        capture.await.unwrap()
    } else {
        capture.abort();
        Vec::new()
    };
    (bytes, error)
}

#[tokio::test]
async fn tls_requests_never_send_plaintext_protocol_bytes() {
    for scheme in ["bolt+s", "neo4j+ssc", "rediss", "falkordbs", "valkeys"] {
        let (bytes, error) =
            first_bytes_sent(|port| format!("{scheme}://localhost:{port}?connect_timeout=3")).await;
        if TLS_AVAILABLE {
            // 0x16 = TLS handshake record (ClientHello), never the Bolt preamble or a RESP command.
            assert_eq!(bytes.first(), Some(&0x16), "{scheme}: sent {bytes:02x?}");
        } else {
            assert!(matches!(error, NetError::TlsError(_)), "{scheme}: {error}");
        }
    }
    for query in ["sslmode=prefer", "sslmode=require", "sslmode=verify-full"] {
        let (bytes, error) = first_bytes_sent(|port| {
            format!("postgresql://postgres@127.0.0.1:{port}/db?{query}&connect_timeout=3")
        })
        .await;
        if TLS_AVAILABLE {
            assert_eq!(
                bytes,
                [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F],
                "{query}: SSLRequest first"
            );
        } else {
            assert!(matches!(error, NetError::TlsError(_)), "{query}: {error}");
        }
    }
}

/// Accepts on a bare TCP listener and reports whether the client ever connected.
async fn connects_at_all(uri_for_port: impl FnOnce(u16) -> String) -> (bool, NetError) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let uri = uri_for_port(port);
    let error = match tokio::time::timeout(Duration::from_secs(5), connect_any(&uri)).await {
        Ok(Ok(_)) => panic!("{uri}: connected"),
        Ok(Err(error)) => error,
        Err(_) => panic!("{uri}: connect hung"),
    };
    let accepted = tokio::time::timeout(Duration::from_millis(200), listener.accept())
        .await
        .is_ok();
    (accepted, error)
}

/// A URI parameter can never turn a TLS scheme, a TLS-requiring `sslmode` or
/// `sslrootcert=system` into plaintext (or into unverified TLS): the URI is rejected
/// before any connection is made, with or without the `tls` feature.
#[tokio::test]
async fn weakening_tls_parameters_never_reach_the_network() {
    let cases = [
        "bolt+s://localhost:{port}?tls=false",
        "neo4j+s://localhost:{port}?ssl=false",
        "memgraph+s://localhost:{port}?tls=0",
        "bolt+ssc://localhost:{port}?ssl=disable",
        "neo4j+s://localhost:{port}?sslmode=disable",
        "bolt+s://localhost:{port}?tls=insecure",
        "rediss://localhost:{port}?tls=false",
        "falkordbs://localhost:{port}?ssl=false",
        "valkeys://localhost:{port}?SSL=disable",
        "postgresql+s://postgres@127.0.0.1:{port}/db?sslmode=disable",
        "postgres+s://postgres@127.0.0.1:{port}/db?ssl=disable",
        "postgresql+s://postgres@127.0.0.1:{port}/db?tls=false",
        "postgresql+s://postgres@127.0.0.1:{port}/db?sslmode=allow",
        "postgresql+s://postgres@127.0.0.1:{port}/db?sslmode=require",
        "age+s://postgres@127.0.0.1:{port}/db?SSLMODE=disable",
        "postgresql://postgres@127.0.0.1:{port}/db?sslmode=verify-full&sslmode=disable",
        "postgresql://postgres@127.0.0.1:{port}/db?sslmode=require&Sslmode=disable",
        "postgresql://postgres@127.0.0.1:{port}/db?sslmode=require&ssl=false",
        "postgresql://postgres@127.0.0.1:{port}/db?sslmode=require&tls_ca_file=system",
        "postgresql://postgres@127.0.0.1:{port}/db?sslrootcert=/ca.pem&tls_ca_file=system",
        "postgresql://postgres@127.0.0.1:{port}/db?sslmode=prefer&sslnegotiation=direct",
    ];
    for template in cases {
        let (connected, error) =
            connects_at_all(|port| template.replace("{port}", &port.to_string())).await;
        assert!(
            matches!(error, NetError::InvalidUri(_)),
            "{template}: expected InvalidUri, got {error:?}"
        );
        assert!(!connected, "{template}: a connection was opened");
    }
}

/// Upper-case TLS parameter names are honoured, not ignored.
#[tokio::test]
async fn tls_parameter_names_are_case_insensitive_on_the_wire() {
    for query in ["SSLMODE=require", "Sslmode=verify-full", "SSL=true"] {
        let (bytes, error) = first_bytes_sent(|port| {
            format!("postgresql://postgres@127.0.0.1:{port}/db?{query}&connect_timeout=3")
        })
        .await;
        if TLS_AVAILABLE {
            assert_eq!(
                bytes,
                [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F],
                "{query}: SSLRequest first"
            );
        } else {
            assert!(matches!(error, NetError::TlsError(_)), "{query}: {error}");
        }
    }
}

#[test]
fn uri_errors_and_display_do_not_show_credentials() {
    let error =
        voyager_drivers::ParsedUri::parse("postgresql://user:SECRET123@127.0.0.1:notaport/db")
            .unwrap_err()
            .to_string();
    assert!(error.contains("Malformed URI"), "{error}");
    assert!(
        !error.contains("SECRET123") && error.contains("user:***@"),
        "{error}"
    );

    // A password containing '/' ends the authority early for a URL parser, so the
    // malformed-URI error must still hide everything up to the last '@'.
    let error = voyager_drivers::ParsedUri::parse("bolt://neo4j:pa/SECRET9@word@db:notaport")
        .unwrap_err()
        .to_string();
    assert!(error.contains("Malformed URI"), "{error}");
    assert!(
        !error.contains("SECRET9") && !error.contains("neo4j:"),
        "{error}"
    );

    let parsed =
        voyager_drivers::ParsedUri::parse("bolt+s://neo4j:SECRET123@db.example:7687").unwrap();
    let shown = parsed.to_string();
    assert!(!shown.contains("SECRET123"), "{shown}");
    assert!(shown.contains("db.example:7687"), "{shown}");

    let error = voyager_drivers::ParsedUri::parse("bolt+s://neo4j:SECRET123@h?tls=false")
        .unwrap_err()
        .to_string();
    assert!(!error.contains("SECRET123"), "{error}");
}

#[cfg(feature = "tls")]
mod tls_enabled {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::{
        AsyncConnection, BoltConnection, ConnectionConfig, NetError, ParsedUri, PostgresConnection,
        RedisConnection, TlsOptions, connect_any,
    };

    use super::assert_tls_error;

    // ---------------------------------------------------------------------------
    // Throwaway PKI
    // ---------------------------------------------------------------------------

    struct Pki {
        dir: PathBuf,
        /// CA that signs the server and client certificates.
        ca_file: PathBuf,
        /// An unrelated CA: trusting only this one must fail verification.
        other_ca_file: PathBuf,
        client_cert_file: PathBuf,
        client_key_file: PathBuf,
        ca_der: CertificateDer<'static>,
        server_chain: Vec<CertificateDer<'static>>,
        server_key: Vec<u8>,
    }

    impl Drop for Pki {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn ca(name: &str) -> (rcgen::Certificate, Issuer<'static, KeyPair>) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).unwrap();
        (cert, Issuer::new(params, key))
    }

    /// The server certificate is valid only for `localhost`, so connecting to `127.0.0.1`
    /// is a hostname mismatch.
    fn pki() -> Pki {
        pki_with_server_key_usage(Vec::new())
    }

    /// Like [`pki`], with the given keyUsage bits on the server certificate (none: no extension).
    fn pki_with_server_key_usage(key_usages: Vec<KeyUsagePurpose>) -> Pki {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "voyager-tls-tests-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let (ca_cert, issuer) = ca("Voyager in-process test CA");
        let (other_ca_cert, _) = ca("Unrelated CA");

        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        server_params.key_usages = key_usages;
        let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

        let client_key = KeyPair::generate().unwrap();
        let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        client_params
            .distinguished_name
            .push(DnType::CommonName, "voyager-test-client");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_cert = client_params.signed_by(&client_key, &issuer).unwrap();

        let write = |name: &str, contents: String| {
            let path = dir.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        };
        Pki {
            ca_file: write("ca.pem", ca_cert.pem()),
            other_ca_file: write("other-ca.pem", other_ca_cert.pem()),
            client_cert_file: write("client.pem", client_cert.pem()),
            client_key_file: write("client.key", client_key.serialize_pem()),
            ca_der: ca_cert.der().clone(),
            server_chain: vec![server_cert.der().clone()],
            server_key: server_key.serialize_der(),
            dir,
        }
    }

    fn acceptor(pki: &Pki, require_client_cert: bool) -> tokio_rustls::TlsAcceptor {
        tokio_rustls::TlsAcceptor::from(Arc::new(server_config(pki, require_client_cert)))
    }

    /// A server that accepts direct TLS only with the ALPN protocol `postgresql`, as
    /// PostgreSQL 17+ does (`alpn = false`: a server that ignores ALPN).
    fn acceptor_with_alpn(pki: &Pki, alpn: bool) -> tokio_rustls::TlsAcceptor {
        let mut config = server_config(pki, false);
        if alpn {
            config.alpn_protocols = vec![b"postgresql".to_vec()];
        }
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    }

    fn server_config(pki: &Pki, require_client_cert: bool) -> rustls::ServerConfig {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap();
        let builder = if require_client_cert {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(pki.ca_der.clone()).unwrap();
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .unwrap();
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pki.server_key.clone()));
        builder
            .with_single_cert(pki.server_chain.clone(), key)
            .unwrap()
    }

    fn url_path(path: &std::path::Path) -> String {
        path.display().to_string()
    }

    // ---------------------------------------------------------------------------
    // Protocol stubs
    // ---------------------------------------------------------------------------

    async fn read_bolt_message<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<()> {
        loop {
            let size = stream.read_u16().await? as usize;
            if size == 0 {
                return Ok(());
            }
            let mut chunk = vec![0u8; size];
            stream.read_exact(&mut chunk).await?;
        }
    }

    /// Bolt 5.0: answers the handshake, then SUCCESS {} to every message.
    async fn serve_bolt<S: AsyncRead + AsyncWrite + Unpin>(mut stream: S) -> std::io::Result<()> {
        let mut handshake = [0u8; 20];
        stream.read_exact(&mut handshake).await?;
        assert_eq!(&handshake[..4], &[0x60, 0x60, 0xB0, 0x17]);
        stream.write_all(&[0, 0, 0, 5]).await?;
        stream.flush().await?;
        loop {
            read_bolt_message(&mut stream).await?;
            stream
                .write_all(&[0x00, 0x03, 0xB1, 0x70, 0xA0, 0x00, 0x00])
                .await?;
            stream.flush().await?;
        }
    }

    /// RESP3: a one-entry map for HELLO, then +PONG for every later command.
    async fn serve_redis<S: AsyncRead + AsyncWrite + Unpin>(mut stream: S) -> std::io::Result<()> {
        let mut buf = [0u8; 4096];
        let mut first = true;
        loop {
            if stream.read(&mut buf).await? == 0 {
                return Ok(());
            }
            let reply: &[u8] = if first {
                b"%1\r\n+server\r\n+stub\r\n"
            } else {
                b"+PONG\r\n"
            };
            first = false;
            stream.write_all(reply).await?;
            stream.flush().await?;
        }
    }

    fn pg_frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![tag];
        frame.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// Reads a StartupMessage, then accepts (AuthenticationOk + ReadyForQuery) or rejects
    /// (a FATAL ErrorResponse, as for a missing pg_hba entry).
    async fn serve_pg_startup<S: AsyncRead + AsyncWrite + Unpin>(
        mut stream: S,
        accept: bool,
    ) -> std::io::Result<()> {
        let len = stream.read_u32().await? as usize;
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body).await?;
        assert_eq!(&body[..4], &196_608u32.to_be_bytes(), "protocol 3.0");
        if accept {
            let mut reply = pg_frame(b'R', &0u32.to_be_bytes());
            reply.extend(pg_frame(b'Z', b"I"));
            stream.write_all(&reply).await?;
        } else {
            stream
                .write_all(&pg_frame(
                    b'E',
                    b"SFATAL\0C28000\0Mno pg_hba.conf entry (no encryption)\0\0",
                ))
                .await?;
        }
        stream.flush().await?;
        // Keep the session open until the client terminates it.
        let mut rest = [0u8; 512];
        while stream.read(&mut rest).await? > 0 {}
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum Proto {
        Bolt,
        Redis,
    }

    /// A TLS-only Bolt or Redis server. Returns its port.
    async fn tls_server(acceptor: tokio_rustls::TlsAcceptor, proto: Proto) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(tls) = acceptor.accept(tcp).await {
                        let _ = match proto {
                            Proto::Bolt => serve_bolt(tls).await,
                            Proto::Redis => serve_redis(tls).await,
                        };
                    }
                });
            }
        });
        port
    }

    /// What the PostgreSQL stub saw, in order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum PgEvent {
        SslRequest,
        PlainStartup,
        TlsStartup,
        TlsHandshakeFailed,
        /// A TLS ClientHello arrived without an SSLRequest (`sslnegotiation=direct`).
        DirectTls,
    }

    /// How the PostgreSQL stub treats a startup that arrives without TLS.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Plaintext {
        Accept,
        /// A FATAL ErrorResponse, as for a hostssl-only pg_hba.conf.
        Reject,
        /// Close the connection without a reply.
        Close,
    }

    /// A PostgreSQL stub. `ssl_reply` answers an SSLRequest (`S` or `N`); `reject_plaintext`
    /// refuses startups that arrive without TLS.
    async fn pg_server(
        acceptor: tokio_rustls::TlsAcceptor,
        ssl_reply: u8,
        reject_plaintext: bool,
    ) -> (u16, Arc<Mutex<Vec<PgEvent>>>) {
        let plaintext = if reject_plaintext {
            Plaintext::Reject
        } else {
            Plaintext::Accept
        };
        pg_server_with(acceptor, ssl_reply, &[], plaintext).await
    }

    /// The general stub: `after_reply` is sent right after the SSLRequest reply (data a
    /// man-in-the-middle could inject before the TLS handshake). A TLS ClientHello that
    /// arrives without an SSLRequest is accepted as direct TLS.
    async fn pg_server_with(
        acceptor: tokio_rustls::TlsAcceptor,
        ssl_reply: u8,
        after_reply: &'static [u8],
        plaintext: Plaintext,
    ) -> (u16, Arc<Mutex<Vec<PgEvent>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let seen = seen.clone();
                tokio::spawn(async move {
                    let mut head = [0u8; 8];
                    if tcp.peek(&mut head).await.unwrap_or(0) < 8 {
                        return;
                    }
                    if head[0] == 0x16 {
                        seen.lock().push(PgEvent::DirectTls);
                        match acceptor.accept(tcp).await {
                            Ok(tls) => {
                                seen.lock().push(PgEvent::TlsStartup);
                                let _ = serve_pg_startup(tls, true).await;
                            }
                            Err(_) => seen.lock().push(PgEvent::TlsHandshakeFailed),
                        }
                        return;
                    }
                    if head == [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F] {
                        tcp.read_exact(&mut head).await.unwrap();
                        seen.lock().push(PgEvent::SslRequest);
                        let mut reply = vec![ssl_reply];
                        reply.extend_from_slice(after_reply);
                        tcp.write_all(&reply).await.unwrap();
                        if ssl_reply == b'S' {
                            match acceptor.accept(tcp).await {
                                Ok(tls) => {
                                    seen.lock().push(PgEvent::TlsStartup);
                                    let _ = serve_pg_startup(tls, true).await;
                                }
                                Err(_) => seen.lock().push(PgEvent::TlsHandshakeFailed),
                            }
                            return;
                        }
                    }
                    // Record a plaintext startup only if one actually arrives.
                    if tcp.peek(&mut head).await.unwrap_or(0) == 0 {
                        return;
                    }
                    seen.lock().push(PgEvent::PlainStartup);
                    if plaintext == Plaintext::Close {
                        return;
                    }
                    let _ = serve_pg_startup(tcp, plaintext == Plaintext::Accept).await;
                });
            }
        });
        (port, events)
    }

    // ---------------------------------------------------------------------------
    // Bolt
    // ---------------------------------------------------------------------------

    async fn bolt(uri: String) -> Result<BoltConnection, NetError> {
        BoltConnection::connect(&ConnectionConfig::from_uri(uri)).await
    }

    #[tokio::test]
    async fn bolt_tls_verifies_ca_and_hostname() {
        let pki = pki();
        let port = tls_server(acceptor(&pki, false), Proto::Bolt).await;
        let ca = url_path(&pki.ca_file);

        let mut conn = bolt(format!("bolt+s://localhost:{port}?tls_ca_file={ca}"))
            .await
            .expect("bolt+s with the right CA");
        assert!(conn.is_tls());
        conn.ping().await.expect("RESET over TLS");

        // neo4j+s is the routing spelling of the same transport.
        assert!(
            bolt(format!("neo4j+s://localhost:{port}?tls_ca_file={ca}"))
                .await
                .unwrap()
                .is_tls()
        );

        let other = url_path(&pki.other_ca_file);
        let message = assert_tls_error(
            bolt(format!("bolt+s://localhost:{port}?tls_ca_file={other}"))
                .await
                .map(|_| ()),
            "wrong CA",
        );
        assert!(message.contains("UnknownIssuer"), "{message}");

        let message = assert_tls_error(
            bolt(format!("bolt+s://127.0.0.1:{port}?tls_ca_file={ca}"))
                .await
                .map(|_| ()),
            "wrong hostname",
        );
        assert!(message.contains("not valid for name"), "{message}");

        // The server name override fixes the mismatch.
        bolt(format!(
            "bolt+s://127.0.0.1:{port}?tls_ca_file={ca}&tls_server_name=localhost"
        ))
        .await
        .expect("server name override");

        // A ConnectionConfig option wins over the URI parameter.
        let config =
            ConnectionConfig::from_uri(format!("bolt+s://localhost:{port}?tls_ca_file={other}"))
                .with_tls_options(TlsOptions::new().with_ca_file(&pki.ca_file));
        BoltConnection::connect(&config)
            .await
            .expect("config CA overrides the URI CA");

        // Without a CA file the system roots are used, which do not include the test CA.
        assert_tls_error(
            bolt(format!("bolt+s://localhost:{port}")).await.map(|_| ()),
            "system roots",
        );
    }

    #[tokio::test]
    async fn bolt_ssc_accepts_any_certificate() {
        let pki = pki();
        let port = tls_server(acceptor(&pki, false), Proto::Bolt).await;
        for uri in [
            format!("bolt+ssc://127.0.0.1:{port}"),
            format!("neo4j+ssc://127.0.0.1:{port}"),
            format!("bolt://127.0.0.1:{port}?tls=insecure"),
        ] {
            let conn = bolt(uri.clone())
                .await
                .unwrap_or_else(|e| panic!("{uri}: {e}"));
            assert!(conn.is_tls(), "{uri}");
        }
    }

    #[tokio::test]
    async fn bolt_mutual_tls() {
        let pki = pki();
        let port = tls_server(acceptor(&pki, true), Proto::Bolt).await;
        let ca = url_path(&pki.ca_file);
        let cert = url_path(&pki.client_cert_file);
        let key = url_path(&pki.client_key_file);

        let mut conn = bolt(format!(
            "bolt+s://localhost:{port}?tls_ca_file={ca}&tls_cert_file={cert}&tls_key_file={key}"
        ))
        .await
        .expect("client certificate accepted");
        conn.ping().await.unwrap();

        // Without a client certificate the server aborts the session (in TLS 1.3 the alert
        // arrives after the client finished its side of the handshake).
        assert!(
            bolt(format!("bolt+s://localhost:{port}?tls_ca_file={ca}"))
                .await
                .is_err()
        );

        // A certificate without its key is a configuration error.
        let message = assert_tls_error(
            bolt(format!(
                "bolt+s://localhost:{port}?tls_ca_file={ca}&tls_cert_file={cert}"
            ))
            .await
            .map(|_| ()),
            "certificate without key",
        );
        assert!(message.contains("private key"), "{message}");
    }

    // ---------------------------------------------------------------------------
    // Redis / FalkorDB
    // ---------------------------------------------------------------------------

    async fn redis(uri: String) -> Result<RedisConnection, NetError> {
        RedisConnection::connect(&ParsedUri::parse(&uri)?).await
    }

    #[tokio::test]
    async fn redis_tls_schemes() {
        let pki = pki();
        let port = tls_server(acceptor(&pki, false), Proto::Redis).await;
        let ca = url_path(&pki.ca_file);
        for scheme in ["rediss", "falkordbs", "valkeys"] {
            let mut conn = redis(format!("{scheme}://localhost:{port}?tls_ca_file={ca}"))
                .await
                .unwrap_or_else(|e| panic!("{scheme}: {e}"));
            assert!(conn.is_tls() && conn.is_resp3(), "{scheme}");
            conn.ping().await.unwrap();
        }
        let other = url_path(&pki.other_ca_file);
        assert_tls_error(
            redis(format!("rediss://localhost:{port}?tls_ca_file={other}"))
                .await
                .map(|_| ()),
            "wrong CA",
        );
        assert_tls_error(
            redis(format!("rediss://127.0.0.1:{port}?tls_ca_file={ca}"))
                .await
                .map(|_| ()),
            "wrong hostname",
        );
        assert!(
            redis(format!("redis://127.0.0.1:{port}?tls=insecure"))
                .await
                .unwrap()
                .is_tls()
        );
    }

    // ---------------------------------------------------------------------------
    // PostgreSQL sslmode
    // ---------------------------------------------------------------------------

    async fn pg(port: u16, query: &str) -> Result<PostgresConnection, NetError> {
        pg_host("localhost", port, query).await
    }

    async fn pg_host(host: &str, port: u16, query: &str) -> Result<PostgresConnection, NetError> {
        let uri = format!("postgresql://postgres:pw@{host}:{port}/db?connect_timeout=5&{query}");
        PostgresConnection::connect(&ParsedUri::parse(&uri)?).await
    }

    #[tokio::test]
    async fn pg_server_refuses_ssl_only_prefer_falls_back() {
        let pki = pki();
        let (port, events) = pg_server(acceptor(&pki, false), b'N', false).await;

        let conn = pg(port, "sslmode=prefer").await.expect("prefer falls back");
        assert!(!conn.is_tls());
        assert_eq!(
            *events.lock(),
            [PgEvent::SslRequest, PgEvent::PlainStartup],
            "the fallback continues on the same connection"
        );

        let ca = url_path(&pki.ca_file);
        for query in [
            "sslmode=require".to_string(),
            "sslmode=verify-ca".to_string(),
            format!("sslmode=verify-full&sslrootcert={ca}"),
            "ssl=true".to_string(),
        ] {
            events.lock().clear();
            let message = assert_tls_error(pg(port, &query).await.map(|_| ()), &query);
            assert!(message.contains("'N'"), "{query}: {message}");
            assert_eq!(
                *events.lock(),
                [PgEvent::SslRequest],
                "{query}: no startup sent"
            );
        }
    }

    #[tokio::test]
    async fn pg_sslmode_verification_matches_libpq() {
        let pki = pki();
        let (port, events) = pg_server(acceptor(&pki, false), b'S', false).await;
        let ca = url_path(&pki.ca_file);
        let other = url_path(&pki.other_ca_file);

        // Connects over TLS: (host, query).
        let ok = [
            ("localhost", format!("sslmode=verify-full&sslrootcert={ca}")),
            ("127.0.0.1", format!("sslmode=verify-ca&sslrootcert={ca}")),
            // require without a root certificate does not verify
            ("127.0.0.1", "sslmode=require".to_string()),
            // require with a root certificate behaves like verify-ca
            ("127.0.0.1", format!("sslmode=require&sslrootcert={ca}")),
            ("127.0.0.1", "sslmode=prefer".to_string()),
            ("127.0.0.1", format!("sslmode=prefer&sslrootcert={ca}")),
            (
                "127.0.0.1",
                format!("sslmode=verify-full&sslrootcert={ca}&tls_server_name=localhost"),
            ),
        ];
        for (host, query) in &ok {
            let conn = pg_host(host, port, query)
                .await
                .unwrap_or_else(|e| panic!("{host} {query}: {e}"));
            assert!(conn.is_tls(), "{host} {query}");
        }
        let mut postgres_plus_s = PostgresConnection::connect(
            &ParsedUri::parse(&format!(
                "postgresql+s://postgres:pw@localhost:{port}/db?sslrootcert={ca}"
            ))
            .unwrap(),
        )
        .await
        .expect("postgresql+s is verify-full");
        assert!(postgres_plus_s.is_tls());
        postgres_plus_s.close().await.unwrap();

        // Fails, and never retries in plaintext: (host, query).
        let fail = [
            ("127.0.0.1", format!("sslmode=verify-full&sslrootcert={ca}")),
            (
                "localhost",
                format!("sslmode=verify-ca&sslrootcert={other}"),
            ),
            ("localhost", format!("sslmode=require&sslrootcert={other}")),
            ("localhost", format!("sslmode=prefer&sslrootcert={other}")),
            // verify-* without a root certificate uses the system roots
            ("localhost", "sslmode=verify-full".to_string()),
        ];
        for (host, query) in &fail {
            events.lock().clear();
            assert_tls_error(
                pg_host(host, port, query).await.map(|_| ()),
                &format!("{host} {query}"),
            );
            assert!(
                !events.lock().contains(&PgEvent::PlainStartup),
                "{host} {query}: fell back to plaintext"
            );
        }
    }

    #[tokio::test]
    async fn pg_disable_and_allow() {
        let pki = pki();
        // A server that rejects plaintext startups (like a hostssl-only pg_hba.conf).
        let (port, events) = pg_server(acceptor(&pki, false), b'S', true).await;

        assert!(matches!(
            pg(port, "sslmode=disable").await,
            Err(NetError::AuthenticationFailed(_))
        ));
        assert_eq!(*events.lock(), [PgEvent::PlainStartup]);

        events.lock().clear();
        let conn = pg(port, "sslmode=allow")
            .await
            .expect("allow retries with TLS");
        assert!(conn.is_tls());
        assert_eq!(
            *events.lock(),
            [
                PgEvent::PlainStartup,
                PgEvent::SslRequest,
                PgEvent::TlsStartup
            ]
        );
    }

    #[tokio::test]
    async fn pg_mutual_tls_and_connect_any() {
        let pki = pki();
        let (port, events) = pg_server(acceptor(&pki, true), b'S', false).await;
        let ca = url_path(&pki.ca_file);
        let cert = url_path(&pki.client_cert_file);
        let key = url_path(&pki.client_key_file);

        let mut conn = connect_any(&format!(
            "postgresql://postgres:pw@localhost:{port}/db?sslmode=verify-full&sslrootcert={ca}&sslcert={cert}&sslkey={key}"
        ))
        .await
        .expect("client certificate accepted");
        conn.close().await.unwrap();
        assert!(events.lock().contains(&PgEvent::TlsStartup));

        events.lock().clear();
        assert!(
            pg(port, &format!("sslmode=verify-full&sslrootcert={ca}"))
                .await
                .is_err(),
            "the server requires a client certificate"
        );
        assert!(!events.lock().contains(&PgEvent::PlainStartup));
    }

    // ---------------------------------------------------------------------------
    // Audit regressions
    // ---------------------------------------------------------------------------

    /// `tls_ca_file=system` behaves like `sslrootcert=system`: it implies verify-full (an
    /// SSLRequest, then a certificate check against the system roots, which do not hold the
    /// test CA), and a weaker sslmode is rejected before connecting.
    #[tokio::test]
    async fn tls_ca_file_system_implies_verify_full() {
        let pki = pki();
        let (port, events) = pg_server(acceptor(&pki, false), b'S', false).await;
        for query in ["tls_ca_file=system", "sslrootcert=system"] {
            events.lock().clear();
            let message = assert_tls_error(pg(port, query).await.map(|_| ()), query);
            assert!(message.contains("UnknownIssuer"), "{query}: {message}");
            let seen = events.lock().clone();
            assert!(seen.contains(&PgEvent::SslRequest), "{query}: {seen:?}");
            assert!(!seen.contains(&PgEvent::PlainStartup), "{query}: {seen:?}");
        }
        for query in [
            "tls_ca_file=system&sslmode=require",
            "tls_ca_file=system&sslmode=verify-ca",
            "TLS_CA_FILE=system&sslmode=prefer",
        ] {
            events.lock().clear();
            assert!(
                matches!(pg(port, query).await, Err(NetError::InvalidUri(_))),
                "{query}"
            );
            assert!(events.lock().is_empty(), "{query}: connected");
        }
    }

    /// Bytes the server (or a man-in-the-middle) sends right after the `S` / `N` reply are
    /// rejected instead of being read as part of the TLS handshake or the session.
    #[tokio::test]
    async fn pg_data_after_the_ssl_reply_is_rejected() {
        let pki = pki();
        let injected: &'static [u8] = &[0x16, 0x03, 0x03, 0x00, 0x05, 1, 2, 3, 4, 5];
        for (reply, query) in [(b'S', "sslmode=require"), (b'N', "sslmode=prefer")] {
            let (port, events) =
                pg_server_with(acceptor(&pki, false), reply, injected, Plaintext::Accept).await;
            match pg(port, query).await {
                Err(NetError::ProtocolError(message)) => {
                    assert!(message.contains("unexpected data"), "{query}: {message}")
                }
                Err(other) => panic!("{query}: expected a protocol error, got {other:?}"),
                Ok(_) => panic!("{query}: connected"),
            }
            assert_eq!(*events.lock(), [PgEvent::SslRequest], "{query}");
        }
        // A clean `S` still works.
        let (port, _) = pg_server_with(acceptor(&pki, false), b'S', &[], Plaintext::Accept).await;
        assert!(pg(port, "sslmode=require").await.unwrap().is_tls());
    }

    /// `sslmode=allow` retries with TLS after any failure of the plaintext attempt, including
    /// a server that closes plaintext connections without a reply (libpq behaviour).
    #[tokio::test]
    async fn pg_allow_retries_tls_after_any_plaintext_failure() {
        let pki = pki();
        let (port, events) =
            pg_server_with(acceptor(&pki, false), b'S', &[], Plaintext::Close).await;
        let conn = pg(port, "sslmode=allow")
            .await
            .expect("allow retries with TLS");
        assert!(conn.is_tls());
        assert_eq!(
            *events.lock(),
            [
                PgEvent::PlainStartup,
                PgEvent::SslRequest,
                PgEvent::TlsStartup
            ]
        );

        // When the TLS retry fails too, the error names both attempts.
        let (port, _) = pg_server_with(acceptor(&pki, false), b'N', &[], Plaintext::Close).await;
        let message = assert_tls_error(pg(port, "sslmode=allow").await.map(|_| ()), "allow");
        assert!(message.contains("plaintext attempt failed"), "{message}");
    }

    /// A server certificate whose keyUsage omits digitalSignature is rejected by the
    /// verifying modes (rustls-webpki does not check the leaf keyUsage bits itself).
    #[tokio::test]
    async fn server_certificate_key_usage_needs_digital_signature() {
        let bad = pki_with_server_key_usage(vec![KeyUsagePurpose::KeyEncipherment]);
        let port = tls_server(acceptor(&bad, false), Proto::Bolt).await;
        let ca = url_path(&bad.ca_file);
        for uri in [
            format!("bolt+s://localhost:{port}?tls_ca_file={ca}"),
            format!("neo4j+s://localhost:{port}?sslmode=verify-full&tls_ca_file={ca}"),
        ] {
            let message = assert_tls_error(bolt(uri.clone()).await.map(|_| ()), &uri);
            assert!(message.contains("digitalSignature"), "{uri}: {message}");
        }
        let (pg_port, _) = pg_server(acceptor(&bad, false), b'S', false).await;
        let message = assert_tls_error(
            pg_host(
                "127.0.0.1",
                pg_port,
                &format!("sslmode=verify-ca&sslrootcert={ca}"),
            )
            .await
            .map(|_| ()),
            "pg verify-ca",
        );
        assert!(message.contains("digitalSignature"), "{message}");
        // `+ssc` does not check the certificate at all.
        assert!(
            bolt(format!("bolt+ssc://127.0.0.1:{port}"))
                .await
                .unwrap()
                .is_tls()
        );

        let good = pki_with_server_key_usage(vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ]);
        let port = tls_server(acceptor(&good, false), Proto::Bolt).await;
        let ca = url_path(&good.ca_file);
        bolt(format!("bolt+s://localhost:{port}?tls_ca_file={ca}"))
            .await
            .expect("digitalSignature allowed");
    }

    /// A Redis server that closes the connection (or does not speak RESP) is not reported as
    /// a working connection; a server without RESP3 (HELLO answered with an error) still is.
    #[tokio::test]
    async fn redis_handshake_needs_a_server_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let mut buf = [0u8; 256];
                let _ = tcp.read(&mut buf).await;
                // drop: close without a reply
            }
        });
        let error = redis(format!("redis://127.0.0.1:{port}?connect_timeout=3"))
            .await
            .err()
            .expect("a silent server is not a connection");
        assert!(
            matches!(error, NetError::ConnectionFailed(ref m) if m.contains("did not answer HELLO")),
            "{error:?}"
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 256];
                    let mut first = true;
                    while tcp.read(&mut buf).await.unwrap_or(0) > 0 {
                        let reply: &[u8] = if first {
                            b"-ERR unknown command 'HELLO'\r\n"
                        } else {
                            b"+PONG\r\n"
                        };
                        first = false;
                        if tcp.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let mut conn = redis(format!("redis://127.0.0.1:{port}"))
            .await
            .expect("RESP2 server");
        assert!(!conn.is_resp3());
        conn.ping().await.unwrap();
    }

    /// `ConnectionConfig::with_tls(SelfSigned)` cannot weaken a verified `+s` URI.
    #[tokio::test]
    async fn config_cannot_weaken_a_verified_bolt_uri() {
        let config = ConnectionConfig::from_uri("bolt+s://localhost:1")
            .with_tls(voyager_drivers::TlsMode::SelfSigned);
        let message = assert_tls_error(
            BoltConnection::connect(&config).await.map(|_| ()),
            "with_tls(SelfSigned)",
        );
        assert!(message.contains("weaken"), "{message}");
    }

    // ---------------------------------------------------------------------------
    // PostgreSQL sslnegotiation=direct
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn pg_direct_tls_skips_the_ssl_request() {
        let pki = pki();
        let ca = url_path(&pki.ca_file);
        let (port, events) =
            pg_server_with(acceptor_with_alpn(&pki, true), b'S', &[], Plaintext::Reject).await;
        for query in [
            "sslmode=require&sslnegotiation=direct".to_string(),
            format!("sslmode=verify-full&sslrootcert={ca}&sslnegotiation=direct"),
            format!("sslmode=verify-ca&sslrootcert={ca}&SSLNEGOTIATION=direct"),
        ] {
            events.lock().clear();
            let mut conn = pg(port, &query)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            assert!(conn.is_tls(), "{query}");
            conn.close().await.unwrap();
            assert_eq!(
                *events.lock(),
                [PgEvent::DirectTls, PgEvent::TlsStartup],
                "{query}: no SSLRequest"
            );
        }
        // The default negotiation still sends an SSLRequest.
        events.lock().clear();
        pg(port, "sslmode=require&sslnegotiation=postgres")
            .await
            .unwrap();
        assert_eq!(events.lock().first(), Some(&PgEvent::SslRequest));

        // Certificate checks apply as usual.
        let other = url_path(&pki.other_ca_file);
        assert_tls_error(
            pg(
                port,
                &format!("sslmode=verify-full&sslrootcert={other}&sslnegotiation=direct"),
            )
            .await
            .map(|_| ()),
            "direct, wrong CA",
        );
    }

    #[tokio::test]
    async fn pg_direct_tls_first_bytes_are_a_client_hello() {
        let (bytes, _) = super::first_bytes_sent(|port| {
            format!(
                "postgresql://postgres@127.0.0.1:{port}/db?sslmode=require&sslnegotiation=direct&connect_timeout=3"
            )
        })
        .await;
        assert_eq!(bytes.first(), Some(&0x16), "sent {bytes:02x?}");
    }

    #[tokio::test]
    async fn pg_direct_tls_requires_alpn() {
        let pki = pki();
        let (port, _) = pg_server_with(
            acceptor_with_alpn(&pki, false),
            b'S',
            &[],
            Plaintext::Reject,
        )
        .await;
        let message = assert_tls_error(
            pg(port, "sslmode=require&sslnegotiation=direct")
                .await
                .map(|_| ()),
            "no ALPN",
        );
        assert!(message.contains("ALPN"), "{message}");
    }

    /// A server without direct TLS support (PostgreSQL 16 and older) reads the ClientHello
    /// as a startup packet and answers with an ErrorResponse; the client fails cleanly.
    #[tokio::test]
    async fn pg_direct_tls_against_an_old_server_fails_cleanly() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = tcp.read(&mut buf).await;
                let _ = tcp
                    .write_all(&pg_frame(
                        b'E',
                        b"SFATAL\0C08P01\0Minvalid length of startup packet\0\0",
                    ))
                    .await;
            }
        });
        let started = std::time::Instant::now();
        let message = assert_tls_error(
            pg(port, "sslmode=require&sslnegotiation=direct")
                .await
                .map(|_| ()),
            "old server",
        );
        assert!(message.contains("PostgreSQL 17"), "{message}");
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
    }

    #[tokio::test]
    async fn bolt_tls_local_liveness_evicts_server_closed_socket() {
        use voyager_drivers::config::{ConnectionConfig, PoolConfig};
        use voyager_drivers::engine::{AsyncConnection, ConnectionFactory};
        use voyager_drivers::pool::ConnectionPool;

        let pki = pki();
        let acceptor = acceptor(&pki, false);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let closed_first = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed_srv = closed_first.clone();

        tokio::spawn(async move {
            let mut count = 0usize;
            while let Ok((tcp, _)) = listener.accept().await {
                count += 1;
                let acc = acceptor.clone();
                let closed_flag = closed_srv.clone();
                tokio::spawn(async move {
                    let Ok(mut tls) = acc.accept(tcp).await else {
                        return;
                    };
                    let mut hs = [0u8; 20];
                    if tls.read_exact(&mut hs).await.is_err() {
                        return;
                    }
                    let _ = tls.write_all(&[0, 0, 0, 5]).await;
                    let _ = tls.flush().await;
                    let _ = read_bolt_message(&mut tls).await;
                    let _ = tls
                        .write_all(&[0x00, 0x03, 0xB1, 0x70, 0xA0, 0x00, 0x00])
                        .await;
                    let _ = tls.flush().await;
                    if count == 1 {
                        // Gracefully shut down TLS (close_notify + TCP FIN) while idle
                        let _ = tls.shutdown().await;
                        closed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    } else {
                        // Keep second connection alive for ping/RESET
                        while read_bolt_message(&mut tls).await.is_ok() {
                            let _ = tls
                                .write_all(&[0x00, 0x03, 0xB1, 0x70, 0xA0, 0x00, 0x00])
                                .await;
                            let _ = tls.flush().await;
                        }
                    }
                });
            }
        });

        struct TlsBoltFactory(ConnectionConfig);
        #[async_trait::async_trait]
        impl ConnectionFactory<BoltConnection> for TlsBoltFactory {
            async fn create(&self) -> voyager_drivers::Result<BoltConnection> {
                BoltConnection::connect(&self.0).await
            }
        }

        let ca = url_path(&pki.ca_file);
        let cfg = ConnectionConfig::from_uri(format!("bolt+s://localhost:{port}?tls_ca_file={ca}"));
        let pool = ConnectionPool::new(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_test_on_acquire(false)
                .with_test_on_idle_after(None),
            std::sync::Arc::new(TlsBoltFactory(cfg)),
        );

        {
            let c1 = pool.acquire().await.expect("first TLS conn");
            assert!(c1.is_tls());
        }
        for _ in 0..50 {
            if closed_first.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut c2 = pool
            .acquire()
            .await
            .expect("second TLS conn after eviction");
        c2.ping().await.expect("second TLS conn should be healthy");
        assert_eq!(pool.metrics().connections_failed_health_check, 1);
        assert_eq!(pool.metrics().connections_created, 2);
    }
}
