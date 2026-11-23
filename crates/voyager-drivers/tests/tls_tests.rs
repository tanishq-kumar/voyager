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
        let config = builder
            .with_single_cert(pki.server_chain.clone(), key)
            .unwrap();
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
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
    }

    /// A PostgreSQL stub. `ssl_reply` answers an SSLRequest (`S` or `N`); `reject_plaintext`
    /// refuses startups that arrive without TLS.
    async fn pg_server(
        acceptor: tokio_rustls::TlsAcceptor,
        ssl_reply: u8,
        reject_plaintext: bool,
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
                    if head == [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F] {
                        tcp.read_exact(&mut head).await.unwrap();
                        seen.lock().push(PgEvent::SslRequest);
                        tcp.write_all(&[ssl_reply]).await.unwrap();
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
                    let _ = serve_pg_startup(tcp, !reject_plaintext).await;
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
}
