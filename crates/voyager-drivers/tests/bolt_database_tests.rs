//! Unit tests for the `db` field in encoded Bolt `RUN` and `BEGIN` messages (#396).
//!
//! A capture server records the decoded request structures, so the assertions are
//! about the bytes the client actually wrote, not about an intermediate helper.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use voyager_drivers::Result;
use voyager_drivers::bolt::{
    BoltConnection, BoltValue, PackStream, encode_chunks, read_message_frame, write_message_frame,
};
use voyager_drivers::config::ConnectionConfig;
use voyager_drivers::engine::AsyncConnection;

const RUN: u8 = 0x10;
const BEGIN: u8 = 0x11;
const HELLO: u8 = 0x01;
const LOGON: u8 = 0x6A;

#[derive(Clone, Debug)]
struct Captured {
    tag: u8,
    fields: Vec<BoltValue>,
}

struct CaptureServer {
    addr: SocketAddr,
    handshake: Arc<Mutex<Option<[u8; 20]>>>,
    requests: Arc<Mutex<Vec<Captured>>>,
    task: JoinHandle<()>,
}

impl CaptureServer {
    async fn start(negotiated_version: [u8; 4]) -> Self {
        Self::start_with_patches(negotiated_version, Vec::new()).await
    }

    async fn start_with_patches(negotiated_version: [u8; 4], hello_patches: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind capture server");
        let addr = listener.local_addr().expect("capture server address");
        let handshake: Arc<Mutex<Option<[u8; 20]>>> = Arc::new(Mutex::new(None));
        let requests: Arc<Mutex<Vec<Captured>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded_hs = Arc::clone(&handshake);
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let _ = Self::handle(
                    &mut socket,
                    negotiated_version,
                    hello_patches,
                    &recorded_hs,
                    &recorded,
                )
                .await;
            }
        });
        Self {
            addr,
            handshake,
            requests,
            task,
        }
    }

    fn uri(&self, database: Option<&str>) -> String {
        match database {
            Some(db) => format!("bolt://{}/{}", self.addr, db),
            None => format!("bolt://{}", self.addr),
        }
    }

    fn captured(&self) -> Vec<Captured> {
        self.requests.lock().expect("capture lock").clone()
    }

    fn extra(&self, tag: u8) -> HashMap<String, BoltValue> {
        let requests = self.captured();
        let request = requests
            .iter()
            .find(|request| request.tag == tag)
            .unwrap_or_else(|| panic!("no request with tag 0x{tag:02X}: {requests:?}"));
        let extra_index = match tag {
            RUN => 2,
            _ => 0,
        };
        request
            .fields
            .get(extra_index)
            .and_then(|value| match value {
                BoltValue::Map(map) => Some(map.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("request 0x{tag:02X} has no extra map: {request:?}"))
    }

    async fn handle(
        socket: &mut tokio::net::TcpStream,
        negotiated_version: [u8; 4],
        hello_patches: Vec<String>,
        handshake_out: &Arc<Mutex<Option<[u8; 20]>>>,
        requests: &Arc<Mutex<Vec<Captured>>>,
    ) -> Result<()> {
        let mut handshake = [0u8; 20];
        socket.read_exact(&mut handshake).await?;
        *handshake_out.lock().expect("handshake lock") = Some(handshake);
        socket.write_all(&negotiated_version).await?;
        socket.flush().await?;

        loop {
            let mut frame = match read_message_frame(socket).await {
                Ok(frame) => frame,
                Err(_) => break,
            };
            let decoded = match PackStream::decode(&mut frame) {
                Ok(BoltValue::Structure { tag, fields }) => Captured { tag, fields },
                _ => break,
            };
            let tag = decoded.tag;
            requests.lock().expect("capture lock").push(decoded);

            match tag {
                0x02 => break, // GOODBYE
                0x10 => {
                    let mut metadata = HashMap::new();
                    metadata.insert("fields".to_string(), BoltValue::List(Vec::new()));
                    Self::success(socket, metadata).await?;
                }
                0x3F => {
                    let mut metadata = HashMap::new();
                    metadata.insert("stats".to_string(), BoltValue::Map(HashMap::new()));
                    metadata.insert("t_last".to_string(), BoltValue::Integer(1));
                    Self::success(socket, metadata).await?;
                }
                0x01 => {
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "server".to_string(),
                        BoltValue::String("Neo4j/5.26.0".to_string()),
                    );
                    if !hello_patches.is_empty() {
                        metadata.insert(
                            "patch_bolt".to_string(),
                            BoltValue::List(
                                hello_patches
                                    .iter()
                                    .cloned()
                                    .map(BoltValue::String)
                                    .collect(),
                            ),
                        );
                    }
                    Self::success(socket, metadata).await?;
                }
                _ => Self::success(socket, HashMap::new()).await?,
            }
        }
        Ok(())
    }

    async fn success(
        socket: &mut tokio::net::TcpStream,
        metadata: HashMap<String, BoltValue>,
    ) -> Result<()> {
        let value = BoltValue::Structure {
            tag: 0x70,
            fields: vec![BoltValue::Map(metadata)],
        };
        let mut payload = BytesMut::new();
        PackStream::encode(&value, &mut payload);
        let mut chunked = BytesMut::new();
        encode_chunks(&payload, &mut chunked);
        write_message_frame(socket, &chunked).await
    }
}

impl Drop for CaptureServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn db_of(extra: &HashMap<String, BoltValue>) -> Option<&str> {
    extra.get("db").and_then(BoltValue::as_str)
}

#[tokio::test]
async fn bolt_5_4_run_and_begin_carry_the_uri_database() {
    let server = CaptureServer::start([0x00, 0x00, 0x04, 0x05]).await;
    let config = ConnectionConfig::from_uri(server.uri(Some("zz_uri_db")));
    let mut conn = BoltConnection::connect(&config).await.expect("connect");

    assert_eq!(conn.database(), Some("zz_uri_db"));

    conn.execute("RETURN 1 AS n", &HashMap::new())
        .await
        .expect("auto-commit query");
    assert_eq!(db_of(&server.extra(RUN)), Some("zz_uri_db"));

    // Bolt 5.1+ names the database per request, not in HELLO.
    assert_eq!(db_of(&server.extra(HELLO)), None);

    conn.begin_transaction().await.expect("begin transaction");
    assert_eq!(db_of(&server.extra(BEGIN)), Some("zz_uri_db"));
    conn.rollback_transaction().await.expect("rollback");
}

#[tokio::test]
async fn bolt_5_4_run_inside_a_transaction_omits_the_database() {
    let server = CaptureServer::start([0x00, 0x00, 0x04, 0x05]).await;
    let config = ConnectionConfig::from_uri(server.uri(Some("zz_tx_db")));
    let mut conn = BoltConnection::connect(&config).await.expect("connect");

    conn.begin_transaction().await.expect("begin transaction");
    conn.execute("RETURN 1 AS n", &HashMap::new())
        .await
        .expect("transaction query");
    assert_eq!(db_of(&server.extra(BEGIN)), Some("zz_tx_db"));
    assert_eq!(
        db_of(&server.extra(RUN)),
        None,
        "RUN inside an explicit transaction must not repeat db"
    );
    conn.rollback_transaction().await.expect("rollback");
}

#[tokio::test]
async fn no_configured_database_sends_no_db_field() {
    let server = CaptureServer::start([0x00, 0x00, 0x04, 0x05]).await;
    let config = ConnectionConfig::from_uri(server.uri(None));
    let mut conn = BoltConnection::connect(&config).await.expect("connect");

    assert_eq!(conn.database(), None);
    conn.execute("RETURN 1 AS n", &HashMap::new())
        .await
        .expect("auto-commit query");
    conn.begin_transaction().await.expect("begin transaction");
    conn.rollback_transaction().await.expect("rollback");

    assert_eq!(db_of(&server.extra(HELLO)), None);
    assert_eq!(db_of(&server.extra(RUN)), None);
    assert_eq!(db_of(&server.extra(BEGIN)), None);
}

#[tokio::test]
async fn bolt_4_2_keeps_db_in_hello_and_adds_it_to_run() {
    let server = CaptureServer::start([0x00, 0x00, 0x02, 0x04]).await;
    let config = ConnectionConfig::from_uri(server.uri(Some("zz_legacy_db")));
    let mut conn = BoltConnection::connect(&config).await.expect("connect");

    conn.execute("RETURN 1 AS n", &HashMap::new())
        .await
        .expect("auto-commit query");
    conn.begin_transaction().await.expect("begin transaction");
    conn.rollback_transaction().await.expect("rollback");

    assert_eq!(db_of(&server.extra(HELLO)), Some("zz_legacy_db"));
    assert_eq!(db_of(&server.extra(RUN)), Some("zz_legacy_db"));
    assert_eq!(db_of(&server.extra(BEGIN)), Some("zz_legacy_db"));
}

#[tokio::test]
async fn bolt_handshake_proposes_ranges_and_handles_v52_v54_v44() {
    use voyager_drivers::bolt::{BOLT_MAGIC_PREAMBLE, BOLT_PROPOSED_VERSIONS, BoltVersion};
    use voyager_drivers::params::{ParamTimezone, ParamValue};

    // 1. Handshake proposes 5.4..=5.0 and 4.4..=4.2
    let server_52 = CaptureServer::start([0x00, 0x00, 0x02, 0x05]).await;
    let config_52 = ConnectionConfig::from_uri(server_52.uri(None));
    let mut conn_52 = BoltConnection::connect(&config_52)
        .await
        .expect("connect 5.2");
    assert_eq!(conn_52.version(), BoltVersion { major: 5, minor: 2 });
    assert!(conn_52.uses_utc_datetimes());
    assert_eq!(conn_52.protocol_version().as_deref(), Some("5.2"));

    let hs = server_52
        .handshake
        .lock()
        .expect("handshake")
        .expect("recorded handshake");
    assert_eq!(&hs[..4], &BOLT_MAGIC_PREAMBLE);
    assert_eq!(&hs[4..20], &BOLT_PROPOSED_VERSIONS);
    assert_eq!(&BOLT_PROPOSED_VERSIONS[0..4], &[0x00, 0x04, 0x04, 0x05]);
    assert_eq!(&BOLT_PROPOSED_VERSIONS[4..8], &[0x00, 0x02, 0x04, 0x04]);

    // Bolt 5.2 HELLO omits bolt_agent and sends LOGON
    let hello_52 = server_52.extra(HELLO);
    assert!(hello_52.contains_key("user_agent"));
    assert!(!hello_52.contains_key("bolt_agent"));
    assert!(server_52.captured().iter().any(|c| c.tag == LOGON));

    // fold=1 Europe/Berlin datetime (2024-10-27T02:30 fold=1 = 1729992600Z) sends Bolt 5 UTC 0x69
    let mut params = HashMap::new();
    params.insert(
        "dt".to_string(),
        ParamValue::DateTime {
            utc_seconds: 1_729_992_600,
            nanosecond: 0,
            tz: ParamTimezone::ZoneId {
                id: "Europe/Berlin".to_string(),
                offset_seconds: 3600,
            },
        },
    );
    conn_52
        .execute("RETURN $dt AS dt", &params)
        .await
        .expect("execute 5.2");
    let run_52 = server_52
        .captured()
        .into_iter()
        .find(|c| c.tag == RUN)
        .expect("RUN");
    let sent_params = run_52.fields[1].as_map().expect("params map");
    let dtz = sent_params["dt"].as_datetime_zone_id().expect("UTC 0x69");
    assert!(!dtz.is_local);
    assert_eq!(dtz.seconds, 1_729_992_600);

    // 2. Bolt 5.4 HELLO includes bolt_agent and sends LOGON
    let server_54 = CaptureServer::start([0x00, 0x00, 0x04, 0x05]).await;
    let conn_54 = BoltConnection::connect(&ConnectionConfig::from_uri(server_54.uri(None)))
        .await
        .expect("connect 5.4");
    assert_eq!(conn_54.version(), BoltVersion { major: 5, minor: 4 });
    assert!(server_54.extra(HELLO).contains_key("bolt_agent"));
    assert!(server_54.captured().iter().any(|c| c.tag == LOGON));

    // 3. Bolt 4.4 without utc patch requests patch_bolt=["utc"], keeps legacy 0x66
    let server_44 = CaptureServer::start([0x00, 0x00, 0x04, 0x04]).await;
    let mut conn_44 = BoltConnection::connect(&ConnectionConfig::from_uri(server_44.uri(None)))
        .await
        .expect("connect 4.4");
    assert!(!conn_44.uses_utc_datetimes());
    let hello_44 = server_44.extra(HELLO);
    assert_eq!(
        hello_44.get("patch_bolt").and_then(BoltValue::as_list),
        Some([BoltValue::String("utc".to_string())].as_slice())
    );
    assert!(!server_44.captured().iter().any(|c| c.tag == LOGON));
    conn_44
        .execute("RETURN $dt AS dt", &params)
        .await
        .expect("execute 4.4");
    let run_44 = server_44
        .captured()
        .into_iter()
        .find(|c| c.tag == RUN)
        .expect("RUN 4.4");
    let sent_44 = run_44.fields[1].as_map().expect("params 4.4");
    let dtz_44 = sent_44["dt"].as_datetime_zone_id().expect("legacy 0x66");
    assert!(dtz_44.is_local);
    assert_eq!(dtz_44.seconds, 1_729_992_600 + 3_600);

    // 4. Bolt 4.4 WITH utc patch uses UTC 0x69
    let server_44_utc =
        CaptureServer::start_with_patches([0x00, 0x00, 0x04, 0x04], vec!["utc".to_string()]).await;
    let mut conn_44_utc =
        BoltConnection::connect(&ConnectionConfig::from_uri(server_44_utc.uri(None)))
            .await
            .expect("connect 4.4 utc");
    assert!(conn_44_utc.uses_utc_datetimes());
    conn_44_utc
        .execute("RETURN $dt AS dt", &params)
        .await
        .expect("execute 4.4 utc");
    let run_44_utc = server_44_utc
        .captured()
        .into_iter()
        .find(|c| c.tag == RUN)
        .expect("RUN 4.4 utc");
    let sent_44_utc = run_44_utc.fields[1].as_map().expect("params 4.4 utc");
    let dtz_44_utc = sent_44_utc["dt"].as_datetime_zone_id().expect("UTC 0x69");
    assert!(!dtz_44_utc.is_local);
    assert_eq!(dtz_44_utc.seconds, 1_729_992_600);
}
