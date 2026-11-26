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

#[derive(Clone, Debug)]
struct Captured {
    tag: u8,
    fields: Vec<BoltValue>,
}

struct CaptureServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Captured>>>,
    task: JoinHandle<()>,
}

impl CaptureServer {
    async fn start(negotiated_version: [u8; 4]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind capture server");
        let addr = listener.local_addr().expect("capture server address");
        let requests: Arc<Mutex<Vec<Captured>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let _ = Self::handle(&mut socket, negotiated_version, &recorded).await;
            }
        });
        Self {
            addr,
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
        requests: &Arc<Mutex<Vec<Captured>>>,
    ) -> Result<()> {
        let mut handshake = [0u8; 20];
        socket.read_exact(&mut handshake).await?;
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
