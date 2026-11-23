"""TLS through the native client (`voyager_ogm.NativeClient`), which builds its connections
from the URI: `bolt+s://`, `rediss://` and PostgreSQL `sslmode`, with `tls_ca_file` /
`sslrootcert` naming the CA.

The live tests need the TLS test stack (`containers/compose.tls.yaml`). They read the host
from `VOYAGER_TEST_HOST` (default 127.0.0.1) and the test CA from `VOYAGER_TEST_TLS_CA`
(default `containers/tls/certs/ca.crt`), and skip when either is unavailable.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest
from voyager_ogm import NativeClient

REPO_ROOT = Path(__file__).resolve().parents[3]
HOST = os.getenv("VOYAGER_TEST_HOST", "127.0.0.1")
CA_FILE = Path(os.getenv("VOYAGER_TEST_TLS_CA", REPO_ROOT / "containers/tls/certs/ca.crt"))


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=0.5):
            return True
    except OSError:
        return False


def _require(port: int) -> str:
    if not CA_FILE.is_file():
        pytest.skip(f"TLS test CA not found at {CA_FILE} (containers/tls/gen-certs.sh)")
    if not _reachable(port):
        pytest.skip(f"{HOST}:{port} is not reachable")
    return str(CA_FILE)


def test_tls_uri_is_accepted_without_connecting() -> None:
    # The pool connects lazily, so constructing a client for a TLS URI does not fail.
    client = NativeClient("bolt+s://localhost:1?tls_ca_file=/nonexistent.pem", min_idle=0)
    assert client.protocol == "Bolt"
    client.close()


@pytest.mark.live
@pytest.mark.parametrize(
    ("port", "uri", "query"),
    [
        (7687, "bolt+s://neo4j:voyagerpass123@{host}:7687?tls_ca_file={ca}", "RETURN 1 AS n"),
        (7690, "bolt+s://{host}:7690?tls_ca_file={ca}", "RETURN 1 AS n"),
        (
            5456,
            "postgresql://postgres:voyagerpass123@{host}:5456/voyager_graph"
            "?sslmode=verify-full&sslrootcert={ca}",
            "SELECT 1 AS n",
        ),
        # Ping only, so no FalkorDB graph is created.
        (6380, "rediss://{host}:6380?tls_ca_file={ca}", None),
    ],
    ids=["neo4j", "memgraph", "postgres19", "falkordb"],
)
def test_live_native_client_over_tls(port: int, uri: str, query: str | None) -> None:
    ca = _require(port)
    client = NativeClient(uri.format(host=HOST, ca=ca), min_idle=0, max_size=1)
    try:
        assert client.ping_sync()
        if query is not None:
            assert client.execute_sync(query).num_rows == 1
    finally:
        client.close()


@pytest.mark.live
def test_live_native_client_rejects_wrong_hostname() -> None:
    ca = _require(7687)
    client = NativeClient(
        f"bolt+s://neo4j:voyagerpass123@{HOST}:7687"
        f"?tls_ca_file={ca}&tls_server_name=wrong-name.voyager.invalid",
        min_idle=0,
        max_size=1,
    )
    try:
        with pytest.raises(Exception, match="(?i)tls"):
            client.execute_sync("RETURN 1 AS n")
    finally:
        client.close()
