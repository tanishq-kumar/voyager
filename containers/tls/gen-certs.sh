#!/usr/bin/env bash
# Generates a throwaway "Voyager test CA", one server certificate for the
# TLS-enabled test databases (containers/compose.tls.yaml) and one client
# certificate for the mutual-TLS tests. Test use only.
#
# Usage: containers/tls/gen-certs.sh [extra SAN ...]
#   Extra SANs are added to the server certificate, for example the address of
#   a remote machine that runs the stack: `gen-certs.sh IP:192.168.1.12 DNS:dbhost`.
#   localhost, 127.0.0.1 and ::1 are always included.
#
# Output (in containers/tls/certs/, git-ignored):
#   ca.crt, ca.key          the test CA (clients trust ca.crt)
#   server.crt, server.key  the server certificate used by every TLS service
#   client.crt, client.key  client certificate (CN voyager-test-client) for mutual TLS
#   neo4j/bolt/             Neo4j SSL-policy layout (private.key, public.crt, trusted/, revoked/)
set -euo pipefail
cd "$(dirname "$0")"
out=certs
mkdir -p "$out"

san="DNS:localhost,IP:127.0.0.1,IP:::1"
for extra in "$@"; do san="$san,$extra"; done

if [ ! -f "$out/ca.key" ]; then
  openssl req -x509 -newkey rsa:3072 -nodes -days 3650 -sha256 \
    -subj "/CN=Voyager test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$out/ca.key" -out "$out/ca.crt" 2>/dev/null
fi

openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=voyager-test-db" \
  -keyout "$out/server.key" -out "$out/server.csr" 2>/dev/null
openssl x509 -req -in "$out/server.csr" -CA "$out/ca.crt" -CAkey "$out/ca.key" \
  -CAcreateserial -days 825 -sha256 -out "$out/server.crt" \
  -extfile <(printf 'subjectAltName=%s\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' "$san") 2>/dev/null
rm -f "$out/server.csr"

openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=voyager-test-client" \
  -keyout "$out/client.key" -out "$out/client.csr" 2>/dev/null
openssl x509 -req -in "$out/client.csr" -CA "$out/ca.crt" -CAkey "$out/ca.key" \
  -CAcreateserial -days 825 -sha256 -out "$out/client.crt" \
  -extfile <(printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n') 2>/dev/null
rm -f "$out/client.csr"

# Neo4j expects a policy directory with private.key, public.crt and trusted/ + revoked/.
mkdir -p "$out/neo4j/bolt/trusted" "$out/neo4j/bolt/revoked"
cp "$out/server.key" "$out/neo4j/bolt/private.key"
cp "$out/server.crt" "$out/neo4j/bolt/public.crt"
cp "$out/ca.crt" "$out/neo4j/bolt/trusted/ca.crt"

# Containers run the servers as non-root users; keep the files readable for them.
chmod 644 "$out"/server.key "$out"/neo4j/bolt/private.key
chmod 600 "$out/ca.key" "$out/client.key"
echo "server certificate SANs: $san"
openssl x509 -in "$out/server.crt" -noout -subject -issuer -enddate
openssl x509 -in "$out/client.crt" -noout -subject -enddate
