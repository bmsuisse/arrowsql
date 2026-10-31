#!/usr/bin/env bash
# Give the podman SQL Server a certificate signed by a throwaway test CA, so
# tests/port/test_tls.py can check real chain and hostname validation.
# SQL Server's auto-generated certificate is X.509 v1, which rustls/webpki
# cannot validate at all, so without this only TrustServerCertificate=yes
# can be exercised.
#
#   scripts/setup_tls_cert.sh [container]     (default: mssql-rs)
#
# Writes the CA to <main checkout>/.tls-test/ca.pem (gitignored) and restarts
# the container. The server certificate is valid for "localhost" only, so
# connecting as 127.0.0.1 with TrustServerCertificate=no must fail on the name.
set -euo pipefail
container="${1:-mssql-rs}"
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
main="$(git -C "$root" worktree list --porcelain | awk 'NR==1{print $2}')"
dir="$main/.tls-test"
mkdir -p "$dir"
cd "$dir"

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=mssql-rs test CA" \
  -keyout ca.key -out ca.pem 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
  -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
  -extfile server.ext -out server.pem 2>/dev/null

podman exec "$container" mkdir -p /var/opt/mssql/certs
podman cp server.pem "$container":/var/opt/mssql/certs/server.pem
podman cp server.key "$container":/var/opt/mssql/certs/server.key
podman exec -u 0 "$container" chown mssql:mssql /var/opt/mssql/certs/server.pem /var/opt/mssql/certs/server.key
podman exec -u 0 "$container" chmod 600 /var/opt/mssql/certs/server.key
podman exec -u 0 "$container" /opt/mssql/bin/mssql-conf set network.tlscert /var/opt/mssql/certs/server.pem >/dev/null
podman exec -u 0 "$container" /opt/mssql/bin/mssql-conf set network.tlskey /var/opt/mssql/certs/server.key >/dev/null
podman restart "$container" >/dev/null
for _ in $(seq 1 60); do
  podman logs --since 2m "$container" 2>&1 | grep -q "ready for client" && break
  sleep 2
done
echo "CA: $dir/ca.pem"
