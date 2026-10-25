#!/usr/bin/env bash
# Disposable local CA/leaf fixtures; never installs trust or edits host settings.
set -euo pipefail
ROOT=${1:?provide a private empty fixture directory}
mkdir -p "$ROOT"
[[ -z "$(ls -A "$ROOT")" ]] || { printf '%s\n' 'TLS fixture directory must be empty' >&2; exit 1; }
umask 077
for name in ca wrong-ca; do
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -sha256 \
    -subj "/CN=Hatchet disposable $name" \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -keyout "$ROOT/$name.key" -out "$ROOT/$name.pem" 2>"$ROOT/$name-openssl.log"
done
openssl req -newkey rsa:2048 -nodes -subj '/CN=localhost' \
  -keyout "$ROOT/server.key" -out "$ROOT/server.csr" 2>"$ROOT/server-openssl.log"
cat > "$ROOT/server.ext" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1
EXT
openssl x509 -req -in "$ROOT/server.csr" -CA "$ROOT/ca.pem" -CAkey "$ROOT/ca.key" \
  -CAcreateserial -days 1 -sha256 -extfile "$ROOT/server.ext" -out "$ROOT/server.pem" 2>>"$ROOT/server-openssl.log"
