#!/usr/bin/env bash
set -euo pipefail

output=${1:?Specify a temporary certificate output directory}
umask 077
mkdir -p "$output/testca" "$output/server"
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
  -config "$(dirname "$0")/ca-cert.cnf" \
  -keyout "$output/ca-key.pem" -out "$output/testca/cacert.pem"
openssl req -new -newkey rsa:2048 -nodes -sha256 \
  -subj '/CN=localhost' \
  -keyout "$output/server/key.pem" -out "$output/server/request.pem"
openssl x509 -req -sha256 -days 2 \
  -in "$output/server/request.pem" \
  -CA "$output/testca/cacert.pem" -CAkey "$output/ca-key.pem" \
  -CAcreateserial -extfile "$(dirname "$0")/server-cert.cnf" -out "$output/server/cert.pem"
chmod 755 "$output" "$output/testca" "$output/server"
chmod 644 "$output/testca/cacert.pem" "$output/server/cert.pem" "$output/server/key.pem"
openssl verify -CAfile "$output/testca/cacert.pem" "$output/server/cert.pem"