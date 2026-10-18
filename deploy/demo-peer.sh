#!/bin/bash
# Abre un peer con su línea de comandos interactiva (la misma que se usará en AWS), con TLS, en la red del compose
# Una terminal por peer: peer1, peer2 o peer3. Cada uno pide usuario y contraseña (alice/alice1234 o bob/bob1234)
#
# Antes hay que levantar solo los superpeers (los peers los abre este script, uno por terminal):
#   bash deploy/certs/gen-certs.sh
#   docker compose -f deploy/docker-compose.yml up -d --build sp1 sp2
#
#   bash deploy/demo-peer.sh peer1
#
# Los archivos para subir se dejan en deploy/demo-files/<peer>/ y los que se bajan caen ahí mismo
# (dentro del peer es la carpeta de trabajo: "put informe.txt" sube deploy/demo-files/peer1/informe.txt)
#
# Al salir del peer (exit) el contenedor se borra, pero sus chunks siguen en el volumen dfsha-<peer>-data

set -u
export MSYS_NO_PATHCONV=1 # Git Bash en Windows convierte rutas como /certs en rutas de Windows
cd "$(dirname "$0")/.."
PEER=${1:-}
case "$PEER" in peer1|peer2|peer3) ;; *) echo "uso: bash deploy/demo-peer.sh <peer1|peer2|peer3>"; exit 1;; esac

ROOT="$(pwd -W 2>/dev/null || pwd)" # Git Bash: ruta de Windows; Linux: la ruta normal
OUT="$ROOT/deploy/certs/out"
FILES="$ROOT/deploy/demo-files/$PEER"
mkdir -p "deploy/demo-files/$PEER"
[ -f "deploy/certs/out/$PEER.crt" ] || { echo "Faltan los certificados: bash deploy/certs/gen-certs.sh"; exit 1; }
docker network inspect dfsha_default > /dev/null 2>&1 || { echo "Los superpeers no están arriba: docker compose -f deploy/docker-compose.yml up -d --build sp1 sp2"; exit 1; }
docker image inspect dfsha-peer > /dev/null 2>&1 || docker build -q -f deploy/Dockerfile --build-arg CMD=peer -t dfsha-peer . > /dev/null

# mdf abre el editor de EDITOR; si no está definido, usa vi. Para probar sin teclado: EDITOR="sed -i s/a/b/"
EDITOR_ARG=(); [ -n "${EDITOR:-}" ] && EDITOR_ARG=(-e "EDITOR=$EDITOR")
TTY=""; [ -t 0 ] && TTY="-t" # con terminal real: -it; sin ella (por ejemplo, en una prueba con pipes) solo -i
docker run -i $TTY "${EDITOR_ARG[@]}" --rm --name "dfsha-$PEER" --hostname "$PEER" --network dfsha_default \
  -e SUPERPEER_URLS=https://sp1:8080,https://sp2:8080 -e PEER_ADDRESS="$PEER" -e PEER_PORT=9000 \
  -e STORAGE_PATH=/data/chunks -e HEARTBEAT_INTERVAL=5s \
  -e TLS_CERT=/certs/server.crt -e TLS_KEY=/certs/server.key -e TLS_CA=/certs/ca.crt -e ALLOW_INSECURE=false \
  -v "dfsha-$PEER-data:/data" -v "$FILES:/files" -w /files \
  -v "$OUT/$PEER.crt:/certs/server.crt:ro" -v "$OUT/$PEER.key:/certs/server.key:ro" -v "$OUT/ca.crt:/certs/ca.crt:ro" \
  dfsha-peer
