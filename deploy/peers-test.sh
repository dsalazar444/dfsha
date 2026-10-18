#!/bin/bash
# Prueba del clúster completo con docker compose, todo en local y con TLS: 2 superpeers y 3 peers
# Lo que se comprueba:
#   1. los 3 peers se registran y sirven HTTPS
#   2. peer1 sube un archivo y peer2 lo baja: el contenido es idéntico (los chunks viajan por HTTPS entre peers)
#   3. cada chunk quedó en 2 peers (factor de replicación 2), repartidos entre los 3
#   4. un peer no atiende HTTP plano
#   5. mTLS: un cliente sin certificado no entra al puerto de datos del peer (los envíos entre peers sí funcionan: usan su certificado)
#
#   bash deploy/peers-test.sh            deja el clúster arriba al terminar (para mirar los logs)
#   bash deploy/peers-test.sh --clean    lo apaga y borra todo al terminar

set -u
export MSYS_NO_PATHCONV=1 # Git Bash en Windows convierte rutas como /tmp/in en rutas de Windows dentro de docker exec
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/docker-compose.yml"
FAILED=0
SIZE=3500000 # 3,5 MB con chunks de 1 MiB -> 4 chunks
CHUNKS=4
REPLICAS=2

check() { # check "descripción" "obtenido" "esperado"
  if [ "$2" = "$3" ]; then echo "  OK   $1 -> $2"; else echo "  FALLA $1 -> $2 (esperado $3)"; FAILED=1; fi
}
wait_for() { # wait_for "descripción" segundos comando...
  local what=$1 limit=$2; shift 2
  local start=$SECONDS
  until "$@"; do
    [ $((SECONDS - start)) -ge "$limit" ] && { echo "  FALLA: no se cumplió '$what' en ${limit}s"; FAILED=1; return 1; }
    sleep 1
  done
  echo "  '$what' tras $((SECONDS - start)) s"
}
send() { docker exec "dfsha-$1" sh -c "echo '$2' >> /tmp/in"; } # send peer1 "ls /": le escribe un comando al CLI de ese peer
logged() { docker logs "dfsha-$1" 2>&1 | grep -q "$2"; }      # ¿el log del peer contiene este texto?
stored() { docker exec "dfsha-$1" sh -c 'find /data/chunks -type f | wc -l' | tr -d '\r'; } # chunks guardados en ese peer

echo "=== 1) Levantando 2 superpeers y 3 peers con TLS"
bash deploy/certs/gen-certs.sh > /dev/null || { echo "No se pudieron generar los certificados (¿está instalado openssl?)"; exit 1; }
$COMPOSE down -v > /dev/null 2>&1
$COMPOSE up -d --build || { echo "No se pudo levantar el clúster (¿Docker está corriendo?)"; exit 1; }
for p in peer1 peer2 peer3; do
  wait_for "$p registrado y sirviendo HTTPS" 90 logged $p "serving HTTPS"
done

echo "=== 2) peer1 sube un archivo y peer2 lo baja"
docker exec dfsha-peer1 sh -c "head -c $SIZE /dev/urandom > /tmp/big.bin"
ORIGINAL=$(docker exec dfsha-peer1 sh -c 'sha256sum /tmp/big.bin' | cut -d' ' -f1)
send peer1 "put /tmp/big.bin /big.bin"
wait_for "peer1 terminó de subir" 60 logged peer1 "uploaded /big.bin"
send peer2 "get /big.bin /tmp/got.bin"
wait_for "peer2 terminó de bajar" 60 logged peer2 "downloaded /big.bin"
DOWNLOADED=$(docker exec dfsha-peer2 sh -c 'sha256sum /tmp/got.bin' | cut -d' ' -f1)
check "el archivo bajado es idéntico al subido" "$DOWNLOADED" "$ORIGINAL"

echo "=== 3) Réplicas"
# Los peers reportan al superpeer cada 5 s; los chunks ya están guardados cuando el put termina
T1=$(stored peer1); T2=$(stored peer2); T3=$(stored peer3)
echo "       chunks guardados: peer1=$T1 peer2=$T2 peer3=$T3"
check "total de copias ($CHUNKS chunks x $REPLICAS réplicas)" "$((T1 + T2 + T3))" "$((CHUNKS * REPLICAS))"
HOLDERS=0; for t in $T1 $T2 $T3; do [ "$t" -gt 0 ] && HOLDERS=$((HOLDERS + 1)); done
check "peers que guardan chunks (al menos 2)" "$([ $HOLDERS -ge 2 ] && echo si || echo no)" "si"

echo "=== 4) HTTP plano contra un peer"
# wget -S imprime la respuesta del servidor; un servidor HTTPS contesta 400 a una petición sin TLS
PLAIN=$(docker exec dfsha-peer2 sh -c 'wget -S -q -O /dev/null http://peer1:9000/chunks/x 2>&1' | grep -c "400")
check "http plano a peer1 -> 400" "$([ "$PLAIN" -ge 1 ] && echo 400 || echo otro)" "400"

echo "=== 5) mTLS: un cliente sin certificado no entra al puerto de datos del peer"
# Control: wget sí sabe hablar TLS (el superpeer no pide certificado de cliente, así que responde)
http_status_lines() { docker exec dfsha-peer2 sh -c "wget -S -q -O /dev/null --no-check-certificate $1 2>&1" | grep -c "HTTP/1"; }
check "control: wget llega al superpeer por https y recibe respuesta" "$([ "$(http_status_lines https://sp1:8080/health)" -ge 1 ] && echo si || echo no)" "si"
# El peer exige certificado de cliente: sin él falla el saludo TLS y no hay ninguna respuesta HTTP
check "sin certificado de cliente el peer no responde nada HTTP" "$(http_status_lines https://peer1:9000/chunks/x)" "0"

echo
[ $FAILED = 0 ] && echo "RESULTADO: todo bien" || echo "RESULTADO: hubo fallas"
[ "${1:-}" = "--clean" ] && $COMPOSE down -v > /dev/null 2>&1 && echo "(clúster apagado y borrado)"
exit $FAILED
