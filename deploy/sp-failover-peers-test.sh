#!/bin/bash
# Caída del superpeer líder con peers de verdad trabajando, con docker compose todo en local y con TLS
# (deploy/failover-test.sh prueba el cambio de líder con curl; esta prueba comprueba que los PEERS siguen trabajando)
# Lo que se comprueba:
#   1. los peers suben y listan archivos con sp1 de líder
#   2. se mata sp1 de golpe y sp2 pasa a líder
#   3. los tres peers vuelven a poder trabajar contra sp2, sin volver a hacer login (el token vale en sp2)
#   4. un archivo subido ANTES de la caída se baja idéntico a través de sp2
#   5. un archivo subido DESPUÉS de la caída (a través de sp2) lo baja otro peer, idéntico
#   6. sp1 vuelve a arrancar como standby, sp2 sigue de líder y todo sigue funcionando
#
#   bash deploy/sp-failover-peers-test.sh            deja el clúster arriba al terminar
#   bash deploy/sp-failover-peers-test.sh --clean    lo apaga y borra todo al terminar

set -u
export MSYS_NO_PATHCONV=1 # Git Bash en Windows convierte rutas como /tmp/in en rutas de Windows dentro de docker exec
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/docker-compose.yml"
SP1=https://localhost:8081
SP2=https://localhost:8082
CA=deploy/certs/out/ca.crt
FAILED=0

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
# --ssl-no-revoke: el curl de Windows (Schannel) rechaza una CA propia por no poder comprobar revocación; en Linux no hace nada
role() { command curl -s -m 3 --cacert "$CA" --ssl-no-revoke -D - -o /dev/null "$1/health" | tr -d '\r' | sed -n 's/^X-Superpeer-Role: //Ip'; }
is_leader() { [ "$(role "$1")" = "leader" ]; }
is_standby() { [ "$(role "$1")" = "standby" ]; }
send() { docker exec "dfsha-$1" sh -c "echo '$2' >> /tmp/in"; }  # send peer1 "ls /": le escribe un comando al CLI de ese peer
count() { docker logs "dfsha-$1" 2>&1 | grep -c -- "$2"; }       # cuántas veces sale este texto en el log del peer
logged() { [ "$(count "$1" "$2")" -ge "${3:-1}" ]; }             # ¿sale al menos N veces? (por defecto 1)
hash_of() { docker exec "dfsha-$1" sh -c "sha256sum $2" | cut -d' ' -f1; }

# listing_works peer N: ejecuta "ls /docs" y espera ver a.txt otra vez (N veces en total en el log del peer)
# Si el peer aún apunta al superpeer caído, el comando falla con un error y se vuelve a intentar
listing_works() { send "$1" "ls /docs"; sleep 4; logged "$1" "> a.txt" "$2"; }

echo "=== 1) Levantando el clúster y subiendo un archivo con sp1 de líder"
bash deploy/certs/gen-certs.sh > /dev/null || { echo "No se pudieron generar los certificados (¿está instalado openssl?)"; exit 1; }
$COMPOSE down -v > /dev/null 2>&1
$COMPOSE up -d --build || { echo "No se pudo levantar el clúster (¿Docker está corriendo?)"; exit 1; }
wait_for "sp2 responde como standby" 60 is_standby "$SP2"
check "rol de sp1" "$(role $SP1)" "leader"
for p in peer1 peer2 peer3; do wait_for "$p sirviendo HTTPS" 90 logged $p "serving HTTPS"; done
docker exec dfsha-peer1 sh -c 'head -c 2500000 /dev/urandom > /tmp/antes.bin'
ANTES=$(hash_of peer1 /tmp/antes.bin)
send peer1 "mkdir /docs"
sleep 2
send peer1 "put /tmp/antes.bin /docs/antes.bin"
wait_for "peer1 subió antes.bin" 60 logged peer1 "uploaded /docs/antes.bin"
docker exec dfsha-peer1 sh -c 'echo contenido > /tmp/a.txt'
send peer1 "put /tmp/a.txt /docs/a.txt"
wait_for "peer1 subió a.txt" 60 logged peer1 "uploaded /docs/a.txt"
for p in peer1 peer2 peer3; do
  send $p "ls /docs"
  wait_for "$p lista /docs con sp1" 30 logged $p "> a.txt"
done

echo "=== 2) Matando sp1 de golpe"
START=$SECONDS
$COMPOSE kill sp1 > /dev/null 2>&1
wait_for "sp2 pasa a líder" 60 is_leader "$SP2"
echo "       FAILOVER: $((SECONDS - START)) s"

echo "=== 3) Los peers vuelven a trabajar, ahora contra sp2, sin login nuevo"
# Los peers siguen con el token de sp1 (un JWT con la clave compartida): si sp2 no lo aceptara, saldría "session expired"
for p in peer1 peer2 peer3; do
  wait_for "$p lista /docs a través de sp2" 120 listing_works $p 2
done
check "ningún peer pidió volver a iniciar sesión" "$(for p in peer1 peer2 peer3; do count $p 'session expired'; done | paste -sd+ - | awk -F+ '{s=0; for(i=1;i<=NF;i++) s+=$i; print s}')" "0"

echo "=== 4) Un archivo subido antes de la caída se baja a través de sp2"
send peer2 "get /docs/antes.bin /tmp/antes-bajado.bin"
wait_for "peer2 bajó antes.bin" 90 logged peer2 "downloaded /docs/antes.bin"
check "antes.bin es idéntico" "$(hash_of peer2 /tmp/antes-bajado.bin)" "$ANTES"

echo "=== 5) Un archivo subido después de la caída (a través de sp2) lo baja otro peer"
docker exec dfsha-peer1 sh -c 'head -c 2500000 /dev/urandom > /tmp/despues.bin'
DESPUES=$(hash_of peer1 /tmp/despues.bin)
send peer1 "put /tmp/despues.bin /docs/despues.bin"
wait_for "peer1 subió despues.bin con sp2" 90 logged peer1 "uploaded /docs/despues.bin"
send peer3 "get /docs/despues.bin /tmp/despues-bajado.bin"
wait_for "peer3 bajó despues.bin" 90 logged peer3 "downloaded /docs/despues.bin"
check "despues.bin es idéntico" "$(hash_of peer3 /tmp/despues-bajado.bin)" "$DESPUES"

echo "=== 6) sp1 vuelve a arrancar"
$COMPOSE start sp1 > /dev/null 2>&1
wait_for "sp1 vuelve como standby" 60 is_standby "$SP1"
check "rol de sp2" "$(role $SP2)" "leader"
BEFORE=$(count peer2 "> a.txt")
wait_for "peer2 lista /docs tras volver sp1" 60 listing_works peer2 $((BEFORE + 1))

echo
[ $FAILED = 0 ] && echo "RESULTADO: todo bien" || echo "RESULTADO: hubo fallas"
[ "${1:-}" = "--clean" ] && $COMPOSE down -v > /dev/null 2>&1 && echo "(clúster apagado y borrado)"
exit $FAILED
