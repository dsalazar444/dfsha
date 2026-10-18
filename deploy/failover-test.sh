# Prueba de failover del superpeer con docker compose todo solo en local
# Lo q se comprueba :
#   1. sp1 arranca como líder y sp2 como standby (responde 421 a todo menos /health)
#   2. se crea estado en sp1 
#   3. se mata sp1 de golpe y sp2 pasa a ser líder; se mide cuánto tarda
#   4. sp2 atiende con el estado que dejó sp1
#   5. sp1 vuelve y se queda como standby, sin quitarle el control a sp2


set -u
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/docker-compose.yml"
SP1=http://localhost:8081
SP2=http://localhost:8082
JSON='Content-Type: application/json'
FAILED=0


check() { # check "descripción" "obtenido" "esperado"
  if [ "$2" = "$3" ]; then echo "  OK   $1 -> $2"; else echo "  FALLA $1 -> $2 (esperado $3)"; FAILED=1; fi
}
role() { # role URL -> leader | standby | (vacío si no responde)
  curl -s -m 3 -D - -o /dev/null "$1/health" | tr -d '\r' | sed -n 's/^X-Superpeer-Role: //Ip'
}
code() { # code curl-args... -> código HTTP
  curl -s -m 5 -o /dev/null -w '%{http_code}' "$@"
}
field() { # field nombre -> valor de ese campo en el JSON que llega por stdin
  sed -E "s/.*\"$1\":\"([^\"]+)\".*/\1/"
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
login() { curl -s -m 5 -X POST "$1/auth/login" -H "$JSON" -d '{"username":"alice","password":"alice1234"}' | field session_token; }
is_leader() { [ "$(role "$1")" = "leader" ]; }
is_standby() { [ "$(role "$1")" = "standby" ]; }

# ---------- 1. levantar el clúster ----------
echo "=== 1) Levantando sp1 y sp2 con un volumen compartido"
$COMPOSE down -v > /dev/null 2>&1
$COMPOSE up -d --build || { echo "No se pudo levantar el clúster (¿Docker está corriendo?)"; exit 1; }
wait_for "sp2 responde" 60 is_standby "$SP2"
check "rol de sp1" "$(role $SP1)" "leader"
check "rol de sp2" "$(role $SP2)" "standby"
check "login en sp2 (standby)" "$(code -X POST $SP2/auth/login -H "$JSON" -d '{}')" "421"

# ---------- 2. crear estado en el líder ----------
echo "=== 2) Creando estado en sp1"
TOKEN=$(login $SP1)
AUTH="Authorization: Bearer $TOKEN"
PEER=$(curl -s -X POST $SP1/peers/register -H "$AUTH" -H "$JSON" -d '{"address":"peer-falso","port":"9000"}' | field peer_id)
check "mkdir /docs" "$(code -X POST $SP1/fs/mkdir -H "$AUTH" -H "$JSON" -d '{"path":"/docs"}')" "200"
CHUNK=$(curl -s -X POST $SP1/files/put/init -H "$AUTH" -H "$JSON" \
  -d '{"logical_path":"/docs/a.txt","chunk_count":1,"hash_of_each_chunk":["h0"]}' | sed -E 's/.*"assignment":\[\{"chunk_id":"([^"]+)".*/\1/')
check "put/ack" "$(code -X POST $SP1/files/put/ack -H "$AUTH" -H "$JSON" -d "{\"chunk_id\":\"$CHUNK\",\"peer_id\":\"$PEER\"}")" "200"
sleep 3 # el líder guarda el estado cada 2 s

# ---------- 3. kill al líder ----------
echo "=== 3) Matando sp1 de golpe (kill, sin apagado limpio)"
$COMPOSE kill sp1 > /dev/null
START=$SECONDS
wait_for "sp2 pasa a líder" 60 is_leader "$SP2"
echo "  FAILOVER: $((SECONDS - START)) s"

# ---------- 4. el standby atiende con el estado del líder ----------
echo "=== 4) sp2 atiende con el estado que dejó sp1"
# Mismo token que dio sp1: con JWT y secreto compartido sp2 lo acepta sin volver a hacer login
check "el token de sp1 vale en sp2" "$(code "$SP2/fs/ls?path=/" -H "$AUTH")" "200"
check "heartbeat con el peer_id registrado en sp1" \
  "$(code -X POST $SP2/peers/heartbeat -H "$AUTH" -H "$JSON" -d "{\"peer_id\":\"$PEER\",\"available_space\":1000,\"chunks\":[\"$CHUNK\"]}")" "200"
GET=$(curl -s $SP2/files/get/docs/a.txt -H "$AUTH")
case "$GET" in *"$CHUNK"*) check "get /docs/a.txt en sp2" "devuelve el archivo" "devuelve el archivo" ;;
               *)          check "get /docs/a.txt en sp2" "$GET" "devuelve el archivo" ;; esac

# ---------- 5. el líder anterior vuelve ----------
echo "=== 5) sp1 vuelve a arrancar"
$COMPOSE start sp1 > /dev/null
wait_for "sp1 responde" 60 is_standby "$SP1"
sleep 6 
check "rol de sp1" "$(role $SP1)" "standby"
check "rol de sp2" "$(role $SP2)" "leader"

# ---------- resultade ----------
echo
[ "$FAILED" = 0 ] && echo "RESULTADO: todo bien" || echo "RESULTADO: hubo fallas (arriba)"
[ "${1:-}" = "--clean" ] && $COMPOSE down -v > /dev/null 2>&1
exit $FAILED
