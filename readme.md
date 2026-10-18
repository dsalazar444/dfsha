# DFSha

Sistema de archivos distribuido peer-to-peer escrito en Go. Cada usuario ve un árbol de directorios único (`mkdir`, `cd`, `ls`, `put`, `get`, `rm`, …), pero sus archivos viven partidos en pedazos (*chunks*), cifrados y replicados en varios peers. Un par de superpeers coordina el sistema, y un peer se puede caer sin que se pierda ningún archivo.

## Índice

1. [Cómo funciona](#1-cómo-funciona)
   - [Componentes](#componentes)
   - [Arquitectura](#arquitectura)
   - [Qué pasa al subir un archivo](#qué-pasa-al-subir-un-archivo-put)
   - [Qué pasa al bajarlo](#qué-pasa-al-bajarlo-get)
   - [Qué pasa cuando algo falla](#qué-pasa-cuando-algo-falla)
   - [Seguridad](#seguridad)
2. [Estructura del repositorio](#2-estructura-del-repositorio)
3. [Reproducirlo en local](#3-reproducirlo-en-local)
   - [Requisitos](#requisitos)
   - [Paso 1: certificados](#paso-1-certificados)
   - [Paso 2: levantar los superpeers](#paso-2-levantar-los-superpeers)
   - [Paso 3: probar la línea de comandos a mano](#paso-3-probar-la-línea-de-comandos-a-mano)
   - [Comandos de la línea de comandos](#comandos-de-la-línea-de-comandos)
   - [Variables de configuración](#variables-de-configuración)
4. [Despliegue en AWS](#4-despliegue-en-aws)
5. [Documentos](#5-documentos)

## 1. Cómo funciona

### Componentes

| Componente | Qué hace |
|---|---|
| **Superpeer** | El plano de control. Autentica usuarios, guarda el árbol de directorios de cada uno, sabe en qué peers está cada chunk, vigila qué peers están vivos, reparte los locks y ordena las replicaciones. **No mueve ningún dato de archivos.** |
| **Superpeer en standby** | Un segundo superpeer que comparte con el primero una carpeta (`STATE_DIR`). Si el líder se cae, toma el mando en unos 15–21 s. |
| **Peer** | Un programa que hace dos cosas: **guarda chunks** de otros usuarios (servidor HTTPS en el puerto 9000) y le da a su usuario una **línea de comandos** (`dfsha:/>`). |
| **CA y certificados** | Una autoridad certificadora propia (`deploy/certs/gen-certs.sh`) firma un certificado por nodo. Con ellos todo viaja por TLS. |

### Arquitectura

*Pendiente.*

### Qué pasa al subir un archivo (`put`)
1. El peer pide la contraseña, inicia sesión en el superpeer y recibe un token y una *sal*. Con ellas deriva su clave maestra (Argon2id). **La clave nunca sale del peer ni se guarda.**
2. Parte el archivo en chunks (64 MiB por defecto), cifra cada uno con AES-GCM usando una clave propia del archivo y calcula el SHA-256 de cada chunk cifrado.
3. Envía al superpeer la ruta, el `file_id` y los hashes (`put/init`). El superpeer anota el archivo en el árbol del usuario y contesta **a qué peers va cada chunk** (factor de replicación 2 por defecto).
4. El peer manda cada chunk directamente a esos peers. Cada peer que lo recibe verifica el hash, lo guarda y avisa al superpeer (`put/ack`).

### Qué pasa al bajarlo (`get`)
El peer le pregunta al superpeer dónde están los chunks (solo le contesta con peers vivos que ya confirmaron tenerlo), los pide a los peers, comprueba cada hash **contra el que dio el superpeer**, los descifra y los une. Un peer que sirve un chunk corrupto o falso es descartado y se prueba con otra réplica.

### Qué pasa cuando algo falla
- **Se cae un peer.** Los peers mandan un *heartbeat* cada 10 s con la lista de chunks que tienen. Si un peer calla 15 s, el superpeer lo da por caído. En la respuesta al siguiente heartbeat de los demás les ordena copiar los chunks que quedaron con menos de 2 réplicas. Si el peer vuelve con sus datos, se reincorpora.
- **Se cae el superpeer líder.** El líder renueva cada pocos segundos un archivo `leader.json` en la carpeta común, y guarda el estado en `state.json`. Si deja de renovarlo, el standby toma el mando con ese estado. Las sesiones siguen valiendo porque los tokens (JWT) se firman con una clave compartida. El superpeer que no es líder contesta `421`, y los peers pasan al otro tras 3 fallos seguidos.
- **Dos usuarios editan lo mismo.** `lock` / `unlock` dan exclusión mutua sobre un archivo. El lock dura 30 s y el CLI lo renueva mientras lo tiene; si el peer que lo tiene se cae, caduca solo.
- **Borrar.** `rm` quita el archivo del árbol al instante, y el superpeer ordena en los heartbeats borrar los chunks de cada peer hasta que desaparecen.

### Seguridad
| Capa | Qué hay |
|---|---|
| Comunicaciones | **TLS obligatorio** entre todos los nodos, con nuestra CA. Sin certificados, el superpeer y el peer no arrancan (salvo `ALLOW_INSECURE=true`, solo para desarrollo). |
| Entre peers | **mTLS** en el puerto de datos: un peer solo acepta clientes con un certificado firmado por la CA. |
| Usuarios | Usuario y contraseña, y un token JWT (HS256) en cada petición al superpeer. Cada usuario ve solo su árbol de directorios. |
| Almacenamiento | Los chunks están cifrados (AES-GCM) antes de salir del peer dueño. Los peers que los guardan solo ven bytes cifrados. |

## 2. Estructura del repositorio

```
cmd/superpeer/        programa del superpeer
cmd/peer/             programa del peer (servidor de chunks + línea de comandos)
internal/superpeer/   auth, árbol de directorios, archivos, peers, locks, líder y persistencia
internal/peer/        almacenamiento, servidor, cliente, transferencia, cifrado, CLI, heartbeat
internal/shared/      modelo de datos común y configuración de TLS (tlsconf)
deploy/               Dockerfile, docker-compose, certificados y scripts de prueba
superpeer-openapi.yaml, peer-openapi.yaml    contratos de las dos APIs
docs/requisitos.md    requisitos del sistema
```

## 3. Reproducirlo en local

Todo corre en contenedores: **2 superpeers y 3 peers**, con TLS, en una sola máquina. Los superpeers los levanta `docker compose` y cada peer se abre en su propia terminal.

### Requisitos
- **Docker** (en Windows, Docker Desktop abierto y corriendo).
- **bash** para los scripts de `deploy/`:
  - En **Windows**, **Git Bash** (no PowerShell: ahí `bash` abre WSL y no encuentra Docker). Ya trae `openssl`.
  - En **Linux**, `bash` y **`openssl`** instalado.

### Paso 1: certificados
```bash
bash deploy/certs/gen-certs.sh
```
Crea la CA y un certificado por nodo en `deploy/certs/out/` (esa carpeta no se sube a git). Si ya existe una CA se reutiliza. Hay que correrlo antes de levantar el compose.

### Paso 2: levantar los superpeers
```bash
docker compose -f deploy/docker-compose.yml up -d --build sp1 sp2
docker ps --format '{{.Names}} {{.Status}}'
```
Deben aparecer `dfsha-sp1` y `dfsha-sp2` como `healthy` (tarda unos 20 s). Para ver quién es el líder:
```bash
curl -si --ssl-no-revoke --cacert deploy/certs/out/ca.crt https://localhost:8081/health | grep -i x-superpeer-role   # leader
curl -si --ssl-no-revoke --cacert deploy/certs/out/ca.crt https://localhost:8082/health | grep -i x-superpeer-role   # standby
```
(`--ssl-no-revoke` solo hace falta con el `curl` de Windows; en Linux no hace nada.)

### Paso 3: probar la línea de comandos a mano
Se usan **cuatro terminales** (en Windows, cuatro Git Bash abiertas en la carpeta del repositorio): una de control (**T0**) y tres peers, cada uno en la suya.

| Terminal | Qué hace | Comando |
|---|---|---|
| **T0** | Control: mirar y matar nodos | los comandos de los ejemplos |
| **T1** | peer1, usuario `alice` / `alice1234` | `bash deploy/demo-peer.sh peer1` |
| **T2** | peer2, usuario `alice` / `alice1234` | `bash deploy/demo-peer.sh peer2` |
| **T3** | peer3, usuario `bob` / `bob1234` | `bash deploy/demo-peer.sh peer3` |

Cada peer pide usuario y contraseña, y muestra el prompt `dfsha:/>`. Los usuarios están dados de alta en el superpeer. Los archivos que quieras subir se dejan en `deploy/demo-files/<peer>/`, que dentro del peer es la carpeta de trabajo. La imagen del peer se construye sola la primera vez; si cambias el código, reconstrúyela con `docker build -f deploy/Dockerfile --build-arg CMD=peer -t dfsha-peer .`.

Los ejemplos siguientes se pueden hacer en ese orden.

#### Ejemplo 1: subir en un peer y bajar en otro (transparencia)
En **T0**, crea un archivo de prueba:
```bash
mkdir -p deploy/demo-files/peer1
printf 'Informe de prueba de DFSha\nlinea dos\n' > deploy/demo-files/peer1/informe.txt
```
En **T1**:
```
mkdir docs
cd docs
put informe.txt
```
Responde `uploaded /docs/informe.txt (37 bytes, id ...)`. En **T2**:
```
ls /docs
get /docs/informe.txt copia.txt
```
`ls` muestra `informe.txt` y `get` responde `downloaded /docs/informe.txt -> copia.txt (37 bytes)`. Comprobar en **T0**: `cat deploy/demo-files/peer2/copia.txt` muestra el mismo texto. El peer2 nunca escribió la dirección de nadie: el superpeer le dijo dónde estaba cada chunk.

#### Ejemplo 2: dónde quedaron los chunks y que están cifrados
En **T0**:
```bash
docker exec dfsha-peer1 sh -c 'find /data/chunks -type f'
docker exec dfsha-peer2 sh -c 'find /data/chunks -type f'
docker exec dfsha-peer1 sh -c 'head -c 48 $(find /data/chunks -type f | head -1) | od -c | head -4'
```
Aparece una ruta `/data/chunks/<usuario>/<id-del-archivo>/0` en dos peers (cada chunk está en 2 de los 3). Los bytes que muestra la tercera orden son ilegibles: lo guardado va cifrado. (Los comandos con rutas absolutas van dentro de `sh -c '…'` para que Git Bash no las convierta en rutas de Windows.)

#### Ejemplo 3: cada usuario ve solo lo suyo
En **T3**, que es `bob`:
```
ls /
ls /docs
get /docs/informe.txt x.txt
```
`ls /` no muestra nada, y las otras dos dan `error: not found (HTTP 404 …)`.

#### Ejemplo 4: locks
| Terminal | Comando | Resultado |
|---|---|---|
| T1 | `lock /docs/informe.txt` | `locked /docs/informe.txt (until unlock or exit)` |
| T2 | `lock /docs/informe.txt` | `error: conflict: it already exists or is locked by someone else (HTTP 409 …)` |
| T1 | `unlock /docs/informe.txt` | `unlocked /docs/informe.txt` |
| T2 | `lock /docs/informe.txt` | `locked /docs/informe.txt …` (ahora sí) |
| T2 | `unlock /docs/informe.txt` | `unlocked /docs/informe.txt` |

El lock dura 30 s en el superpeer, pero el CLI lo renueva mientras lo tiene: aunque pasen más de 30 s, T2 sigue recibiendo conflicto hasta que T1 haga `unlock`.

#### Ejemplo 5: modificar un archivo
En **T1**: `mdf /docs/informe.txt`. Abre el editor `vi`: pulsa `i`, escribe algo, pulsa `Esc`, escribe `:wq` y Enter. Responde `modified /docs/informe.txt (… bytes, id …)`. Si sales sin cambios (`Esc`, `:q!`), responde `no changes, nothing uploaded`. Para ver el cambio, en **T2** haz `get /docs/informe.txt nuevo.txt` y en **T0** `cat deploy/demo-files/peer2/nuevo.txt`.

#### Ejemplo 6: se cae un peer
Sube un archivo desde T1 (`put informe.txt /docs/otro.txt`) y en **T0**:
```bash
docker kill dfsha-peer3
```
Espera unos 25 s. El superpeer lo da por caído y re-replica lo que tenía. En **T2**, `get /docs/otro.txt otro.txt` sigue funcionando, porque cada chunk tiene otra copia.

#### Ejemplo 7: se cae el superpeer líder y el standby lo sustituye
En **T0**:
```bash
docker compose -f deploy/docker-compose.yml kill sp1
```
Mientras esperas unos 20 s, T1 y T2 muestran líneas de log como éstas, que son lo esperado:
```
heartbeat to https://sp1:8080 failed (1/3): superpeer unavailable ...
switching superpeer: https://sp1:8080 -> https://sp2:8080
heartbeat to https://sp2:8080 failed (1/3): ... HTTP 421: this superpeer is on standby ...
```
Después, en **T0**, sp2 ya es el líder:
```bash
curl -si --ssl-no-revoke --cacert deploy/certs/out/ca.crt https://localhost:8082/health | grep -i x-superpeer-role   # leader
```
Y en **T1** y **T2**, `ls /docs` funciona de nuevo, sin volver a iniciar sesión (si escribes antes de que termine el cambio, dice `superpeer unreachable; try again…`: espera unos segundos). Para devolver el nodo:
```bash
docker compose -f deploy/docker-compose.yml start sp1
```
sp1 vuelve como `standby` y sp2 sigue de `leader`.

#### Ejemplo 8: TLS
En **T0**:
```bash
curl -s --ssl-no-revoke -o /dev/null -w '%{http_code}\n' https://localhost:8082/health                                          # 000: no confía en la CA
curl -s --ssl-no-revoke --cacert deploy/certs/out/ca.crt -o /dev/null -w '%{http_code}\n' https://localhost:8082/health        # 200
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8082/health                                                            # 400: HTTP plano rechazado
```

#### Al terminar
Escribe `exit` en T1, T2 y T3, y en **T0**:
```bash
docker compose -f deploy/docker-compose.yml down -v
docker volume rm dfsha-peer1-data dfsha-peer2-data dfsha-peer3-data
rm -rf deploy/demo-files
```
El `volume rm` es necesario: los datos de los peers abiertos a mano viven en volúmenes que `down -v` no borra.

### Comandos de la línea de comandos
| Comando | Qué hace |
|---|---|
| `pwd`, `cd [ruta]`, `ls [ruta]` | Navegar el árbol de directorios |
| `mkdir <ruta>`, `rmdir <ruta>` | Crear y borrar directorios (`rmdir` borra también los archivos de dentro) |
| `put <local> [remoto]` | Subir un archivo (máximo 200 MiB) |
| `get <remoto> [local]` | Bajar un archivo (no sobrescribe) |
| `rm <ruta>` | Borrar un archivo |
| `lock <ruta>` / `unlock <ruta>` | Bloquear y desbloquear un archivo; el CLI renueva el lock mientras lo tiene |
| `mdf <ruta>` | Editar un archivo en `$EDITOR`: lo bloquea, lo baja, abre el editor y sube el resultado solo si cambió |
| `login` | Volver a iniciar sesión cuando la sesión caducó |
| `help`, `exit` | Ayuda y salir |

Las rutas con espacios van entre comillas, y las relativas parten del directorio actual.

### Variables de configuración

**Superpeer**

| Variable | Qué es | Por defecto |
|---|---|---|
| `PORT` | Puerto | `8080` |
| `STATE_DIR` | Carpeta compartida del estado. Vacía: sin standby | vacío |
| `SUPERPEER_ID` | Nombre del superpeer; debe ser distinto en cada uno | el hostname |
| `SESSION_SECRET` | Clave (64 hex) con que se firman los tokens; **igual en los dos superpeers** | clave de desarrollo, con aviso |
| `REPLICATION_FACTOR` | Copias de cada chunk | `2` |
| `HEARTBEAT_TIMEOUT_SECONDS` | Silencio tras el cual un peer se da por caído | `15` |
| `TLS_CERT`, `TLS_KEY` | Certificado y clave del superpeer | vacío |
| `ALLOW_INSECURE` | Permite HTTP plano si no hay certificado (solo desarrollo) | `false` |
| `PEER_SCHEME` | `http` o `https`: cómo se llega a los peers | `https` |

**Peer**

| Variable | Qué es | Por defecto |
|---|---|---|
| `SUPERPEER_URLS` | Lista de superpeers, separada por comas | obligatoria |
| `PEER_ADDRESS`, `PEER_PORT` | Nombre y puerto por el que los demás llegan a este peer | obligatorias |
| `STORAGE_PATH` | Carpeta donde se guardan los chunks | `./data` |
| `HEARTBEAT_INTERVAL` | Cada cuánto avisa que sigue vivo | `10s` |
| `CHUNK_SIZE` | Bytes por chunk al subir | `67108864` (64 MiB) |
| `PARALLELISM` | Transferencias de chunks simultáneas | `4` |
| `DFSHA_USERNAME` | Rellena el usuario en el inicio de sesión | vacío |
| `TLS_CERT`, `TLS_KEY` | Certificado y clave del peer | vacío |
| `TLS_CA` | Certificado de la CA; obligatorio si hay `TLS_CERT` | vacío |
| `ALLOW_INSECURE` | Permite HTTP plano (solo desarrollo) | `false` |

## 4. Despliegue en AWS

Ver [`docs/despliegueaws.md`](docs/despliegueaws.md). *Pendiente.*

## 5. Documentos
- [`superpeer-openapi.yaml`](superpeer-openapi.yaml) y [`peer-openapi.yaml`](peer-openapi.yaml): contratos de las APIs.
