# DFSha — Requisitos del Sistema
### P2P centralizado (1 superpeer) → migración futura a descentralizado
### Stack: Go

---

## 1. Arquitectura general

- **1 Superpeer** (por ahora): coordina metadata, usuarios, ubicación de chunks y salud de peers. No almacena chunks él mismo (o si lo hace, es opcional/cache).
- **N Peers**: cada peer es cliente *y* nodo de almacenamiento a la vez. Exponen su propia API para que otros peers les hablen directo.
- **Transferencia de datos:** siempre peer-a-peer (el superpeer nunca mueve bytes, solo coordina).
- Diseño pensado para que, en una fase futura, el rol de "superpeer único" se reemplace por varios superpeers federados (opción P2P descentralizada del enunciado).


## 2. Componentes y API (alto nivel)

### Superpeer expone:
- `POST /auth/login`
- `POST /peers/register` (dirección del peer)
- `POST /peers/heartbeat` (estado, espacio, chunks)
- `POST /fs/mkdir`, `POST /fs/rmdir`, `GET /fs/ls`, `GET /fs/cd`
- `POST /files/put/init` → devuelve asignación de chunks a peers
- `POST /files/put/ack` → peer confirma que recibió un chunk
- `GET /files/get/{ruta}` → devuelve ubicación de chunks
- `DELETE /files/{ruta}`
- `POST /files/lock`, `POST /files/unlock`

### Peer expone:
- `POST /chunks/receive` (recibe un chunk de otro peer)
- `GET /chunks/{chunk_id}` (sirve un chunk a quien lo pida)
- `DELETE /chunks/{chunk_id}`

---
### 2.1 Especificaciones de APIs

Todos los endpoints salvo `/auth/login` requieren un token de sesión
(Authorization header), obtenido en el login. El superpeer valida este
token y aplica aislamiento de usuarios en cada operación.

Códigos de estado usados consistentemente:
- 400 → body malformado / faltan campos
- 401 → no autenticado (token ausente o expirado)
- 403 → autenticado pero sin permiso sobre el recurso
- 404 → recurso no existe (ruta, file_id, chunk_id, peer)
- 409 → conflicto (ej. lock ya tomado por otro usuario)
- 500 → error interno del servidor

#### Superpeer expone:

- `POST /auth/login`
  Recibe: str:usuario, str:contraseña.
  Responde: str:token_de_sesión (200), 401 si credenciales inválidas.

- `POST /peers/register`
  Recibe: str:dirección, str:puerto.
  Responde: str:`peer_id` generado por el superpeer (200).

- `POST /peers/heartbeat`
  Recibe: str:`peer_id`, str:espacio_disponible, array:lista_de_chunks_almacenados.
  (Sin campo "estado" — el superpeer infiere "healthy" por la sola
  llegada del heartbeat dentro del timeout configurado.)
  Responde: 200 / 404 si el peer no está registrado.

- `POST /fs/mkdir`      body: { str:path } - Responde: 200, 4xx
- `DELETE /fs/rmdir`    body: { str:path } - Responde: 200, 4xx
- `GET /fs/ls?path=...`
  (Sin endpoint para `cd`: el directorio "actual" es un concepto
  puramente local del cliente. El cliente antepone su pwd local a
  cualquier ruta relativa antes de llamar a estos endpoints — el
  superpeer siempre recibe rutas absolutas.)

- `POST /files/put/init`
  Recibe: str:ruta_logica, int:cantidad_de_chunks, vector<str>:hash_de_cada_chunk
  (calculados por el cliente antes de subir).
  Responde (200): str:`file_id` generado por el superpeer, y por cada
  chunk: str:`chunk_id` (string compuesto `{file_id}_{indice}`) + lista
  de URLs de peers destino según factor de replicación.

- `POST /files/put/ack`
  Recibe: str:`chunk_id` (ya compuesto, no requiere file_id separado).
  Responde: 200 / 404 si el chunk_id no corresponde a ningún put
  iniciado.

- `GET /files/get/{ruta}`
  Responde (200): por cada str:`chunk_id`, lista de URLs de peers que lo
  tienen (para permitir retry si alguno está caído).
  (Pendiente evaluar: token de acceso firmado por chunk, para que el
  peer no tenga que validar usuarios/permisos — ver sección de
  decisiones pendientes. De momento, no se implementa.)

- `DELETE /files/{ruta}`
  Responde 200 de inmediato ("empezando eliminación") sin esperar
  confirmación de todos los peers — borrado relajado/eventual.

- `POST /files/lock/{ruta}`
  Responde (200): str:`lock_id`, str:`expires_at` (TTL del lock).
  Responde 409 si ya está bloqueado por otro usuario.
  (El cliente los llama internamente como parte de su comando
  `modify` — el usuario del CLI nunca invoca lock/unlock
  directamente, solo ejecuta `modify <archivo>` y el cliente orquesta
  lock → get → edición → put → unlock por detrás.)

- `POST /files/unlock/{ruta}`
  Recibe: str:`lock_id`.
  Responde 200, o 404 si el lock ya expiró/no existe (idempotente).

#### Peer expone:

- `POST /chunks/receive`
  Recibe: binario:chunk, str:hash_esperado.
  Responde: 400 si el hash no coincide (chunk corrupto) — el cliente
  debe reintentar la subida con **otro peer** de la lista de réplicas,
  no con el mismo. 200 si coincide.

- `GET /chunks/{chunk_id}`
  Responde: 404 si no tiene el chunk, 200 con el chunk en el body si
  lo tiene.
  (Por ahora sin validación de token firmado, se asume que si superpeer da rutas de peers, es porque se valido correctamente — ver nota en
  `/files/get` y en decisiones pendientes.)

- `DELETE /chunks/{chunk_id}`
  Responde 200 de inmediato ("empezando eliminación") — relajado,
  igual que el borrado de archivo en el superpeer.

---

## 3. Decisiones pendientes (a resolver antes de implementar)
- [ ] César vs. AES para cifrado de contenido.
- [ ] Tamaño fijo de chunk (ej. 4MB) — definir valor.
- [ ] Factor de replicación por defecto.
- [ ] Timeout de heartbeat para marcar un peer como caído.
- [ ] Alcance real del stretch goal de edición a nivel de chunk (¿entra en las 4 semanas o se documenta como trabajo futuro?).
