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

## 4. Decisiones pendientes (a resolver antes de implementar)
- [ ] César vs. AES para cifrado de contenido.
- [ ] Tamaño fijo de chunk (ej. 4MB) — definir valor.
- [ ] Factor de replicación por defecto.
- [ ] Timeout de heartbeat para marcar un peer como caído.
- [ ] Alcance real del stretch goal de edición a nivel de chunk (¿entra en las 4 semanas o se documenta como trabajo futuro?).
