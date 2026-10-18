// Package heartbeat implements the periodic "I am alive" report a peer
// sends to the superpeer, and the execution of the replication tasks the
// superpeer sends back in the reply.
//
// Two jobs, one loop:
//
//  1. Report: peer ID, free disk space and the list of stored chunks.
//     The superpeer uses it to detect dead peers (no heartbeat within its
//     timeout) and to keep its chunk-location table accurate.
//  2. Obey: the reply may contain ReplicationTasks ("copy this chunk from
//     one of these peers"). This is how the superpeer repairs the
//     replication factor after a peer dies, without ever opening a
//     connection to a peer: it only answers requests, like every other
//     endpoint.
//
// The superpeer keeps re-sending a task on every heartbeat until the peer
// acknowledges the chunk (put/ack). That makes the heartbeat itself the
// retry loop, so this package needs no retry logic of its own.
package heartbeat

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"dfsha/internal/peer/hash"
	"dfsha/internal/peer/http_client"
	"dfsha/internal/peer/storage"
	"dfsha/internal/shared/model"
)

const (
	// defaultInterval is how often a heartbeat is sent. The superpeer
	// should declare a peer dead after several missed beats (a common
	// rule is 3x this value), so one lost packet never triggers a
	// re-replication.
	defaultInterval = 10 * time.Second // está en seg, con * time.seconds convertimos

	// defaultMaxParallelTasks bounds how many replication tasks run at
	// once, so a big failure does not make this peer download hundreds
	// of chunks simultaneously.
	defaultMaxParallelTasks = 4

	// taskTimeout is the total time budget of one task (all sources).
	taskTimeout = 2 * time.Minute // cuanto puede durar una tarea, considerando todas las fuentes (peers que devuelve task)

	// heartbeatTimeout bounds one heartbeat request. Without it, a hung
	// superpeer would block the loop and the "every 10 s" would stop
	// being true, delaying the failure detection below
	heartbeatTimeout = 5 * time.Second // cuánto tiempo como máximo puede tardar una petición de heartbeat antes de que se cancele su contexto. Para que peer no se quede esperando indefinidamente hasta que SP le conteste

	// failuresBeforeSwitch is how many consecutive heartbeats must fail
	// before the peer moves to the next superpeer. With the default
	// interval that is about 30 s, so one lost packet never triggers it.
	failuresBeforeSwitch = 3
)

// SuperpeerAPI is what the heartbeat needs from the superpeer client.
// It is declared here (by the consumer) so tests can use a fake and this
// package does not depend on how the HTTP client is built.
type SuperpeerAPI interface {
	// Heartbeat sends the report and returns the superpeer's reply.
	Heartbeat(ctx context.Context, payload model.HeartbeatPayload) (model.HeartbeatResponse, error)
	// PutAck tells the superpeer that this peer now stores the chunk.
	PutAck(chunkID model.ChunkID, peerID string) error
}

// PeerAPI is what the heartbeat needs to download a chunk from another peer.
type PeerAPI interface {
	// FetchChunk calls GET {peerURL}/chunks/{chunkID}?owner_user_id=...
	// and returns the envelope bytes exactly as the other peer stored them.
	FetchChunk(ctx context.Context, peerURL, ownerUserID string, chunkID model.ChunkID) ([]byte, error)
}

// Failover is the list of superpeers, seen from the heartbeat. It is
// implemented by client.SuperpeerPool
type Failover interface {
	Active() string //  Active returns the superpeer currently in use.
	// Rotate moves to the next superpeer if failed is still the active
	// one, and returns the active one afterwards
	Rotate(failed string) string
}

// Sender sends heartbeats and runs the replication tasks they bring back.
type Sender struct {
	peerID    string
	store     *storage.Store
	superpeer SuperpeerAPI
	peers     PeerAPI
	interval  time.Duration
	failover  Failover

	// consecutiveFailures counts heartbeats in a row that failed because
	// the superpeer was unavailable. It needs no mutex: only beat touches
	// it, and beat only runs on the goroutine of Run.
	consecutiveFailures int

	// slots is a counting semaphore: one token per running task.
	slots chan struct{} // es un semaforo, que tendrá n (4) elementos, donde cada uno representa permiso de ejecutar tarea.

	// inFlight holds the tasks being executed right now. The superpeer
	// repeats a task on every heartbeat until it is acknowledged, so
	// without this set a slow download would be started again every 10 s.
	mu       sync.Mutex                 // mutex para aplicar lock sobre inFlight y que no haya condiciones de carrera
	inFlight map[model.ChunkID]struct{} // map que registra los chunks cuyas tareas de replicación ya están ejecutándose.
	// ejm: inFlight:
	// "file1_0" → struct{}{}
	// "file1_3" → struct{}{} -> Indica que esos dos chunks ya están siendo pedidos

	// wg lets Run wait for running tasks before returning.
	wg sync.WaitGroup // Si run() necesita terminar, necesitamos evitar que se salga si hay tareas ejecutandose
	// wg lleva la cuenta de las tareas que todavía no han terminado y permite esperarlas.

	// resumen
	// slots: limita la concurrencia. inFlight: evita duplicar tareas en ejecución. mu: protege el acceso concurrente a inFlight. wg: permite esperar a que terminen las goroutines
}

// NewSender builds a Sender. interval <= 0 selects the default.
func NewSender(peerID string, store *storage.Store, superpeer SuperpeerAPI, peers PeerAPI, interval time.Duration, failover Failover) *Sender {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Sender{
		peerID:    peerID,
		store:     store,
		superpeer: superpeer,
		peers:     peers,
		interval:  interval,
		failover:  failover,
		slots:     make(chan struct{}, defaultMaxParallelTasks),
		inFlight:  make(map[model.ChunkID]struct{}),
	}
}

// Run sends one heartbeat right away (so the superpeer learns about this
// peer without waiting a full interval) and then one per interval, until
// ctx is cancelled. It blocks, so main starts it in its own goroutine:
//
//	go sender.Run(ctx)
//
// On cancellation it waits for the running tasks to finish: they use the
// same ctx, so they stop promptly.
func (s *Sender) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval) // crea un temporizador que envía una señal periódicamente por el canal ticker.C.
	defer ticker.Stop()                  // Detiene el ticker cuando Run() termina

	s.beat(ctx) // Enviamos primer herthbeat inmediatamente
	for {
		select { // Esperamos a que alguno de los dos eventos pase -> listener de canales
		case <-ctx.Done(): // Recibe señal de que se cancele el contexto ctx. Espera a que tareas terminen, y sale
			s.wg.Wait()
			return
		case <-ticker.C: // recibir un valor del canal del ticker. Indica que es tiempo de volver a mandar HB
			s.beat(ctx)
		}
	}
}

// beat performs one heartbeat round. Every failure is logged and
// swallowed: the next tick is the retry.
func (s *Sender) beat(ctx context.Context) {
	// If we cannot read our own state we skip the beat instead of
	// sending a partial one. An empty chunk list would tell the
	// superpeer "this peer lost everything" and trigger a mass
	// re-replication; a missing beat only delays things.
	chunks, err := s.store.ListChunkIDs()
	if err != nil {
		log.Printf("heartbeat skipped: listing chunks: %v", err)
		return
	}

	freeSpace, err := s.store.FreeSpace()
	if err != nil {
		log.Printf("heartbeat skipped: reading free space: %v", err)
		return
	}

	// TODO: OJO fileID debe ser único -> usar valor diferente (random) al que dice user
	// Chunk IDs embed a unique file ID, so the superpeer does not need
	// the owner to identify a chunk. The owner is a storage detail.
	ids := make([]model.ChunkID, 0, len(chunks))
	for _, c := range chunks {
		ids = append(ids, c.ChunkID)
	}

	// Remember which superpeer this beat goes to. The client reads the
	// same value for the request, and only this goroutine ever rotates
	// it, so the two always agree.
	spUsed := s.failover.Active()

	reqCtx, cancel := context.WithTimeout(ctx, heartbeatTimeout) // context con timeout limite que tiene el sp para responder a heartbeat
	resp, err := s.superpeer.Heartbeat(reqCtx, model.HeartbeatPayload{
		PeerID:    s.peerID,
		FreeSpace: freeSpace,
		Chunks:    ids,
	})
	cancel()

	if err != nil {
		s.handleHeartbeatError(ctx, spUsed, err)
		return
	}

	// Any successful answer proves the superpeer is alive.
	s.consecutiveFailures = 0

	for _, task := range resp.Tasks {
		s.startTask(ctx, task)
	}
}

// handleHeartbeatError decides what a failed heartbeat means: a superpeer
// that is down (counts toward switching) or one that answered with a
// refusal (it is alive, so it does not count).
func (s *Sender) handleHeartbeatError(ctx context.Context, used string, err error) {
	// During shutdown the request is cancelled on purpose; that says
	// nothing about the superpeer.
	if ctx.Err() != nil {
		return
	}

	if !errors.Is(err, http_client.ErrSuperpeerUnavailable) {
		// It answered (400, 401, ...). Alive, but unhappy with us:
		// log it and reset the counter, do not switch.
		s.consecutiveFailures = 0
		log.Printf("heartbeat rejected by %s: %v", used, err)
		return
	}

	s.consecutiveFailures++
	log.Printf("heartbeat to %s failed (%d/%d): %v", used, s.consecutiveFailures, failuresBeforeSwitch, err)
	if s.consecutiveFailures < failuresBeforeSwitch {
		return
	}

	// Threshold reached: move to the next superpeer and start counting
	// again from zero for it.
	s.consecutiveFailures = 0
	next := s.failover.Rotate(used)
	if next == used {
		log.Printf("no other superpeer configured, staying on %s", used)
		return
	}
	log.Printf("switching superpeer: %s -> %s", used, next)

	// The heartbeat itself is the availability check of the new
	// superpeer, so send one right now instead of waiting a full
	// interval. This cannot loop: the counter is 0 here, so the nested
	// call cannot reach the threshold again.
	s.beat(ctx)
}

// startTask launches a task in its own goroutine, unless it is already
// running or all slots are busy. Skipping is safe in both cases because
// the superpeer will send the task again on the next heartbeat.
//
// It must not block: beat runs in the heartbeat loop, and a blocked loop
// would stop the heartbeats and get this healthy peer declared dead.
func (s *Sender) startTask(ctx context.Context, task model.ReplicationTask) {
	s.mu.Lock()
	if _, running := s.inFlight[task.ChunkID]; running { // busca task en inFlight, y si sí esta, se sale
		s.mu.Unlock()
		return
	}

	select {
	case s.slots <- struct{}{}: // Intentamos introducir token en el canal para reservar slot
	default: // Con este, indicamos que si no pudimos reservar slot inmediatamente, se ejecute esto
		s.mu.Unlock()
		return // all slots busy, retry on a later heartbeat
	}
	s.inFlight[task.ChunkID] = struct{}{} // Añadimos en inFlight
	s.mu.Unlock()

	s.wg.Add(1) // Añadimos a waitGroup
	go func() { // Sale un nuevo goroutine, y startTask sigue su ejecución, retornando a tiempo para mandar siguiente beat en Run()
		// Cuando se termine esta funcion anonima, se marca como terminado en wg
		// y eliminamos chunkID de inFlight
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			delete(s.inFlight, task.ChunkID)
			s.mu.Unlock()
			<-s.slots // Liberamos slot
		}()

		taskCtx, cancel := context.WithTimeout(ctx, taskTimeout) // Se crea contexto derivado de ctx, con timeout de dos mins
		// es decir, cuando llege ese timeout, se cancelará goroutine automaticamente, sin necesidad de cancel()
		defer cancel() // devuelto por withtimeout, función que puedes llamar para cancelar ese contexto manualmente, util al finalizar funcion - >no tiene que esperar los 2 min de withtimeout, sino que la cancelas

		if err := s.runTask(taskCtx, task); err != nil { // Pasamos taskctx, y la task
			log.Printf("replication of chunk %s failed: %v", task.ChunkID, err)
		}
	}()
}

// runTask copies one chunk from another peer to this one.
//
// Steps for GET: (1) skip the download if we already have it, (2) try each source
// in order, (3) check the SHA-256 of what arrived against the hash the
// SUPERPEER gave us (never against anything the source peer says),
// (4) save atomically, (5) acknowledge.
//
// Like receiveChunk, it never decrypts anything: this peer has no keys.
func (s *Sender) runTask(ctx context.Context, task model.ReplicationTask) error {
	// DELETE tasks only remove the local chunk and then acknowledge it.
	if task.Action == model.ReplicationActionDelete {
		if err := s.store.Delete(task.OwnerUserID, task.ChunkID); err != nil {
			return fmt.Errorf("deleting chunk: %w", err)
		}
		return nil // No es necesario mandar ACK
	}

	// We may already hold the chunk if an earlier attempt saved it but
	// its ack was lost. Re-sending the ack is enough; downloading again
	// would waste bandwidth.
	if s.store.Exists(task.OwnerUserID, task.ChunkID) {
		return s.ack(task.ChunkID)
	}

	var errs []error
	for _, url := range task.SourceURLs {
		envelope, err := s.peers.FetchChunk(ctx, url, task.OwnerUserID, task.ChunkID)
		if err != nil {
			errs = append(errs, fmt.Errorf("fetching from %s: %w", url, err))
			continue // try the next source
		}

		if !hash.VerifySHA256(envelope, task.ExpectedHash) {
			// This source holds a corrupt copy. Do not save it, try
			// another replica.
			errs = append(errs, fmt.Errorf("source %s: hash mismatch", url))
			continue
		}

		// Save validates owner and chunk ID against path traversal, so
		// a malformed task cannot write outside the storage directory.
		if err := s.store.Save(task.OwnerUserID, task.ChunkID, envelope); err != nil {
			// Not the source's fault (disk, permissions, bad ID):
			// another source would fail the same way.
			return fmt.Errorf("saving chunk: %w", err)
		}
		return s.ack(task.ChunkID)
	}

	if len(errs) == 0 {
		return errors.New("task has no source URLs")
	}
	return fmt.Errorf("all sources failed: %w", errors.Join(errs...))
}

// ack confirms to the superpeer that the chunk is stored here. If it
// fails the chunk stays on disk and the next heartbeat lists it anyway,
// so the superpeer can still learn about it from the report.
func (s *Sender) ack(chunkID model.ChunkID) error {
	if err := s.superpeer.PutAck(chunkID, s.peerID); err != nil {
		return fmt.Errorf("put/ack: %w", err)
	}
	return nil
}
