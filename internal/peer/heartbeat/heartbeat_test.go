package heartbeat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"dfsha/internal/peer/hash"
	"dfsha/internal/peer/http_client"
	"dfsha/internal/peer/storage"
	"dfsha/internal/shared/model"
)

func newTestStore(t *testing.T) *storage.Store {
	t.Helper()

	store, err := storage.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}

	return store
}

// fakeSuperpeer records what it receives and replies with canned tasks.
type fakeSuperpeer struct {
	mu         sync.Mutex
	payloads   []model.HeartbeatPayload // contendrá payloads que le manden en heartbeat
	acks       []model.ChunkID          // contendrá lista de chunks que han sido ack.
	tasks      []model.ReplicationTask  // contendrá las tareas
	heartbeatE error                    // para indicar que hubo error, y serpuesta sera con error
	// errFn, if set, decides the error of each heartbeat call (0-based).
	errFn func(call int) error // permite que el fakeSuperpeer simule errores de forma configurable según el número de heartbeat que recibe.
}

func (f *fakeSuperpeer) Heartbeat(_ context.Context, p model.HeartbeatPayload) (model.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	call := len(f.payloads)
	f.payloads = append(f.payloads, p)

	if f.errFn != nil {
		if err := f.errFn(call); err != nil {
			return model.HeartbeatResponse{}, err
		}
	}
	// si queremos que falle. Retorna resp vacia, y el error
	if f.heartbeatE != nil {
		return model.HeartbeatResponse{}, f.heartbeatE
	}
	return model.HeartbeatResponse{Tasks: f.tasks}, nil // retorna respuesta de SP, que es lista de task
}

func (f *fakeSuperpeer) PutAck(id model.ChunkID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.acks = append(f.acks, id)
	return nil
}

func (f *fakeSuperpeer) ackCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.acks)
}

// fakePeers serves chunks by URL and counts the calls.
type fakePeers struct {
	mu      sync.Mutex
	data    map[string][]byte // url -> envelope. Simula los chunks disponibles. La clave es la URL y el valor son los bytes del chunk cifrado (envelope)
	calls   []string          // Registra las URLs a las que se ha intentado acceder. Sirve para comprobar qué peers se consultaron durante una prueba.
	release chan struct{}     // if set, FetchChunk blocks until closed
	// Es un canal que sirve para enviar una señal. En este caso, la señal será cerrar el canal.
}

func (f *fakePeers) FetchChunk(ctx context.Context, url, _ string, _ model.ChunkID) ([]byte, error) {
	f.mu.Lock()

	f.calls = append(f.calls, url)
	data, ok := f.data[url]
	release := f.release

	f.mu.Unlock()

	if release != nil { // si canal fue configurado, entra
		select {
		case <-release: // La función deja de esperar y continúa su ejecución
		case <-ctx.Done(): // función deja de esperar y devuelve el error de cancelación o de deadline.
			return nil, ctx.Err()
		}
	}
	if !ok {
		return nil, errors.New("peer unreachable")
	}
	return data, nil
}

func (f *fakePeers) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.calls)
}

// waitFor polls until cond holds, so tests do not depend on fixed sleeps.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// fakeFailover is a two-superpeer list that records the rotations.
type fakeFailover struct {
	urls    []string // urls de SP activos
	active  int      // index de SP activo en urls
	rotated []string // failed URLs passed to Rotate -> historico
}

func newFakeFailover() *fakeFailover {
	return &fakeFailover{urls: []string{"http://sp-a", "http://sp-b"}}
}

func (f *fakeFailover) Active() string { return f.urls[f.active] }

// Retorna url de nuevo sp
func (f *fakeFailover) Rotate(failed string) string { // failed es una url
	f.rotated = append(f.rotated, failed)

	if f.urls[f.active] == failed { // si el que estaba como activo es el que pasaron com sp que habia fallado
		f.active = (f.active + 1) % len(f.urls) // avanzamos en lista
	}
	return f.urls[f.active]
}

func TestBeatReportsStateAndRunsTask(t *testing.T) {
	envelope := []byte("sealed-bytes") // chunk a pedir en task
	store := newTestStore(t)

	// almacenamos chunk para reportarle a sp
	if err := store.Save("u1", "fileA_0", []byte("x")); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// con &fake.. se crea un objeto (no es necesario constructor), siendo un puntero a ese objeto
	sp := &fakeSuperpeer{tasks: []model.ReplicationTask{{
		ChunkID: "fileB_3", OwnerUserID: "u1",
		SourceURLs: []string{"http://p2"}, ExpectedHash: hash.CalculateSHA256(envelope),
	}}}

	// fakepeer a solicitar task. Creamos su contenido falsamente (su almac. es un [] url -> envelope)
	peers := &fakePeers{data: map[string][]byte{"http://p2": envelope}}

	s := NewSender("peer-1", store, sp, peers, time.Hour, newFakeFailover())

	// contexto vacío que sirve como punto de partida para operaciones que necesitan un context.Context
	s.beat(context.Background()) // manda beat, ejecuta tareas y manda ack (a fakePeer, entonces quedará registrado en su ack[])
	s.wg.Wait()

	// The payload carries the peer ID, the free space and the stored chunk.
	p := sp.payloads[0] // obtenemos primer payload, que se debio haber añadido en beat
	if p.PeerID != "peer-1" || p.FreeSpace <= 0 || len(p.Chunks) != 1 || p.Chunks[0] != "fileA_0" {
		t.Fatalf("unexpected payload: %+v", p)
	}
	// The task was executed: saved and acknowledged.
	got, err := store.Load("u1", "fileB_3") // verificamos que si quedo en nosotros, subiendolo
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if string(got) != string(envelope) {
		t.Error("task chunk was not saved")
	}
	if sp.ackCount() != 1 {
		t.Errorf("got %d acks, want 1", sp.ackCount())
	}
}

func TestTaskSkipsCorruptSourceAndUsesNext(t *testing.T) {
	good := []byte("good-envelope") // chunk correcto
	store := newTestStore(t)

	sp := &fakeSuperpeer{}
	peers := &fakePeers{data: map[string][]byte{ // creamos 3 peers
		"http://bad":  []byte("corrupted"),
		"http://down": nil, // missing key below
		"http://good": good,
	}}

	delete(peers.data, "http://down") // unreachable source

	s := NewSender("peer-1", store, sp, peers, time.Hour, newFakeFailover())

	err := s.runTask(context.Background(), model.ReplicationTask{
		ChunkID: "f_0", OwnerUserID: "u1",
		SourceURLs:   []string{"http://down", "http://bad", "http://good"}, // debe llegar hasta good, 1ero is unreachable, 2do es corrupto
		ExpectedHash: hash.CalculateSHA256(good),
	})

	if err != nil {
		t.Fatalf("runTask: %v", err)
	}
	got, err := store.Load("u1", "f_0") // veridficamos que si se nos haya guardado
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if string(got) != string(good) {
		t.Error("the corrupt copy was saved or nothing was saved")
	}
	if peers.callCount() != 3 { // cantidad de urls que se intentaron
		t.Errorf("tried %d sources, want 3", peers.callCount())
	}
}

func TestTaskAllSourcesCorruptSavesNothing(t *testing.T) {
	store := newTestStore(t)
	sp := &fakeSuperpeer{}

	peers := &fakePeers{data: map[string][]byte{"http://bad": []byte("corrupted")}}

	s := NewSender("peer-1", store, sp, peers, time.Hour, newFakeFailover())
	err := s.runTask(context.Background(), model.ReplicationTask{
		ChunkID: "f_0", OwnerUserID: "u1",
		SourceURLs: []string{"http://bad"}, ExpectedHash: hash.CalculateSHA256([]byte("real")),
	})

	//como solo hay un peer, que tiene chunk corrupto, deberia retornar un error
	if err == nil {
		t.Fatal("expected an error")
	}
	chunks, listErr := store.ListChunkIDs()
	if listErr != nil {
		t.Fatalf("ListChunkIDs() error = %v", listErr)
	}
	if len(chunks) != 0 || sp.ackCount() != 0 { // no deberia haber guardado nada, ni haber mandado ack a sp
		t.Error("a corrupt chunk was stored or acknowledged")
	}
}

// se testea que si nos asignan tarea para chunk que ya teniamos, solo mandamos ack
func TestTaskAlreadyStoredOnlyAcks(t *testing.T) {
	store := newTestStore(t)
	if err := store.Save("u1", "f_0", []byte("already here")); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	sp := &fakeSuperpeer{}
	peers := &fakePeers{}
	s := NewSender("peer-1", store, sp, peers, time.Hour, newFakeFailover())

	err := s.runTask(context.Background(), model.ReplicationTask{ChunkID: "f_0", OwnerUserID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	if peers.callCount() != 0 { // no deberia haber llamado a nadie porque ya tenia chunk
		t.Error("downloaded a chunk that was already stored")
	}
	if sp.ackCount() != 1 {
		t.Errorf("got %d acks, want 1", sp.ackCount())
	}
}

func TestDeleteTaskRemovesLocalChunkOnly(t *testing.T) {
	store := newTestStore(t)
	if err := store.Save("u1", "f_0", []byte("to be deleted")); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	sp := &fakeSuperpeer{}
	peers := &fakePeers{}
	s := NewSender("peer-1", store, sp, peers, time.Hour, newFakeFailover())

	err := s.runTask(context.Background(), model.ReplicationTask{
		Action:     model.ReplicationActionDelete,
		ChunkID:    "f_0",
		OwnerUserID: "u1",
	})
	if err != nil {
		t.Fatalf("runTask: %v", err)
	}
	if store.Exists("u1", "f_0") {
		t.Fatal("delete task did not remove the local chunk")
	}
	if peers.callCount() != 0 {
		t.Errorf("delete task contacted %d peers, want 0", peers.callCount())
	}
	if sp.ackCount() != 1 {
		t.Errorf("delete task sent %d acks, want 1", sp.ackCount())
	}
}

func TestTaskWithoutSourcesFails(t *testing.T) {
	s := NewSender("p", newTestStore(t), &fakeSuperpeer{}, &fakePeers{}, time.Hour, newFakeFailover())

	if err := s.runTask(context.Background(), model.ReplicationTask{ChunkID: "f_0", OwnerUserID: "u1"}); err == nil {
		t.Error("expected an error for a task with no sources")
	}
}

func TestSameTaskIsNotStartedTwiceWhileRunning(t *testing.T) {
	envelope := []byte("slow")
	store := newTestStore(t)

	// task que soliciará envelope
	task := model.ReplicationTask{
		ChunkID: "f_0", OwnerUserID: "u1",
		SourceURLs: []string{"http://p2"}, ExpectedHash: hash.CalculateSHA256(envelope),
	}

	sp := &fakeSuperpeer{tasks: []model.ReplicationTask{task}} // sp con task
	// peers con realese
	peers := &fakePeers{data: map[string][]byte{"http://p2": envelope}, release: make(chan struct{})}
	s := NewSender("peer-1", store, sp, peers, time.Hour, newFakeFailover())

	// Three heartbeats arrive while the download is still blocked.
	// Con realese, entra en select, y se queda esperando a que lo cierren, entonces task no se termina mientras manda los sig heartbeats, que traeran misma tarea
	// No deberia tomarla porque ya está en inFlight
	s.beat(context.Background())
	s.beat(context.Background())
	s.beat(context.Background())
	close(peers.release)
	s.wg.Wait()

	if peers.callCount() != 1 { // solo se debió haber llamado una vez, no se debio volver a ejecutar tarea
		t.Errorf("task ran %d times, want 1", peers.callCount())
	}
	if len(s.inFlight) != 0 || len(s.slots) != 0 { // Tarea debe ser limpiada de inflight, porque ya se termino (por wait)
		t.Error("in-flight state was not cleaned up")
	}
}

func TestParallelTasksAreBounded(t *testing.T) {
	store := newTestStore(t)
	sp := &fakeSuperpeer{}

	peers := &fakePeers{data: map[string][]byte{}, release: make(chan struct{})} // realease para que no termine uno, y pueda seguir otra task. Queremos que queden llenas por un tiempo
	s := NewSender("peer-1", store, sp, peers, time.Hour, newFakeFailover())

	// More tasks than slots, all blocked in FetchChunk.
	for i := 0; i < defaultMaxParallelTasks+3; i++ {
		id := model.ChunkID("f_" + string(rune('0'+i)))

		sp.tasks = append(sp.tasks, model.ReplicationTask{
			ChunkID: id, OwnerUserID: "u1", SourceURLs: []string{"http://p2"},
		})
	}

	s.beat(context.Background()) // le llegan 4+3 task, debe descartar resto
	// esperamos que slots se llenen, es como un sleep controlado hasta que se ejecute, o que se alcence tiempo más de espera
	// porque si siguieramos con test sin esperar, no tendría sentido
	waitFor(t, "slots to fill", func() bool { return peers.callCount() == defaultMaxParallelTasks })

	if got := peers.callCount(); got != defaultMaxParallelTasks { // solo se llamaron los que obtuvieron slot
		t.Errorf("%d tasks running, limit is %d", got, defaultMaxParallelTasks)
	}
	close(peers.release)
	s.wg.Wait() // esperamos que se termine ejecución de hilos
}

// que no se mande heartbeat si no se pudieron obtener chunks guardadas con listingIDs
func TestBeatSkippedWhenListingFails(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewStore(root)

	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("RemoveAll() error = %v", err)
	}
	sp := &fakeSuperpeer{}
	s := NewSender("peer-1", store, sp, &fakePeers{}, time.Hour, newFakeFailover())

	s.beat(context.Background()) // en beat se maneja que si no se puede obtener lsiting, no se manda

	if len(sp.payloads) != 0 { // no le debe llegar nada al sp
		t.Error("a heartbeat with unknown chunk state was sent")
	}
}

func TestHeartbeatErrorDoesNotRunTasksOrPanic(t *testing.T) {
	sp := &fakeSuperpeer{heartbeatE: errors.New("superpeer down")} // creamos con err, y cuando en beat
	// se llame a hertbeat, se llamará nuestra implementación, donde si hay err, retorna err, que
	// llegará a flujo de beat y mandará a handleErr..

	s := NewSender("peer-1", newTestStore(t), sp, &fakePeers{}, time.Hour, newFakeFailover())
	s.beat(context.Background()) // must just log and return (because its the first time)
}

// comprueba que Run envía un heartbeat inmediatamente, sigue enviándolos periódicamente y se detiene cuando se cancela el contexto.
func TestRunSendsImmediatelyThenPeriodicallyAndStops(t *testing.T) {
	sp := &fakeSuperpeer{}
	s := NewSender("peer-1", newTestStore(t), sp, &fakePeers{}, 20*time.Millisecond, newFakeFailover())

	// contexto que recibe run, y que nos permite cancelarlo
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}) // creamos canal para avisarle a ctx que termine
	// ejecutamos run en otra goroutine
	go func() { s.Run(ctx); close(done) }() // cuando run termina, se ejecuta close(done)

	// esperamos a que hayan 3 heratbeats
	waitFor(t, "3 heartbeats", func() bool {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		return len(sp.payloads) >= 3
	})
	cancel() // cancelamos contexto

	// verificamos que terminó correctamente
	select {
	case <-done: // canal se cerró -> terminó bien
	case <-time.After(2 * time.Second): // pasaron 2 seg y no ha terminado -> test falla
		t.Fatal("Run did not return after cancel")
	}
}

var errDown = fmt.Errorf("connection refused: %w", http_client.ErrSuperpeerUnavailable)

func TestTwoFailuresDoNotSwitchThirdDoes(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewStore(root)

	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}

	// Every call to sp-a fails; the new one (call 3, the immediate probe) works.
	sp := &fakeSuperpeer{errFn: func(call int) error {
		if call < 3 { // porque queremos que hayan 3 fallos y haga switch
			return errDown
		}
		return nil
	}}

	fo := newFakeFailover()
	s := NewSender("peer-1", store, sp, &fakePeers{}, time.Hour, fo)

	s.beat(context.Background())
	s.beat(context.Background())
	if len(fo.rotated) != 0 { // no deberia rotar por dos fallos, sino 3
		t.Fatalf("rotated after 2 failures: %v", fo.rotated)
	}

	s.beat(context.Background()) // 3rd failure -> rotate -> immediate probe
	if len(fo.rotated) != 1 || fo.rotated[0] != "http://sp-a" {
		t.Fatalf("rotations = %v, want [http://sp-a]", fo.rotated)
	}
	if fo.Active() != "http://sp-b" {
		t.Errorf("active = %s, want http://sp-b", fo.Active())
	}
	// 3 failed beats + 1 immediate probe on the new superpeer.
	if len(sp.payloads) != 4 { // porque en deat, entran a handleError, y ahi si es 3, se hace beat a nuevo superpeer
		// en rotate es que se settea como active el siguiente sp
		t.Errorf("sent %d heartbeats, want 4 (3 failures + probe)", len(sp.payloads))
	}
	if s.consecutiveFailures != 0 {
		t.Errorf("counter = %d after a successful probe, want 0", s.consecutiveFailures)
	}
}

func TestSuccessResetsFailureCounter(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewStore(root)

	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}

	// fail, fail, ok, fail, fail: never 3 in a row.
	pattern := []bool{true, true, false, true, true}
	sp := &fakeSuperpeer{errFn: func(call int) error {
		if pattern[call] {
			return errDown
		}
		return nil
	}}
	fo := newFakeFailover()
	s := NewSender("peer-1", store, sp, &fakePeers{}, time.Hour, fo)

	for range pattern {
		s.beat(context.Background()) // aqui se llama a healthcheck, que fake implementa con errFn usando len de call
	}
	if len(fo.rotated) != 0 {
		t.Errorf("rotated although failures were never consecutive: %v", fo.rotated)
	}
}

// un error de rechazo, como un 401 Unauthorized, no debe contarse como una caída del superpeer y debe reiniciar el contador de fallos consecutivos.
func TestRefusalIsNotCountedAndResetsCounter(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewStore(root)

	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}

	// A 401-style error does NOT wrap ErrSuperpeerUnavailable.
	errRefused := errors.New("401 unauthorized")
	sp := &fakeSuperpeer{errFn: func(call int) error {
		if call == 0 || call == 1 { // simula dos fallos (sp unavailable), y luego rechazos
			return errDown
		}
		return errRefused
	}}

	fo := newFakeFailover()
	s := NewSender("peer-1", store, sp, &fakePeers{}, time.Hour, fo)

	for i := 0; i < 6; i++ {
		s.beat(context.Background())
	}
	if len(fo.rotated) != 0 { // rotó para un error que no indicaba que sp estaba caido
		t.Errorf("rotated on refusals: %v", fo.rotated)
	}
	if s.consecutiveFailures != 0 { // 3er error no era de ripo unavailable sp, entonces deberia resetear contador
		t.Errorf("counter = %d, want 0 (a refusal proves the superpeer is alive)", s.consecutiveFailures)
	}
}

func TestSingleSuperpeerStaysPut(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewStore(root)

	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}

	sp := &fakeSuperpeer{heartbeatE: errDown}
	fo := &fakeFailover{urls: []string{"http://only"}}
	s := NewSender("peer-1", store, sp, &fakePeers{}, time.Hour, fo)

	for i := 0; i < failuresBeforeSwitch; i++ {
		s.beat(context.Background()) // fallarán porque sp tiene heratbeatE
	}
	// Rotate was asked, but the URL did not change, so no probe beat.
	if len(sp.payloads) != failuresBeforeSwitch { // no manda beat si es mismo, por eso payloads tiene solo las que trigerearon switch
		t.Errorf("sent %d heartbeats, want %d (no immediate probe)", len(sp.payloads), failuresBeforeSwitch)
	}
	if fo.Active() != "http://only" {
		t.Errorf("active = %s", fo.Active())
	}
}

func TestCancelledContextIsNotAFailure(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewStore(root)

	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}

	sp := &fakeSuperpeer{heartbeatE: errDown}
	fo := newFakeFailover()

	s := NewSender("peer-1", store, sp, &fakePeers{}, time.Hour, fo)

	// creamos y cancelamos contexto
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // shutting down

	for i := 0; i < 5; i++ {
		s.beat(ctx) // fallarán por heartBeatE, pero no deberia importar porque contexto se canceló
	}
	if len(fo.rotated) != 0 || s.consecutiveFailures != 0 {
		t.Error("failures during shutdown were counted")
	}
}
