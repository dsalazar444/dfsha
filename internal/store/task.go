// Package store holds in-memory data stores for the superpeer

package store

import "sync"

// DeleteTask es la orden "borra este chunk" que un peer recibe en la respuesta de su heartbeat
type DeleteTask struct {
	ChunkID     string
	OwnerUserID string // el peer lo necesita para ubicar el chunk en su disco
}

// TaskStore guarda las tareas de borrado pendientes de cada peer.
// Una tarea se reenvía en cada heartbeat hasta que el peer deja de reportar el chunk,
// así un peer que estaba caído al momento del borrado lo recibe cuando vuelve
type TaskStore struct {
	mu      sync.Mutex
	deletes map[string]map[string]DeleteTask // peerURL -> chunkID -> tarea
}

// NewTaskStore crea un TaskStore vacío
func NewTaskStore() *TaskStore {
	return &TaskStore{deletes: make(map[string]map[string]DeleteTask)}
}

// AddDelete agrega una tarea de borrado para un peer (si ya existía, no se duplica)
func (s *TaskStore) AddDelete(peerURL, chunkID, ownerUserID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.deletes[peerURL] == nil {
		s.deletes[peerURL] = make(map[string]DeleteTask)
	}
	s.deletes[peerURL][chunkID] = DeleteTask{ChunkID: chunkID, OwnerUserID: ownerUserID}
}

// PendingDeletes devuelve las tareas de borrado que el peer todavía no cumplió
func (s *TaskStore) PendingDeletes(peerURL string) []DeleteTask {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]DeleteTask, 0, len(s.deletes[peerURL]))
	for _, task := range s.deletes[peerURL] {
		tasks = append(tasks, task)
	}
	return tasks
}

// CompleteDeletes quita las tareas cuyo chunk ya no aparece en la lista del heartbeat:
// si el peer ya no lo tiene, el borrado está hecho (o no había nada que borrar)
func (s *TaskStore) CompleteDeletes(peerURL string, heldChunkIDs []string) {
	held := make(map[string]bool, len(heldChunkIDs))
	for _, chunkID := range heldChunkIDs {
		held[chunkID] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for chunkID := range s.deletes[peerURL] {
		if !held[chunkID] {
			delete(s.deletes[peerURL], chunkID)
		}
	}
	if len(s.deletes[peerURL]) == 0 {
		delete(s.deletes, peerURL)
	}
}
