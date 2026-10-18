// Package files  cómo el superpeer pide a los peers que borren chunks

package files

import "dfsha/internal/store"

// ChunkDeleter es "algo que sabe pedirle a un peer que borre un chunk"
// Es una interfaz para que Service no dependa de cómo se entrega la orden
// (en las pruebas se usa uno falso que solo anota las llamadas)
type ChunkDeleter interface {
	DeleteChunk(peerURL, chunkID, ownerUserID string) error
}

// TaskChunkDeleter no llama al peer: deja la orden como tarea pendiente,
// y el peer la recibe en la respuesta de su próximo heartbeat.
// Así el borrado se reintenta solo si el peer estaba caído
type TaskChunkDeleter struct {
	tasks *store.TaskStore
}

// NewTaskChunkDeleter crea un TaskChunkDeleter que guarda las tareas en el TaskStore dado
func NewTaskChunkDeleter(tasks *store.TaskStore) *TaskChunkDeleter {
	return &TaskChunkDeleter{tasks: tasks}
}

// DeleteChunk encola la tarea de borrado para ese peer (nunca falla)
func (d *TaskChunkDeleter) DeleteChunk(peerURL, chunkID, ownerUserID string) error {
	d.tasks.AddDelete(peerURL, chunkID, ownerUserID)
	return nil
}
