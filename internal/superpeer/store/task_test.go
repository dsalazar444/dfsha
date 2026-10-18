package store

import "testing"

func TestTaskStoreDeletes(t *testing.T) {
	s := NewTaskStore()
	s.AddDelete("http://p1:9000", "f1_0", "u1")
	s.AddDelete("http://p1:9000", "f1_0", "u1") // repetida: no se duplica
	s.AddDelete("http://p1:9000", "f1_1", "u1")

	if got := len(s.PendingDeletes("http://p1:9000")); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}

	// El peer todavía reporta f1_0 (no lo borró) pero ya no f1_1: solo f1_1 queda cumplida
	s.CompleteDeletes("http://p1:9000", []string{"f1_0", "otro_0"})
	pending := s.PendingDeletes("http://p1:9000")
	if len(pending) != 1 || pending[0].ChunkID != "f1_0" || pending[0].OwnerUserID != "u1" {
		t.Fatalf("pending = %+v, want only f1_0 of u1", pending)
	}

	// Cuando deja de reportar f1_0, no queda nada pendiente
	s.CompleteDeletes("http://p1:9000", nil)
	if got := len(s.PendingDeletes("http://p1:9000")); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
}
