package leader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Tiempos cortos para que la prueba dure poco: el lease vence a los 300 ms y se revisa cada 50 ms
const (
	testTTL   = 300 * time.Millisecond
	testRenew = 50 * time.Millisecond
)

// waitFor repite cond hasta que se cumpla o pase el tiempo límite
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestStandbyTakesOverWhenLeaderDies(t *testing.T) {
	dir := t.TempDir()
	var promotions atomic.Int32
	onPromote := func() error { promotions.Add(1); return nil }

	// sp1 arranca primero: no hay lease, queda como líder
	ctx1, stop1 := context.WithCancel(context.Background())
	sp1 := New(dir, "sp1", testTTL, testRenew, onPromote)
	sp1.Run(ctx1)
	if !sp1.IsLeader() {
		t.Fatal("first superpeer should become leader")
	}

	// sp2 arranca después: el lease de sp1 está vigente, queda en standby
	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()
	sp2 := New(dir, "sp2", testTTL, testRenew, onPromote)
	sp2.Run(ctx2)
	time.Sleep(2 * testTTL) // aunque pase más de un TTL, sp1 renueva y sp2 sigue en standby
	if sp2.IsLeader() {
		t.Fatal("standby took over while the leader was alive")
	}

	// sp1 "muere": deja de renovar. Cuando su lease vence, sp2 toma el control
	stop1()
	waitFor(t, "sp2 becomes leader", sp2.IsLeader)
	if promotions.Load() != 2 { // una vez sp1 al arrancar, otra sp2 al tomar el control
		t.Fatalf("onPromote ran %d times, want 2 (it must load the state on every promotion)", promotions.Load())
	}

	// sp1 vuelve: ve el lease de sp2 vigente y se queda en standby (no le quita el control)
	ctx3, stop3 := context.WithCancel(context.Background())
	defer stop3()
	sp1again := New(dir, "sp1", testTTL, testRenew, onPromote)
	sp1again.Run(ctx3)
	time.Sleep(2 * testTTL)
	if sp1again.IsLeader() || !sp2.IsLeader() {
		t.Fatal("the superpeer that came back must stay on standby")
	}
}

func TestLeaderStepsDownWhenLeaseIsTaken(t *testing.T) {
	dir := t.TempDir()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	sp1 := New(dir, "sp1", testTTL, testRenew, func() error { return nil })
	sp1.Run(ctx)
	if !sp1.IsLeader() {
		t.Fatal("sp1 should be leader")
	}

	// Simula que sp2 tomó el lease (por ejemplo, sp1 estuvo congelado más que el TTL)
	other := New(dir, "sp2", time.Hour, testRenew, nil)
	if err := other.write(lease{Owner: "sp2", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "sp1 steps down", func() bool { return !sp1.IsLeader() })
}

func TestNoLeadershipIfStateCannotBeLoaded(t *testing.T) {
	sp := New(t.TempDir(), "sp1", testTTL, testRenew, func() error { return context.DeadlineExceeded })
	sp.check()
	if sp.IsLeader() {
		t.Fatal("a superpeer that cannot load the saved state must not lead")
	}
}
