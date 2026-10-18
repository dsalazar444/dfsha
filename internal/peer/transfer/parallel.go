package transfer

import (
	"context"
	"sync"
)

// forEach runs fn(ctx, i) for every i in [0, n), with at most limit calls
// running at the same time.
//
// On the first error it cancels the context given to the other calls,
// stops starting new ones, waits for the running ones, and returns that
// first error (not the "context canceled" errors the cancellation causes
// in the others). If the parent ctx is cancelled it returns ctx.Err().
//
// It exists because the standard library has no bounded, cancel-on-error
// group, and a third-party one is not worth a dependency for ten lines.
//
// Recibe
// ctx	Contexto que permite cancelar las operaciones desde fuera.
// n	Número de operaciones que se quieren ejecutar.
// limit	Máximo de operaciones simultáneas.
// fn	Función que se ejecutará para cada índice i.
func forEach(ctx context.Context, n, limit int, fn func(ctx context.Context, i int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// preparamos sincronización
	var (
		wg       sync.WaitGroup
		once     sync.Once // garantiza que una operación se ejecute una sola vez, aunque varias goroutines intenten ejecutarla simultáneamente.
		firstErr error     // almacena el primer error que se registre
	)
	// Record the error BEFORE cancelling: calls that fail only because of
	// the cancellation then lose the race to be "first".

	// Esta función recibe un error y hace dos cosas, una sola vez:
	// 1. Guarda el error en firstErr.
	// 2. Cancela el contexto compartido por las operaciones.
	fail := func(err error) {
		once.Do(func() {
			firstErr = err
			cancel()
		})
	}

	// Crear el semáforo para limitar cuantas goroutines pueden ejecutar fn al mismo tiempo -> que será el limit de concurrencia que se pasa
	sem := make(chan struct{}, limit) // counting semaphore: one token per running call

loop:
	for i := 0; i < n; i++ { // Recorremos cantidad de tareas
		select { // esperamos una plaza en semaforo o detectar cancelación
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}

		wg.Add(1)
		go func() { // ejecutan operacion en paralelo
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, i); err != nil {
				fail(err)
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
