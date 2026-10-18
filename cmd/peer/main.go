package peer

// Command peer is a DFSha peer: it stores chunks for other peers, serves
// them on request, reports to the superpeer, and gives its user an
// interactive command line to work with the distributed file system.
//
// Configuration comes from a .env file and environment variables (see
// internal/peer/config). The password is asked at startup and never
// stored.

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"dfsha/internal/peer/config"

	"golang.org/x/term"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("peer: %v", err)
	}
}

// cargar la configuración, preparar la cancelación del programa y construir la aplicación para finalmente ejecutarla.
func run() error {
	cfg, err := config.Load(".env")
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	// Ctrl-C and SIGTERM cancel ctx; every long-running part (heartbeat,
	// transfers, the command line) stops when it is cancelled.
	// crea contexto que responde a ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a := &app{
		cfg:      cfg,
		in:       bufio.NewReader(os.Stdin),
		out:      os.Stdout,
		terminal: term.IsTerminal(int(os.Stdin.Fd())),
	}
	return a.run(ctx) // Llama a run de app
}
