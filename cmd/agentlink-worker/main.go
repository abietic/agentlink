// agentlink-worker runs on the machine that does the work: it pulls tasks for this
// device from agentlink-server and runs them with Claude Code.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"agentlink/internal/worker"
)

func main() {
	home, _ := os.UserHomeDir()
	cfgPath := flag.String("config", filepath.Join(home, ".agentlink", "worker.json"), "config file")
	flag.Parse()
	cfg, err := worker.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	w, err := worker.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := w.Register(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("device %s ready; pair a Tanka room by mentioning the bot with: pair %s", w.DeviceID(), w.DeviceID())
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
