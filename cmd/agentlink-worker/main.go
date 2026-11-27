// agentlink-worker runs on the machine that does the work: it pulls tasks for this
// device from agentlink-server and runs them with Claude Code.
//
// `agentlink-worker set-secret tanka` reads a secret from stdin and stores it in worker.db
// (encrypted with worker.key); an empty stdin removes it.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"agentlink/internal/worker"
)

func main() {
	home, _ := os.UserHomeDir()
	defaultConfig := filepath.Join(home, ".agentlink", "worker.json")

	if len(os.Args) > 1 && os.Args[1] == "set-secret" {
		fs := flag.NewFlagSet("set-secret", flag.ExitOnError)
		cfgPath := fs.String("config", defaultConfig, "config file")
		fs.Parse(os.Args[2:])
		if fs.NArg() != 1 {
			log.Fatal("usage: agentlink-worker set-secret [-config file] tanka   (the secret on stdin; empty removes it)")
		}
		cfg, err := worker.LoadConfig(*cfgPath)
		if err != nil {
			log.Fatal(err)
		}
		secret, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
		if err != nil {
			log.Fatal("reading the secret: ", err)
		}
		secret = bytes.TrimSpace(secret)
		if err := worker.SetSecret(cfg, fs.Arg(0), secret); err != nil {
			log.Fatal(err)
		}
		if len(secret) == 0 {
			log.Printf("tanka secret removed from %s", cfg.StateDir)
		} else {
			log.Printf("tanka secret stored in %s", cfg.StateDir)
		}
		return
	}

	cfgPath := flag.String("config", defaultConfig, "config file")
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
