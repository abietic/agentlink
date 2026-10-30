// agentlink-server is the Tanka bot endpoint: it receives app_mention events and hands
// them to paired worker devices. Put it behind HTTPS; Tanka only posts to https URLs.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agentlink/internal/server"
)

func main() {
	cfgPath := flag.String("config", "agentlink-server.json", "config file")
	flag.Parse()
	cfg, err := server.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	s, err := server.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go s.Reap(ctx)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      cfg.PollWait() + 30*time.Second, // long polls hold the response open
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Printf("agentlink-server listening on %s", cfg.Listen)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
