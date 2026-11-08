// agentlink-server is the Tanka bot endpoint: it receives app_mention events and hands
// them to paired worker devices. Put it behind HTTPS; Tanka only posts to https URLs.
//
// `agentlink-server stats [-config f] [-days n] [-task id]` prints the devices, rooms and
// task log from the server's database; it only reads, so it runs beside the server.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agentlink/internal/jsonfile"
	"agentlink/internal/server"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "stats" {
		stats(os.Args[2:])
		return
	}
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
	defer s.Close()
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
		log.Fatal(err) // SQLite recovers from the unclosed database on the next start
	}
}

func stats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	cfgPath := fs.String("config", "agentlink-server.json", "config file, for db_path")
	dbPath := fs.String("db", "", "database (default: db_path from the config)")
	days := fs.Int("days", 7, "count tasks queued in the last n days")
	task := fs.String("task", "", "show everything logged about this task id")
	fs.Parse(args)
	if *dbPath == "" {
		// Only db_path is needed: the secrets in the server's environment are not.
		var c struct {
			DBPath string `json:"db_path"`
		}
		if _, err := jsonfile.Load(*cfgPath, &c); err != nil {
			log.Fatal(err)
		}
		*dbPath = cmp.Or(c.DBPath, "agentlink-server.db")
	}
	if err := server.Stats(os.Stdout, *dbPath, *days, *task, time.Now()); err != nil {
		log.Fatal(err)
	}
}
