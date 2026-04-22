// Turso Sync Example
//
// Demonstrates how to use the Go sync bindings to:
//   - Open a local database that syncs with a Turso Cloud remote
//   - Perform periodic reads and writes against the local DB
//   - Run a background worker that calls Push() and Pull() to sync with remote
//   - Run a background worker that calls Checkpoint() to truncate the local WAL
//
// Push/Pull and Checkpoint cannot run concurrently, so the two workers are
// offset by ~30s. The high-level TursoSyncDb wrapper already serializes
// these operations via an internal mutex, but offsetting avoids contention.
//
// Environment variables:
//   TURSO_DATABASE_URL - remote database URL (libsql://, https://, or http://)
//   TURSO_AUTH_TOKEN   - auth token for the remote database

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	turso "turso.tech/database/tursogo"
)

const (
	localDbPath  = "local.db"
	syncInterval = 60 * time.Second
	workerOffset = 30 * time.Second
	writeEvery   = 5 * time.Second
	readEvery    = 7 * time.Second
)

func main() {
	remoteUrl := os.Getenv("TURSO_DATABASE_URL")
	authToken := os.Getenv("TURSO_AUTH_TOKEN")
	if remoteUrl == "" {
		log.Fatal("TURSO_DATABASE_URL env var is required")
	}
	if authToken == "" {
		log.Fatal("TURSO_AUTH_TOKEN env var is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("opening synced database at %s (remote=%s)", localDbPath, remoteUrl)
	syncDb, err := turso.NewTursoSyncDb(ctx, turso.TursoSyncDbConfig{
		Path:       localDbPath,
		RemoteUrl:  remoteUrl,
		AuthToken:  authToken,
		ClientName: "turso-sync-go-example",
	})
	if err != nil {
		log.Fatalf("NewTursoSyncDb: %v", err)
	}

	db, err := syncDb.Connect(ctx)
	if err != nil {
		log.Fatalf("Connect: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS events (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			message    TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)
	`); err != nil {
		log.Fatalf("create table: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); runWriter(ctx, db) }()
	go func() { defer wg.Done(); runReader(ctx, db) }()
	go func() { defer wg.Done(); runSyncWorker(ctx, syncDb) }()
	go func() {
		defer wg.Done()
		// offset the checkpoint worker by ~30s so Push/Pull and Checkpoint
		// don't immediately collide on the same tick.
		select {
		case <-ctx.Done():
			return
		case <-time.After(workerOffset):
		}
		runCheckpointWorker(ctx, syncDb)
	}()

	<-ctx.Done()
	log.Printf("shutdown signal received, waiting for workers...")
	wg.Wait()
	log.Printf("bye")
}

func runWriter(ctx context.Context, db *sql.DB) {
	t := time.NewTicker(writeEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			msg := fmt.Sprintf("hello-%d", rand.Intn(1_000_000))
			res, err := db.ExecContext(ctx,
				"INSERT INTO events (message, created_at) VALUES (?, ?)",
				msg, time.Now().Unix())
			if err != nil {
				log.Printf("[writer] insert failed: %v", err)
				continue
			}
			id, _ := res.LastInsertId()
			log.Printf("[writer] inserted id=%d message=%q", id, msg)
		}
	}
}

func runReader(ctx context.Context, db *sql.DB) {
	t := time.NewTicker(readEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var count int64
			var latest sql.NullString
			row := db.QueryRowContext(ctx,
				"SELECT COUNT(*), MAX(message) FROM events")
			if err := row.Scan(&count, &latest); err != nil {
				log.Printf("[reader] query failed: %v", err)
				continue
			}
			log.Printf("[reader] rows=%d latest=%s", count, latest.String)
		}
	}
}

func runSyncWorker(ctx context.Context, syncDb *turso.TursoSyncDb) {
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := syncDb.Push(ctx); err != nil {
				log.Printf("[sync] push failed: %v", err)
			} else {
				log.Printf("[sync] push ok")
			}
			changed, err := syncDb.Pull(ctx)
			if err != nil {
				log.Printf("[sync] pull failed: %v", err)
				continue
			}
			log.Printf("[sync] pull ok (changes=%v)", changed)
			if stats, err := syncDb.Stats(ctx); err == nil {
				log.Printf("[sync] stats wal=%d revert=%d sent=%d recv=%d",
					stats.MainWalSize, stats.RevertWalSize,
					stats.NetworkSentBytes, stats.NetworkReceivedBytes)
			}
		}
	}
}

func runCheckpointWorker(ctx context.Context, syncDb *turso.TursoSyncDb) {
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := syncDb.Checkpoint(ctx); err != nil {
				log.Printf("[checkpoint] failed: %v", err)
				continue
			}
			log.Printf("[checkpoint] ok")
		}
	}
}
