package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
	turso "turso.tech/database/tursogo"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Context key for worker ID
type contextKey string

const workerIDKey contextKey = "worker_id"

// Custom logger that logs ALL statements with worker ID
type WorkerLogger struct{}

func (l *WorkerLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	return l
}

func (l *WorkerLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	log.Printf("[INFO] "+msg, data...)
}

func (l *WorkerLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	log.Printf("[WARN] "+msg, data...)
}

func (l *WorkerLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	log.Printf("[ERROR] "+msg, data...)
}

func (l *WorkerLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, rows := fc()
	elapsed := time.Since(begin)
	workerID := "http"
	if id := ctx.Value(workerIDKey); id != nil {
		workerID = fmt.Sprintf("worker-%v", id)
	}
	if err != nil {
		log.Printf("[%s] [%.3fms] [rows:%d] [ERROR: %v] %s", workerID, float64(elapsed.Nanoseconds())/1e6, rows, err, sql)
	} else {
		log.Printf("[%s] [%.3fms] [rows:%d] %s", workerID, float64(elapsed.Nanoseconds())/1e6, rows, sql)
	}
}

// Model for stress testing
type Record struct {
	ID        uint   `gorm:"primarykey" json:"id"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
	Name      string `gorm:"index" json:"name"`
	Value     int    `json:"value"`
	Data      string `json:"data"`
}

// Stats tracking
type Stats struct {
	Inserts     atomic.Int64
	Updates     atomic.Int64
	Deletes     atomic.Int64
	Selects     atomic.Int64
	Errors      atomic.Int64
	Checkpoints atomic.Int64
}

var (
	db              *gorm.DB
	stats           Stats
	checkpointMutex sync.Mutex
	// workerPauseMu is used to pause all workers during integrity checks.
	// Workers hold a read lock, integrity check holds a write lock.
	workerPauseMu sync.RWMutex
	// dbPath is stored globally for the integrity check worker
	globalDbPath string
)

func main() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "stress_test.db"
	}
	globalDbPath = dbPath

	checkpointInterval := 1000 * time.Millisecond
	if intervalStr := os.Getenv("CHECKPOINT_INTERVAL_MS"); intervalStr != "" {
		if ms, err := time.ParseDuration(intervalStr + "ms"); err == nil {
			checkpointInterval = ms
		}
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Number of concurrent stress workers
	numWorkers := 10
	if workerStr := os.Getenv("NUM_WORKERS"); workerStr != "" {
		if n, err := fmt.Sscanf(workerStr, "%d", &numWorkers); err == nil && n > 0 {
			_ = n
		}
	}

	// Initialize turso library with "system" strategy to load from DYLD_LIBRARY_PATH
	turso.InitLibrary(turso_libs.LoadTursoLibraryConfig{
		LoadStrategy: "system",
	})

	// Initialize database with turso driver
	// Use _busy_timeout to wait up to 5 seconds when database is locked
	dsn := dbPath + "?_busy_timeout=5000"
	var err error

	// Create custom logger that logs ALL SQL statements with worker ID
	customLogger := &WorkerLogger{}

	db, err = gorm.Open(sqlite.Dialector{
		DriverName: "turso",
		DSN:        dsn,
	}, &gorm.Config{
		Logger: customLogger,
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Configure connection pool
	// Note: busy_timeout PRAGMA only applies to this connection, not pooled connections
	// The turso driver doesn't support ?_busy_timeout=N in DSN yet
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to get underlying sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(0)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// Set WAL mode
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		log.Fatalf("Failed to set journal mode: %v", err)
	}

	// Auto-migrate the schema
	if err := db.AutoMigrate(&Record{}); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	log.Printf("Database initialized at %s", dbPath)
	log.Printf("Checkpoint interval: %v", checkpointInterval)

	// Start background checkpoint worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go checkpointWorker(ctx, checkpointInterval)

	// Start stats reporter
	go statsReporter(ctx)

	// Start integrity check worker (every 30 seconds)
	go integrityCheckWorker(ctx, 30*time.Second)

	// Setup HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/insert", handleInsert)
	mux.HandleFunc("/update", handleUpdate)
	mux.HandleFunc("/delete", handleDelete)
	mux.HandleFunc("/select", handleSelect)
	mux.HandleFunc("/random", handleRandom)
	mux.HandleFunc("/bulk", handleBulk)
	mux.HandleFunc("/stats", handleStats)
	mux.HandleFunc("/health", handleHealth)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down...")
		cancel()
		server.Shutdown(context.Background())
	}()

	log.Printf("Server starting on port %s", port)
	log.Printf("Endpoints: /insert, /update, /delete, /select, /random, /bulk, /stats, /health")
	log.Printf("Starting %d stress workers", numWorkers)

	// Start stress workers that hammer the HTTP endpoints
	baseURL := fmt.Sprintf("http://localhost:%s", port)
	for i := 0; i < numWorkers; i++ {
		go stressWorker(ctx, i, baseURL)
	}

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}

	log.Println("Server stopped")
}

// Background checkpoint worker - simulates production checkpoint behavior
func checkpointWorker(ctx context.Context, interval time.Duration) {
	log.Println("Checkpoint worker started")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Checkpoint worker stopped")
			return
		case <-ticker.C:
			if err := doCheckpoint(); err != nil {
				log.Printf("Checkpoint error: %v", err)
				stats.Errors.Add(1)
			} else {
				stats.Checkpoints.Add(1)
			}
		}
	}
}

func doCheckpoint() error {
	checkpointMutex.Lock()
	defer checkpointMutex.Unlock()

	// Use a raw SQL connection to run checkpoint
	// This simulates what Gorm does in production
	modes := []string{"TRUNCATE", "RESTART", "FULL", "PASSIVE"}
	mode := modes[rand.Intn(len(modes))]
	sql := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode)
	log.Printf("[checkpoint] Executing: %s", sql)
	result := db.Exec(sql)
	if result.Error != nil {
		log.Printf("[checkpoint] Error: %v", result.Error)
	} else {
		log.Printf("[checkpoint] Success: %s", mode)
	}
	return result.Error
}

// Stats reporter
func statsReporter(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("Stats - Inserts: %d, Updates: %d, Deletes: %d, Selects: %d, Checkpoints: %d, Errors: %d",
				stats.Inserts.Load(),
				stats.Updates.Load(),
				stats.Deletes.Load(),
				stats.Selects.Load(),
				stats.Checkpoints.Load(),
				stats.Errors.Load(),
			)
		}
	}
}

// Integrity check worker - periodically pauses all workers and runs sqlite3 integrity_check
func integrityCheckWorker(ctx context.Context, interval time.Duration) {
	log.Println("Integrity check worker started")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Integrity check worker stopped")
			return
		case <-ticker.C:
			doIntegrityCheck()
		}
	}
}

func doIntegrityCheck() {
	log.Println("[integrity] Pausing all workers for integrity check...")

	// Acquire write lock to pause all workers
	workerPauseMu.Lock()
	defer workerPauseMu.Unlock()

	// Also pause checkpoints during integrity check
	checkpointMutex.Lock()
	defer checkpointMutex.Unlock()

	log.Println("[integrity] All workers paused, running sqlite3 integrity_check...")

	// Run sqlite3 PRAGMA integrity_check
	cmd := exec.Command("sqlite3", globalDbPath, "PRAGMA integrity_check;")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[integrity] ERROR running sqlite3: %v", err)
		log.Printf("[integrity] Output: %s", string(output))
		stats.Errors.Add(1)
		return
	}

	result := strings.TrimSpace(string(output))
	if result == "ok" {
		log.Println("[integrity] Database integrity check PASSED")
	} else {
		log.Printf("[integrity] DATABASE CORRUPTION DETECTED!")
		log.Printf("[integrity] Output: %s", result)
		stats.Errors.Add(1)
		// Optionally exit on corruption
		log.Fatal("[integrity] Exiting due to database corruption")
	}

	log.Println("[integrity] Resuming workers...")
}

// Stress worker that continuously hammers HTTP endpoints
func stressWorker(ctx context.Context, id int, baseURL string) {
	client := &http.Client{Timeout: 30 * time.Second}
	endpoints := []string{"/insert", "/update", "/delete", "/select", "/bulk"}
	weights := []int{20, 15, 5, 10, 50} // Probability weights (insert, update, delete, select, bulk)
	workerID := fmt.Sprintf("%d", id)

	// Build weighted selection slice
	var weighted []string
	for i, ep := range endpoints {
		for j := 0; j < weights[i]; j++ {
			weighted = append(weighted, ep)
		}
	}

	// Small delay before starting to let server initialize
	time.Sleep(100 * time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stress worker %d stopped", id)
			return
		default:
			// Acquire read lock - will block if integrity check is running
			workerPauseMu.RLock()

			endpoint := weighted[rand.Intn(len(weighted))]
			url := baseURL + endpoint

			var resp *http.Response
			var err error

			if endpoint == "/select" {
				req, _ := http.NewRequest(http.MethodGet, url, nil)
				req.Header.Set("X-Worker-ID", workerID)
				resp, err = client.Do(req)
			} else {
				req, _ := http.NewRequest(http.MethodPost, url, nil)
				req.Header.Set("X-Worker-ID", workerID)
				req.Header.Set("Content-Type", "application/json")
				resp, err = client.Do(req)
			}

			workerPauseMu.RUnlock()

			if err != nil {
				// Connection errors are expected during high load, don't spam logs
				continue
			}
			resp.Body.Close()

			// Small random delay between requests (1-10ms)
			time.Sleep(time.Duration(10+rand.Intn(400)) * time.Millisecond)
		}
	}
}

// HTTP Handlers

// Helper to get context with worker ID from request
func getWorkerContext(r *http.Request) context.Context {
	ctx := r.Context()
	if workerID := r.Header.Get("X-Worker-ID"); workerID != "" {
		ctx = context.WithValue(ctx, workerIDKey, workerID)
	}
	return ctx
}

func handleInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := getWorkerContext(r)
	now := time.Now().Format(time.RFC3339)
	record := Record{
		CreatedAt: now,
		UpdatedAt: now,
		Name:      fmt.Sprintf("record_%d", rand.Int63()),
		Value:     rand.Intn(10000),
		Data:      randomString(100),
	}

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(&record).Error
	})
	if err != nil {
		stats.Errors.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats.Inserts.Add(1)
	json.NewEncoder(w).Encode(record)
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := getWorkerContext(r)
	var record Record
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Pick a random ID - may not exist, that's fine
		if err := tx.First(&record, rand.Intn(100000)+1).Error; err != nil {
			return err
		}
		record.Value = rand.Intn(10000)
		record.Data = randomString(100)
		record.UpdatedAt = time.Now().Format(time.RFC3339)
		return tx.Save(&record).Error
	})
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			http.Error(w, "No records to update", http.StatusNotFound)
			return
		}
		stats.Errors.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats.Updates.Add(1)
	json.NewEncoder(w).Encode(record)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := getWorkerContext(r)
	// Pick a random ID - may not exist, that's fine
	id := rand.Intn(100000) + 1
	var rowsAffected int64
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Delete(&Record{}, id)
		rowsAffected = result.RowsAffected
		return result.Error
	})
	if err != nil {
		stats.Errors.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if rowsAffected == 0 {
		http.Error(w, "No records to delete", http.StatusNotFound)
		return
	}

	stats.Deletes.Add(1)
	json.NewEncoder(w).Encode(map[string]interface{}{"deleted_id": id})
}

func handleSelect(w http.ResponseWriter, r *http.Request) {
	ctx := getWorkerContext(r)
	// Pick random IDs - some may not exist, that's fine
	ids := make([]int, 10)
	for i := range ids {
		ids[i] = rand.Intn(100000) + 1
	}

	var records []Record
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Find(&records, ids).Error
	})
	if err != nil {
		stats.Errors.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats.Selects.Add(1)
	json.NewEncoder(w).Encode(records)
}

func handleRandom(w http.ResponseWriter, r *http.Request) {
	// Randomly choose an operation
	ops := []string{"insert", "update", "delete", "select"}
	op := ops[rand.Intn(len(ops))]

	switch op {
	case "insert":
		handleInsert(w, &http.Request{Method: http.MethodPost})
	case "update":
		handleUpdate(w, &http.Request{Method: http.MethodPost})
	case "delete":
		handleDelete(w, &http.Request{Method: http.MethodPost})
	case "select":
		handleSelect(w, r)
	}
}

func handleBulk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := getWorkerContext(r)
	count := 100 // Insert 100 records in a transaction
	records := make([]Record, count)
	now := time.Now().Format(time.RFC3339)
	for i := 0; i < count; i++ {
		records[i] = Record{
			CreatedAt: now,
			UpdatedAt: now,
			Name:      fmt.Sprintf("bulk_%d_%d", time.Now().UnixNano(), i),
			Value:     rand.Intn(10000),
			Data:      randomString(100),
		}
	}

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(&records).Error
	})

	if err != nil {
		stats.Errors.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats.Inserts.Add(int64(count))
	json.NewEncoder(w).Encode(map[string]interface{}{"inserted": count})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"inserts":     stats.Inserts.Load(),
		"updates":     stats.Updates.Load(),
		"deletes":     stats.Deletes.Load(),
		"selects":     stats.Selects.Load(),
		"checkpoints": stats.Checkpoints.Load(),
		"errors":      stats.Errors.Load(),
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	// Quick health check
	sqlDB, err := db.DB()
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if err := sqlDB.Ping(); err != nil {
		http.Error(w, "Database ping failed", http.StatusInternalServerError)
		return
	}
	w.Write([]byte("OK"))
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
