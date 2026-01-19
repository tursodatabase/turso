package turso

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	turso_libs "github.com/tursodatabase/turso-go-platform-libs"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Context key for worker ID
type contextKey string

const workerIDKey contextKey = "worker_id"

// Custom logger that logs ALL statements with worker ID
type queryLogger struct{ t *testing.T }

func (l *queryLogger) LogMode(level logger.LogLevel) logger.Interface { return l }
func (l *queryLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	l.t.Logf("[INFO] "+msg, data...)
}
func (l *queryLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	l.t.Logf("[WARN] "+msg, data...)
}
func (l *queryLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	l.t.Logf("[ERROR] "+msg, data...)
}
func (l *queryLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, rows := fc()
	elapsed := time.Since(begin)
	if err != nil {
		l.t.Logf("[%.3fms] [rows:%d] [ERROR: %v] %s", float64(elapsed.Nanoseconds())/1e6, rows, err, sql)
	} else {
		l.t.Logf("[%.3fms] [rows:%d] %s", float64(elapsed.Nanoseconds())/1e6, rows, sql)
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

func stress(ctx context.Context, rng *rand.Rand, db *gorm.DB, weights []float64) error {
	actions := []string{
		"insert",
		"update",
		"delete",
		"select",
		"bulk",
		"checkpoint",
	}
	action := PickRand(rng, actions, weights)
	switch action {
	case "insert":
		now := time.Now().Format(time.RFC3339)
		record := Record{
			CreatedAt: now,
			UpdatedAt: now,
			Name:      fmt.Sprintf("record_%d", rng.Int63()),
			Value:     rng.Intn(10000),
			Data:      StringRand(rng, 100),
		}
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return tx.Create(&record).Error })
	case "update":
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var record Record
			if err := tx.First(&record, rng.Intn(100000)+1).Error; err != nil {
				return err
			}
			record.Value = rng.Intn(10000)
			record.Data = StringRand(rng, 100)
			record.UpdatedAt = time.Now().Format(time.RFC3339)
			return tx.Save(&record).Error
		})
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return nil
	case "delete":
		id := rng.Intn(100000)
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			result := tx.Delete(&Record{}, id)
			return result.Error
		})
	case "select":
		ids := make([]int, 10)
		for i := range ids {
			ids[i] = rng.Intn(100000) + 1
		}
		var records []Record
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Find(&records, ids).Error
		})
	case "bulk":
		count := 100 // Insert 100 records in a transaction
		records := make([]Record, count)
		now := time.Now().Format(time.RFC3339)
		for i := 0; i < count; i++ {
			records[i] = Record{
				CreatedAt: now,
				UpdatedAt: now,
				Name:      fmt.Sprintf("bulk_%d_%d", time.Now().UnixNano(), i),
				Value:     rng.Intn(10000),
				Data:      StringRand(rng, 100),
			}
		}
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Create(&records).Error
		})
	case "checkpoint":
		modes := []string{"TRUNCATE", "RESTART", "FULL", "PASSIVE"}
		mode := modes[rng.Intn(len(modes))]
		sql := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode)
		return db.Exec(sql).Error
	default:
		return nil
	}
}

func FuzzStress(f *testing.F) {
	InitLibrary(turso_libs.LoadTursoLibraryConfig{LoadStrategy: "system"})
	f.Fuzz(run)
}

func TestStress(t *testing.T) {
	testData := os.Getenv("TESTDATA")
	if testData == "" {
		t.Skipf("TESTDATA env var is not set, skipping test")
		return
	}
	attempts, _ := strconv.ParseInt(os.Getenv("ATTEMPTS"), 10, 64)
	if attempts == 0 {
		attempts = 1
	}
	smallSeed, _ := strconv.ParseInt(os.Getenv("SMALL_SEED"), 10, 64)
	content, err := os.ReadFile(testData)
	require.Nil(t, err)
	lines := strings.Split(string(content), "\n")

	for attempt := 0; attempt < int(attempts); attempt++ {
		t.Logf("attempt#%v: SMALL_SEED=%v", attempt, int(smallSeed)+attempt)
		synctest.Test(t, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < int(smallSeed)+attempt; i++ {
				wg.Add(1)
				go func() { wg.Done() }()
			}
			wg.Wait()

			run(
				t,
				mustInt64(lines[1]),
				mustUint(lines[2]),
				mustUint(lines[3]),
				mustUint(lines[4]),
				mustUint(lines[5]),
				mustUint(lines[6]),
				mustUint(lines[7]),
				mustUint(lines[8]),
				mustUint(lines[9]),
				mustUint(lines[10]),
				mustUint(lines[11]),
				mustUint(lines[12]),
			)
		})
	}
}

func run(
	t *testing.T,
	seed int64,
	workers uint,
	iterations uint,
	maxOpenConnections uint,
	maxIdleConnections uint,
	maxLifetimeSeconds uint,
	insertW uint,
	updateW uint,
	deleteW uint,
	selectW uint,
	bulkW uint,
	checkpointW uint,
) {
	weights := []float64{
		float64(insertW),
		float64(updateW),
		float64(deleteW),
		float64(selectW),
		float64(bulkW),
		float64(checkpointW),
	}
	workers = min(workers, 8)
	iterations = min(iterations, 32)
	if maxOpenConnections == 0 {
		maxOpenConnections = 1
	}

	workerSeeds := make([]int64, 0)
	rng := rand.New(rand.NewSource(seed))
	workerRngs := make([]*rand.Rand, 0)
	for i := 0; i < int(workers); i++ {
		workerSeed := rng.Int63()
		workerSeeds = append(workerSeeds, workerSeed)
		workerRngs = append(workerRngs, rand.New(rand.NewSource(workerSeed)))
	}
	t.Logf(
		`start fuzz test: 
			workers=%v
			iterations=%v
			maxOpenConnections=%v
			maxIdleConnections=%v
			maxLifetimeSeconds=%v
			seed=%v
			weights=%+v
			
			workerSeeds=%+v`,
		workers, iterations, maxOpenConnections, maxIdleConnections, maxLifetimeSeconds, seed, weights, workerSeeds,
	)

	dbDir := t.TempDir()
	dbPath := path.Join(dbDir, "local.db")

	dsn := dbPath + "?_busy_timeout=5000"
	var err error

	dialector := sqlite.Dialector{DriverName: "turso", DSN: dsn}

	Setup(TursoConfig{
		Logger: func(log TursoLog) {
			t.Logf(
				"%v [%v]: %v: %v::%v: %v",
				log.Level,
				time.Unix(int64(log.Timestamp), 0),
				log.Target,
				log.File,
				log.Line,
				log.Message,
			)
		},
		LogLevel: "info",
	})
	// verboseConfig := &gorm.Config{Logger: &queryLogger{t: t}}
	// db, err := gorm.Open(dialector, verboseConfig)
	silentConfig := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	db, err := gorm.Open(dialector, silentConfig)
	require.Nil(t, err)

	sqlDB, err := db.DB()
	require.Nil(t, err)

	sqlDB.SetMaxOpenConns(int(maxOpenConnections))
	sqlDB.SetMaxIdleConns(int(maxIdleConnections))
	sqlDB.SetConnMaxLifetime(time.Second * time.Duration(maxLifetimeSeconds))

	// Set WAL mode
	err = db.Exec("PRAGMA journal_mode=WAL").Error
	require.Nil(t, err)

	err = db.AutoMigrate(&Record{})
	require.Nil(t, err)

	t.Logf("Database initialized at %s", dbPath)
	var wg sync.WaitGroup
	for i := 0; i < int(workers); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := 0; s < int(iterations); s++ {
				t.Logf("worker#%v: query=%v started", i, s)
				stress(t.Context(), workerRngs[i], db, weights)
				t.Logf("worker#%v: query=%v finished", i, s)
			}
			t.Logf("worker#%v: completed", i)
		}()
	}
	wg.Wait()

	sqlDb, err := db.DB()
	if err == nil {
		sqlDb.Close()
	}
}

func mustInt64(s string) int64 {
	if !strings.HasPrefix(s, "int64(") || !strings.HasSuffix(s, ")") {
		panic(fmt.Errorf("failed to parse int64 %v", s))
	}
	value, err := strconv.ParseInt(s[len("int64("):len(s)-1], 10, 64)
	if err != nil {
		panic(fmt.Errorf("failed to parse int64 %v: %w", s, err))
	}
	return value
}

func mustUint(s string) uint {
	if !strings.HasPrefix(s, "uint(") || !strings.HasSuffix(s, ")") {
		panic(fmt.Errorf("failed to parse uint %v", s))
	}
	value, err := strconv.ParseInt(s[len("uint("):len(s)-1], 10, 64)
	if err != nil {
		panic(fmt.Errorf("failed to parse uint %v: %w", s, err))
	}
	return uint(value)
}

func PickRand[T any](rng *rand.Rand, values []T, weights []float64) T {
	sum := 0.0
	for _, w := range weights {
		sum += math.Max(math.Abs(w), 0.0001)
	}
	value := rng.Float64() * sum
	for i := range values {
		value -= math.Max(math.Abs(weights[i]), 0.0001)
		if value < 0 {
			return values[i]
		}
	}
	return values[len(values)-1]
}

func StringRand(rng *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}
