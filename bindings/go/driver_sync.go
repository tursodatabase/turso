package turso

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
)

// TursoPartialSyncConfig configures partial sync behavior.
type TursoPartialSyncConfig struct {
	// if positive, prefix partial bootstrap strategy will be used
	BootstrapStrategyPrefix int
	// if not empty, query partial bootstrap strategy will be used
	BootstrapStrategyQuery string
	// optional parameter which defines segment size for lazy loading from remote server
	// one of valid BootstrapStrategy* values MUST be set in order for this setting to have some effect
	SegmentSize int
	// optional parameter which defines if pages prefetch must be enabled
	// one of valid BootstrapStrategy* values MUST be set in order for this setting to have some effect
	Prefetch bool
}

// Public configuration for a synced database.
type TursoSyncDbConfig struct {
	// path to the main database file locally
	Path string

	// remote url for the sync
	// remote_url MUST be used in all sync engine operations: during bootstrap and all further operations
	RemoteUrl string

	// remote namespace for the sync client (optional)
	Namespace string

	// token for remote authentication
	// auth token value WILL not have any prefix and must be used as "Authorization" header prepended with "Bearer " prefix
	AuthToken string

	// optional unique client name (library MUST use `turso-sync-go` if omitted)
	ClientName string

	// long polling timeout
	LongPollTimeoutMs int

	// if not set, initial bootstrap phase will be skipped and caller must call .pull(...) explicitly in order to get initial state from remote
	// default value is true
	BootstrapIfEmpty *bool

	// configuration for partial sync (disabled by default)
	// WARNING: This feature is EXPERIMENTAL
	PartialSyncExperimental TursoPartialSyncConfig

	// pass it as-is to the underlying connection
	ExperimentalFeatures string
}

// statistics for the synced database.
type TursoSyncDbStats struct {
	// amount of local operations written since last Pull(...) call
	CdcOperations int64
	// size of the main WAL file
	MainWalSize int64
	// size of the revert WAL file
	RevertWalSize int64
	// last successful pull time
	LastPullUnixTime int64
	// last successful push time
	LastPushUnixTime int64
	// total amount of bytes sent over the network (both Push and Pull operations are tracked together)
	NetworkSentBytes int64
	// total amount of bytes received over the network (both Push and Pull operations are tracked together)
	NetworkReceivedBytes int64
	// opaque server revision - it MUST NOT be interpreted/parsed in any way
	Revision string
}

// define public structs here

// TursoSyncDb is a high-level synced database wrapper built over driver_db.go and bindings_sync.go.
// It provides push/pull sync operations and creates SQL connections backed by the sync engine.
type TursoSyncDb struct {
	db        TursoSyncDatabase
	baseURL   string
	authToken string
	namespace string
	client    *http.Client

	mu sync.Mutex
}

// main constructor to create synced database
func NewTursoSyncDb(ctx context.Context, config TursoSyncDbConfig) (*TursoSyncDb, error) {
	InitLibrary(turso_libs.LoadTursoLibraryConfig{})
	if strings.TrimSpace(config.Path) == "" {
		return nil, errors.New("turso: empty Path in TursoSyncDbConfig")
	}
	clientName := config.ClientName
	if clientName == "" {
		clientName = "turso-sync-go"
	}
	bootstrap := true
	if config.BootstrapIfEmpty != nil {
		bootstrap = *config.BootstrapIfEmpty
	}

	remoteUrl := normalizeUrl(config.RemoteUrl)
	// Create sync database holder
	dbCfg := TursoDatabaseConfig{
		Path:                 config.Path,
		ExperimentalFeatures: config.ExperimentalFeatures,
		AsyncIO:              true, // MUST be true for external IO handling
	}
	syncCfg := TursoSyncDatabaseConfig{
		Path:                           config.Path,
		RemoteUrl:                      remoteUrl,
		Namespace:                      config.Namespace,
		ClientName:                     clientName,
		LongPollTimeoutMs:              config.LongPollTimeoutMs,
		BootstrapIfEmpty:               bootstrap,
		ReservedBytes:                  0,
		PartialBootstrapStrategyPrefix: config.PartialSyncExperimental.BootstrapStrategyPrefix,
		PartialBootstrapStrategyQuery:  config.PartialSyncExperimental.BootstrapStrategyQuery,
		PartialBootstrapSegmentSize:    config.PartialSyncExperimental.SegmentSize,
		PartialBootstrapPrefetch:       config.PartialSyncExperimental.Prefetch,
	}
	sdb, err := turso_sync_database_new(dbCfg, syncCfg)
	if err != nil {
		return nil, err
	}

	d := &TursoSyncDb{
		db:        sdb,
		baseURL:   strings.TrimRight(remoteUrl, "/"),
		authToken: strings.TrimSpace(config.AuthToken),
		client: &http.Client{
			// No global timeout to allow long-poll; rely on request context.
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxIdleConns:        32,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  false,
			},
		},
	}

	// Create/open database with bootstrap logic as needed.
	op, err := turso_sync_database_create(d.db)
	if err != nil {
		return nil, err
	}
	_, _, err = d.driveOpUntilDone(ctx, op)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// create turso db local connnection

// internal connector to integrate with database/sql pool
type tursoSyncConnector struct{ db *TursoSyncDb }

func (c *tursoSyncConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	op, err := turso_sync_database_connect(c.db.db)
	if err != nil {
		return nil, err
	}
	kind, opFinal, err := c.db.driveOpUntilDone(ctx, op)
	if err != nil {
		return nil, err
	}
	defer turso_sync_operation_deinit(opFinal)
	if kind != TURSO_ASYNC_RESULT_CONNECTION {
		return nil, errors.New("turso: unexpected result kind when connecting")
	}
	conn, err := turso_sync_operation_result_extract_connection(opFinal)
	if err != nil {
		return nil, err
	}
	// Integrate with the base driver; provide extra IO hook to process one IO item per iteration.
	extra := func() error { return c.db.processOneIo() }
	return NewConnection(conn, extra), nil
}

func (c *tursoSyncConnector) Driver() driver.Driver { return &tursoDbDriver{} }

// create tursodb connection using NewConnection(...) from driver_db.go and tursoSyncConnector helper
func (d *TursoSyncDb) Connect(ctx context.Context) (*sql.DB, error) {
	return sql.OpenDB(&tursoSyncConnector{db: d}), nil
}

// implement EXTRA sync methods

// Pull fresh data from the remote
// Pull DO NOT sent any local changes to the remote and instead "rebase" them on top of new changes from remote
// Return true, if new changes were applied locally - otherwise return false
func (d *TursoSyncDb) Pull(ctx context.Context) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1) Wait for remote changes
	waitOp, err := turso_sync_database_wait_changes(d.db)
	if err != nil {
		return false, err
	}
	kind, waitFinal, err := d.driveOpUntilDone(ctx, waitOp)
	if err != nil {
		return false, err
	}
	defer turso_sync_operation_deinit(waitFinal)
	if kind != TURSO_ASYNC_RESULT_CHANGES {
		return false, errors.New("turso: unexpected result kind for wait_changes")
	}
	changes, err := turso_sync_operation_result_extract_changes(waitFinal)
	if err != nil {
		return false, err
	}
	// No changes available
	if changes == nil {
		return false, nil
	}

	// 2) Apply fetched changes locally
	applyOp, err := turso_sync_database_apply_changes(d.db, changes)
	if err != nil {
		// changes ownership is transferred to apply_changes even in case of error
		return false, err
	}
	_, applyFinal, err := d.driveOpUntilDone(ctx, applyOp)
	if err != nil {
		return false, err
	}
	turso_sync_operation_deinit(applyFinal)
	return true, nil
}

// Push local changes to the remote
// Push DO NOT fetch any remote changes
func (d *TursoSyncDb) Push(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	op, err := turso_sync_database_push_changes(d.db)
	if err != nil {
		return err
	}
	_, opFinal, err := d.driveOpUntilDone(ctx, op)
	if err != nil {
		return err
	}
	turso_sync_operation_deinit(opFinal)
	return nil
}

// Get stats for the synced database
func (d *TursoSyncDb) Stats(ctx context.Context) (TursoSyncDbStats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	op, err := turso_sync_database_stats(d.db)
	if err != nil {
		return TursoSyncDbStats{}, err
	}
	kind, opFinal, err := d.driveOpUntilDone(ctx, op)
	if err != nil {
		return TursoSyncDbStats{}, err
	}
	defer turso_sync_operation_deinit(opFinal)
	if kind != TURSO_ASYNC_RESULT_STATS {
		return TursoSyncDbStats{}, errors.New("turso: unexpected result kind for stats")
	}
	stats, err := turso_sync_operation_result_extract_stats(opFinal)
	if err != nil {
		return TursoSyncDbStats{}, err
	}
	return TursoSyncDbStats{
		CdcOperations:        stats.CDcOperations,
		MainWalSize:          stats.MainWalSize,
		RevertWalSize:        stats.RevertWalSize,
		LastPullUnixTime:     stats.LastPullUnixTime,
		LastPushUnixTime:     stats.LastPushUnixTime,
		NetworkSentBytes:     stats.NetworkSentBytes,
		NetworkReceivedBytes: stats.NetworkReceivedBytes,
		Revision:             stats.Revision,
	}, nil
}

// Checkpoint local WAL of the database
func (d *TursoSyncDb) Checkpoint(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	op, err := turso_sync_database_checkpoint(d.db)
	if err != nil {
		return err
	}
	_, opFinal, err := d.driveOpUntilDone(ctx, op)
	if err != nil {
		return err
	}
	turso_sync_operation_deinit(opFinal)
	return nil
}

// driveOpUntilDone resumes an async operation until completion, serving IO requests as needed.
// It returns the final result kind and the operation handle that must be deinitialized by the caller.
func (d *TursoSyncDb) driveOpUntilDone(ctx context.Context, op TursoSyncOperation) (TursoSyncOperationResultType, TursoSyncOperation, error) {
	for {
		if ctx != nil && ctx.Err() != nil {
			return TURSO_ASYNC_RESULT_NONE, op, ctx.Err()
		}
		code, err := turso_sync_operation_resume(op)
		if err != nil {
			return TURSO_ASYNC_RESULT_NONE, op, err
		}
		switch code {
		case TURSO_DONE:
			return turso_sync_operation_result_kind(op), op, nil
		case TURSO_IO:
			if err := d.processIoQueue(ctx); err != nil {
				return TURSO_ASYNC_RESULT_NONE, op, err
			}
			continue
		case TURSO_OK:
			// Just continue
			continue
		default:
			return TURSO_ASYNC_RESULT_NONE, op, statusToError(code, "")
		}
	}
}

// processOneIo handles at most one IO item (used as extra IO iteration inside SQL driver).
func (d *TursoSyncDb) processOneIo() error {
	item, err := turso_sync_database_io_take_item(d.db)
	if err != nil {
		return err
	}
	if item == nil {
		// Still run callbacks to allow engine to progress timers/state.
		return turso_sync_database_io_step_callbacks(d.db)
	}
	_ = d.handleIoItem(context.Background(), item)
	turso_sync_database_io_item_deinit(item)
	return turso_sync_database_io_step_callbacks(d.db)
}

// processIoQueue drains IO queue until it's empty.
func (d *TursoSyncDb) processIoQueue(ctx context.Context) error {
	for {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		item, err := turso_sync_database_io_take_item(d.db)
		if err != nil {
			return err
		}
		if item == nil {
			break
		}
		_ = d.handleIoItem(ctx, item)
		turso_sync_database_io_item_deinit(item)
	}
	return turso_sync_database_io_step_callbacks(d.db)
}

func buildHostname(baseURL, namespace string) (string, error) {
	if namespace == "" {
		return baseURL, nil
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	return namespace + "." + u.Host, nil
}

// handleIoItem performs execution of a single IO item.
// It streams data in chunks for HTTP and file operations to avoid loading whole payloads in memory.
func (d *TursoSyncDb) handleIoItem(ctx context.Context, item TursoSyncIoItem) error {
	switch turso_sync_database_io_request_kind(item) {
	case TURSO_SYNC_IO_HTTP:
		req, err := turso_sync_database_io_request_http(item)
		if err != nil {
			_ = turso_sync_database_io_poison(item, err.Error())
			_ = turso_sync_database_io_done(item)
			return err
		}
		// Build URL
		buildUrl := joinUrl(d.baseURL, req.Path)

		// Build headers
		hdr := make(http.Header, req.Headers+2)
		for i := 0; i < req.Headers; i++ {
			h, err := turso_sync_database_io_request_http_header(item, i)
			if err != nil {
				_ = turso_sync_database_io_poison(item, err.Error())
				_ = turso_sync_database_io_done(item)
				return err
			}
			if h.Key != "" {
				hdr.Add(h.Key, h.Value)
			}
		}
		if d.authToken != "" {
			hdr.Set("Authorization", "Bearer "+d.authToken)
		}
		// Propagate sensible defaults
		if hdr.Get("User-Agent") == "" {
			hdr.Set("User-Agent", "turso-sync-go")
		}

		// Prepare request body reader
		var body io.Reader
		if len(req.Body) > 0 {
			body = bytes.NewReader(req.Body)
		}
		httpReq, err := http.NewRequestWithContext(ctx, req.Method, buildUrl, body)
		if err != nil {
			_ = turso_sync_database_io_poison(item, err.Error())
			_ = turso_sync_database_io_done(item)
			return err
		}
		host, err := buildHostname(d.baseURL, d.namespace)
		if err != nil {
			return err
		}
		httpReq.Host = host
		httpReq.Header = hdr

		resp, err := d.client.Do(httpReq)
		if err != nil {
			_ = turso_sync_database_io_poison(item, err.Error())
			_ = turso_sync_database_io_done(item)
			return err
		}
		defer resp.Body.Close()

		// Send status
		_ = turso_sync_database_io_status(item, resp.StatusCode)

		// Stream body
		buf := make([]byte, 64*1024)
		for {
			if ctx != nil && ctx.Err() != nil {
				_ = turso_sync_database_io_poison(item, ctx.Err().Error())
				break
			}
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				// push the exact slice view; underlying call copies bytes synchronously
				_ = turso_sync_database_io_push_buffer(item, buf[:n])
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				_ = turso_sync_database_io_poison(item, rerr.Error())
				break
			}
		}
		_ = turso_sync_database_io_done(item)
		return nil

	case TURSO_SYNC_IO_FULL_READ:
		r, err := turso_sync_database_io_request_full_read(item)
		if err != nil {
			_ = turso_sync_database_io_poison(item, err.Error())
			_ = turso_sync_database_io_done(item)
			return err
		}
		f, ferr := os.Open(r.Path)
		if errors.Is(ferr, os.ErrNotExist) {
			_ = turso_sync_database_io_done(item)
			return nil
		} else if ferr != nil {
			_ = turso_sync_database_io_poison(item, ferr.Error())
			_ = turso_sync_database_io_done(item)
			return ferr
		}
		defer f.Close()
		buf := make([]byte, 64*1024)
		for {
			if ctx != nil && ctx.Err() != nil {
				_ = turso_sync_database_io_poison(item, ctx.Err().Error())
				break
			}
			n, rerr := f.Read(buf)
			if n > 0 {
				_ = turso_sync_database_io_push_buffer(item, buf[:n])
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				_ = turso_sync_database_io_poison(item, rerr.Error())
				break
			}
		}
		_ = turso_sync_database_io_done(item)
		return nil

	case TURSO_SYNC_IO_FULL_WRITE:
		r, err := turso_sync_database_io_request_full_write(item)
		if err != nil {
			_ = turso_sync_database_io_poison(item, err.Error())
			_ = turso_sync_database_io_done(item)
			return err
		}
		// Ensure directory exists
		if dir := filepath.Dir(r.Path); dir != "" && dir != "." {
			_ = os.MkdirAll(dir, 0o755)
		}
		// Write file atomically-ish by writing to a temp and renaming
		tmp := r.Path + ".tmp"
		if werr := os.WriteFile(tmp, r.Content, 0o644); werr != nil {
			_ = turso_sync_database_io_poison(item, werr.Error())
			_ = turso_sync_database_io_done(item)
			return werr
		}
		if rerr := os.Rename(tmp, r.Path); rerr != nil {
			_ = turso_sync_database_io_poison(item, rerr.Error())
			_ = turso_sync_database_io_done(item)
			return rerr
		}
		_ = turso_sync_database_io_done(item)
		return nil

	default:
		// Unknown or none; mark done
		_ = turso_sync_database_io_done(item)
		return nil
	}
}

func joinUrl(base, p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimRight(base, "/") + p
}

func normalizeUrl(base string) string {
	if cut, ok := strings.CutPrefix(base, "libsql://"); ok {
		return "https://" + cut
	}
	return base
}
