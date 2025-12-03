package turso

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// TursoSyncDbConfig holds configuration for the synced database wrapper.
// It configures local database path, remote endpoint, authentication, bootstrap strategy and IO behavior.
// All HTTP requests are performed against RemoteUrl and will include Authorization: Bearer <AuthToken> if provided.
type TursoSyncDbConfig struct {
	// Path to the main database file locally
	Path string

	// remote url for the sync
	// remote_url MUST be used in all sync engine operations: during bootstrap and all further operations
	RemoteUrl string

	// token for remote authentication; used as "Authorization: Bearer <token>"
	AuthToken string

	// optional unique client name (library MUST use `turso-sync-go` if omitted)
	ClientName string

	// long polling timeout
	LongPollTimeoutMs int

	// if not set, initial bootstrap phase will be skipped and caller must call .pull(...) explicitly
	// default value is true
	BootstrapIfEmpty *bool

	// if positive, prefix partial bootstrap strategy will be used
	PartialBoostrapStrategyPrefix int
	// if not empty, query partial bootstrap strategy will be used
	PartialBoostrapStrategyQuery string

	// pass it as-is to the underlying connection
	ExperimentalFeatures string
}

// TursoSyncDb is a high-level wrapper that integrates the embedded Turso driver with the sync engine.
// It owns a TursoSyncDatabase handle and provides helper methods to connect, push and pull changes.
type TursoSyncDb struct {
	// immutable configuration
	conf      TursoSyncDbConfig
	baseURL   *url.URL
	userAgent string

	// underlying sync database handle
	sdb TursoSyncDatabase

	// http client used for sync IO; zero Timeout to allow long-poll driven by context
	http *http.Client
}

// NewTursoSyncDb creates or opens a synced database at config.Path and prepares it for use.
// The method initializes sync engine with async_io enabled and runs turso_sync_database_create flow.
// Honor config.BootstrapIfEmpty default true unless explicitly disabled.
// remote_url MUST be used in all sync operations; it is validated and stored.
func NewTursoSyncDb(ctx context.Context, config TursoSyncDbConfig) (*TursoSyncDb, error) {
	if strings.TrimSpace(config.Path) == "" {
		return nil, errors.New("turso: empty database path")
	}
	if strings.TrimSpace(config.RemoteUrl) == "" {
		return nil, errors.New("turso: empty remote url")
	}
	base, err := url.Parse(config.RemoteUrl)
	if err != nil {
		return nil, fmt.Errorf("turso: invalid remote url: %w", err)
	}
	// defaults
	clientName := config.ClientName
	if clientName == "" {
		clientName = "turso-sync-go"
	}
	bootstrap := true
	if config.BootstrapIfEmpty != nil {
		bootstrap = *config.BootstrapIfEmpty
	}

	// Build DB and Sync configs
	dbConf := TursoDatabaseConfig{
		Path:                 config.Path,
		ExperimentalFeatures: config.ExperimentalFeatures,
		// IMPORTANT: async_io=true to drive IO outside of bindings
		AsyncIO: true,
	}
	syncConf := TursoSyncDatabaseConfig{
		Path:                           config.Path,
		ClientName:                     clientName,
		LongPollTimeoutMs:              config.LongPollTimeoutMs,
		BootstrapIfEmpty:               bootstrap,
		ReservedBytes:                  0, // rely on server configuration; can be exposed later if desired
		PartialBootstrapStrategyPrefix: config.PartialBoostrapStrategyPrefix,
		PartialBootstrapStrategyQuery:  config.PartialBoostrapStrategyQuery,
	}

	// Create sync database holder
	sdb, err := turso_sync_database_new(dbConf, syncConf)
	if err != nil {
		return nil, err
	}

	d := &TursoSyncDb{
		conf:      config,
		baseURL:   base,
		userAgent: clientName,
		sdb:       sdb,
		http: &http.Client{
			Timeout: 0, // long operations; rely on ctx deadlines/timeouts
		},
	}

	// Create/open/prepare synced database (bootstrap if necessary)
	op, err := turso_sync_database_create(d.sdb)
	if err != nil {
		turso_sync_database_deinit(d.sdb)
		return nil, err
	}
	if err := d.runOp(ctx, op); err != nil {
		turso_sync_operation_deinit(op)
		turso_sync_database_deinit(d.sdb)
		return nil, err
	}
	turso_sync_operation_deinit(op)
	return d, nil
}

// Connect creates an ordinary database/sql connection pool bound to the synced database.
// Each connection in the pool is created via sync engine connect operation and is wrapped with the embedded driver.
// The pool connections automatically integrate the sync IO pump required for partial sync and network operations.
func (d *TursoSyncDb) Connect(ctx context.Context) (*sql.DB, error) {
	connector := &tursoSyncConnector{db: d}
	return sql.OpenDB(connector), nil
}

// Pull performs one full pull cycle:
// 1) Push local changes to the remote
// 2) Wait for remote changes (long poll)
// 3) Apply received changes locally
// This method can be called periodically or from a background loop; it is safe to call sequentially.
func (d *TursoSyncDb) Pull(ctx context.Context) error {
	// 1) Push local changes
	opPush, err := turso_sync_database_push_changes(d.sdb)
	if err != nil {
		return err
	}
	if err := d.runOp(ctx, opPush); err != nil {
		turso_sync_operation_deinit(opPush)
		return err
	}
	turso_sync_operation_deinit(opPush)

	// 2) Wait for remote changes
	opWait, err := turso_sync_database_wait_changes(d.sdb)
	if err != nil {
		return err
	}
	if err := d.runOp(ctx, opWait); err != nil {
		turso_sync_operation_deinit(opWait)
		return err
	}
	// Extract changes from finished operation
	if kind := turso_sync_operation_result_kind(opWait); kind != TURSO_ASYNC_RESULT_CHANGES {
		turso_sync_operation_deinit(opWait)
		return fmt.Errorf("turso: unexpected async result kind %d (expected CHANGES)", kind)
	}
	changes, err := turso_sync_operation_result_extract_changes(opWait)
	turso_sync_operation_deinit(opWait)
	if err != nil {
		return err
	}
	// 3) Apply changes locally
	opApply, err := turso_sync_database_apply_changes(d.sdb, changes)
	if err != nil {
		// drop changes object
		turso_sync_changes_deinit(changes)
		return err
	}
	applyErr := d.runOp(ctx, opApply)
	turso_sync_operation_deinit(opApply)
	return applyErr
}

// Below are complementary helpers; kept after the main API to keep public block order minimal and clear.

// RegisterTursoSyncLib registers Turso Sync C API from the loaded shared library handle.
func RegisterTursoSyncLib(handle uintptr) error {
	return register_turso_sync(handle)
}

// Close releases the underlying sync database.
// Connections created earlier will remain usable but further sync operations on this handle will fail.
func (d *TursoSyncDb) Close() {
	if d == nil || d.sdb == nil {
		return
	}
	turso_sync_database_deinit(d.sdb)
	d.sdb = nil
}

// Push only local changes to the remote without pulling.
func (d *TursoSyncDb) Push(ctx context.Context) error {
	op, err := turso_sync_database_push_changes(d.sdb)
	if err != nil {
		return err
	}
	defer turso_sync_operation_deinit(op)
	return d.runOp(ctx, op)
}

// Stats returns current sync stats collected by the engine.
func (d *TursoSyncDb) Stats(ctx context.Context) (TursoSyncStats, error) {
	op, err := turso_sync_database_stats(d.sdb)
	if err != nil {
		return TursoSyncStats{}, err
	}
	defer turso_sync_operation_deinit(op)
	if err := d.runOp(ctx, op); err != nil {
		return TursoSyncStats{}, err
	}
	kind := turso_sync_operation_result_kind(op)
	if kind != TURSO_ASYNC_RESULT_STATS {
		return TursoSyncStats{}, fmt.Errorf("turso: unexpected async result kind %d (expected STATS)", kind)
	}
	return turso_sync_operation_result_extract_stats(op)
}

// Checkpoint performs a WAL checkpoint for the synced database.
func (d *TursoSyncDb) Checkpoint(ctx context.Context) error {
	op, err := turso_sync_database_checkpoint(d.sdb)
	if err != nil {
		return err
	}
	defer turso_sync_operation_deinit(op)
	return d.runOp(ctx, op)
}

// internal connector to integrate with database/sql pool
type tursoSyncConnector struct {
	db *TursoSyncDb
}

func (c *tursoSyncConnector) Connect(ctx context.Context) (driver.Conn, error) {
	// Run connect operation
	op, err := turso_sync_database_connect(c.db.sdb)
	if err != nil {
		return nil, err
	}
	if err := c.db.runOp(ctx, op); err != nil {
		turso_sync_operation_deinit(op)
		return nil, err
	}
	if kind := turso_sync_operation_result_kind(op); kind != TURSO_ASYNC_RESULT_CONNECTION {
		turso_sync_operation_deinit(op)
		return nil, fmt.Errorf("turso: unexpected async result kind %d (expected CONNECTION)", kind)
	}
	conn, err := turso_sync_operation_result_extract_connection(op)
	turso_sync_operation_deinit(op)
	if err != nil {
		return nil, err
	}
	// Wrap into driver connection integrating extra IO loop
	return NewConnection(conn, func() error {
		// process IO queue once (non-blocking)
		return c.db.processIoQueue(context.Background())
	}), nil
}

func (c *tursoSyncConnector) Driver() driver.Driver {
	// Return embedded driver instance to satisfy interface; not used for connection.
	return &tursoDbDriver{}
}

// runOp drives a sync operation until completion, executing required IO in-between.
// It never loads full HTTP responses into memory; instead, responses are streamed chunk-by-chunk to the engine.
func (d *TursoSyncDb) runOp(ctx context.Context, op TursoSyncOperation) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		status, err := turso_sync_operation_resume(op)
		if err != nil {
			return err
		}
		switch status {
		case TURSO_DONE:
			return nil
		case TURSO_IO:
			// pump IO until queue is drained; then step callbacks
			if err := d.processIoQueue(ctx); err != nil {
				return err
			}
			if err := turso_sync_database_io_step_callbacks(d.sdb); err != nil {
				return err
			}
		case TURSO_OK:
			// try to progress further in the next loop iteration
		default:
			return statusToError(status, "")
		}
	}
}

// processIoQueue drains pending IO items from the engine and completes them.
// It handles HTTP streaming, as well as atomic file reads and writes.
func (d *TursoSyncDb) processIoQueue(ctx context.Context) error {
	for {
		err := turso_sync_database_io_step_callbacks(d.sdb)
		if err != nil {
			return err
		}
		item, err := turso_sync_database_io_take_item(d.sdb)
		if err != nil {
			return err
		}
		if item == nil {
			// nothing to do
			return nil
		}
		// Always release IO item at the end of processing
		func() {
			defer turso_sync_database_io_item_deinit(item)
			switch turso_sync_database_io_request_kind(item) {
			case TURSO_SYNC_IO_HTTP:
				if e := d.handleHttpItem(ctx, item); e != nil {
					_ = turso_sync_database_io_poison(item, e.Error())
					_ = turso_sync_database_io_done(item)
				}
			case TURSO_SYNC_IO_FULL_READ:
				if e := d.handleFullReadItem(item); e != nil {
					_ = turso_sync_database_io_poison(item, e.Error())
				}
				_ = turso_sync_database_io_done(item)
			case TURSO_SYNC_IO_FULL_WRITE:
				if e := d.handleFullWriteItem(item); e != nil {
					_ = turso_sync_database_io_poison(item, e.Error())
				}
				_ = turso_sync_database_io_done(item)
			default:
				// unknown IO type
				_ = turso_sync_database_io_poison(item, "unsupported io request type")
				_ = turso_sync_database_io_done(item)
			}
		}()
	}
}

// handleHttpItem performs one HTTP request and streams the response back to the engine.
// It forwards headers requested by the engine and adds Authorization if configured.
func (d *TursoSyncDb) handleHttpItem(ctx context.Context, item TursoSyncIoItem) error {
	reqDesc, err := turso_sync_database_io_request_http(item)
	if err != nil {
		return err
	}
	// Build URL relative to base RemoteUrl (MUST be used for all operations)
	targetURL, err := d.joinRelativeURL(reqDesc.Path)
	if err != nil {
		return err
	}

	// Prepare request body
	var body io.Reader
	if len(reqDesc.Body) > 0 {
		body = strings.NewReader(string(reqDesc.Body))
	}

	req, err := http.NewRequestWithContext(ctx, reqDesc.Method, targetURL, body)
	if err != nil {
		return err
	}
	// Add engine-provided headers
	for i := 0; i < reqDesc.Headers; i++ {
		h, herr := turso_sync_database_io_request_http_header(item, i)
		if herr != nil {
			return herr
		}
		if h.Key != "" {
			// Use Add to allow duplicate header keys if engine requires
			req.Header.Add(h.Key, h.Value)
		}
	}
	// Add Authorization if present or override existing one
	if d.conf.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+d.conf.AuthToken)
	}
	// Identify the client
	if d.userAgent != "" {
		req.Header.Set("User-Agent", d.userAgent)
	}

	// Execute request
	resp, err := d.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Set HTTP status first
	if err := turso_sync_database_io_status(item, resp.StatusCode); err != nil {
		return err
	}

	// Stream response in chunks, pushing buffers to the engine
	const chunkSize = 32 * 1024
	buf := make([]byte, chunkSize)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if perr := turso_sync_database_io_push_buffer(item, buf[:n]); perr != nil {
				return perr
			}
			// Allow engine to run callbacks; avoid excessive FFI by stepping periodically
			if perr := turso_sync_database_io_step_callbacks(d.sdb); perr != nil {
				return perr
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return rerr
		}
	}
	// Mark completion
	return turso_sync_database_io_done(item)
}

// handleFullReadItem executes atomic file read and pushes content to the engine.
func (d *TursoSyncDb) handleFullReadItem(item TursoSyncIoItem) error {
	req, err := turso_sync_database_io_request_full_read(item)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(req.Path)
	if err != nil {
		return err
	}
	if err := turso_sync_database_io_push_buffer(item, data); err != nil {
		return err
	}
	return nil
}

// handleFullWriteItem executes atomic file write with provided content.
func (d *TursoSyncDb) handleFullWriteItem(item TursoSyncIoItem) error {
	req, err := turso_sync_database_io_request_full_write(item)
	if err != nil {
		return err
	}
	// Ensure parent dir exists; ignore error if already exists
	if dir := dirOfPath(req.Path); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	return os.WriteFile(req.Path, req.Content, 0o600)
}

// joinRelativeURL resolves a relative path returned by the engine against the configured RemoteUrl.
func (d *TursoSyncDb) joinRelativeURL(rel string) (string, error) {
	// Ensure base URL present
	if d.baseURL == nil {
		return "", errors.New("turso: remote baseURL is not set")
	}
	// Use ResolveReference to handle both absolute and relative paths safely, but always force base host.
	ref, err := url.Parse(rel)
	if err != nil {
		return "", err
	}
	// Always use base host: overwrite scheme/host of ref with base if it tries to escape
	ref.Scheme = ""
	ref.Host = ""
	joined := d.baseURL.ResolveReference(ref)
	return joined.String(), nil
}

// dirOfPath returns directory for a path; empty string for current dir or if no separator found.
func dirOfPath(p string) string {
	i := strings.LastIndexAny(p, "/\\")
	if i <= 0 {
		return ""
	}
	return p[:i]
}

// WithTimeout returns a derived context with timeout if ms > 0; otherwise returns ctx as is.
// Helper for convenient per-call deadlines.
func WithTimeout(ctx context.Context, ms int) (context.Context, context.CancelFunc) {
	if ms > 0 {
		return context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
	}
	return ctx, func() {}
}
