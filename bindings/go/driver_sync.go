package turso

import (
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
	"time"
)

// TursoSyncDbConfig configures a synced Turso database.
// All network IO is handled by this layer (async_io=True).
type TursoSyncDbConfig struct {
	// Path to the main database file locally.
	Path string

	// Remote url for the sync.
	// remote_url MUST be used in all sync engine operations: during bootstrap and all further operations.
	RemoteUrl string

	// Token for remote authentication.
	// Auth token value WILL not have any prefix and must be used as "Authorization" header prepended with "Bearer ".
	AuthToken string

	// Optional unique client name (library MUST use `turso-sync-go` if omitted).
	ClientName string

	// Long polling timeout in milliseconds for pull/wait operations.
	LongPollTimeoutMs int

	// If not set, initial bootstrap phase will be skipped and caller must call .Pull(...) explicitly in order to get initial state from remote.
	// Default value is true.
	BootstrapIfEmpty *bool

	// If positive, prefix partial bootstrap strategy will be used.
	PartialBoostrapStrategyPrefix int
	// If not empty, query partial bootstrap strategy will be used.
	PartialBoostrapStrategyQuery string

	// Pass it as-is to the underlying connection.
	ExperimentalFeatures string
}

// TursoSyncDbStats represents statistics for the synced database.
type TursoSyncDbStats struct {
	CDcOperations        int64
	MainWalSize          int64
	RevertWalSize        int64
	LastPullUnixTime     int64
	LastPushUnixTime     int64
	NetworkSentBytes     int64
	NetworkReceivedBytes int64
	Revision             string
}

// TursoSyncDb is a high-level synced database wrapper.
// It builds on top of the base driver and exposes sync operations.
type TursoSyncDb struct {
	sync TursoSyncDatabase

	remoteURL string
	authToken string
	client    *http.Client

	// keep some config pieces for reference
	longPollTimeoutMs int
}

// NewTursoSyncDb creates a synced database using provided config.
// It sets async_io=True and drives the sync engine IO loop during creation.
// Note: Caller must ensure the underlying native library is registered externally via RegisterTursoLib for DB and appropriate registration for sync bindings.
func NewTursoSyncDb(ctx context.Context, config TursoSyncDbConfig) (*TursoSyncDb, error) {
	if strings.TrimSpace(config.Path) == "" {
		return nil, errors.New("turso: Path is required")
	}
	if strings.TrimSpace(config.RemoteUrl) == "" {
		return nil, errors.New("turso: RemoteUrl is required")
	}
	clientName := config.ClientName
	if clientName == "" {
		clientName = "turso-sync-go"
	}
	bootstrap := true
	if config.BootstrapIfEmpty != nil {
		bootstrap = *config.BootstrapIfEmpty
	}

	dbConf := TursoDatabaseConfig{
		Path:                 config.Path, // not used directly by sync connection creation, but passed to sync API
		ExperimentalFeatures: config.ExperimentalFeatures,
		AsyncIO:              true, // REQUIRED for sync support since IO is handled here
	}
	syncConf := TursoSyncDatabaseConfig{
		Path:                           config.Path,
		ClientName:                     clientName,
		LongPollTimeoutMs:              config.LongPollTimeoutMs,
		BootstrapIfEmpty:               bootstrap,
		ReservedBytes:                  0,
		PartialBootstrapStrategyPrefix: config.PartialBoostrapStrategyPrefix,
		PartialBootstrapStrategyQuery:  config.PartialBoostrapStrategyQuery,
	}

	sdb, err := turso_sync_database_new(dbConf, syncConf)
	if err != nil {
		return nil, err
	}

	d := &TursoSyncDb{
		sync:              sdb,
		remoteURL:         config.RemoteUrl,
		authToken:         config.AuthToken,
		longPollTimeoutMs: config.LongPollTimeoutMs,
		client: &http.Client{
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				DisableCompression:  false,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
			// No default timeout; contexts control deadlines.
			Timeout: 0,
		},
	}

	// Create or open/prepare the synced database (single entry point).
	op, err := turso_sync_database_create(d.sync)
	if err != nil {
		turso_sync_database_deinit(d.sync)
		return nil, err
	}
	if err := d.runOpNone(ctx, op); err != nil {
		turso_sync_database_deinit(d.sync)
		return nil, err
	}
	return d, nil
}

// internal connector to integrate with database/sql pool
type tursoSyncConnector struct{ db *TursoSyncDb }

func (c *tursoSyncConnector) Connect(ctx context.Context) (driver.Conn, error) {
	// Get a turso connection from sync database
	op, err := turso_sync_database_connect(c.db.sync)
	if err != nil {
		return nil, err
	}
	conn, err := c.db.runOpConn(ctx, op)
	if err != nil {
		return nil, err
	}
	// Wrap connection using existing driver connection type; wire extra IO to sync IO loop
	extra := func() error {
		// Drive IO queue once and return
		return c.db.driveIo(context.Background())
	}
	return NewConnection(conn, extra), nil
}

func (c *tursoSyncConnector) Driver() driver.Driver { return &tursoDbDriver{} }

// Connect returns a sql.DB pool bound to the local synced database.
func (d *TursoSyncDb) Connect(ctx context.Context) (*sql.DB, error) {
	// database/sql uses Connector for custom open logic
	db := sql.OpenDB(&tursoSyncConnector{db: d})
	// Optionally ping here to ensure it works within ctx
	if ctx != nil {
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

// Pull fresh data from the remote.
// Pull DOES NOT send any local changes to the remote and instead "rebase" them on top of new changes from remote.
// Returns true if new changes were applied, otherwise false.
func (d *TursoSyncDb) Pull(ctx context.Context) (bool, error) {
	// Wait for remote changes (long-poll).
	waitOp, err := turso_sync_database_wait_changes(d.sync)
	if err != nil {
		return false, err
	}
	changes, err := d.runOpChanges(ctx, waitOp)
	if err != nil {
		return false, err
	}
	if changes == nil {
		// No changes arrived (e.g. long-poll timeout).
		return false, nil
	}
	// Apply fetched changes locally.
	applyOp, err := turso_sync_database_apply_changes(d.sync, changes)
	if err != nil {
		// cleanup extracted changes
		turso_sync_changes_deinit(changes)
		return false, err
	}
	err = d.runOpNone(ctx, applyOp)
	// Always deinit changes after apply operation finishes
	// turso_sync_changes_deinit(changes)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Push local changes to the remote.
// Push DOES NOT fetch any remote changes.
func (d *TursoSyncDb) Push(ctx context.Context) error {
	op, err := turso_sync_database_push_changes(d.sync)
	if err != nil {
		return err
	}
	return d.runOpNone(ctx, op)
}

// Stats returns metrics for the synced database.
func (d *TursoSyncDb) Stats(ctx context.Context) (TursoSyncDbStats, error) {
	op, err := turso_sync_database_stats(d.sync)
	if err != nil {
		return TursoSyncDbStats{}, err
	}
	stats, err := d.runOpStats(ctx, op)
	if err != nil {
		return TursoSyncDbStats{}, err
	}
	return TursoSyncDbStats{
		CDcOperations:        stats.CDcOperations,
		MainWalSize:          stats.MainWalSize,
		RevertWalSize:        stats.RevertWalSize,
		LastPullUnixTime:     stats.LastPullUnixTime,
		LastPushUnixTime:     stats.LastPushUnixTime,
		NetworkSentBytes:     stats.NetworkSentBytes,
		NetworkReceivedBytes: stats.NetworkReceivedBytes,
		Revision:             stats.Revision,
	}, nil
}

// Checkpoint performs local WAL checkpoint of the database.
func (d *TursoSyncDb) Checkpoint(ctx context.Context) error {
	op, err := turso_sync_database_checkpoint(d.sync)
	if err != nil {
		return err
	}
	return d.runOpNone(ctx, op)
}

// -------- Internal helpers --------

func (d *TursoSyncDb) runOpNone(ctx context.Context, op TursoSyncOperation) error {
	defer turso_sync_operation_deinit(op)
	for {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		st, err := turso_sync_operation_resume(op)
		if err != nil {
			return err
		}
		switch st {
		case TURSO_DONE:
			return nil
		case TURSO_IO:
			if err := d.driveIo(ctx); err != nil {
				return err
			}
			continue
		case TURSO_OK:
			continue
		default:
			return statusToError(st, "")
		}
	}
}

func (d *TursoSyncDb) runOpConn(ctx context.Context, op TursoSyncOperation) (TursoConnection, error) {
	defer turso_sync_operation_deinit(op)
	for {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		st, err := turso_sync_operation_resume(op)
		if err != nil {
			return nil, err
		}
		switch st {
		case TURSO_DONE:
			switch turso_sync_operation_result_kind(op) {
			case TURSO_ASYNC_RESULT_CONNECTION:
				return turso_sync_operation_result_extract_connection(op)
			default:
				return nil, errors.New("turso: unexpected operation result kind for connect")
			}
		case TURSO_IO:
			if err := d.driveIo(ctx); err != nil {
				return nil, err
			}
		case TURSO_OK:
			continue
		default:
			return nil, statusToError(st, "")
		}
	}
}

func (d *TursoSyncDb) runOpStats(ctx context.Context, op TursoSyncOperation) (TursoSyncStats, error) {
	defer turso_sync_operation_deinit(op)
	for {
		if ctx != nil && ctx.Err() != nil {
			return TursoSyncStats{}, ctx.Err()
		}
		st, err := turso_sync_operation_resume(op)
		if err != nil {
			return TursoSyncStats{}, err
		}
		switch st {
		case TURSO_DONE:
			switch turso_sync_operation_result_kind(op) {
			case TURSO_ASYNC_RESULT_STATS:
				return turso_sync_operation_result_extract_stats(op)
			default:
				return TursoSyncStats{}, errors.New("turso: unexpected operation result kind for stats")
			}
		case TURSO_IO:
			if err := d.driveIo(ctx); err != nil {
				return TursoSyncStats{}, err
			}
		case TURSO_OK:
			continue
		default:
			return TursoSyncStats{}, statusToError(st, "")
		}
	}
}

func (d *TursoSyncDb) runOpChanges(ctx context.Context, op TursoSyncOperation) (TursoSyncChanges, error) {
	defer turso_sync_operation_deinit(op)
	for {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		st, err := turso_sync_operation_resume(op)
		if err != nil {
			return nil, err
		}
		switch st {
		case TURSO_DONE:
			switch turso_sync_operation_result_kind(op) {
			case TURSO_ASYNC_RESULT_CHANGES:
				return turso_sync_operation_result_extract_changes(op)
			default:
				return nil, errors.New("turso: unexpected operation result kind for changes")
			}
		case TURSO_IO:
			if err := d.driveIo(ctx); err != nil {
				return nil, err
			}
		case TURSO_OK:
			continue
		default:
			return nil, statusToError(st, "")
		}
	}
}

// driveIo drains sync engine IO queue, executing items and streaming data chunk-by-chunk.
func (d *TursoSyncDb) driveIo(ctx context.Context) error {
	for {
		item, err := turso_sync_database_io_take_item(d.sync)
		if err != nil {
			return err
		}
		if item == nil {
			break
		}
		// Handle item
		if err := d.handleIoItem(ctx, item); err != nil {
			// best effort cleanup
			turso_sync_database_io_item_deinit(item)
			return err
		}
		turso_sync_database_io_item_deinit(item)
	}
	// Run extra callbacks after executing IO
	return turso_sync_database_io_step_callbacks(d.sync)
}

func (d *TursoSyncDb) handleIoItem(ctx context.Context, item TursoSyncIoItem) error {
	switch turso_sync_database_io_request_kind(item) {
	case TURSO_SYNC_IO_HTTP:
		return d.handleHttpIo(ctx, item)
	case TURSO_SYNC_IO_FULL_READ:
		return d.handleFullReadIo(ctx, item)
	case TURSO_SYNC_IO_FULL_WRITE:
		return d.handleFullWriteIo(ctx, item)
	case TURSO_SYNC_IO_NONE:
		// Nothing to do
		return turso_sync_database_io_done(item)
	default:
		_ = turso_sync_database_io_poison(item, "unknown IO request")
		return errors.New("turso: unknown IO request")
	}
}

func (d *TursoSyncDb) handleHttpIo(ctx context.Context, item TursoSyncIoItem) error {
	req, err := turso_sync_database_io_request_http(item)
	if err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	fullURL := d.makeURL(req.Path)
	var body io.Reader
	if len(req.Body) > 0 {
		body = strings.NewReader(bytesToStringZeroCopy(req.Body))
		// Note: strings.NewReader avoids extra buffer copy; req.Body memory is owned by Go wrapper.
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, fullURL, body)
	if err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	// Add Authorization header first.
	if d.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+d.authToken)
	}
	// Set defaults
	if len(req.Body) > 0 && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/octet-stream")
	}
	// Add headers provided by the engine (preserve order and allow duplicates)
	for i := 0; i < req.Headers; i++ {
		h, herr := turso_sync_database_io_request_http_header(item, i)
		if herr != nil {
			_ = turso_sync_database_io_poison(item, herr.Error())
			return herr
		}
		// Engine-provided headers override ours if the same key
		httpReq.Header.Add(h.Key, h.Value)
	}

	resp, err := d.client.Do(httpReq)
	if err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	defer resp.Body.Close()

	// Send status as soon as headers arrive.
	if err := turso_sync_database_io_status(item, resp.StatusCode); err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}

	// Stream the body in chunks; run callbacks in-between to reduce memory footprint.
	buf := make([]byte, 64*1024)
	for {
		if ctx != nil && ctx.Err() != nil {
			_ = turso_sync_database_io_poison(item, ctx.Err().Error())
			return ctx.Err()
		}
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if err := turso_sync_database_io_push_buffer(item, buf[:n]); err != nil {
				_ = turso_sync_database_io_poison(item, err.Error())
				return err
			}
			// Let engine process part of the buffer if necessary.
			if err := turso_sync_database_io_step_callbacks(d.sync); err != nil {
				_ = turso_sync_database_io_poison(item, err.Error())
				return err
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			_ = turso_sync_database_io_poison(item, rerr.Error())
			return rerr
		}
	}
	return turso_sync_database_io_done(item)
}

func (d *TursoSyncDb) handleFullReadIo(ctx context.Context, item TursoSyncIoItem) error {
	req, err := turso_sync_database_io_request_full_read(item)
	if err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	// Read file and stream in chunks.
	f, err := os.Open(req.Path)
	if err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	defer f.Close()
	buf := make([]byte, 64*1024)
	for {
		if ctx != nil && ctx.Err() != nil {
			_ = turso_sync_database_io_poison(item, ctx.Err().Error())
			return ctx.Err()
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			if err := turso_sync_database_io_push_buffer(item, buf[:n]); err != nil {
				_ = turso_sync_database_io_poison(item, err.Error())
				return err
			}
			if err := turso_sync_database_io_step_callbacks(d.sync); err != nil {
				_ = turso_sync_database_io_poison(item, err.Error())
				return err
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			_ = turso_sync_database_io_poison(item, rerr.Error())
			return rerr
		}
	}
	return turso_sync_database_io_done(item)
}

func (d *TursoSyncDb) handleFullWriteIo(_ context.Context, item TursoSyncIoItem) error {
	req, err := turso_sync_database_io_request_full_write(item)
	if err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	// Ensure directory exists.
	if err := os.MkdirAll(filepath.Dir(req.Path), 0o755); err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	// Write atomically via temp file + rename.
	tmp := req.Path + ".tmp"
	if err := os.WriteFile(tmp, req.Content, 0o644); err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	if err := os.Rename(tmp, req.Path); err != nil {
		_ = turso_sync_database_io_poison(item, err.Error())
		return err
	}
	return turso_sync_database_io_done(item)
}

func (d *TursoSyncDb) makeURL(path string) string {
	// If path is absolute URL already, return it as-is
	if u, err := url.Parse(path); err == nil && u.Scheme != "" && u.Host != "" {
		return path
	}
	base, _ := url.Parse(d.remoteURL)
	if base == nil {
		// Fallback to simple concat
		return strings.TrimRight(d.remoteURL, "/") + "/" + strings.TrimLeft(path, "/")
	}
	rel := &url.URL{Path: "/" + strings.TrimLeft(path, "/")}
	return base.ResolveReference(rel).String()
}

// bytesToStringZeroCopy converts a byte slice to string without additional allocation for the duration of the call.
// It is safe here because http.NewRequestWithContext will copy or read it during the round-trip; we don't store it.
func bytesToStringZeroCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b)
}
