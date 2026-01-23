package turso // import "github.com/tursodatabase/turso/go"

import (
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// ------------- Opaque types for sync engine -------------

type turso_sync_database_t struct{}
type turso_sync_operation_t struct{}
type turso_sync_io_item_t struct{}
type turso_sync_changes_t struct{}

type TursoSyncDatabase *turso_sync_database_t
type TursoSyncOperation *turso_sync_operation_t
type TursoSyncIoItem *turso_sync_io_item_t
type TursoSyncChanges *turso_sync_changes_t

// ------------- Enums -------------

type TursoSyncIoRequestType int32

const (
	TURSO_SYNC_IO_NONE       TursoSyncIoRequestType = 0
	TURSO_SYNC_IO_HTTP       TursoSyncIoRequestType = 1
	TURSO_SYNC_IO_FULL_READ  TursoSyncIoRequestType = 2
	TURSO_SYNC_IO_FULL_WRITE TursoSyncIoRequestType = 3
)

type TursoSyncOperationResultType int32

const (
	TURSO_ASYNC_RESULT_NONE       TursoSyncOperationResultType = 0
	TURSO_ASYNC_RESULT_CONNECTION TursoSyncOperationResultType = 1
	TURSO_ASYNC_RESULT_CHANGES    TursoSyncOperationResultType = 2
	TURSO_ASYNC_RESULT_STATS      TursoSyncOperationResultType = 3
)

// ------------- Public binding types -------------

// TursoSyncDatabaseConfig describes database sync configuration.
type TursoSyncDatabaseConfig struct {
	// Path to the main database file (auxiliary files will derive names from this path)
	Path string
	// optional remote url (libsql://..., https://... or http://...)
	// this URL will be saved in the database metadata file in order to be able to reuse it if later client will be constructed without explicit remote url
	RemoteUrl string
	// Arbitrary client name used as a prefix for unique client id
	ClientName string
	// Long poll timeout for pull method in milliseconds
	LongPollTimeoutMs int
	// Bootstrap db if empty; if set - client will be able to connect to fresh db only when network is online
	BootstrapIfEmpty bool
	// Reserved bytes set for the database - necessary if remote encryption is set for the db in cloud
	ReservedBytes int
	// Prefix bootstrap strategy enabling partial sync that lazily pulls pages on demand and bootstraps db with first N bytes
	PartialBootstrapStrategyPrefix int
	// Query bootstrap strategy enabling partial sync - bootstraps db with pages touched by the server with given SQL query
	PartialBootstrapStrategyQuery string
	// optional parameter which defines segment size for lazy loading from remote server
	// one of valid PartialBootstrapStrategy* values MUST be set in order for this setting to have some effect
	PartialBootstrapSegmentSize int
	// optional parameter which defines if pages prefetch must be enabled
	// one of valid PartialBootstrapStrategy* values MUST be set in order for this setting to have some effect
	PartialBootstrapPrefetch bool
	// optional base64-encoded encryption key for remote encrypted databases
	RemoteEncryptionKey string
	// optional encryption cipher name (e.g. "aes256gcm", "chacha20poly1305")
	RemoteEncryptionCipher string
}

// TursoSyncStats holds sync engine stats.
type TursoSyncStats struct {
	CDcOperations        int64
	MainWalSize          int64
	RevertWalSize        int64
	LastPullUnixTime     int64
	LastPushUnixTime     int64
	NetworkSentBytes     int64
	NetworkReceivedBytes int64
	Revision             string
}

// HTTP request description used by IO layer.
type TursoSyncIoHttpRequest struct {
	Url     string
	Method  string
	Path    string
	Body    []byte
	Headers int
}

// HTTP header key-value pair.
type TursoSyncIoHttpHeader struct {
	Key   string
	Value string
}

// Atomic read request description.
type TursoSyncIoFullReadRequest struct {
	Path string
}

// Atomic write request description.
type TursoSyncIoFullWriteRequest struct {
	Path    string
	Content []byte
}

// ------------- Private C-compatible structs -------------

type turso_sync_database_config_t struct {
	path                              uintptr // const char*
	remote_url                        uintptr // const char*
	client_name                       uintptr // const char*
	long_poll_timeout_ms              int32
	bootstrap_if_empty                bool
	reserved_bytes                    int32
	partial_bootstrap_strategy_prefix int32
	partial_bootstrap_strategy_query  uintptr // const char*
	partial_bootstrap_segment_size    uintptr
	partial_bootstrap_prefetch        bool
	remote_encryption_key             uintptr // const char*
	remote_encryption_cipher          uintptr // const char*
}

type turso_sync_io_http_request_t struct {
	url     turso_slice_ref_t
	method  turso_slice_ref_t
	path    turso_slice_ref_t
	body    turso_slice_ref_t
	headers int32
}

type turso_sync_io_http_header_t struct {
	key   turso_slice_ref_t
	value turso_slice_ref_t
}

type turso_sync_io_full_read_request_t struct {
	path turso_slice_ref_t
}

type turso_sync_io_full_write_request_t struct {
	path    turso_slice_ref_t
	content turso_slice_ref_t
}

type turso_sync_stats_t struct {
	cdc_operations         int64
	main_wal_size          int64
	revert_wal_size        int64
	last_pull_unix_time    int64
	last_push_unix_time    int64
	network_sent_bytes     int64
	network_received_bytes int64
	revision               turso_slice_ref_t
}

// ------------- C extern function vars -------------

var (
	c_turso_sync_database_new func(
		dbConfig *turso_database_config_t,
		syncConfig *turso_sync_database_config_t,
		database **turso_sync_database_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_open func(
		self TursoSyncDatabase,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_create func(
		self TursoSyncDatabase,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_connect func(
		self TursoSyncDatabase,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_stats func(
		self TursoSyncDatabase,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_checkpoint func(
		self TursoSyncDatabase,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_push_changes func(
		self TursoSyncDatabase,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_wait_changes func(
		self TursoSyncDatabase,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_apply_changes func(
		self TursoSyncDatabase,
		changes TursoSyncChanges,
		operation **turso_sync_operation_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_operation_resume func(
		self TursoSyncOperation,
		errorOptOut **byte,
	) int32

	c_turso_sync_operation_result_kind func(
		self TursoSyncOperation,
	) int32

	c_turso_sync_operation_result_extract_connection func(
		self TursoSyncOperation,
		connection **turso_connection_t,
	) int32

	c_turso_sync_operation_result_extract_changes func(
		self TursoSyncOperation,
		changes **turso_sync_changes_t,
	) int32

	c_turso_sync_operation_result_extract_stats func(
		self TursoSyncOperation,
		stats *turso_sync_stats_t,
	) int32

	c_turso_sync_database_io_take_item func(
		self TursoSyncDatabase,
		item **turso_sync_io_item_t,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_io_step_callbacks func(
		self TursoSyncDatabase,
		errorOptOut **byte,
	) int32

	c_turso_sync_database_io_request_kind func(
		self TursoSyncIoItem,
	) int32

	c_turso_sync_database_io_request_http func(
		self TursoSyncIoItem,
		request *turso_sync_io_http_request_t,
	) int32

	c_turso_sync_database_io_request_http_header func(
		self TursoSyncIoItem,
		index uintptr,
		header *turso_sync_io_http_header_t,
	) int32

	c_turso_sync_database_io_request_full_read func(
		self TursoSyncIoItem,
		request *turso_sync_io_full_read_request_t,
	) int32

	c_turso_sync_database_io_request_full_write func(
		self TursoSyncIoItem,
		request *turso_sync_io_full_write_request_t,
	) int32

	c_turso_sync_database_io_poison func(
		self TursoSyncIoItem,
		err *turso_slice_ref_t,
	) int32

	c_turso_sync_database_io_status func(
		self TursoSyncIoItem,
		status int32,
	) int32

	c_turso_sync_database_io_push_buffer func(
		self TursoSyncIoItem,
		buffer *turso_slice_ref_t,
	) int32

	c_turso_sync_database_io_done func(
		self TursoSyncIoItem,
	) int32

	c_turso_sync_database_deinit func(
		self TursoSyncDatabase,
	)

	c_turso_sync_operation_deinit func(
		self TursoSyncOperation,
	)

	c_turso_sync_database_io_item_deinit func(
		self TursoSyncIoItem,
	)

	c_turso_sync_changes_deinit func(
		self TursoSyncChanges,
	)
)

// ------------- Registration -------------

// registerTursoSync registers Turso Sync C API function pointers from the given library handle.
// Do not load library here; it is done externally.
func registerTursoSync(handle uintptr) error {
	purego.RegisterLibFunc(&c_turso_sync_database_new, handle, "turso_sync_database_new")
	purego.RegisterLibFunc(&c_turso_sync_database_open, handle, "turso_sync_database_open")
	purego.RegisterLibFunc(&c_turso_sync_database_create, handle, "turso_sync_database_create")
	purego.RegisterLibFunc(&c_turso_sync_database_connect, handle, "turso_sync_database_connect")
	purego.RegisterLibFunc(&c_turso_sync_database_stats, handle, "turso_sync_database_stats")
	purego.RegisterLibFunc(&c_turso_sync_database_checkpoint, handle, "turso_sync_database_checkpoint")
	purego.RegisterLibFunc(&c_turso_sync_database_push_changes, handle, "turso_sync_database_push_changes")
	purego.RegisterLibFunc(&c_turso_sync_database_wait_changes, handle, "turso_sync_database_wait_changes")
	purego.RegisterLibFunc(&c_turso_sync_database_apply_changes, handle, "turso_sync_database_apply_changes")
	purego.RegisterLibFunc(&c_turso_sync_operation_resume, handle, "turso_sync_operation_resume")
	purego.RegisterLibFunc(&c_turso_sync_operation_result_kind, handle, "turso_sync_operation_result_kind")
	purego.RegisterLibFunc(&c_turso_sync_operation_result_extract_connection, handle, "turso_sync_operation_result_extract_connection")
	purego.RegisterLibFunc(&c_turso_sync_operation_result_extract_changes, handle, "turso_sync_operation_result_extract_changes")
	purego.RegisterLibFunc(&c_turso_sync_operation_result_extract_stats, handle, "turso_sync_operation_result_extract_stats")
	purego.RegisterLibFunc(&c_turso_sync_database_io_take_item, handle, "turso_sync_database_io_take_item")
	purego.RegisterLibFunc(&c_turso_sync_database_io_step_callbacks, handle, "turso_sync_database_io_step_callbacks")
	purego.RegisterLibFunc(&c_turso_sync_database_io_request_kind, handle, "turso_sync_database_io_request_kind")
	purego.RegisterLibFunc(&c_turso_sync_database_io_request_http, handle, "turso_sync_database_io_request_http")
	purego.RegisterLibFunc(&c_turso_sync_database_io_request_http_header, handle, "turso_sync_database_io_request_http_header")
	purego.RegisterLibFunc(&c_turso_sync_database_io_request_full_read, handle, "turso_sync_database_io_request_full_read")
	purego.RegisterLibFunc(&c_turso_sync_database_io_request_full_write, handle, "turso_sync_database_io_request_full_write")
	purego.RegisterLibFunc(&c_turso_sync_database_io_poison, handle, "turso_sync_database_io_poison")
	purego.RegisterLibFunc(&c_turso_sync_database_io_status, handle, "turso_sync_database_io_status")
	purego.RegisterLibFunc(&c_turso_sync_database_io_push_buffer, handle, "turso_sync_database_io_push_buffer")
	purego.RegisterLibFunc(&c_turso_sync_database_io_done, handle, "turso_sync_database_io_done")
	purego.RegisterLibFunc(&c_turso_sync_database_deinit, handle, "turso_sync_database_deinit")
	purego.RegisterLibFunc(&c_turso_sync_operation_deinit, handle, "turso_sync_operation_deinit")
	purego.RegisterLibFunc(&c_turso_sync_database_io_item_deinit, handle, "turso_sync_database_io_item_deinit")
	purego.RegisterLibFunc(&c_turso_sync_changes_deinit, handle, "turso_sync_changes_deinit")
	return nil
}

// ------------- Helpers -------------

func sliceRefToBytesCopy(s turso_slice_ref_t) []byte {
	if s.ptr == 0 || s.len == 0 {
		return nil
	}
	p := (*byte)(unsafe.Pointer(s.ptr))
	n := int(s.len)
	src := unsafe.Slice(p, n)
	dst := make([]byte, n)
	copy(dst, src)
	return dst
}

func sliceRefToStringCopy(s turso_slice_ref_t) string {
	b := sliceRefToBytesCopy(s)
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

// ------------- Go wrappers over C API -------------

// turso_sync_database_new creates the database sync holder but does not open it.
func turso_sync_database_new(dbConfig TursoDatabaseConfig, syncConfig TursoSyncDatabaseConfig) (TursoSyncDatabase, error) {
	// Build C database config
	var cdb turso_database_config_t
	var pathBytes, expBytes []byte
	pathBytes, cdb.path = makeCStringBytes(dbConfig.Path)
	if dbConfig.ExperimentalFeatures != "" {
		expBytes, cdb.experimental_features = makeCStringBytes(dbConfig.ExperimentalFeatures)
	}
	cdb.async_io = 0
	if dbConfig.AsyncIO {
		cdb.async_io = 1
	}

	// Build C sync config
	var csync turso_sync_database_config_t
	var syncPathBytes, remoteUrlBytes, clientNameBytes, queryBytes []byte
	var encryptionKeyBytes, encryptionCipherBytes []byte
	syncPathBytes, csync.path = makeCStringBytes(syncConfig.Path)
	remoteUrlBytes, csync.remote_url = makeCStringBytes(syncConfig.RemoteUrl)
	clientNameBytes, csync.client_name = makeCStringBytes(syncConfig.ClientName)
	csync.long_poll_timeout_ms = int32(syncConfig.LongPollTimeoutMs)
	csync.bootstrap_if_empty = syncConfig.BootstrapIfEmpty
	csync.reserved_bytes = int32(syncConfig.ReservedBytes)
	csync.partial_bootstrap_strategy_prefix = int32(syncConfig.PartialBootstrapStrategyPrefix)
	csync.partial_bootstrap_segment_size = uintptr(syncConfig.PartialBootstrapSegmentSize)
	csync.partial_bootstrap_prefetch = syncConfig.PartialBootstrapPrefetch
	if syncConfig.PartialBootstrapStrategyQuery != "" {
		queryBytes, csync.partial_bootstrap_strategy_query = makeCStringBytes(syncConfig.PartialBootstrapStrategyQuery)
	}
	if syncConfig.RemoteEncryptionKey != "" {
		encryptionKeyBytes, csync.remote_encryption_key = makeCStringBytes(syncConfig.RemoteEncryptionKey)
	}
	if syncConfig.RemoteEncryptionCipher != "" {
		encryptionCipherBytes, csync.remote_encryption_cipher = makeCStringBytes(syncConfig.RemoteEncryptionCipher)
	}

	var db *turso_sync_database_t
	var errPtr *byte
	status := c_turso_sync_database_new(&cdb, &csync, &db, &errPtr)

	// Keep Go memory alive during C call
	runtime.KeepAlive(pathBytes)
	runtime.KeepAlive(remoteUrlBytes)
	runtime.KeepAlive(expBytes)
	runtime.KeepAlive(syncPathBytes)
	runtime.KeepAlive(clientNameBytes)
	runtime.KeepAlive(queryBytes)
	runtime.KeepAlive(encryptionKeyBytes)
	runtime.KeepAlive(encryptionCipherBytes)

	if status == int32(TURSO_OK) {
		return TursoSyncDatabase(db), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_open opens prepared synced database. Fails if no properly setup database exists.
// AsyncOperation returns None.
func turso_sync_database_open(self TursoSyncDatabase) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_open(self, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_create opens or prepares synced database or creates it if needed.
// AsyncOperation returns None.
func turso_sync_database_create(self TursoSyncDatabase) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_create(self, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_connect creates a turso database connection.
// SAFETY: synced database must be opened before this operation.
// AsyncOperation returns Connection.
func turso_sync_database_connect(self TursoSyncDatabase) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_connect(self, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_stats collects stats about synced database.
// AsyncOperation returns Stats.
func turso_sync_database_stats(self TursoSyncDatabase) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_stats(self, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_checkpoint performs WAL checkpoint for the synced database.
// AsyncOperation returns None.
func turso_sync_database_checkpoint(self TursoSyncDatabase) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_checkpoint(self, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_push_changes pushes local changes to remote.
// AsyncOperation returns None.
func turso_sync_database_push_changes(self TursoSyncDatabase) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_push_changes(self, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_wait_changes waits for remote changes.
// AsyncOperation returns Changes.
func turso_sync_database_wait_changes(self TursoSyncDatabase) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_wait_changes(self, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_apply_changes applies remote changes locally.
// SAFETY: caller must ensure that no other methods are executing concurrently (push/wait/checkpoint).
//
// the method CONSUMES turso_sync_changes_t instance and caller no longer owns it after the call
// So, the changes MUST NOT be explicitly deallocated after the method call (either successful or not)
//
// AsyncOperation returns None.
func turso_sync_database_apply_changes(self TursoSyncDatabase, changes TursoSyncChanges) (TursoSyncOperation, error) {
	var op *turso_sync_operation_t
	var errPtr *byte
	status := c_turso_sync_database_apply_changes(self, changes, &op, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncOperation(op), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_operation_resume resumes async operation.
// Returns status code (OK/IO/DONE or error code) and error if any.
func turso_sync_operation_resume(self TursoSyncOperation) (TursoStatusCode, error) {
	var errPtr *byte
	status := c_turso_sync_operation_resume(self, &errPtr)
	switch TursoStatusCode(status) {
	case TURSO_OK, TURSO_IO, TURSO_DONE:
		return TursoStatusCode(status), nil
	default:
		msg := decodeAndFreeCString(errPtr)
		return TursoStatusCode(status), statusToError(TursoStatusCode(status), msg)
	}
}

// turso_sync_operation_result_kind extracts operation result kind.
func turso_sync_operation_result_kind(self TursoSyncOperation) TursoSyncOperationResultType {
	return TursoSyncOperationResultType(c_turso_sync_operation_result_kind(self))
}

// turso_sync_operation_result_extract_connection extracts Connection result from finished operation.
func turso_sync_operation_result_extract_connection(self TursoSyncOperation) (TursoConnection, error) {
	var conn *turso_connection_t
	status := c_turso_sync_operation_result_extract_connection(self, &conn)
	if status == int32(TURSO_OK) {
		return TursoConnection(conn), nil
	}
	return nil, statusToError(TursoStatusCode(status), "")
}

// turso_sync_operation_result_extract_changes extracts Changes result from finished operation.
// If no changes were fetched - return TURSO_OK and set changes to null pointer
func turso_sync_operation_result_extract_changes(self TursoSyncOperation) (TursoSyncChanges, error) {
	var ch *turso_sync_changes_t
	status := c_turso_sync_operation_result_extract_changes(self, &ch)
	if status == int32(TURSO_OK) {
		return TursoSyncChanges(ch), nil
	}
	return nil, statusToError(TursoStatusCode(status), "")
}

// turso_sync_operation_result_extract_stats extracts Stats result from finished operation.
func turso_sync_operation_result_extract_stats(self TursoSyncOperation) (TursoSyncStats, error) {
	var cstats turso_sync_stats_t
	status := c_turso_sync_operation_result_extract_stats(self, &cstats)
	if status != int32(TURSO_OK) {
		return TursoSyncStats{}, statusToError(TursoStatusCode(status), "")
	}
	return TursoSyncStats{
		CDcOperations:        cstats.cdc_operations,
		MainWalSize:          cstats.main_wal_size,
		RevertWalSize:        cstats.revert_wal_size,
		LastPullUnixTime:     cstats.last_pull_unix_time,
		LastPushUnixTime:     cstats.last_push_unix_time,
		NetworkSentBytes:     cstats.network_sent_bytes,
		NetworkReceivedBytes: cstats.network_received_bytes,
		Revision:             sliceRefToStringCopy(cstats.revision),
	}, nil
}

// turso_sync_database_io_take_item tries to take IO request from the sync engine IO queue.
// if queue is empty - returns nil pointer to the TursoSyncIoItem
func turso_sync_database_io_take_item(self TursoSyncDatabase) (TursoSyncIoItem, error) {
	var item *turso_sync_io_item_t
	var errPtr *byte
	status := c_turso_sync_database_io_take_item(self, &item, &errPtr)
	if status == int32(TURSO_OK) {
		return TursoSyncIoItem(item), nil
	}
	msg := decodeAndFreeCString(errPtr)
	return nil, statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_io_step_callbacks runs extra database callbacks after IO execution.
func turso_sync_database_io_step_callbacks(self TursoSyncDatabase) error {
	var errPtr *byte
	status := c_turso_sync_database_io_step_callbacks(self, &errPtr)
	if status == int32(TURSO_OK) {
		return nil
	}
	msg := decodeAndFreeCString(errPtr)
	return statusToError(TursoStatusCode(status), msg)
}

// turso_sync_database_io_request_kind returns the IO request kind.
func turso_sync_database_io_request_kind(self TursoSyncIoItem) TursoSyncIoRequestType {
	return TursoSyncIoRequestType(c_turso_sync_database_io_request_kind(self))
}

// turso_sync_database_io_request_http gets HTTP request fields for an IO item.
func turso_sync_database_io_request_http(self TursoSyncIoItem) (TursoSyncIoHttpRequest, error) {
	var creq turso_sync_io_http_request_t
	status := c_turso_sync_database_io_request_http(self, &creq)
	if status != int32(TURSO_OK) {
		return TursoSyncIoHttpRequest{}, statusToError(TursoStatusCode(status), "")
	}
	return TursoSyncIoHttpRequest{
		Url:     sliceRefToStringCopy(creq.url),
		Method:  sliceRefToStringCopy(creq.method),
		Path:    sliceRefToStringCopy(creq.path),
		Body:    sliceRefToBytesCopy(creq.body),
		Headers: int(creq.headers),
	}, nil
}

// turso_sync_database_io_request_http_header returns HTTP header key-value pair at index.
func turso_sync_database_io_request_http_header(self TursoSyncIoItem, index int) (TursoSyncIoHttpHeader, error) {
	var ch turso_sync_io_http_header_t
	status := c_turso_sync_database_io_request_http_header(self, uintptr(index), &ch)
	if status != int32(TURSO_OK) {
		return TursoSyncIoHttpHeader{}, statusToError(TursoStatusCode(status), "")
	}
	return TursoSyncIoHttpHeader{
		Key:   sliceRefToStringCopy(ch.key),
		Value: sliceRefToStringCopy(ch.value),
	}, nil
}

// turso_sync_database_io_request_full_read returns atomic read request fields.
func turso_sync_database_io_request_full_read(self TursoSyncIoItem) (TursoSyncIoFullReadRequest, error) {
	var r turso_sync_io_full_read_request_t
	status := c_turso_sync_database_io_request_full_read(self, &r)
	if status != int32(TURSO_OK) {
		return TursoSyncIoFullReadRequest{}, statusToError(TursoStatusCode(status), "")
	}
	return TursoSyncIoFullReadRequest{Path: sliceRefToStringCopy(r.path)}, nil
}

// turso_sync_database_io_request_full_write returns atomic write request fields.
func turso_sync_database_io_request_full_write(self TursoSyncIoItem) (TursoSyncIoFullWriteRequest, error) {
	var r turso_sync_io_full_write_request_t
	status := c_turso_sync_database_io_request_full_write(self, &r)
	if status != int32(TURSO_OK) {
		return TursoSyncIoFullWriteRequest{}, statusToError(TursoStatusCode(status), "")
	}
	return TursoSyncIoFullWriteRequest{
		Path:    sliceRefToStringCopy(r.path),
		Content: sliceRefToBytesCopy(r.content),
	}, nil
}

// turso_sync_database_io_poison marks IO request completion with error.
func turso_sync_database_io_poison(self TursoSyncIoItem, errMsg string) error {
	var ref turso_slice_ref_t
	var bytes []byte
	if errMsg != "" {
		bytes = []byte(errMsg)
		ref.ptr = uintptr(unsafe.Pointer(&bytes[0]))
		ref.len = uintptr(len(bytes))
	}
	status := c_turso_sync_database_io_poison(self, &ref)
	// Keep Go memory alive during call
	runtime.KeepAlive(bytes)
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_sync_database_io_status sets IO request completion status (e.g. HTTP status).
func turso_sync_database_io_status(self TursoSyncIoItem, statusCode int) error {
	status := c_turso_sync_database_io_status(self, int32(statusCode))
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_sync_database_io_push_buffer pushes bytes to the IO completion buffer.
func turso_sync_database_io_push_buffer(self TursoSyncIoItem, buffer []byte) error {
	var ref turso_slice_ref_t
	if len(buffer) > 0 {
		ref.ptr = uintptr(unsafe.Pointer(&buffer[0]))
		ref.len = uintptr(len(buffer))
	}
	status := c_turso_sync_database_io_push_buffer(self, &ref)
	// Keep Go memory alive during call
	runtime.KeepAlive(buffer)
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_sync_database_io_done sets IO request completion as done.
func turso_sync_database_io_done(self TursoSyncIoItem) error {
	status := c_turso_sync_database_io_done(self)
	if status == int32(TURSO_OK) {
		return nil
	}
	return statusToError(TursoStatusCode(status), "")
}

// turso_sync_database_deinit deallocates a TursoDatabaseSync.
func turso_sync_database_deinit(self TursoSyncDatabase) {
	c_turso_sync_database_deinit(self)
}

// turso_sync_operation_deinit deallocates a TursoAsyncOperation.
func turso_sync_operation_deinit(self TursoSyncOperation) {
	c_turso_sync_operation_deinit(self)
}

// turso_sync_database_io_item_deinit deallocates a SyncEngineIoQueueItem.
func turso_sync_database_io_item_deinit(self TursoSyncIoItem) {
	c_turso_sync_database_io_item_deinit(self)
}

// turso_sync_changes_deinit deallocates a TursoDatabaseSyncChanges.
func turso_sync_changes_deinit(self TursoSyncChanges) {
	c_turso_sync_changes_deinit(self)
}
