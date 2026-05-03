package tursogo_serverless

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

// TursoServerlessConfig holds configuration for a serverless Turso connection.
type TursoServerlessConfig struct {
	// URL can use turso://, https://, http://, or libsql:// schemes.
	URL string
	// AuthToken for remote authentication (optional for local dev).
	AuthToken string
}

// TursoServerlessDb is a high-level serverless database wrapper.
type TursoServerlessDb struct {
	url       string
	authToken string
}

// NewTursoServerlessDb creates a new serverless database handle.
func NewTursoServerlessDb(config TursoServerlessConfig) (*TursoServerlessDb, error) {
	if config.URL == "" {
		return nil, errors.New("turso: empty URL in TursoServerlessConfig")
	}
	return &TursoServerlessDb{
		url:       normalizeURL(config.URL),
		authToken: config.AuthToken,
	}, nil
}

// Connect returns a *sql.DB backed by the serverless Turso server.
func (db *TursoServerlessDb) Connect() (*sql.DB, error) {
	connector := &TursoServerlessConnector{
		url:       db.url,
		authToken: db.authToken,
	}
	return sql.OpenDB(connector), nil
}

// TursoServerlessConnector implements driver.Connector for programmatic use.
type TursoServerlessConnector struct {
	url       string
	authToken string
}

var _ driver.Connector = (*TursoServerlessConnector)(nil)

// Connect creates a new serverless connection.
func (c *TursoServerlessConnector) Connect(_ context.Context) (driver.Conn, error) {
	return newRemoteConn(c.url, c.authToken), nil
}

// Driver returns the underlying driver.
func (c *TursoServerlessConnector) Driver() driver.Driver {
	return &remoteDriver{}
}
