// Package rundb provides a SQLite-backed store for kilroy run operational state.
// Every run, node execution, outcome, and edge decision is recorded and queryable.
package rundb

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// DB wraps a SQLite database for run state storage.
type DB struct {
	db   *sql.DB
	path string
}

// DefaultPath returns the default global database path.
func DefaultPath() string {
	stateHome := os.Getenv("XDG_STATE_HOME")
	if stateHome == "" {
		home, _ := os.UserHomeDir()
		stateHome = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(stateHome, "kilroy", "runs.db")
}

// Open opens (or creates) the run database at the given path and applies
// any pending migrations. Uses WAL mode for concurrent reads and a 5-second
// busy timeout so concurrent writers retry instead of failing immediately.
// Includes retry logic to handle concurrent initialization races.
func Open(path string) (*DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(wal)&_pragma=foreign_keys(on)&_pragma=busy_timeout(5000)&_pragma=synchronous(normal)")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(1)

	rdb := &DB{db: db, path: path}

	// Retry migration with exponential backoff to handle concurrent
	// initialization races when multiple goroutines open a fresh DB.
	const maxRetries = 10
	const baseDelay = 50 * time.Millisecond
	var migrateErr error
	for i := 0; i < maxRetries; i++ {
		migrateErr = rdb.migrate()
		if migrateErr == nil {
			return rdb, nil
		}
		// Only retry on database locked errors.
		if !isBusyError(migrateErr) {
			break
		}
		// Wait before retrying with exponential backoff.
		time.Sleep(baseDelay * time.Duration(1<<i))
	}

	db.Close()
	return nil, fmt.Errorf("migrate: %w", migrateErr)
}

// isBusyError checks if an error is a SQLite busy/locked error.
func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "busy")
}

// Close closes the database connection.
func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.Close()
}

// SQL returns the underlying *sql.DB for advanced queries.
func (d *DB) SQL() *sql.DB {
	return d.db
}

// migrate applies numbered SQL migration files from the embedded filesystem.
// Uses a single BEGIN EXCLUSIVE transaction to wrap all migration steps,
// ensuring only one process can migrate at a time.
func (d *DB) migrate() error {
	ctx := context.Background()

	// Use a single connection for the entire migration to ensure exclusive access.
	// This prevents concurrent migrations from interfering with each other.
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	// Begin exclusive transaction on this connection.
	// BEGIN EXCLUSIVE acquires a write lock immediately and prevents other
	// connections from accessing the database until the transaction completes.
	if _, err := conn.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return fmt.Errorf("begin exclusive transaction: %w", err)
	}
	// Ensure we roll back on error (use a defer that checks the error state).
	committed := false
	defer func() {
		if !committed {
			conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	// Create the migrations table first (idempotent).
	_, err = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	if err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	// Sort by filename to ensure order.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version := 0
		if _, err := fmt.Sscanf(entry.Name(), "%d_", &version); err != nil || version == 0 {
			continue
		}

		// Check if already applied.
		var applied int
		row := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version)
		if err := row.Scan(&applied); err != nil {
			return fmt.Errorf("check migration %d: %w", version, err)
		}
		if applied > 0 {
			continue
		}

		// Apply the migration.
		content, err := migrationsFS.ReadFile(filepath.Join("migrations", entry.Name()))
		if err != nil {
			return fmt.Errorf("read migration %d: %w", version, err)
		}

		if _, err := conn.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("apply migration %d: %w", version, err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (?)", version); err != nil {
			return fmt.Errorf("record migration %d: %w", version, err)
		}
	}

	// Commit the exclusive transaction.
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit migration transaction: %w", err)
	}
	committed = true
	return nil
}
