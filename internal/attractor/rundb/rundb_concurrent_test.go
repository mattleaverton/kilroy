// Concurrent access tests for rundb.
package rundb

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestConcurrentOpen_FreshDB spawns 12 goroutines that all try to open
// a fresh database file simultaneously. All should succeed with no
// "database is locked" errors due to the exclusive transaction wrapping
// the migration process.
func TestConcurrentOpen_FreshDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent.db")

	const numGoroutines = 12
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			db, err := Open(dbPath)
			if err != nil {
				errors <- err
				return
			}
			db.Close()
		}(i)
	}

	wg.Wait()
	close(errors)

	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}

	if len(allErrors) > 0 {
		t.Fatalf("got %d errors from %d concurrent opens: %v", len(allErrors), numGoroutines, allErrors)
	}

	// Verify the database was created and is usable.
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("final Open: %v", err)
	}
	defer db.Close()

	// Verify migrations were applied.
	var count int
	err = db.SQL().QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if count == 0 {
		t.Fatal("no migrations applied")
	}
}

// TestConcurrentOpen_NoBusyErrors specifically checks that no busy/locked
// errors occur during concurrent initialization.
func TestConcurrentOpen_NoBusyErrors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "no_busy.db")

	const numGoroutines = 12
	var wg sync.WaitGroup
	busyErrors := make(chan string, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			db, err := Open(dbPath)
			if err != nil {
				// Check if it's a busy/locked error.
				if isBusyError(err) {
					busyErrors <- err.Error()
				}
				return
			}
			db.Close()
		}(i)
	}

	wg.Wait()
	close(busyErrors)

	var allBusyErrors []string
	for msg := range busyErrors {
		allBusyErrors = append(allBusyErrors, msg)
	}

	if len(allBusyErrors) > 0 {
		t.Fatalf("got %d busy/locked errors (expected 0):\n%s", len(allBusyErrors), strings.Join(allBusyErrors, "\n"))
	}
}
