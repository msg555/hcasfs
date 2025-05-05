package hcas

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
)

// checkRefCountConsistency checks if reference counts match actual references
func checkRefCountConsistency(t *testing.T, baseDir string) {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(baseDir, MetadataPath))
	require.NoError(t, err, "Failed to open database")
	defer db.Close()

	// Get all objects and their reference counts
	rows, err := db.Query("SELECT id, name, ref_count FROM objects")
	require.NoError(t, err, "Failed to query objects")
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name []byte
		var refCount int
		err = rows.Scan(&id, &name, &refCount)
		require.NoError(t, err, "Failed to scan object")

		// Count references from dependencies
		var depCount int
		err = db.QueryRow("SELECT COUNT(*) FROM object_deps WHERE child_id = ?", id).Scan(&depCount)
		require.NoError(t, err, "Failed to count dependencies")

		// Count references from sessions
		var sessionCount int
		err = db.QueryRow("SELECT COUNT(*) FROM session_deps WHERE object_id = ?", id).Scan(&sessionCount)
		require.NoError(t, err, "Failed to count session dependencies")

		// Count references from labels
		var labelCount int
		err = db.QueryRow("SELECT COUNT(*) FROM labels WHERE object_id = ?", id).Scan(&labelCount)
		require.NoError(t, err, "Failed to count label references")

		// Calculate expected reference count
		expectedCount := depCount + sessionCount + labelCount

		// Log the counts
		t.Logf("Object %x: ref_count=%d, expected=%d (deps=%d, sessions=%d, labels=%d)",
			name, refCount, expectedCount, depCount, sessionCount, labelCount)

		// Check if reference count matches expected count
		assert.Equal(t, expectedCount, refCount,
			"Reference count mismatch for object %x", name)
	}
}

// TestReferenceCountConsistency tests that reference counts are consistent
func TestReferenceCountConsistency(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	// Create objects with various references
	session1 := env.createSession()
	session2 := env.createSession()

	// Create objects
	obj1 := env.createObject(session1, []byte("Object 1"))
	obj2 := env.createObject(session1, []byte("Object 2"))
	obj3 := env.createObject(session2, []byte("Object 3"), obj1, obj2)

	// Set labels
	env.setLabel(session1, "test", "obj1", obj1)
	env.setLabel(session2, "test", "obj3", obj3)

	// Check reference count consistency
	t.Run("AfterCreation", func(t *testing.T) {
		checkRefCountConsistency(t, env.baseDir)
	})

	// Close session1
	env.closeSession(session1)

	// Check reference count consistency after closing session1
	t.Run("AfterClosingSession1", func(t *testing.T) {
		checkRefCountConsistency(t, env.baseDir)
	})

	// Remove label from obj3
	env.setLabel(session2, "test", "obj3", nil)

	// Check reference count consistency after removing label
	t.Run("AfterRemovingLabel", func(t *testing.T) {
		checkRefCountConsistency(t, env.baseDir)
	})

	// Close session2
	env.closeSession(session2)

	// Check reference count consistency after closing all sessions
	t.Run("AfterClosingAllSessions", func(t *testing.T) {
		checkRefCountConsistency(t, env.baseDir)
	})

	// Run garbage collection
	env.runGarbageCollection(1)

	// Check reference count consistency after garbage collection
	t.Run("AfterGarbageCollection", func(t *testing.T) {
		checkRefCountConsistency(t, env.baseDir)
	})
}
