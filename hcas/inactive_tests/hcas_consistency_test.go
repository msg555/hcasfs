package hcas_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
	"github.com/msg555/hcas/hcas"
)

// ConsistencyChecker provides methods to verify HCAS consistency rules
type ConsistencyChecker struct {
	t       *testing.T
	baseDir string
	db      *sql.DB
}

// newConsistencyChecker creates a new consistency checker for the given HCAS directory
func newConsistencyChecker(t *testing.T, baseDir string) *ConsistencyChecker {
	t.Helper()
	
	// Open the database directly
	db, err := sql.Open("sqlite3", filepath.Join(baseDir, hcas.MetadataPath))
	require.NoError(t, err, "Failed to open database for consistency checking")
	
	// Add cleanup
	t.Cleanup(func() {
		db.Close()
	})
	
	return &ConsistencyChecker{
		t:       t,
		baseDir: baseDir,
		db:      db,
	}
}

// checkObjectToDataConsistency verifies that every object in the database has a corresponding file
// This checks the "Object implies data" rule from CONSISTENCY.md
func (c *ConsistencyChecker) checkObjectToDataConsistency() {
	c.t.Helper()
	
	rows, err := c.db.Query("SELECT name FROM objects")
	require.NoError(c.t, err, "Failed to query objects")
	defer rows.Close()
	
	for rows.Next() {
		var name []byte
		err := rows.Scan(&name)
		require.NoError(c.t, err, "Failed to scan object name")
		
		// Check if the corresponding file exists
		nameHex := fmt.Sprintf("%x", name)
		dataPath := filepath.Join(c.baseDir, hcas.DataPath, nameHex[:2], nameHex[2:])
		
		_, err = os.Stat(dataPath)
		assert.NoError(c.t, err, "Object data file should exist for object %s", nameHex)
	}
	
	require.NoError(c.t, rows.Err(), "Error iterating over objects")
}

// checkRefCountConsistency verifies that reference counts match actual references
// This checks the "Reference count consistency" rule from CONSISTENCY.md
func (c *ConsistencyChecker) checkRefCountConsistency() {
	c.t.Helper()
	
	type objectRefInfo struct {
		id       int64
		name     []byte
		refCount int
	}
	
	// Get all objects and their reference counts
	rows, err := c.db.Query("SELECT id, name, ref_count FROM objects")
	require.NoError(c.t, err, "Failed to query objects")
	
	objects := make(map[int64]*objectRefInfo)
	for rows.Next() {
		var obj objectRefInfo
		err := rows.Scan(&obj.id, &obj.name, &obj.refCount)
		require.NoError(c.t, err, "Failed to scan object info")
		objects[obj.id] = &obj
	}
	rows.Close()
	
	// Count references from object_deps
	rows, err = c.db.Query("SELECT child_id, COUNT(*) FROM object_deps GROUP BY child_id")
	require.NoError(c.t, err, "Failed to query object_deps")
	
	objectDepsCount := make(map[int64]int)
	for rows.Next() {
		var childId int64
		var count int
		err := rows.Scan(&childId, &count)
		require.NoError(c.t, err, "Failed to scan dependency count")
		objectDepsCount[childId] = count
	}
	rows.Close()
	
	// Count references from session_deps
	rows, err = c.db.Query("SELECT object_id, COUNT(*) FROM session_deps GROUP BY object_id")
	require.NoError(c.t, err, "Failed to query session_deps")
	
	sessionDepsCount := make(map[int64]int)
	for rows.Next() {
		var objectId int64
		var count int
		err := rows.Scan(&objectId, &count)
		require.NoError(c.t, err, "Failed to scan session dependency count")
		sessionDepsCount[objectId] = count
	}
	rows.Close()
	
	// Count references from labels
	rows, err = c.db.Query("SELECT object_id, COUNT(*) FROM labels GROUP BY object_id")
	require.NoError(c.t, err, "Failed to query labels")
	
	labelCount := make(map[int64]int)
	for rows.Next() {
		var objectId int64
		var count int
		err := rows.Scan(&objectId, &count)
		require.NoError(c.t, err, "Failed to scan label count")
		labelCount[objectId] = count
	}
	rows.Close()
	
	// Verify reference counts
	for id, obj := range objects {
		expectedCount := 0
		expectedCount += objectDepsCount[id]
		expectedCount += sessionDepsCount[id]
		expectedCount += labelCount[id]
		
		nameHex := fmt.Sprintf("%x", obj.name)
		assert.Equal(c.t, expectedCount, obj.refCount, 
			"Reference count mismatch for object %s (id: %d): expected %d, got %d", 
			nameHex, id, expectedCount, obj.refCount)
	}
}

// checkDataFileTracking verifies that all files in the data directory are tracked
// This checks the "Data file tracking" rule from CONSISTENCY.md
func (c *ConsistencyChecker) checkDataFileTracking() {
	c.t.Helper()
	
	// Get all tracked object names
	rows, err := c.db.Query("SELECT name FROM objects UNION SELECT name FROM temp_objects")
	require.NoError(c.t, err, "Failed to query object names")
	
	trackedObjects := make(map[string]bool)
	for rows.Next() {
		var name []byte
		err := rows.Scan(&name)
		require.NoError(c.t, err, "Failed to scan object name")
		
		nameHex := fmt.Sprintf("%x", name)
		trackedObjects[nameHex] = true
	}
	rows.Close()
	
	// Walk through the data directory
	dataDir := filepath.Join(c.baseDir, hcas.DataPath)
	err = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		// Skip directories
		if info.IsDir() {
			return nil
		}
		
		// Get the relative path from the data directory
		relPath, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		
		// Extract the object name from the path (first two characters are the directory)
		components := filepath.SplitList(relPath)
		if len(components) != 2 {
			c.t.Logf("Unexpected path format: %s", relPath)
			return nil
		}
		
		objectNameHex := components[0] + components[1]
		
		// Verify that this object is tracked
		assert.True(c.t, trackedObjects[objectNameHex], 
			"Data file %s is not tracked in objects or temp_objects", objectNameHex)
		
		return nil
	})
	
	require.NoError(c.t, err, "Failed to walk data directory")
}

// checkTempFileTracking verifies that all files in the temp directory are tracked
// This checks the "Temp file tracking" rule from CONSISTENCY.md
func (c *ConsistencyChecker) checkTempFileTracking() {
	c.t.Helper()
	
	// Get all tracked temp file IDs
	rows, err := c.db.Query("SELECT id FROM temp_files")
	require.NoError(c.t, err, "Failed to query temp files")
	
	trackedTempFiles := make(map[string]bool)
	for rows.Next() {
		var id int64
		err := rows.Scan(&id)
		require.NoError(c.t, err, "Failed to scan temp file ID")
		
		trackedTempFiles[fmt.Sprintf("%d", id)] = true
	}
	rows.Close()
	
	// Check files in the temp directory
	tempDir := filepath.Join(c.baseDir, hcas.TempPath)
	entries, err := os.ReadDir(tempDir)
	require.NoError(c.t, err, "Failed to read temp directory")
	
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		
		// Verify that this temp file is tracked
		assert.True(c.t, trackedTempFiles[entry.Name()], 
			"Temp file %s is not tracked in temp_files", entry.Name())
	}
}

// checkSessionExpiration verifies that no sessions exist beyond the 24-hour limit
// This checks the "Session expiration" rule from CONSISTENCY.md
func (c *ConsistencyChecker) checkSessionExpiration() {
	c.t.Helper()
	
	// Get the oldest session creation time
	var oldestTime string
	err := c.db.QueryRow("SELECT MIN(created_at) FROM sessions").Scan(&oldestTime)
	require.NoError(c.t, err, "Failed to query oldest session")
	
	if oldestTime == "" {
		// No sessions exist
		return
	}
	
	// Parse the time
	layout := "2006-01-02 15:04:05"
	oldest, err := time.Parse(layout, oldestTime)
	require.NoError(c.t, err, "Failed to parse oldest session time")
	
	// Check that it's not older than 24 hours
	assert.True(c.t, time.Since(oldest) < 24*time.Hour, 
		"Sessions older than 24 hours exist: %s", oldestTime)
}

// TestConsistency runs all consistency checks
func TestConsistency(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	
	// Create some sample data
	session := env.createSession()
	obj1 := env.createObject(session, []byte("Test object 1"))
	obj2 := env.createObject(session, []byte("Test object 2"))
	obj3 := env.createObject(session, []byte("Test object 3"), obj1, obj2)
	
	// Set some labels
	env.setLabel(session, "test", "obj1", obj1)
	env.setLabel(session, "test", "obj3", obj3)
	
	// Close session
	env.closeSession(session)
	
	// Close instance to ensure all data is persisted
	env.closeInstance()
	
	// Create consistency checker
	checker := newConsistencyChecker(t, env.baseDir)
	
	// Run consistency checks
	t.Run("ObjectToDataConsistency", checker.checkObjectToDataConsistency)
	t.Run("RefCountConsistency", checker.checkRefCountConsistency)
	t.Run("DataFileTracking", checker.checkDataFileTracking)
	t.Run("TempFileTracking", checker.checkTempFileTracking)
	t.Run("SessionExpiration", checker.checkSessionExpiration)
}