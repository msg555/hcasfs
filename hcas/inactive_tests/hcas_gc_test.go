package hcas_test

import (
	"bytes"
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

// getObjectRefCount returns the reference count of an object from the database
func getObjectRefCount(t *testing.T, baseDir string, objectName []byte) int {
	t.Helper()
	
	db, err := sql.Open("sqlite3", filepath.Join(baseDir, hcas.MetadataPath))
	require.NoError(t, err, "Failed to open database")
	defer db.Close()
	
	var refCount int
	err = db.QueryRow("SELECT ref_count FROM objects WHERE name = ?", objectName).Scan(&refCount)
	require.NoError(t, err, "Failed to get object reference count")
	
	return refCount
}

// setSessionTimestamp artificially sets a session's creation timestamp for testing expiration
func setSessionTimestamp(t *testing.T, baseDir string, sessionId int64, daysOld int) {
	t.Helper()
	
	db, err := sql.Open("sqlite3", filepath.Join(baseDir, hcas.MetadataPath))
	require.NoError(t, err, "Failed to open database")
	defer db.Close()
	
	// Set timestamp to X days in the past
	_, err = db.Exec(
		"UPDATE sessions SET created_at = datetime('now', '-' || ? || ' days') WHERE id = ?",
		daysOld, sessionId,
	)
	require.NoError(t, err, "Failed to update session timestamp")
}

// getSessionId gets the session ID for a session (for test purposes)
func getSessionId(t *testing.T, baseDir string) int64 {
	t.Helper()
	
	db, err := sql.Open("sqlite3", filepath.Join(baseDir, hcas.MetadataPath))
	require.NoError(t, err, "Failed to open database")
	defer db.Close()
	
	var sessionId int64
	err = db.QueryRow("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").Scan(&sessionId)
	require.NoError(t, err, "Failed to get session ID")
	
	return sessionId
}

// createTempObject creates a temporary object without committing it to test temp object collection
func createTempObject(t *testing.T, baseDir string, name []byte) {
	t.Helper()
	
	db, err := sql.Open("sqlite3", filepath.Join(baseDir, hcas.MetadataPath))
	require.NoError(t, err, "Failed to open database")
	defer db.Close()
	
	_, err = db.Exec("INSERT INTO temp_objects (name) VALUES (?)", name)
	require.NoError(t, err, "Failed to insert temp object")
	
	// Create directory if needed
	nameHex := fmt.Sprintf("%x", name)
	dir := filepath.Join(baseDir, hcas.DataPath, nameHex[:2])
	err = os.MkdirAll(dir, 0o777)
	require.NoError(t, err, "Failed to create directory")
	
	// Create empty data file
	path := filepath.Join(dir, nameHex[2:])
	err = os.WriteFile(path, []byte("temp data"), 0o666)
	require.NoError(t, err, "Failed to create data file")
}

// countObjects counts the number of objects in the database
func countObjects(t *testing.T, baseDir string) int {
	t.Helper()
	
	db, err := sql.Open("sqlite3", filepath.Join(baseDir, hcas.MetadataPath))
	require.NoError(t, err, "Failed to open database")
	defer db.Close()
	
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM objects").Scan(&count)
	require.NoError(t, err, "Failed to count objects")
	
	return count
}

// countDataFiles counts the number of files in the data directory
func countDataFiles(t *testing.T, baseDir string) int {
	t.Helper()
	
	count := 0
	err := filepath.Walk(filepath.Join(baseDir, hcas.DataPath), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	require.NoError(t, err, "Failed to walk data directory")
	
	return count
}

// TestBasicGarbageCollection tests basic garbage collection of objects with no references
func TestBasicGarbageCollection(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	
	// Create session
	session := env.createSession()
	
	// Create objects
	obj1 := env.createObject(session, []byte("Test object 1"))
	obj2 := env.createObject(session, []byte("Test object 2"))
	obj3 := env.createObject(session, []byte("Test object 3"), obj1, obj2)
	
	// Set a label for obj3
	env.setLabel(session, "test", "obj3", obj3)
	
	// Close session (obj1, obj2, obj3 are now referenced only by obj3's dependencies and the label)
	env.closeSession(session)
	
	// Initial reference counts should be:
	// obj1: 1 (referenced by obj3)
	// obj2: 1 (referenced by obj3)
	// obj3: 1 (referenced by label)
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, obj1), "obj1 should have 1 reference")
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, obj2), "obj2 should have 1 reference")
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, obj3), "obj3 should have 1 reference")
	
	// Create a new session
	session = env.createSession()
	
	// Remove label from obj3
	env.setLabel(session, "test", "obj3", nil)
	
	// Now obj3 should be unreferenced and available for garbage collection
	assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, obj3), "obj3 should have 0 references after removing label")
	
	// Close session
	env.closeSession(session)
	
	// Initial object and file count
	initialObjectCount := countObjects(t, env.baseDir)
	initialFileCount := countDataFiles(t, env.baseDir)
	
	// Run garbage collection to collect obj3
	complete := env.runGarbageCollection(1)
	
	// Basic GC should collect at least obj3
	objectCountAfterFirstGC := countObjects(t, env.baseDir)
	assert.Less(t, objectCountAfterFirstGC, initialObjectCount, "Garbage collection should have removed obj3")
	
	// At this point, obj1 and obj2 should have ref_count=0 since obj3 was collected
	// Run another round of GC to collect obj1 and obj2
	complete = env.runGarbageCollection(2)
	assert.True(t, complete, "Garbage collection should complete in 2 iterations")
	
	// Final object and file count
	finalObjectCount := countObjects(t, env.baseDir)
	finalFileCount := countDataFiles(t, env.baseDir)
	
	// Should have removed all objects and their data files
	assert.Equal(t, 0, finalObjectCount, "All objects should be collected")
	assert.Less(t, finalFileCount, initialFileCount, "Data files should be removed")
	
	// Close instance
	env.closeInstance()
}

// TestSessionExpirationGC tests garbage collection of expired sessions
func TestSessionExpirationGC(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	
	// Create session
	session := env.createSession()
	
	// Get session ID for later
	sessionId := getSessionId(t, env.baseDir)
	
	// Create objects
	obj1 := env.createObject(session, []byte("Test object 1"))
	obj2 := env.createObject(session, []byte("Test object 2"))
	
	// Keep session open (don't close it)
	
	// Artificially age the session to make it expire (2 days old)
	setSessionTimestamp(t, env.baseDir, sessionId, 2)
	
	// Initial reference counts should be 1 (due to session references)
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, obj1), "obj1 should have 1 reference")
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, obj2), "obj2 should have 1 reference")
	
	// Run garbage collection specifically targeting expired sessions
	complete := env.runGarbageCollection(-1) // Full GC
	assert.True(t, complete, "Garbage collection should complete")
	
	// Session should be collected, and objects should have ref_count=0
	assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, obj1), "obj1 should have 0 references after session GC")
	assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, obj2), "obj2 should have 0 references after session GC")
	
	// Objects should also be collected
	finalObjectCount := countObjects(t, env.baseDir)
	assert.Equal(t, 0, finalObjectCount, "All objects should be collected")
	
	// Close instance
	env.closeInstance()
}

// TestTempObjectCollection tests garbage collection of temporary objects
func TestTempObjectCollection(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	
	// Create a temporary object directly
	tempData := []byte{0x01, 0x02, 0x03, 0x04}
	createTempObject(t, env.baseDir, tempData)
	
	// Check that the file exists
	nameHex := fmt.Sprintf("%x", tempData)
	tempFilePath := filepath.Join(env.baseDir, hcas.DataPath, nameHex[:2], nameHex[2:])
	_, err := os.Stat(tempFilePath)
	require.NoError(t, err, "Temp file should exist")
	
	// Run garbage collection
	complete := env.runGarbageCollection(-1) // Full GC
	assert.True(t, complete, "Garbage collection should complete")
	
	// File should be removed
	_, err = os.Stat(tempFilePath)
	assert.True(t, os.IsNotExist(err), "Temp file should be deleted after GC")
	
	// Close instance
	env.closeInstance()
}

// TestIncrementalGarbageCollection tests incremental garbage collection
func TestIncrementalGarbageCollection(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	
	// Create 10 objects
	session := env.createSession()
	
	objects := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		objects[i] = env.createObject(session, []byte(fmt.Sprintf("Object %d", i)))
	}
	
	// Close session
	env.closeSession(session)
	
	// All objects should have ref_count=0
	for i, obj := range objects {
		assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, obj), 
			"Object %d should have 0 references", i)
	}
	
	// Initial object count
	initialCount := countObjects(t, env.baseDir)
	assert.Equal(t, 10, initialCount, "Should have 10 objects")
	
	// Run garbage collection with limit of 3
	complete := env.runGarbageCollection(3)
	assert.False(t, complete, "Garbage collection should not complete with limit of 3")
	
	// Should have collected at most 3 objects
	midCount := countObjects(t, env.baseDir)
	assert.GreaterOrEqual(t, initialCount-3, midCount, 
		"Should have collected at most 3 objects")
	assert.Less(t, midCount, initialCount, 
		"Should have collected some objects")
	
	// Run another round to completion
	complete = env.runGarbageCollection(-1)
	assert.True(t, complete, "Full garbage collection should complete")
	
	// All objects should be collected
	finalCount := countObjects(t, env.baseDir)
	assert.Equal(t, 0, finalCount, "All objects should be collected")
	
	// Close instance
	env.closeInstance()
}

// TestDependencyRefCounting tests that reference counting with dependencies works correctly
func TestDependencyRefCounting(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	
	// Create objects with dependencies
	session := env.createSession()
	
	// Create a dependency chain: A -> B -> C
	objC := env.createObject(session, []byte("Object C"))
	objB := env.createObject(session, []byte("Object B"), objC)
	objA := env.createObject(session, []byte("Object A"), objB)
	
	// Reference counts with session should be:
	// objA: 1 (session)
	// objB: 2 (session + objA dependency)
	// objC: 2 (session + objB dependency)
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, objA), "objA should have 1 reference")
	assert.Equal(t, 2, getObjectRefCount(t, env.baseDir, objB), "objB should have 2 references")
	assert.Equal(t, 2, getObjectRefCount(t, env.baseDir, objC), "objC should have 2 references")
	
	// Close session - removes session references
	env.closeSession(session)
	
	// Reference counts should now be:
	// objA: 0
	// objB: 1 (objA dependency)
	// objC: 1 (objB dependency)
	assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, objA), "objA should have 0 references")
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, objB), "objB should have 1 reference")
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, objC), "objC should have 1 reference")
	
	// Run first GC - should collect objA
	env.runGarbageCollection(1)
	
	// After collecting objA:
	// objA: gone
	// objB: 0
	// objC: 1 (objB dependency)
	assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, objB), "objB should have 0 references after collecting objA")
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, objC), "objC should have 1 reference")
	
	// Run second GC - should collect objB
	env.runGarbageCollection(1)
	
	// After collecting objB:
	// objA: gone
	// objB: gone
	// objC: 0
	assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, objC), "objC should have 0 references after collecting objB")
	
	// Run third GC - should collect objC
	env.runGarbageCollection(1)
	
	// All objects should be gone
	finalCount := countObjects(t, env.baseDir)
	assert.Equal(t, 0, finalCount, "All objects should be collected")
	
	// Close instance
	env.closeInstance()
}

// TestLabelRefCounting tests that reference counting with labels works correctly
func TestLabelRefCounting(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	
	// Create session
	session := env.createSession()
	
	// Create object
	obj := env.createObject(session, []byte("Labeled object"))
	
	// Set label
	env.setLabel(session, "test", "labeled", obj)
	
	// Reference count should be 2 (session + label)
	assert.Equal(t, 2, getObjectRefCount(t, env.baseDir, obj), "Object should have 2 references")
	
	// Close session
	env.closeSession(session)
	
	// Reference count should be 1 (label only)
	assert.Equal(t, 1, getObjectRefCount(t, env.baseDir, obj), "Object should have 1 reference from label")
	
	// Create new session to remove label
	session = env.createSession()
	
	// Remove label
	env.setLabel(session, "test", "labeled", nil)
	
	// Reference count should be 0
	assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, obj), "Object should have 0 references after removing label")
	
	// Close session
	env.closeSession(session)
	
	// Run garbage collection
	env.runGarbageCollection(-1)
	
	// Object should be gone
	finalCount := countObjects(t, env.baseDir)
	assert.Equal(t, 0, finalCount, "Object should be collected")
	
	// Close instance
	env.closeInstance()
}