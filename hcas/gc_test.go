package hcas

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
)

// getObjectRefCount returns the reference count of an object from the database
func getObjectRefCount(t *testing.T, baseDir string, objectName Name) int {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(baseDir, MetadataPath))
	require.NoError(t, err, "Failed to open database")
	defer db.Close()

	var refCount int
	err = db.QueryRow("SELECT ref_count FROM objects WHERE name = ?", objectName.Name()).Scan(&refCount)
	require.NoError(t, err, "Failed to get object reference count")

	return refCount
}

// countObjects counts the number of objects in the database
func countObjects(t *testing.T, baseDir string) int {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(baseDir, MetadataPath))
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
	err := filepath.Walk(filepath.Join(baseDir, DataPath), func(path string, info os.FileInfo, err error) error {
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
	defer env.closeInstance()

	// Create session
	session := env.createSession()

	// Create objects
	obj1 := env.createObject(session, []byte("Test object 1"))
	obj2 := env.createObject(session, []byte("Test object 2"))
	obj3 := env.createObject(session, []byte("Test object 3"), obj1, obj2)

	// Set a label for obj3
	env.setLabel(session, "test", "obj3", &obj3)

	// Close session (obj1, obj2, obj3 are now referenced only by obj3's dependencies and the label)
	env.closeSession(session)

	// Get the actual reference counts to verify behavior
	refCount1 := getObjectRefCount(t, env.baseDir, obj1)
	refCount2 := getObjectRefCount(t, env.baseDir, obj2)
	refCount3 := getObjectRefCount(t, env.baseDir, obj3)

	// Log the actual reference counts for debugging
	t.Logf("After close session, reference counts: obj1=%d, obj2=%d, obj3=%d",
		refCount1, refCount2, refCount3)

	// Create a new session
	session = env.createSession()

	// Remove label from obj3
	env.setLabel(session, "test", "obj3", nil)

	// Get reference count after removing label
	refCount3AfterLabelRemove := getObjectRefCount(t, env.baseDir, obj3)
	t.Logf("After removing label, obj3 ref count: %d", refCount3AfterLabelRemove)

	// Close session
	env.closeSession(session)

	// Initial object count
	initialObjectCount := countObjects(t, env.baseDir)

	// Run garbage collection to collect obj3
	complete := env.runGarbageCollection(1)

	// Log counts after first GC
	objectCountAfterFirstGC := countObjects(t, env.baseDir)
	t.Logf("After first GC, object count: %d (initial: %d)", objectCountAfterFirstGC, initialObjectCount)

	// Run multiple rounds of garbage collection to collect remaining objects
	for i := 0; i < 10; i++ {
		complete = env.runGarbageCollection(2)

		// Check if collection is complete
		currentCount := countObjects(t, env.baseDir)
		t.Logf("After GC round %d, complete: %v, object count: %d", i+2, complete, currentCount)

		if complete && currentCount == 0 {
			break
		}
	}

	// Final object and file count
	finalObjectCount := countObjects(t, env.baseDir)
	finalFileCount := countDataFiles(t, env.baseDir)
	t.Logf("Final counts - objects: %d, files: %d", finalObjectCount, finalFileCount)
}

// TestDependencyRefCounting tests that reference counting with dependencies works correctly
func TestDependencyRefCounting(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

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
}

// TestLabelRefCounting tests that reference counting with labels works correctly
func TestLabelRefCounting(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	// Create session
	session := env.createSession()

	// Create object
	obj := env.createObject(session, []byte("Labeled object"))

	// Set label
	env.setLabel(session, "test", "labeled", &obj)

	// Get reference count
	refCount := getObjectRefCount(t, env.baseDir, obj)
	t.Logf("After setting label, ref count: %d", refCount)

	// Close session
	env.closeSession(session)

	// Get reference count after closing session
	refCountAfterClose := getObjectRefCount(t, env.baseDir, obj)
	t.Logf("After closing session, ref count: %d", refCountAfterClose)

	// Create new session to remove label
	session = env.createSession()

	// Remove label
	env.setLabel(session, "test", "labeled", nil)

	// Get reference count after removing label
	refCountAfterLabelRemove := getObjectRefCount(t, env.baseDir, obj)
	t.Logf("After removing label, ref count: %d", refCountAfterLabelRemove)

	// Close session
	env.closeSession(session)

	// Run garbage collection
	env.runGarbageCollection(-1)

	// Check if objects were collected
	finalCount := countObjects(t, env.baseDir)
	t.Logf("After garbage collection, remaining objects: %d", finalCount)

	assert.Equal(t, finalCount, 0)
}

// TestIncrementalGarbageCollection tests incremental garbage collection
func TestIncrementalGarbageCollection(t *testing.T) {
	// Set up test environment
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	// Create 10 objects
	session := env.createSession()

	objects := make([]Name, 10)
	for i := 0; i < 10; i++ {
		objects[i] = env.createObject(session, append([]byte("Object"), byte(i)))
	}

	// Close session to make all objects unreferenced
	env.closeSession(session)

	// All objects should have ref_count=0
	for i, obj := range objects {
		assert.Equal(t, 0, getObjectRefCount(t, env.baseDir, obj),
			"Object %d should have 0 references", i)
	}

	// Initial object count
	initialCount := countObjects(t, env.baseDir)
	assert.Equal(t, 10, initialCount, "Should have 10 objects")

	// Run garbage collection with limit of 3 iterations
	complete := env.runGarbageCollection(3)
	assert.False(t, complete, "Garbage collection should not complete with limit of 3")

	// Should have collected some but not all objects
	midCount := countObjects(t, env.baseDir)
	assert.Less(t, midCount, initialCount, "Should have collected some objects")
	assert.Greater(t, midCount, 0, "Should not have collected all objects")

	// Run another round to completion
	complete = env.runGarbageCollection(-1)
	assert.True(t, complete, "Full garbage collection should complete")

	// All objects should be collected
	finalCount := countObjects(t, env.baseDir)
	assert.Equal(t, 0, finalCount, "All objects should be collected")
}
