package hcas

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
)

// testEnv encapsulates a testing environment
type testEnv struct {
	t        *testing.T
	baseDir  string
	hcasInst Hcas
}

// newTestEnv creates a new test environment with a temporary directory
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	// Create a temporary directory for the test
	baseDir, err := os.MkdirTemp("", "hcas-test-*")
	require.NoError(t, err, "Failed to create temp directory")

	t.Logf("Created test environment in %s", baseDir)

	// Clean up the directory when the test completes
	t.Cleanup(func() {
		t.Logf("Cleaning up test environment in %s", baseDir)
		os.RemoveAll(baseDir)
	})

	return &testEnv{
		t:       t,
		baseDir: baseDir,
	}
}

// createInstance creates a new HCAS instance in the test environment
func (env *testEnv) createInstance() {
	env.t.Helper()

	// Create a new HCAS instance
	hcasInst, err := CreateHcas(env.baseDir)
	require.NoError(env.t, err, "Failed to create HCAS instance")

	// Close any previous instance
	if env.hcasInst != nil {
		env.hcasInst.Close()
	}

	env.hcasInst = hcasInst
}

// openInstance opens an existing HCAS instance in the test environment
func (env *testEnv) openInstance() {
	env.t.Helper()

	// Open the existing HCAS instance
	hcasInst, err := OpenHcas(env.baseDir)
	require.NoError(env.t, err, "Failed to open HCAS instance")

	// Close any previous instance
	if env.hcasInst != nil {
		env.hcasInst.Close()
	}

	env.hcasInst = hcasInst
}

// createSession creates a new session in the HCAS instance
func (env *testEnv) createSession() Session {
	env.t.Helper()

	session, err := env.hcasInst.CreateSession()
	require.NoError(env.t, err, "Failed to create session")

	return session
}

// createObject is a helper to create an object with specified data and dependencies
func (env *testEnv) createObject(session Session, data []byte, deps ...[]byte) []byte {
	env.t.Helper()

	name, err := session.CreateObject(data, deps...)
	require.NoError(env.t, err, "Failed to create object")

	return name
}

// setLabel is a helper to set a label for an object
func (env *testEnv) setLabel(session Session, namespace, label string, name []byte) {
	env.t.Helper()

	err := session.SetLabel(namespace, label, name)
	require.NoError(env.t, err, "Failed to set label")
}

// getLabel is a helper to get an object by label
func (env *testEnv) getLabel(session Session, namespace, label string) []byte {
	env.t.Helper()

	name, err := session.GetLabel(namespace, label)
	require.NoError(env.t, err, "Failed to get label")

	return name
}

// readObject is a helper to read an object's content
func (env *testEnv) readObject(name []byte) []byte {
	env.t.Helper()

	file, err := env.hcasInst.ObjectOpen(name)
	require.NoError(env.t, err, "Failed to open object")
	defer file.Close()

	content, err := io.ReadAll(file)
	require.NoError(env.t, err, "Failed to read object content")

	return content
}

// verifyObjectExists checks if an object with the given name exists
func (env *testEnv) verifyObjectExists(name []byte) bool {
	env.t.Helper()

	// Check if the object file exists
	path := env.hcasInst.ObjectPath(name)
	_, err := os.Stat(path)

	return err == nil
}

// runGarbageCollection runs garbage collection
func (env *testEnv) runGarbageCollection(iterations int) bool {
	env.t.Helper()

	complete, err := env.hcasInst.GarbageCollect(iterations)
	require.NoError(env.t, err, "Failed to run garbage collection")

	return complete
}

// closeSession closes a session
func (env *testEnv) closeSession(session Session) {
	env.t.Helper()

	err := session.Close()
	require.NoError(env.t, err, "Failed to close session")
}

// closeInstance closes the HCAS instance
func (env *testEnv) closeInstance() {
	env.t.Helper()

	if env.hcasInst != nil {
		err := env.hcasInst.Close()
		require.NoError(env.t, err, "Failed to close HCAS instance")
		env.hcasInst = nil
	}
}

// Test basic HCAS instance creation and closing
func TestInstanceCreateAndClose(t *testing.T) {
	env := newTestEnv(t)
	env.createInstance()
	env.closeInstance()
	env.openInstance()
	env.closeInstance()
}

// Test basic session creation and closing
func TestSessionCreateAndClose(t *testing.T) {
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	// Create session
	session := env.createSession()

	// Close session
	env.closeSession(session)
}

// Test creating and reading objects
func TestObjectCreateAndRead(t *testing.T) {
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	session := env.createSession()
	defer env.closeSession(session)

	// Create test data
	testData := []byte("Hello, HCAS!")

	// Create object
	name := env.createObject(session, testData)

	// Verify object exists
	assert.True(t, env.verifyObjectExists(name), "Object should exist")

	// Read object
	content := env.readObject(name)

	// Verify content
	assert.Equal(t, testData, content, "Object content should match")
}

// Test object deduplication
func TestObjectDeduplication(t *testing.T) {
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	session := env.createSession()
	defer env.closeSession(session)

	// Create test data
	testData := []byte("Duplicate data")

	// Create object twice
	name1 := env.createObject(session, testData)
	name2 := env.createObject(session, testData)

	// Names should be identical
	assert.Equal(t, name1, name2, "Names of identical objects should match")
}

// Test streaming objects
func TestStreamObject(t *testing.T) {
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	session := env.createSession()
	defer env.closeSession(session)

	// Create test data
	testData := []byte("Streamed data")

	// Stream object
	writer, err := session.StreamObject()
	require.NoError(t, err, "Failed to create object stream")

	// Write data
	n, err := writer.Write(testData)
	require.NoError(t, err, "Failed to write to object stream")
	assert.Equal(t, len(testData), n, "Should write all bytes")

	// Close stream
	err = writer.Close()
	require.NoError(t, err, "Failed to close object stream")

	// Get object name
	name := writer.Name()
	require.NotNil(t, name, "Object name should not be nil")

	// Verify content
	content := env.readObject(name)
	assert.Equal(t, testData, content, "Streamed object content should match")
}

// Test creating objects with dependencies
func TestObjectWithDependencies(t *testing.T) {
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	session := env.createSession()
	defer env.closeSession(session)

	// Create dependency objects
	dep1Data := []byte("Dependency 1")
	dep2Data := []byte("Dependency 2")

	dep1Name := env.createObject(session, dep1Data)
	dep2Name := env.createObject(session, dep2Data)

	// Create object with dependencies
	parentData := []byte("Parent object")
	parentName := env.createObject(session, parentData, dep1Name, dep2Name)

	// Read the parent object
	content := env.readObject(parentName)
	assert.Equal(t, parentData, content, "Parent object content should match")

	// Also verify dependencies can be read
	dep1Content := env.readObject(dep1Name)
	dep2Content := env.readObject(dep2Name)
	assert.Equal(t, dep1Data, dep1Content, "Dependency 1 content should match")
	assert.Equal(t, dep2Data, dep2Content, "Dependency 2 content should match")
}

// Test label operations
func TestLabelOperations(t *testing.T) {
	env := newTestEnv(t)
	env.createInstance()
	defer env.closeInstance()

	session := env.createSession()
	defer env.closeSession(session)

	// Create test objects
	obj1Data := []byte("Object 1")
	obj2Data := []byte("Object 2")

	obj1Name := env.createObject(session, obj1Data)
	obj2Name := env.createObject(session, obj2Data)

	// Set labels
	const namespace = "test"
	env.setLabel(session, namespace, "obj1", obj1Name)

	// Get object by label
	retrievedName := env.getLabel(session, namespace, "obj1")
	assert.Equal(t, obj1Name, retrievedName, "Retrieved object name should match")

	// Change label to point to obj2
	env.setLabel(session, namespace, "obj1", obj2Name)

	// Get updated label
	retrievedName = env.getLabel(session, namespace, "obj1")
	assert.Equal(t, obj2Name, retrievedName, "Updated label should point to obj2")

	// Remove label
	env.setLabel(session, namespace, "obj1", nil)

	// Get non-existent label
	retrievedName = env.getLabel(session, namespace, "obj1")
	assert.Nil(t, retrievedName, "Label should be removed")
}
