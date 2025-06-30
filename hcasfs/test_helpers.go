package hcasfs

import (
	"io"
	"testing"

	"github.com/msg555/hcas/hcas"
)

// Test environment to manage HCAS instance and session
type testEnvironment struct {
	store   hcas.Hcas
	session hcas.Session
}

// Helper function to create a test environment with HCAS store and session
func createTestEnvironment(t *testing.T) *testEnvironment {
	// Create a temporary directory for the HCAS store
	tempDir := t.TempDir()
	
	store, err := hcas.CreateHcas(tempDir)
	if err != nil {
		t.Fatalf("Failed to create HCAS store: %v", err)
	}
	
	session, err := store.CreateSession()
	if err != nil {
		t.Fatalf("Failed to create HCAS session: %v", err)
	}
	
	return &testEnvironment{
		store:   store,
		session: session,
	}
}

// Helper function to create a mock HCAS session for testing
func createMockSession(t *testing.T) hcas.Session {
	env := createTestEnvironment(t)
	return env.session
}

// Helper function to read object data
func readObjectData(store hcas.Hcas, name hcas.Name) ([]byte, error) {
	file, err := store.ObjectOpen(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	return io.ReadAll(file)
}