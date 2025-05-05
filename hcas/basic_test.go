package hcas

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateAndOpen(t *testing.T) {
	// Create a temporary directory for the test
	baseDir, err := os.MkdirTemp("", "hcas-test-*")
	require.NoError(t, err, "Failed to create temp directory")
	defer os.RemoveAll(baseDir)

	// Create a new HCAS instance
	hcasInst, err := CreateHcas(baseDir)
	require.NoError(t, err, "Failed to create HCAS instance")

	// Close instance
	err = hcasInst.Close()
	require.NoError(t, err, "Failed to close HCAS instance")

	// Open the existing HCAS instance
	hcasInst, err = OpenHcas(baseDir)
	require.NoError(t, err, "Failed to open HCAS instance")

	// Close instance again
	err = hcasInst.Close()
	require.NoError(t, err, "Failed to close HCAS instance")
}
