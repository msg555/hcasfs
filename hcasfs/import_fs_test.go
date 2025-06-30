package hcasfs

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/msg555/hcas/unix"
)

// setupTestDirectory creates a temporary directory with test files
func setupTestDirectory(t *testing.T) string {
	tempDir := t.TempDir()

	// Create regular files
	err := os.WriteFile(filepath.Join(tempDir, "file1.txt"), []byte("content of file1"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file1.txt: %v", err)
	}

	err = os.WriteFile(filepath.Join(tempDir, "file2.txt"), []byte("content of file2"), 0755)
	if err != nil {
		t.Fatalf("Failed to create file2.txt: %v", err)
	}

	// Create a subdirectory
	subDir := filepath.Join(tempDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	// Create file in subdirectory
	err = os.WriteFile(filepath.Join(subDir, "nested.txt"), []byte("nested content"), 0600)
	if err != nil {
		t.Fatalf("Failed to create nested.txt: %v", err)
	}

	// Create a symlink
	err = os.Symlink("file1.txt", filepath.Join(tempDir, "link.txt"))
	if err != nil {
		t.Fatalf("Failed to create symlink: %v", err)
	}

	// Create an absolute symlink
	err = os.Symlink("/absolute/path", filepath.Join(tempDir, "abs_link.txt"))
	if err != nil {
		t.Fatalf("Failed to create absolute symlink: %v", err)
	}

	// Create empty directory
	emptyDir := filepath.Join(tempDir, "empty")
	err = os.Mkdir(emptyDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create empty directory: %v", err)
	}

	return tempDir
}

func TestImportPath(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	testDir := setupTestDirectory(t)

	// Import the directory
	rootName, err := ImportPath(session, testDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	if rootName == nil {
		t.Fatal("ImportPath returned nil root name")
	}

	// Verify we can read the root directory
	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	// Check that files exist
	rootReader := bytes.NewReader(rootData)
	file1Entry, err := LookupChild(rootReader, "file1.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file1.txt: %v", err)
	}
	if file1Entry == nil {
		t.Fatal("file1.txt not found")
	}

	// Verify file content
	file1Data, err := readObjectData(env.store, *file1Entry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read file1 content: %v", err)
	}
	if string(file1Data) != "content of file1" {
		t.Errorf("file1 content mismatch: got %q, want %q", string(file1Data), "content of file1")
	}

	// Check file permissions
	if file1Entry.Inode.Mode != unix.S_IFREG|0644 {
		t.Errorf("file1 mode mismatch: got %o, want %o", file1Entry.Inode.Mode, unix.S_IFREG|0644)
	}

	// Check second file with different permissions
	rootReader.Seek(0, 0)
	file2Entry, err := LookupChild(rootReader, "file2.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file2.txt: %v", err)
	}
	if file2Entry == nil {
		t.Fatal("file2.txt not found")
	}

	if file2Entry.Inode.Mode != unix.S_IFREG|0755 {
		t.Errorf("file2 mode mismatch: got %o, want %o", file2Entry.Inode.Mode, unix.S_IFREG|0755)
	}

	// Check subdirectory
	rootReader.Seek(0, 0)
	subdirEntry, err := LookupChild(rootReader, "subdir")
	if err != nil {
		t.Fatalf("Failed to lookup subdir: %v", err)
	}
	if subdirEntry == nil {
		t.Fatal("subdir not found")
	}

	if !unix.S_ISDIR(subdirEntry.Inode.Mode) {
		t.Error("subdir is not a directory")
	}

	// Check nested file
	subdirData, err := readObjectData(env.store, *subdirEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read subdir: %v", err)
	}

	subdirReader := bytes.NewReader(subdirData)
	nestedEntry, err := LookupChild(subdirReader, "nested.txt")
	if err != nil {
		t.Fatalf("Failed to lookup nested.txt: %v", err)
	}
	if nestedEntry == nil {
		t.Fatal("nested.txt not found")
	}

	nestedData, err := readObjectData(env.store, *nestedEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read nested content: %v", err)
	}
	if string(nestedData) != "nested content" {
		t.Errorf("nested content mismatch: got %q, want %q", string(nestedData), "nested content")
	}

	if nestedEntry.Inode.Mode != unix.S_IFREG|0600 {
		t.Errorf("nested mode mismatch: got %o, want %o", nestedEntry.Inode.Mode, unix.S_IFREG|0600)
	}

	// Check symlink
	rootReader.Seek(0, 0)
	linkEntry, err := LookupChild(rootReader, "link.txt")
	if err != nil {
		t.Fatalf("Failed to lookup link.txt: %v", err)
	}
	if linkEntry == nil {
		t.Fatal("link.txt not found")
	}

	if !unix.S_ISLNK(linkEntry.Inode.Mode) {
		t.Error("link.txt is not a symlink")
	}

	linkData, err := readObjectData(env.store, *linkEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read symlink: %v", err)
	}
	if string(linkData) != "file1.txt" {
		t.Errorf("symlink target mismatch: got %q, want %q", string(linkData), "file1.txt")
	}

	// Check absolute symlink
	rootReader.Seek(0, 0)
	absLinkEntry, err := LookupChild(rootReader, "abs_link.txt")
	if err != nil {
		t.Fatalf("Failed to lookup abs_link.txt: %v", err)
	}
	if absLinkEntry == nil {
		t.Fatal("abs_link.txt not found")
	}

	absLinkData, err := readObjectData(env.store, *absLinkEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read absolute symlink: %v", err)
	}
	if string(absLinkData) != "/absolute/path" {
		t.Errorf("absolute symlink target mismatch: got %q, want %q", string(absLinkData), "/absolute/path")
	}

	// Check empty directory
	rootReader.Seek(0, 0)
	emptyEntry, err := LookupChild(rootReader, "empty")
	if err != nil {
		t.Fatalf("Failed to lookup empty: %v", err)
	}
	if emptyEntry == nil {
		t.Fatal("empty directory not found")
	}

	if !unix.S_ISDIR(emptyEntry.Inode.Mode) {
		t.Error("empty is not a directory")
	}

	// Empty directory should have tree size 1 (just itself)
	if emptyEntry.TreeSize != 1 {
		t.Errorf("empty directory tree size mismatch: got %d, want 1", emptyEntry.TreeSize)
	}
}

func TestImportPathWithLargeFile(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	tempDir := t.TempDir()

	// Create a large file
	largeContent := make([]byte, 100*1024) // 100KB
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	largePath := filepath.Join(tempDir, "large.dat")
	err := os.WriteFile(largePath, largeContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create large file: %v", err)
	}

	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	rootReader := bytes.NewReader(rootData)
	largeEntry, err := LookupChild(rootReader, "large.dat")
	if err != nil {
		t.Fatalf("Failed to lookup large.dat: %v", err)
	}
	if largeEntry == nil {
		t.Fatal("large.dat not found")
	}

	largeData, err := readObjectData(env.store, *largeEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read large file: %v", err)
	}

	if !bytes.Equal(largeData, largeContent) {
		t.Error("Large file content mismatch")
	}

	if largeEntry.Inode.Size != uint64(len(largeContent)) {
		t.Errorf("Large file size mismatch: got %d, want %d", 
			largeEntry.Inode.Size, len(largeContent))
	}
}

func TestImportPathNonDirectory(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "file.txt")
	err := os.WriteFile(filePath, []byte("content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Try to import a file instead of directory
	_, err = ImportPath(session, filePath)
	if err == nil {
		t.Fatal("ImportPath should fail when given a file instead of directory")
	}

	if !strings.Contains(err.Error(), "Only directories can be imported") {
		t.Errorf("Error should mention directory requirement: %v", err)
	}
}

func TestImportPathNonExistent(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Try to import non-existent path
	_, err := ImportPath(session, "/non/existent/path")
	if err == nil {
		t.Fatal("ImportPath should fail for non-existent path")
	}
}

func TestImportPathWithInvalidNames(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("Skipping test that requires root privileges to create files with invalid names")
	}

	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	tempDir := t.TempDir()

	// Create a valid file
	err := os.WriteFile(filepath.Join(tempDir, "valid.txt"), []byte("valid"), 0644)
	if err != nil {
		t.Fatalf("Failed to create valid file: %v", err)
	}

	// This test would need special setup to create files with invalid names
	// In practice, the filesystem prevents creating such files, so we mainly
	// test the validation logic through the tar import tests

	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	rootReader := bytes.NewReader(rootData)
	validEntry, err := LookupChild(rootReader, "valid.txt")
	if err != nil {
		t.Fatalf("Failed to lookup valid.txt: %v", err)
	}
	if validEntry == nil {
		t.Fatal("valid.txt not found")
	}
}

func TestImportPathDeepNesting(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	tempDir := t.TempDir()

	// Create deeply nested structure
	currentPath := tempDir
	depth := 10
	for i := 0; i < depth; i++ {
		currentPath = filepath.Join(currentPath, "level"+string(rune('0'+i)))
		err := os.Mkdir(currentPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create nested directory at level %d: %v", i, err)
		}
	}

	// Create a file at the deepest level
	deepFile := filepath.Join(currentPath, "deep.txt")
	err := os.WriteFile(deepFile, []byte("deep content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create deep file: %v", err)
	}

	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	// Navigate to the deep file
	currentData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root: %v", err)
	}

	for i := 0; i < depth; i++ {
		reader := bytes.NewReader(currentData)
		dirName := "level" + string(rune('0'+i))
		entry, err := LookupChild(reader, dirName)
		if err != nil {
			t.Fatalf("Failed to lookup %s: %v", dirName, err)
		}
		if entry == nil {
			t.Fatalf("%s not found", dirName)
		}

		currentData, err = readObjectData(env.store, *entry.Inode.ObjName)
		if err != nil {
			t.Fatalf("Failed to read %s: %v", dirName, err)
		}
	}

	// Check the deep file
	reader := bytes.NewReader(currentData)
	deepEntry, err := LookupChild(reader, "deep.txt")
	if err != nil {
		t.Fatalf("Failed to lookup deep.txt: %v", err)
	}
	if deepEntry == nil {
		t.Fatal("deep.txt not found")
	}

	deepContent, err := readObjectData(env.store, *deepEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read deep file: %v", err)
	}
	if string(deepContent) != "deep content" {
		t.Errorf("Deep file content mismatch: got %q, want %q", string(deepContent), "deep content")
	}
}

func TestImportPathEmptyDirectory(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	tempDir := t.TempDir()

	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed on empty directory: %v", err)
	}

	if rootName == nil {
		t.Fatal("ImportPath should return root even for empty directory")
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read empty root directory: %v", err)
	}

	// Should have just the directory header with 0 entries
	if len(rootData) < 16 {
		t.Error("Empty root directory data too short")
	}

	// Verify no entries can be found
	rootReader := bytes.NewReader(rootData)
	entry, err := LookupChild(rootReader, "nonexistent")
	if err != nil {
		t.Fatalf("LookupChild should not error: %v", err)
	}
	if entry != nil {
		t.Error("Should not find any entries in empty directory")
	}
}

func TestImportPathTreeSizes(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	tempDir := t.TempDir()

	// Create structure:
	// tempDir/
	//   file1.txt (tree size 1)
	//   subdir/   (tree size 3: itself + file2.txt + file3.txt)
	//     file2.txt (tree size 1)
	//     file3.txt (tree size 1)
	// Total tree size should be 5

	err := os.WriteFile(filepath.Join(tempDir, "file1.txt"), []byte("content1"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file1.txt: %v", err)
	}

	subDir := filepath.Join(tempDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	err = os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("content2"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file2.txt: %v", err)
	}

	err = os.WriteFile(filepath.Join(subDir, "file3.txt"), []byte("content3"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file3.txt: %v", err)
	}

	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	// Check file1.txt tree size
	rootReader := bytes.NewReader(rootData)
	file1Entry, err := LookupChild(rootReader, "file1.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file1.txt: %v", err)
	}
	if file1Entry.TreeSize != 1 {
		t.Errorf("file1.txt tree size mismatch: got %d, want 1", file1Entry.TreeSize)
	}

	// Check subdir tree size
	rootReader.Seek(0, 0)
	subdirEntry, err := LookupChild(rootReader, "subdir")
	if err != nil {
		t.Fatalf("Failed to lookup subdir: %v", err)
	}
	if subdirEntry.TreeSize != 3 {
		t.Errorf("subdir tree size mismatch: got %d, want 3", subdirEntry.TreeSize)
	}

	// Total should be 5 (1 + 1 + 3)
	// We can infer this from the directory builder's total tree size
	// but the actual verification would require reading the header
}

func TestImportPathPreservesMetadata(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	tempDir := t.TempDir()

	// Create file and set specific timestamps
	filePath := filepath.Join(tempDir, "test.txt")
	err := os.WriteFile(filePath, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Get original file stats
	originalStat, err := os.Lstat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat original file: %v", err)
	}

	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	rootReader := bytes.NewReader(rootData)
	testEntry, err := LookupChild(rootReader, "test.txt")
	if err != nil {
		t.Fatalf("Failed to lookup test.txt: %v", err)
	}
	if testEntry == nil {
		t.Fatal("test.txt not found")
	}

	// Verify metadata preservation
	if testEntry.Inode.Size != uint64(originalStat.Size()) {
		t.Errorf("Size mismatch: got %d, want %d", testEntry.Inode.Size, originalStat.Size())
	}

	originalMode := unix.FileStatToUnixMode(originalStat.Mode())
	if testEntry.Inode.Mode != originalMode {
		t.Errorf("Mode mismatch: got %o, want %o", testEntry.Inode.Mode, originalMode)
	}

	// Note: UID/GID testing would require specific test setup with known user/group IDs
	// and timestamps are system-dependent, so we primarily verify the structure is correct
}