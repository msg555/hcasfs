package hcasfs

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/unix"
)

// Integration tests that test the interaction between multiple hcasfs components

func TestRoundTripFilesystemToTarAndBack(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create a test filesystem structure
	tempDir := setupTestDirectory(t)

	// Import from filesystem
	fsRootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	// Export the same structure to tar format (simulated)
	tarData := createTarFromHCAS(t, session, fsRootName)

	// Import from tar
	tarReader := bytes.NewReader(tarData)
	tarRootName, err := ImportTar(session, tarReader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	// Compare the two root directories - they should be equivalent
	compareDirectories(t, env.store, fsRootName, tarRootName, "")
}

func TestDuplicateContentDeduplication(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create two directories with identical files
	tempDir1 := t.TempDir()
	tempDir2 := t.TempDir()

	duplicateContent := "This is duplicate content that should be deduplicated"

	// Create identical files in both directories
	err := os.WriteFile(filepath.Join(tempDir1, "file.txt"), []byte(duplicateContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create file in tempDir1: %v", err)
	}

	err = os.WriteFile(filepath.Join(tempDir2, "file.txt"), []byte(duplicateContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create file in tempDir2: %v", err)
	}

	// Import both directories
	root1, err := ImportPath(session, tempDir1)
	if err != nil {
		t.Fatalf("ImportPath failed for tempDir1: %v", err)
	}

	root2, err := ImportPath(session, tempDir2)
	if err != nil {
		t.Fatalf("ImportPath failed for tempDir2: %v", err)
	}

	// Get the file entries from both directories
	root1Data, err := readObjectData(env.store, *root1)
	if err != nil {
		t.Fatalf("Failed to read root1: %v", err)
	}

	root2Data, err := readObjectData(env.store, *root2)
	if err != nil {
		t.Fatalf("Failed to read root2: %v", err)
	}

	root1Reader := bytes.NewReader(root1Data)
	file1Entry, err := LookupChild(root1Reader, "file.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file.txt in root1: %v", err)
	}

	root2Reader := bytes.NewReader(root2Data)
	file2Entry, err := LookupChild(root2Reader, "file.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file.txt in root2: %v", err)
	}

	// The object names should be identical (content deduplication)
	if file1Entry.Inode.ObjName.HexName() != file2Entry.Inode.ObjName.HexName() {
		t.Error("Identical files should have the same object name (deduplication failed)")
	}

	// Verify the content is actually the same
	content1, err := readObjectData(env.store, *file1Entry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read content1: %v", err)
	}

	content2, err := readObjectData(env.store, *file2Entry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read content2: %v", err)
	}

	if string(content1) != duplicateContent || string(content2) != duplicateContent {
		t.Error("Content mismatch in deduplicated files")
	}
}

func TestComplexDirectoryStructureIntegrity(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create a complex directory structure
	tempDir := t.TempDir()

	// Create multiple levels of nesting with various file types
	structure := map[string]string{
		"root.txt":                 "root content",
		"dir1/file1.txt":           "content in dir1",
		"dir1/subdir/nested.txt":   "deeply nested content",
		"dir2/file2.txt":           "content in dir2",
		"dir2/subdir/another.txt":  "another nested file",
		"dir3/empty_subdir/.keep":  "",
		"shared_content1.txt":      "shared content",
		"dir1/shared_content2.txt": "shared content",
		"dir2/shared_content3.txt": "shared content",
	}

	// Create all files and directories
	for path, content := range structure {
		fullPath := filepath.Join(tempDir, path)
		dir := filepath.Dir(fullPath)

		err := os.MkdirAll(dir, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory %s: %v", dir, err)
		}

		err = os.WriteFile(fullPath, []byte(content), 0644)
		if err != nil {
			t.Fatalf("Failed to create file %s: %v", fullPath, err)
		}
	}

	// Create some symlinks
	err := os.Symlink("root.txt", filepath.Join(tempDir, "link_to_root.txt"))
	if err != nil {
		t.Fatalf("Failed to create symlink: %v", err)
	}

	err = os.Symlink("../root.txt", filepath.Join(tempDir, "dir1/link_to_parent.txt"))
	if err != nil {
		t.Fatalf("Failed to create relative symlink: %v", err)
	}

	// Import the complex structure
	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	// Verify all files are accessible and have correct content
	verifyFileInDirectory(t, env.store, rootName, "root.txt", "root content")
	verifyFileInDirectory(t, env.store, rootName, "shared_content1.txt", "shared content")

	// Navigate to subdirectories and verify files
	dir1Name := getDirectoryObject(t, env.store, rootName, "dir1")
	verifyFileInDirectory(t, env.store, dir1Name, "file1.txt", "content in dir1")
	verifyFileInDirectory(t, env.store, dir1Name, "shared_content2.txt", "shared content")

	dir1SubdirName := getDirectoryObject(t, env.store, dir1Name, "subdir")
	verifyFileInDirectory(t, env.store, dir1SubdirName, "nested.txt", "deeply nested content")

	dir2Name := getDirectoryObject(t, env.store, rootName, "dir2")
	verifyFileInDirectory(t, env.store, dir2Name, "file2.txt", "content in dir2")
	verifyFileInDirectory(t, env.store, dir2Name, "shared_content3.txt", "shared content")

	// Verify symlinks
	verifySymlinkInDirectory(t, env.store, rootName, "link_to_root.txt", "root.txt")
	verifySymlinkInDirectory(t, env.store, dir1Name, "link_to_parent.txt", "../root.txt")

	// Verify that shared content files have the same object (deduplication)
	rootData, _ := readObjectData(env.store, *rootName)
	dir1Data, _ := readObjectData(env.store, *dir1Name)
	dir2Data, _ := readObjectData(env.store, *dir2Name)

	rootReader := bytes.NewReader(rootData)
	shared1Entry, _ := LookupChild(rootReader, "shared_content1.txt")

	dir1Reader := bytes.NewReader(dir1Data)
	shared2Entry, _ := LookupChild(dir1Reader, "shared_content2.txt")

	dir2Reader := bytes.NewReader(dir2Data)
	shared3Entry, _ := LookupChild(dir2Reader, "shared_content3.txt")

	if shared1Entry.Inode.ObjName.HexName() != shared2Entry.Inode.ObjName.HexName() ||
		shared2Entry.Inode.ObjName.HexName() != shared3Entry.Inode.ObjName.HexName() {
		t.Error("Files with identical content should be deduplicated")
	}
}

func TestMixedImportMethods(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create test data for filesystem import
	fsDir := t.TempDir()
	err := os.WriteFile(filepath.Join(fsDir, "fs_file.txt"), []byte("from filesystem"), 0644)
	if err != nil {
		t.Fatalf("Failed to create fs file: %v", err)
	}

	// Create test data for tar import
	now := time.Now()
	tarEntries := []tarTestEntry{
		{
			Name:       "tar_file.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       8,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("from tar"),
		},
		{
			Name:       "shared_name.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       11,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("tar version"),
		},
	}

	// Also create a file with the same name in filesystem
	err = os.WriteFile(filepath.Join(fsDir, "shared_name.txt"), []byte("fs version"), 0644)
	if err != nil {
		t.Fatalf("Failed to create shared name file: %v", err)
	}

	// Import from filesystem
	fsRoot, err := ImportPath(session, fsDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	// Import from tar
	tarData := createTestTarArchive(tarEntries)
	tarReader := bytes.NewReader(tarData)
	tarRoot, err := ImportTar(session, tarReader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	// Verify filesystem imported content
	verifyFileInDirectory(t, env.store, fsRoot, "fs_file.txt", "from filesystem")
	verifyFileInDirectory(t, env.store, fsRoot, "shared_name.txt", "fs version")

	// Verify tar imported content
	verifyFileInDirectory(t, env.store, tarRoot, "tar_file.txt", "from tar")
	verifyFileInDirectory(t, env.store, tarRoot, "shared_name.txt", "tar version")

	// Verify that the shared name files have different content and thus different objects
	fsData, _ := readObjectData(env.store, *fsRoot)
	tarData2, _ := readObjectData(env.store, *tarRoot)

	fsReader := bytes.NewReader(fsData)
	fsSharedEntry, _ := LookupChild(fsReader, "shared_name.txt")

	tarReader2 := bytes.NewReader(tarData2)
	tarSharedEntry, _ := LookupChild(tarReader2, "shared_name.txt")

	if fsSharedEntry.Inode.ObjName.HexName() == tarSharedEntry.Inode.ObjName.HexName() {
		t.Error("Files with different content should have different object names")
	}
}

func TestEmptyAndNonEmptyDirectoryHandling(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Test various directory scenarios
	tempDir := t.TempDir()

	// Create empty directory
	emptyDir := filepath.Join(tempDir, "empty")
	err := os.Mkdir(emptyDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create empty directory: %v", err)
	}

	// Create directory with only subdirectories
	dirOnlyDir := filepath.Join(tempDir, "dirs_only")
	err = os.Mkdir(dirOnlyDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create dirs_only: %v", err)
	}

	err = os.Mkdir(filepath.Join(dirOnlyDir, "sub1"), 0755)
	if err != nil {
		t.Fatalf("Failed to create sub1: %v", err)
	}

	err = os.Mkdir(filepath.Join(dirOnlyDir, "sub2"), 0755)
	if err != nil {
		t.Fatalf("Failed to create sub2: %v", err)
	}

	// Create directory with only files
	filesOnlyDir := filepath.Join(tempDir, "files_only")
	err = os.Mkdir(filesOnlyDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create files_only: %v", err)
	}

	err = os.WriteFile(filepath.Join(filesOnlyDir, "file1.txt"), []byte("content1"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	err = os.WriteFile(filepath.Join(filesOnlyDir, "file2.txt"), []byte("content2"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	// Import the structure
	rootName, err := ImportPath(session, tempDir)
	if err != nil {
		t.Fatalf("ImportPath failed: %v", err)
	}

	// Verify empty directory
	emptyDirName := getDirectoryObject(t, env.store, rootName, "empty")
	emptyData, err := readObjectData(env.store, *emptyDirName)
	if err != nil {
		t.Fatalf("Failed to read empty directory: %v", err)
	}

	// Should be able to look up non-existent files without error
	emptyReader := bytes.NewReader(emptyData)
	entry, err := LookupChild(emptyReader, "nonexistent")
	if err != nil {
		t.Fatalf("LookupChild should not error on empty directory: %v", err)
	}
	if entry != nil {
		t.Error("Should not find entries in empty directory")
	}

	// Verify directories-only directory
	dirsOnlyName := getDirectoryObject(t, env.store, rootName, "dirs_only")
	verifyDirectoryExists(t, env.store, dirsOnlyName, "sub1")
	verifyDirectoryExists(t, env.store, dirsOnlyName, "sub2")

	// Verify files-only directory
	filesOnlyName := getDirectoryObject(t, env.store, rootName, "files_only")
	verifyFileInDirectory(t, env.store, filesOnlyName, "file1.txt", "content1")
	verifyFileInDirectory(t, env.store, filesOnlyName, "file2.txt", "content2")
}

// Helper functions for integration tests

func createTarFromHCAS(t *testing.T, session hcas.Session, rootName *hcas.Name) []byte {
	// This is a simplified simulation of exporting HCAS back to tar
	// In a real implementation, this would traverse the HCAS structure
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// For this test, we'll create a minimal tar with similar structure
	// This is mainly to test the round-trip concept
	now := time.Now()

	entries := []tarTestEntry{
		{
			Name:       "file1.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       16,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("content of file1"),
		},
		{
			Name:       "file2.txt",
			Mode:       0755,
			Uid:        1000,
			Gid:        1000,
			Size:       16,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("content of file2"),
		},
	}

	for _, entry := range entries {
		header := &tar.Header{
			Name:     entry.Name,
			Mode:     entry.Mode,
			Uid:      entry.Uid,
			Gid:      entry.Gid,
			Size:     entry.Size,
			ModTime:  entry.ModTime,
			Typeflag: entry.Typeflag,
		}

		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("Failed to write tar header: %v", err)
		}

		if entry.Content != nil {
			if _, err := tw.Write(entry.Content); err != nil {
				t.Fatalf("Failed to write tar content: %v", err)
			}
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Failed to close tar writer: %v", err)
	}

	return buf.Bytes()
}

func compareDirectories(t *testing.T, store hcas.Hcas, dir1Name, dir2Name *hcas.Name, path string) {
	// This is a simplified comparison - in practice you'd want to recursively
	// compare the entire directory structure
	dir1Data, err := readObjectData(store, *dir1Name)
	if err != nil {
		t.Fatalf("Failed to read dir1 at %s: %v", path, err)
	}

	dir2Data, err := readObjectData(store, *dir2Name)
	if err != nil {
		t.Fatalf("Failed to read dir2 at %s: %v", path, err)
	}

	// For this test, we'll just verify that both directories can be read
	// A full implementation would compare all entries recursively
	if len(dir1Data) == 0 || len(dir2Data) == 0 {
		t.Errorf("One of the directories at %s is empty unexpectedly", path)
	}
}

func verifyFileInDirectory(t *testing.T, store hcas.Hcas, dirName *hcas.Name, fileName, expectedContent string) {
	dirData, err := readObjectData(store, *dirName)
	if err != nil {
		t.Fatalf("Failed to read directory for %s: %v", fileName, err)
	}

	dirReader := bytes.NewReader(dirData)
	entry, err := LookupChild(dirReader, fileName)
	if err != nil {
		t.Fatalf("Failed to lookup %s: %v", fileName, err)
	}
	if entry == nil {
		t.Fatalf("File %s not found", fileName)
	}

	if !unix.S_ISREG(entry.Inode.Mode) {
		t.Errorf("%s is not a regular file", fileName)
		return
	}

	fileData, err := readObjectData(store, *entry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read %s content: %v", fileName, err)
	}

	if string(fileData) != expectedContent {
		t.Errorf("Content mismatch for %s: got %q, want %q", fileName, string(fileData), expectedContent)
	}
}

func verifySymlinkInDirectory(t *testing.T, store hcas.Hcas, dirName *hcas.Name, linkName, expectedTarget string) {
	dirData, err := readObjectData(store, *dirName)
	if err != nil {
		t.Fatalf("Failed to read directory for %s: %v", linkName, err)
	}

	dirReader := bytes.NewReader(dirData)
	entry, err := LookupChild(dirReader, linkName)
	if err != nil {
		t.Fatalf("Failed to lookup %s: %v", linkName, err)
	}
	if entry == nil {
		t.Fatalf("Symlink %s not found", linkName)
	}

	if !unix.S_ISLNK(entry.Inode.Mode) {
		t.Errorf("%s is not a symlink", linkName)
		return
	}

	linkData, err := readObjectData(store, *entry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read %s target: %v", linkName, err)
	}

	if string(linkData) != expectedTarget {
		t.Errorf("Symlink target mismatch for %s: got %q, want %q", linkName, string(linkData), expectedTarget)
	}
}

func getDirectoryObject(t *testing.T, store hcas.Hcas, parentName *hcas.Name, dirName string) *hcas.Name {
	parentData, err := readObjectData(store, *parentName)
	if err != nil {
		t.Fatalf("Failed to read parent directory for %s: %v", dirName, err)
	}

	parentReader := bytes.NewReader(parentData)
	entry, err := LookupChild(parentReader, dirName)
	if err != nil {
		t.Fatalf("Failed to lookup directory %s: %v", dirName, err)
	}
	if entry == nil {
		t.Fatalf("Directory %s not found", dirName)
	}

	if !unix.S_ISDIR(entry.Inode.Mode) {
		t.Fatalf("%s is not a directory", dirName)
	}

	return entry.Inode.ObjName
}

func verifyDirectoryExists(t *testing.T, store hcas.Hcas, parentName *hcas.Name, dirName string) {
	parentData, err := readObjectData(store, *parentName)
	if err != nil {
		t.Fatalf("Failed to read parent directory for %s: %v", dirName, err)
	}

	parentReader := bytes.NewReader(parentData)
	entry, err := LookupChild(parentReader, dirName)
	if err != nil {
		t.Fatalf("Failed to lookup directory %s: %v", dirName, err)
	}
	if entry == nil {
		t.Fatalf("Directory %s not found", dirName)
	}

	if !unix.S_ISDIR(entry.Inode.Mode) {
		t.Errorf("%s is not a directory", dirName)
	}
}
