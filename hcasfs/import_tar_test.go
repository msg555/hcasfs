package hcasfs

import (
	"archive/tar"
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/msg555/hcas/unix"
)

// Helper function to create a tar archive in memory for testing
func createTestTarArchive(entries []tarTestEntry) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, entry := range entries {
		header := &tar.Header{
			Name:       entry.Name,
			Mode:       entry.Mode,
			Uid:        entry.Uid,
			Gid:        entry.Gid,
			Size:       entry.Size,
			ModTime:    entry.ModTime,
			AccessTime: entry.AccessTime,
			ChangeTime: entry.ChangeTime,
			Typeflag:   entry.Typeflag,
			Linkname:   entry.Linkname,
			Devmajor:   entry.Devmajor,
			Devminor:   entry.Devminor,
		}

		if err := tw.WriteHeader(header); err != nil {
			panic(err)
		}

		if entry.Content != nil {
			if _, err := tw.Write(entry.Content); err != nil {
				panic(err)
			}
		}
	}

	if err := tw.Close(); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

type tarTestEntry struct {
	Name       string
	Mode       int64
	Uid        int
	Gid        int
	Size       int64
	ModTime    time.Time
	AccessTime time.Time
	ChangeTime time.Time
	Typeflag   byte
	Linkname   string
	Devmajor   int64
	Devminor   int64
	Content    []byte
}

func TestImportTarBasicFiles(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "file1.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       11,
			ModTime:    now,
			AccessTime: now.Add(-time.Hour),
			ChangeTime: now.Add(-time.Minute),
			Typeflag:   tar.TypeReg,
			Content:    []byte("hello world"),
		},
		{
			Name:       "file2.txt",
			Mode:       0755,
			Uid:        0,
			Gid:        0,
			Size:       7,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("goodbye"),
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	rootName, err := ImportTar(session, reader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	if rootName == nil {
		t.Fatal("ImportTar returned nil root name")
	}

	// Verify we can read the root directory
	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	// Look up the files
	rootReader := bytes.NewReader(rootData)
	entry1, err := LookupChild(rootReader, "file1.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file1.txt: %v", err)
	}
	if entry1 == nil {
		t.Fatal("file1.txt not found")
	}

	rootReader.Seek(0, 0)
	entry2, err := LookupChild(rootReader, "file2.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file2.txt: %v", err)
	}
	if entry2 == nil {
		t.Fatal("file2.txt not found")
	}

	// Verify file contents
	file1Data, err := readObjectData(env.store, *entry1.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read file1 content: %v", err)
	}
	if string(file1Data) != "hello world" {
		t.Errorf("file1 content mismatch: got %q, want %q", string(file1Data), "hello world")
	}

	file2Data, err := readObjectData(env.store, *entry2.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read file2 content: %v", err)
	}
	if string(file2Data) != "goodbye" {
		t.Errorf("file2 content mismatch: got %q, want %q", string(file2Data), "goodbye")
	}

	// Verify metadata
	if entry1.Inode.Mode != unix.S_IFREG|0644 {
		t.Errorf("file1 mode mismatch: got %o, want %o", entry1.Inode.Mode, unix.S_IFREG|0644)
	}
	if entry1.Inode.Uid != 1000 {
		t.Errorf("file1 uid mismatch: got %d, want %d", entry1.Inode.Uid, 1000)
	}
	if entry1.Inode.Size != 11 {
		t.Errorf("file1 size mismatch: got %d, want %d", entry1.Inode.Size, 11)
	}
}

func TestImportTarDirectories(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "dir1/",
			Mode:       0755,
			Uid:        1000,
			Gid:        1000,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeDir,
		},
		{
			Name:       "dir1/file.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       8,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("in dir1"),
		},
		{
			Name:       "dir2/",
			Mode:       0700,
			Uid:        0,
			Gid:        0,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeDir,
		},
		{
			Name:       "dir2/subdir/",
			Mode:       0755,
			Uid:        0,
			Gid:        0,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeDir,
		},
		{
			Name:       "dir2/subdir/nested.txt",
			Mode:       0600,
			Uid:        0,
			Gid:        0,
			Size:       6,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("nested"),
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	rootName, err := ImportTar(session, reader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	// Verify directory structure
	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	rootReader := bytes.NewReader(rootData)
	dir1Entry, err := LookupChild(rootReader, "dir1")
	if err != nil {
		t.Fatalf("Failed to lookup dir1: %v", err)
	}
	if dir1Entry == nil {
		t.Fatal("dir1 not found")
	}
	if !unix.S_ISDIR(dir1Entry.Inode.Mode) {
		t.Error("dir1 is not a directory")
	}

	// Look inside dir1
	dir1Data, err := readObjectData(env.store, *dir1Entry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read dir1: %v", err)
	}

	dir1Reader := bytes.NewReader(dir1Data)
	fileEntry, err := LookupChild(dir1Reader, "file.txt")
	if err != nil {
		t.Fatalf("Failed to lookup file.txt in dir1: %v", err)
	}
	if fileEntry == nil {
		t.Fatal("file.txt not found in dir1")
	}

	// Verify nested directory
	rootReader.Seek(0, 0)
	dir2Entry, err := LookupChild(rootReader, "dir2")
	if err != nil {
		t.Fatalf("Failed to lookup dir2: %v", err)
	}
	if dir2Entry == nil {
		t.Fatal("dir2 not found")
	}

	dir2Data, err := readObjectData(env.store, *dir2Entry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read dir2: %v", err)
	}

	dir2Reader := bytes.NewReader(dir2Data)
	subdirEntry, err := LookupChild(dir2Reader, "subdir")
	if err != nil {
		t.Fatalf("Failed to lookup subdir in dir2: %v", err)
	}
	if subdirEntry == nil {
		t.Fatal("subdir not found in dir2")
	}

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

	// Verify nested file content
	nestedData, err := readObjectData(env.store, *nestedEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read nested.txt: %v", err)
	}
	if string(nestedData) != "nested" {
		t.Errorf("nested.txt content mismatch: got %q, want %q", string(nestedData), "nested")
	}
}

func TestImportTarSymlinks(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "target.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       6,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("target"),
		},
		{
			Name:       "link.txt",
			Mode:       0777,
			Uid:        1000,
			Gid:        1000,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeSymlink,
			Linkname:   "target.txt",
		},
		{
			Name:       "abs_link.txt",
			Mode:       0777,
			Uid:        1000,
			Gid:        1000,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeSymlink,
			Linkname:   "/absolute/path",
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	rootName, err := ImportTar(session, reader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	// Check symlink
	rootReader := bytes.NewReader(rootData)
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

	// Verify symlink target
	linkData, err := readObjectData(env.store, *linkEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read symlink data: %v", err)
	}
	if string(linkData) != "target.txt" {
		t.Errorf("Symlink target mismatch: got %q, want %q", string(linkData), "target.txt")
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
		t.Fatalf("Failed to read absolute symlink data: %v", err)
	}
	if string(absLinkData) != "/absolute/path" {
		t.Errorf("Absolute symlink target mismatch: got %q, want %q", string(absLinkData), "/absolute/path")
	}

	// Verify symlink size is set to target length
	if linkEntry.Inode.Size != uint64(len("target.txt")) {
		t.Errorf("Symlink size mismatch: got %d, want %d", linkEntry.Inode.Size, len("target.txt"))
	}
}

func TestImportTarHardLinks(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "original.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       8,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("original"),
		},
		{
			Name:       "hardlink.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       0, // hard links don't store size in tar
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeLink,
			Linkname:   "original.txt",
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	rootName, err := ImportTar(session, reader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	// Check original file
	rootReader := bytes.NewReader(rootData)
	originalEntry, err := LookupChild(rootReader, "original.txt")
	if err != nil {
		t.Fatalf("Failed to lookup original.txt: %v", err)
	}
	if originalEntry == nil {
		t.Fatal("original.txt not found")
	}

	// Check hard link
	rootReader.Seek(0, 0)
	hardlinkEntry, err := LookupChild(rootReader, "hardlink.txt")
	if err != nil {
		t.Fatalf("Failed to lookup hardlink.txt: %v", err)
	}
	if hardlinkEntry == nil {
		t.Fatal("hardlink.txt not found")
	}

	// Verify both files have the same object name (same content)
	if originalEntry.Inode.ObjName.HexName() != hardlinkEntry.Inode.ObjName.HexName() {
		t.Error("Hard link doesn't point to same object as original")
	}

	// Verify content is the same
	originalData, err := readObjectData(env.store, *originalEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read original file: %v", err)
	}

	hardlinkData, err := readObjectData(env.store, *hardlinkEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read hardlink file: %v", err)
	}

	if string(originalData) != string(hardlinkData) {
		t.Errorf("Hard link content differs from original: %q vs %q",
			string(hardlinkData), string(originalData))
	}

	// Verify metadata is copied
	if originalEntry.Inode.Mode != hardlinkEntry.Inode.Mode {
		t.Error("Hard link mode differs from original")
	}
	if originalEntry.Inode.Size != hardlinkEntry.Inode.Size {
		t.Error("Hard link size differs from original")
	}
}

func TestImportTarSpecialFiles(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "dev/console",
			Mode:       0666,
			Uid:        0,
			Gid:        5,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeChar,
			Devmajor:   5,
			Devminor:   1,
		},
		{
			Name:       "dev/sda1",
			Mode:       0660,
			Uid:        0,
			Gid:        6,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeBlock,
			Devmajor:   8,
			Devminor:   1,
		},
		{
			Name:       "tmp/mypipe",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeFifo,
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	rootName, err := ImportTar(session, reader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	// Check character device
	rootReader := bytes.NewReader(rootData)
	devEntry, err := LookupChild(rootReader, "dev")
	if err != nil {
		t.Fatalf("Failed to lookup dev: %v", err)
	}
	if devEntry == nil {
		t.Fatal("dev directory not found")
	}

	devData, err := readObjectData(env.store, *devEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read dev directory: %v", err)
	}

	devReader := bytes.NewReader(devData)
	consoleEntry, err := LookupChild(devReader, "console")
	if err != nil {
		t.Fatalf("Failed to lookup console: %v", err)
	}
	if consoleEntry == nil {
		t.Fatal("console not found")
	}

	// Verify character device properties
	if !unix.S_ISCHR(consoleEntry.Inode.Mode) {
		t.Error("console is not a character device")
	}
	if consoleEntry.Inode.ObjName != nil {
		t.Error("character device should not have object data")
	}
	expectedDev := uint64(5)<<8 | uint64(1) // major=5, minor=1
	if consoleEntry.Inode.Dev != expectedDev {
		t.Errorf("console device number mismatch: got %d, want %d", consoleEntry.Inode.Dev, expectedDev)
	}

	// Check block device
	devReader.Seek(0, 0)
	sda1Entry, err := LookupChild(devReader, "sda1")
	if err != nil {
		t.Fatalf("Failed to lookup sda1: %v", err)
	}
	if sda1Entry == nil {
		t.Fatal("sda1 not found")
	}

	// Verify block device properties
	if !unix.S_ISBLK(sda1Entry.Inode.Mode) {
		t.Error("sda1 is not a block device")
	}
	if sda1Entry.Inode.ObjName != nil {
		t.Error("block device should not have object data")
	}
	expectedDev = uint64(8)<<8 | uint64(1) // major=8, minor=1
	if sda1Entry.Inode.Dev != expectedDev {
		t.Errorf("sda1 device number mismatch: got %d, want %d", sda1Entry.Inode.Dev, expectedDev)
	}

	// Check FIFO
	rootReader.Seek(0, 0)
	tmpEntry, err := LookupChild(rootReader, "tmp")
	if err != nil {
		t.Fatalf("Failed to lookup tmp: %v", err)
	}
	if tmpEntry == nil {
		t.Fatal("tmp directory not found")
	}

	tmpData, err := readObjectData(env.store, *tmpEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read tmp directory: %v", err)
	}

	tmpReader := bytes.NewReader(tmpData)
	pipeEntry, err := LookupChild(tmpReader, "mypipe")
	if err != nil {
		t.Fatalf("Failed to lookup mypipe: %v", err)
	}
	if pipeEntry == nil {
		t.Fatal("mypipe not found")
	}

	// Verify FIFO properties
	if (pipeEntry.Inode.Mode & unix.S_IFMT) != unix.S_IFIFO {
		t.Error("mypipe is not a FIFO")
	}
	if pipeEntry.Inode.ObjName != nil {
		t.Error("FIFO should not have object data")
	}
}

func TestImportTarInvalidNames(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "valid.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       5,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("valid"),
		},
		{
			Name:       "invalid\x00name.txt", // null byte in name
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       7,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("invalid"),
		},
		{
			Name:       strings.Repeat("a", unix.NAME_MAX+1), // too long name
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       8,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    []byte("toolong"),
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	// Capture stderr to check for skipped file messages
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	rootName, err := ImportTar(session, reader)

	w.Close()
	os.Stderr = oldStderr

	stderrOutput := make([]byte, 1024)
	n, _ := r.Read(stderrOutput)
	stderrStr := string(stderrOutput[:n])

	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	// Should contain only the valid file
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

	// Invalid files should be skipped
	rootReader.Seek(0, 0)
	invalidEntry, err := LookupChild(rootReader, "invalid\x00name.txt")
	if err != nil {
		t.Fatalf("Lookup should not error: %v", err)
	}
	if invalidEntry != nil {
		t.Error("invalid file should have been skipped")
	}

	// Check that stderr contains skip messages
	if !strings.Contains(stderrStr, "skipped file with invalid name") {
		t.Error("Should have printed skip message for invalid names")
	}
}

func TestImportTarBrokenHardlink(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "hardlink.txt",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       0,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeLink,
			Linkname:   "nonexistent.txt", // links to non-existent file
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	_, err := ImportTar(session, reader)
	if err == nil {
		t.Fatal("ImportTar should have failed with broken hardlink")
	}

	if !strings.Contains(err.Error(), "broken hardlink") {
		t.Errorf("Error should mention broken hardlink: %v", err)
	}
}

func TestImportTarEmptyArchive(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create empty tar archive
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.Close()

	reader := bytes.NewReader(buf.Bytes())
	rootName, err := ImportTar(session, reader)
	if err != nil {
		t.Fatalf("ImportTar failed on empty archive: %v", err)
	}

	if rootName == nil {
		t.Fatal("ImportTar should return root even for empty archive")
	}

	// Root should be an empty directory
	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	// Should have just the directory header with 0 entries
	if len(rootData) < 16 {
		t.Error("Root directory data too short")
	}
}

func TestImportTarLargeFile(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create a larger file to test streaming
	largeContent := make([]byte, 100*1024) // 100KB
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	now := time.Now()
	entries := []tarTestEntry{
		{
			Name:       "large.dat",
			Mode:       0644,
			Uid:        1000,
			Gid:        1000,
			Size:       int64(len(largeContent)),
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
			Typeflag:   tar.TypeReg,
			Content:    largeContent,
		},
	}

	tarData := createTestTarArchive(entries)
	reader := bytes.NewReader(tarData)

	rootName, err := ImportTar(session, reader)
	if err != nil {
		t.Fatalf("ImportTar failed: %v", err)
	}

	rootData, err := readObjectData(env.store, *rootName)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}

	rootReader := bytes.NewReader(rootData)
	fileEntry, err := LookupChild(rootReader, "large.dat")
	if err != nil {
		t.Fatalf("Failed to lookup large.dat: %v", err)
	}
	if fileEntry == nil {
		t.Fatal("large.dat not found")
	}

	// Verify large file content
	fileData, err := readObjectData(env.store, *fileEntry.Inode.ObjName)
	if err != nil {
		t.Fatalf("Failed to read large file: %v", err)
	}

	if !bytes.Equal(fileData, largeContent) {
		t.Error("Large file content mismatch")
	}

	if fileEntry.Inode.Size != uint64(len(largeContent)) {
		t.Errorf("Large file size mismatch: got %d, want %d",
			fileEntry.Inode.Size, len(largeContent))
	}
}
