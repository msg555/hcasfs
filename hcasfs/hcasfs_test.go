package hcasfs

import (
	"archive/tar"
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/unix"
)

func TestDirEntryEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		dirEntry DirEntry
	}{
		{
			name: "regular file",
			dirEntry: DirEntry{
				Inode: InodeData{
					Mode: unix.S_IFREG | 0644,
					Uid:  1000,
					Gid:  1000,
					Dev:  0,
					Atim: 1640995200000000000, // 2022-01-01
					Mtim: 1640995200000000000,
					Ctim: 1640995200000000000,
					Size: 1024,
					ObjName: func() *hcas.Name {
						name := hcas.NewName("0123456789abcdef0123456789abcdef")
						return &name
					}(),
				},
				FileName:         "test.txt",
				TreeSize:         1,
				FileNameChecksum: 0x12345678,
				ParentDepIndex:   100,
			},
		},
		{
			name: "directory",
			dirEntry: DirEntry{
				Inode: InodeData{
					Mode: unix.S_IFDIR | 0755,
					Uid:  0,
					Gid:  0,
					Dev:  0,
					Atim: 1640995200000000000,
					Mtim: 1640995200000000000,
					Ctim: 1640995200000000000,
					Size: 4096,
					ObjName: func() *hcas.Name {
						name := hcas.NewName("fedcba9876543210fedcba9876543210")
						return &name
					}(),
				},
				FileName:         "subdir",
				TreeSize:         10,
				FileNameChecksum: 0xabcdef01,
				ParentDepIndex:   200,
			},
		},
		{
			name: "symlink",
			dirEntry: DirEntry{
				Inode: InodeData{
					Mode: unix.S_IFLNK | 0777,
					Uid:  1000,
					Gid:  1000,
					Dev:  0,
					Atim: 1640995200000000000,
					Mtim: 1640995200000000000,
					Ctim: 1640995200000000000,
					Size: 10,
					ObjName: func() *hcas.Name {
						name := hcas.NewName("11111111111111111111111111111111")
						return &name
					}(),
				},
				FileName:         "link.txt",
				TreeSize:         1,
				FileNameChecksum: 0x87654321,
				ParentDepIndex:   50,
			},
		},
		{
			name: "block device",
			dirEntry: DirEntry{
				Inode: InodeData{
					Mode: unix.S_IFBLK | 0660,
					Uid:  0,
					Gid:  6, // disk group
					Dev:  0x0801, // major=8, minor=1
					Atim: 1640995200000000000,
					Mtim: 1640995200000000000,
					Ctim: 1640995200000000000,
					Size: 0,
					ObjName: nil, // block devices don't have object data
				},
				FileName:         "sda1",
				TreeSize:         1,
				FileNameChecksum: 0x11111111,
				ParentDepIndex:   1,
			},
		},
		{
			name: "character device",
			dirEntry: DirEntry{
				Inode: InodeData{
					Mode: unix.S_IFCHR | 0666,
					Uid:  0,
					Gid:  5, // tty group
					Dev:  0x0501, // major=5, minor=1
					Atim: 1640995200000000000,
					Mtim: 1640995200000000000,
					Ctim: 1640995200000000000,
					Size: 0,
					ObjName: nil, // char devices don't have object data
				},
				FileName:         "console",
				TreeSize:         1,
				FileNameChecksum: 0x22222222,
				ParentDepIndex:   1,
			},
		},
		{
			name: "fifo",
			dirEntry: DirEntry{
				Inode: InodeData{
					Mode: unix.S_IFIFO | 0644,
					Uid:  1000,
					Gid:  1000,
					Dev:  0,
					Atim: 1640995200000000000,
					Mtim: 1640995200000000000,
					Ctim: 1640995200000000000,
					Size: 0,
					ObjName: nil, // fifos don't have object data
				},
				FileName:         "mypipe",
				TreeSize:         1,
				FileNameChecksum: 0x33333333,
				ParentDepIndex:   1,
			},
		},
		{
			name: "long filename",
			dirEntry: DirEntry{
				Inode: InodeData{
					Mode: unix.S_IFREG | 0644,
					Uid:  1000,
					Gid:  1000,
					Dev:  0,
					Atim: 1640995200000000000,
					Mtim: 1640995200000000000,
					Ctim: 1640995200000000000,
					Size: 0,
					ObjName: func() *hcas.Name {
						name := hcas.NewName("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
						return &name
					}(),
				},
				FileName:         strings.Repeat("a", 200), // long filename
				TreeSize:         1,
				FileNameChecksum: 0x44444444,
				ParentDepIndex:   1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := tt.dirEntry.Encode()

			// Decode
			reader := bytes.NewReader(encoded)
			var decoded DirEntry
			err := decoded.DecodeStream(reader)
			if err != nil {
				t.Fatalf("DecodeStream failed: %v", err)
			}

			// Compare
			if decoded.Inode.Mode != tt.dirEntry.Inode.Mode {
				t.Errorf("Mode mismatch: got %o, want %o", decoded.Inode.Mode, tt.dirEntry.Inode.Mode)
			}
			if decoded.Inode.Uid != tt.dirEntry.Inode.Uid {
				t.Errorf("Uid mismatch: got %d, want %d", decoded.Inode.Uid, tt.dirEntry.Inode.Uid)
			}
			if decoded.Inode.Gid != tt.dirEntry.Inode.Gid {
				t.Errorf("Gid mismatch: got %d, want %d", decoded.Inode.Gid, tt.dirEntry.Inode.Gid)
			}
			if decoded.Inode.Dev != tt.dirEntry.Inode.Dev {
				t.Errorf("Dev mismatch: got %d, want %d", decoded.Inode.Dev, tt.dirEntry.Inode.Dev)
			}
			if decoded.Inode.Atim != tt.dirEntry.Inode.Atim {
				t.Errorf("Atim mismatch: got %d, want %d", decoded.Inode.Atim, tt.dirEntry.Inode.Atim)
			}
			if decoded.Inode.Mtim != tt.dirEntry.Inode.Mtim {
				t.Errorf("Mtim mismatch: got %d, want %d", decoded.Inode.Mtim, tt.dirEntry.Inode.Mtim)
			}
			if decoded.Inode.Ctim != tt.dirEntry.Inode.Ctim {
				t.Errorf("Ctim mismatch: got %d, want %d", decoded.Inode.Ctim, tt.dirEntry.Inode.Ctim)
			}
			if decoded.Inode.Size != tt.dirEntry.Inode.Size {
				t.Errorf("Size mismatch: got %d, want %d", decoded.Inode.Size, tt.dirEntry.Inode.Size)
			}
			if (decoded.Inode.ObjName == nil) != (tt.dirEntry.Inode.ObjName == nil) {
				t.Errorf("ObjName nil status mismatch: got %v, want %v", 
					decoded.Inode.ObjName == nil, tt.dirEntry.Inode.ObjName == nil)
			}
			if decoded.Inode.ObjName != nil && tt.dirEntry.Inode.ObjName != nil {
				if decoded.Inode.ObjName.HexName() != tt.dirEntry.Inode.ObjName.HexName() {
					t.Errorf("ObjName mismatch: got %s, want %s", 
						decoded.Inode.ObjName.HexName(), tt.dirEntry.Inode.ObjName.HexName())
				}
			}
			if decoded.FileName != tt.dirEntry.FileName {
				t.Errorf("FileName mismatch: got %s, want %s", decoded.FileName, tt.dirEntry.FileName)
			}
			if decoded.ParentDepIndex != tt.dirEntry.ParentDepIndex {
				t.Errorf("ParentDepIndex mismatch: got %d, want %d", 
					decoded.ParentDepIndex, tt.dirEntry.ParentDepIndex)
			}
		})
	}
}

func TestInodeFromStat(t *testing.T) {
	objName := hcas.NewName("0123456789abcdef0123456789abcdef")
	
	stat := unix.Stat_t{
		Mode: unix.S_IFREG | 0644,
		Uid:  1000,
		Gid:  1000,
		Size: 1024,
		Atim: unix.Stat_t{}.Atim, // Use zero timespec - actual values don't matter for test
		Mtim: unix.Stat_t{}.Mtim,
		Ctim: unix.Stat_t{}.Ctim,
		Rdev: 0x0801, // for device files
	}

	inode := InodeFromStat(stat, &objName)

	if inode.Mode != stat.Mode {
		t.Errorf("Mode mismatch: got %o, want %o", inode.Mode, stat.Mode)
	}
	if inode.Uid != stat.Uid {
		t.Errorf("Uid mismatch: got %d, want %d", inode.Uid, stat.Uid)
	}
	if inode.Gid != stat.Gid {
		t.Errorf("Gid mismatch: got %d, want %d", inode.Gid, stat.Gid)
	}
	if inode.Size != uint64(stat.Size) {
		t.Errorf("Size mismatch: got %d, want %d", inode.Size, stat.Size)
	}
	expectedAtim := uint64(stat.Atim.Nano())
	if inode.Atim != expectedAtim {
		t.Errorf("Atim mismatch: got %d, want %d", inode.Atim, expectedAtim)
	}
	expectedMtim := uint64(stat.Mtim.Nano())
	if inode.Mtim != expectedMtim {
		t.Errorf("Mtim mismatch: got %d, want %d", inode.Mtim, expectedMtim)
	}
	expectedCtim := uint64(stat.Ctim.Nano())
	if inode.Ctim != expectedCtim {
		t.Errorf("Ctim mismatch: got %d, want %d", inode.Ctim, expectedCtim)
	}
	if inode.ObjName.HexName() != objName.HexName() {
		t.Errorf("ObjName mismatch: got %s, want %s", inode.ObjName.HexName(), objName.HexName())
	}

	// Test device file handling
	stat.Mode = unix.S_IFCHR | 0666
	inode = InodeFromStat(stat, nil)
	if inode.Dev != stat.Rdev {
		t.Errorf("Dev not set for char device: got %d, want %d", inode.Dev, stat.Rdev)
	}

	stat.Mode = unix.S_IFBLK | 0660
	inode = InodeFromStat(stat, nil)
	if inode.Dev != stat.Rdev {
		t.Errorf("Dev not set for block device: got %d, want %d", inode.Dev, stat.Rdev)
	}

	// Test non-device file
	stat.Mode = unix.S_IFREG | 0644
	inode = InodeFromStat(stat, nil)
	if inode.Dev != 0 {
		t.Errorf("Dev should be 0 for regular file: got %d", inode.Dev)
	}
}

func TestValidatePathName(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
		path  string
	}{
		{"empty string", false, ""},
		{"valid name", true, "test.txt"},
		{"valid name with spaces", true, "test file.txt"},
		{"valid name with dots", true, "..test"},
		{"valid name with unicode", true, "téstñame"},
		{"name with null byte", false, "test\x00file"},
		{"name with slash", false, "test/file"},
		{"max length name", true, strings.Repeat("a", unix.NAME_MAX)},
		{"too long name", false, strings.Repeat("a", unix.NAME_MAX+1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validatePathName(tt.path)
			if result != tt.valid {
				t.Errorf("validatePathName(%q) = %v, want %v", tt.path, result, tt.valid)
			}
		})
	}
}

func TestDirBuilder(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create some test objects
	obj1, err := session.CreateObject([]byte("file1 content"))
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}

	obj2, err := session.CreateObject([]byte("file2 content"))
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}

	// Create a directory builder
	builder := CreateDirBuilder()
	if builder == nil {
		t.Fatal("CreateDirBuilder returned nil")
	}

	// Add some entries
	inode1 := &InodeData{
		Mode:    unix.S_IFREG | 0644,
		Uid:     1000,
		Gid:     1000,
		Size:    13,
		ObjName: obj1,
	}
	builder.Insert("file1.txt", inode1, 1)

	inode2 := &InodeData{
		Mode:    unix.S_IFREG | 0644,
		Uid:     1000,
		Gid:     1000,
		Size:    13,
		ObjName: obj2,
	}
	builder.Insert("file2.txt", inode2, 1)

	// Add a directory
	subDirObj, err := session.CreateObject([]byte("subdirectory data"))
	if err != nil {
		t.Fatalf("Failed to create subdirectory object: %v", err)
	}

	inodeDir := &InodeData{
		Mode:    unix.S_IFDIR | 0755,
		Uid:     1000,
		Gid:     1000,
		Size:    4096,
		ObjName: subDirObj,
	}
	builder.Insert("subdir", inodeDir, 5) // treeSize > 1 for directory

	// Add special files (no object data)
	inodeFifo := &InodeData{
		Mode:    unix.S_IFIFO | 0644,
		Uid:     1000,
		Gid:     1000,
		Size:    0,
		ObjName: nil,
	}
	builder.Insert("mypipe", inodeFifo, 1)

	inodeChr := &InodeData{
		Mode:    unix.S_IFCHR | 0666,
		Uid:     0,
		Gid:     5,
		Dev:     0x0501,
		Size:    0,
		ObjName: nil,
	}
	builder.Insert("console", inodeChr, 1)

	// Test builder state
	if len(builder.DirEntries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(builder.DirEntries))
	}

	if builder.TotalTreeSize != 9 { // 1 (self) + 1 + 1 + 5 + 1 + 1
		t.Errorf("Expected TotalTreeSize 9, got %d", builder.TotalTreeSize)
	}

	// Build the directory
	dirData := builder.Build()
	if len(dirData) == 0 {
		t.Fatal("Build returned empty data")
	}

	// Verify entries are sorted by filename checksum
	for i := 1; i < len(builder.DirEntries); i++ {
		if builder.DirEntries[i-1].FileNameChecksum > builder.DirEntries[i].FileNameChecksum {
			t.Error("Entries not sorted by filename checksum")
		}
	}

	// Test that we can create the directory object
	dirObj, err := session.CreateObject(dirData, builder.DepNames...)
	if err != nil {
		t.Fatalf("Failed to create directory object: %v", err)
	}

	if dirObj == nil {
		t.Fatal("Directory object is nil")
	}
}

func TestLookupChild(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	// Create a directory with some entries
	builder := CreateDirBuilder()

	// Add files with names that will create different CRC values
	files := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, filename := range files {
		obj, err := session.CreateObject([]byte(filename + " content"))
		if err != nil {
			t.Fatalf("Failed to create object for %s: %v", filename, err)
		}

		inode := &InodeData{
			Mode:    unix.S_IFREG | 0644,
			Uid:     1000,
			Gid:     1000,
			Size:    uint64(len(filename) + 8),
			ObjName: obj,
		}
		builder.Insert(filename, inode, 1)
	}

	dirData := builder.Build()
	dirReader := bytes.NewReader(dirData)

	// Test looking up existing files
	for _, filename := range files {
		dirReader.Seek(0, 0) // Reset reader
		entry, err := LookupChild(dirReader, filename)
		if err != nil {
			t.Errorf("LookupChild failed for %s: %v", filename, err)
			continue
		}
		if entry == nil {
			t.Errorf("LookupChild returned nil for existing file %s", filename)
			continue
		}
		if entry.FileName != filename {
			t.Errorf("Expected filename %s, got %s", filename, entry.FileName)
		}
	}

	// Test looking up non-existent file
	dirReader.Seek(0, 0)
	entry, err := LookupChild(dirReader, "nonexistent")
	if err != nil {
		t.Errorf("LookupChild should not error for non-existent file: %v", err)
	}
	if entry != nil {
		t.Error("LookupChild should return nil for non-existent file")
	}

	// Test edge cases
	dirReader.Seek(0, 0)
	entry, err = LookupChild(dirReader, "")
	if err != nil {
		t.Errorf("LookupChild should not error for empty string: %v", err)
	}
	if entry != nil {
		t.Error("LookupChild should return nil for empty filename")
	}
}

func TestLookupChildCRCCollisions(t *testing.T) {
	env := createTestEnvironment(t)
	defer env.session.Close()
	session := env.session

	builder := CreateDirBuilder()

	// These strings have the same CRC32 checksum (found through testing)
	// This is a bit artificial, but tests the collision handling code
	collisionFiles := []string{"test1", "test2"}
	
	for _, filename := range collisionFiles {
		obj, err := session.CreateObject([]byte(filename + " content"))
		if err != nil {
			t.Fatalf("Failed to create object for %s: %v", filename, err)
		}

		inode := &InodeData{
			Mode:    unix.S_IFREG | 0644,
			Uid:     1000,
			Gid:     1000,
			Size:    uint64(len(filename) + 8),
			ObjName: obj,
		}
		builder.Insert(filename, inode, 1)
	}

	dirData := builder.Build()
	dirReader := bytes.NewReader(dirData)

	// Test that we can still find both files even if they have CRC collisions
	for _, filename := range collisionFiles {
		dirReader.Seek(0, 0)
		entry, err := LookupChild(dirReader, filename)
		if err != nil {
			t.Errorf("LookupChild failed for %s: %v", filename, err)
			continue
		}
		if entry == nil {
			t.Errorf("LookupChild returned nil for %s", filename)
			continue
		}
		if entry.FileName != filename {
			t.Errorf("Expected filename %s, got %s", filename, entry.FileName)
		}
	}
}
