package hcasfs

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/go-errors/errors"

	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/unix"
)

func tarModeToUnixMode(tarMode int64, typeflag byte) uint32 {
	mode := uint32(tarMode & 0777)

	switch typeflag {
	case tar.TypeReg, tar.TypeRegA, tar.TypeLink:
		mode |= unix.S_IFREG
	case tar.TypeDir:
		mode |= unix.S_IFDIR
	case tar.TypeSymlink:
		mode |= unix.S_IFLNK
	case tar.TypeChar:
		mode |= unix.S_IFCHR
	case tar.TypeBlock:
		mode |= unix.S_IFBLK
	case tar.TypeFifo:
		mode |= unix.S_IFIFO
	default:
		mode |= unix.S_IFREG
	}

	return mode
}

func InodeFromTarHeader(header *tar.Header) *InodeData {
	size := uint64(header.Size)
	if header.Typeflag == tar.TypeSymlink {
		size = uint64(len(header.Linkname))
	}
	return &InodeData{
		Mode: tarModeToUnixMode(header.Mode, header.Typeflag),
		Uid:  uint32(header.Uid),
		Gid:  uint32(header.Gid),
		Dev:  0,
		Atim: uint64(header.AccessTime.UnixNano()),
		Mtim: uint64(header.ModTime.UnixNano()),
		Ctim: uint64(header.ChangeTime.UnixNano()),
		Size: size,
	}
}

func importTarRegular(hs hcas.Session, tarReader *tar.Reader, size int64) (*hcas.Name, error) {
	writer, err := hs.StreamObject()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 1<<16)
	var totalBytesRead int64

	for totalBytesRead < size {
		toRead := int64(len(buf))
		if size-totalBytesRead < toRead {
			toRead = size - totalBytesRead
		}

		bytesRead, err := tarReader.Read(buf[:toRead])
		if err != nil && err != io.EOF {
			return nil, err
		}
		if bytesRead == 0 {
			break
		}

		totalBytesRead += int64(bytesRead)
		bufRead := buf[:bytesRead]
		for written := 0; written < bytesRead; {
			bytesWritten, err := writer.Write(bufRead[written:])
			if err != nil {
				return nil, err
			}
			written += bytesWritten
		}
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return writer.Name(), nil
}

func importTarSymlink(hs hcas.Session, linkTarget string) (*hcas.Name, error) {
	return hs.CreateObject([]byte(linkTarget))
}

type tarDirEntry struct {
	inode    InodeData
	treeSize uint64
	children map[string]*tarDirEntry
}

type hardlinkData struct {
	fileEntry *tarDirEntry
	linkname  string
}

func ImportTar(hs hcas.Session, tarReader io.Reader) (*hcas.Name, error) {
	tr := tar.NewReader(tarReader)

	rootEntry := tarDirEntry{
		children: make(map[string]*tarDirEntry),
	}
	dirEntries := map[string]*tarDirEntry{
		"/": &rootEntry,
	}
	hardlinks := make([]hardlinkData, 0, 8)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		name := filepath.Clean("/" + header.Name)

		fileName := filepath.Base(name)
		if !validatePathName(fileName) {
			fmt.Fprintf(os.Stderr, "skipped file with invalid name '%s'\n", fileName)
			continue
		}

		dirPath := filepath.Dir(name)
		fileEntry := tarDirEntry{
			inode:    *InodeFromTarHeader(header),
			treeSize: 1,
		}

		var objName *hcas.Name
		switch header.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
			objName, err = importTarRegular(hs, tr, header.Size)
			if err != nil {
				return nil, err
			}

		case tar.TypeDir:
			fileEntry.children = make(map[string]*tarDirEntry)
			dirEntries[name] = &fileEntry

		case tar.TypeSymlink:
			objName, err = importTarSymlink(hs, header.Linkname)
			fmt.Printf("Got symlink to %s %s\n", header.Linkname, objName.HexName())
			if err != nil {
				return nil, err
			}

		case tar.TypeLink:
			hardlinks = append(hardlinks, hardlinkData{
				fileEntry: &fileEntry,
				linkname:  header.Linkname,
			})

		case tar.TypeChar:
			// Character device files don't need object data, just inode metadata
			// Device major/minor numbers are stored in the Dev field of InodeData
			fileEntry.inode.Dev = uint64(header.Devmajor)<<8 | uint64(header.Devminor)

		case tar.TypeBlock:
			// Block device files don't need object data, just inode metadata
			// Device major/minor numbers are stored in the Dev field of InodeData
			fileEntry.inode.Dev = uint64(header.Devmajor)<<8 | uint64(header.Devminor)

		case tar.TypeFifo:
			// FIFO (named pipe) files don't need object data, just inode metadata

		default:
			fmt.Fprintf(os.Stderr, "skipped unsupported file type '%s' (type %c)\n", name, header.Typeflag)
			continue
		}
		fileEntry.inode.ObjName = objName

		if name != "/" {
			parentEntry := dirEntries[dirPath]
			if parentEntry == nil {
				return nil, errors.New("Refusing to import tar archive. Directory entries must appear before children")
			}

			parentEntry.children[fileName] = &fileEntry
		}
	}

	// Fix up hardlinks by copying the object data from the object they link to.
	for _, hardlink := range hardlinks {
		linkName := filepath.Clean("/" + hardlink.linkname)
		linkFileName := filepath.Base(linkName)
		linkDirPath := filepath.Dir(linkName)

		var linkEntry *tarDirEntry
		linkDirEntry := dirEntries[linkDirPath]
		if linkDirEntry != nil {
			linkEntry = linkDirEntry.children[linkFileName]
		}
		if linkEntry == nil {
			return nil, errors.New("archive contains broken hardlink to " + linkName)
		}
		if !unix.S_ISREG(linkEntry.inode.Mode) {
			return nil, errors.New("archive contains hardlink to non regular file " + linkName)
		}

		hardlink.fileEntry.inode = linkEntry.inode
	}

	var paths []string
	for path := range dirEntries {
		paths = append(paths, path)
	}

	sort.Slice(paths, func(i, j int) bool {
		return paths[i] > paths[j]
	})

	for _, dirPath := range paths {
		dirBuilder := CreateDirBuilder()
		dirEntry := dirEntries[dirPath]

		for filePath, child := range dirEntry.children {
			dirBuilder.Insert(filePath, &child.inode, child.treeSize)
		}

		name, err := hs.CreateObject(dirBuilder.Build(), dirBuilder.DepNames...)
		if err != nil {
			return nil, err
		}

		dirEntry.inode.ObjName = name
		dirEntry.treeSize = dirBuilder.TotalTreeSize
	}

	return rootEntry.inode.ObjName, nil
}
