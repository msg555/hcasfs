package hcasfs

import (
	"fmt"
	"os"

	"github.com/go-errors/errors"

	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/unix"
)

func importLink(hs hcas.Session, fd int) (*hcas.Name, uint64, error) {
	buf := make([]byte, unix.PATH_MAX)
	bytesRead, err := unix.Readlinkat(fd, "", buf)
	if err != nil {
		return nil, 0, err
	}

	writer, err := hs.StreamObject()
	if err != nil {
		return nil, 0, err
	}

	bufRead := buf[:bytesRead]
	for total := 0; total < bytesRead; {
		bytesWritten, err := writer.Write(bufRead[total:])
		if err != nil {
			return nil, 0, err
		}
		total += bytesWritten
	}

	err = writer.Close()
	if err != nil {
		return nil, 0, err
	}

	return writer.Name(), uint64(bytesRead), nil
}

func importDirectory(hs hcas.Session, fd int) (*hcas.Name, uint64, error) {
	buf := make([]byte, 1<<16)
	dirBuilder := CreateDirBuilder()

	for {
		bytesRead, err := unix.Getdents(fd, buf)
		if err != nil {
			return nil, 0, err
		}
		if bytesRead == 0 {
			break
		}

		// TODO: Verify we don't have to handle dir entries that straddle reads
		for pos := 0; pos < bytesRead; {
			ino := unix.Hbo.Uint64(buf[pos:])
			// not needed
			// off := unix.Hbo.Uint64(buf[pos+8:])
			reclen := unix.Hbo.Uint16(buf[pos+16:])
			tp := uint8(buf[pos+18])
			fileName := nullTerminatedString(buf[pos+19 : pos+int(reclen)])
			pos += int(reclen)

			if ino == 0 || fileName == "." || fileName == ".." {
				continue // Skip fake/deleted files
			}
			if !validatePathName(fileName) {
				fmt.Fprintf(os.Stderr, "skipped file with invalid name '%s'\n", fileName)
				continue
			}

			flags := unix.O_PATH | unix.O_NOFOLLOW
			if tp == unix.DT_REG {
				flags = unix.O_RDONLY | unix.O_NOFOLLOW
			} else if tp == unix.DT_DIR {
				flags = unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_DIRECTORY
			}
			childFd, err := unix.Openat(fd, fileName, flags, 0)
			if err != nil {
				return nil, 0, err
			}

			var childSt unix.Stat_t
			err = unix.Fstat(childFd, &childSt)
			if err != nil {
				unix.Close(childFd)
				return nil, 0, err
			}

			if (childSt.Mode & unix.S_IFMT) != (uint32(tp) << 12) {
				unix.Close(childFd)
				return nil, 0, errors.New("Unexpected file type statting file")
			}

			var childObjName *hcas.Name
			var childSize uint64
			var childTreeSize uint64 = 1

			if tp == unix.DT_REG {
				childObjName, childSize, err = importRegular(hs, childFd)
			} else if tp == unix.DT_DIR {
				childObjName, childTreeSize, err = importDirectory(hs, childFd)
			} else if tp == unix.DT_LNK {
				childObjName, childSize, err = importLink(hs, childFd)
			}
			if err != nil {
				unix.Close(childFd)
				return nil, 0, err
			}
			err = unix.Close(childFd)
			if err != nil {
				return nil, 0, err
			}
			if (tp == unix.DT_REG || tp == unix.DT_LNK) && childSize != uint64(childSt.Size) {
				return nil, 0, errors.New("File size changed while reading data")
			}

			dirBuilder.Insert(fileName, InodeFromStat(childSt, childObjName), childTreeSize)
		}
	}

	name, err := hs.CreateObject(dirBuilder.Build(), dirBuilder.DepNames...)
	if err != nil {
		return nil, 0, err
	}
	return name, dirBuilder.TotalTreeSize, nil
}

func ImportPath(hs hcas.Session, path string) (*hcas.Name, error) {
	flags := unix.O_DIRECTORY | unix.O_RDONLY
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	var st unix.Stat_t
	err = unix.Fstat(fd, &st)
	if err != nil {
		return nil, err
	}

	if !unix.S_ISDIR(st.Mode) {
		return nil, errors.New("Only directories can be imported directly")
	}
	name, _, err := importDirectory(hs, fd)
	return name, err
}
