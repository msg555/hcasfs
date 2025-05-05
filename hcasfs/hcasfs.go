package hcasfs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"

	"github.com/go-errors/errors"

	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/unix"
)

type DirEntry struct {
	Mode    uint32
	Uid     uint32
	Gid     uint32
	Dev     uint64
	Atim    uint64
	Mtim    uint64
	Ctim    uint64
	Size    uint64
	ObjName [32]byte

	FileName         string
	ChildDeps        uint64
	FileNameChecksum uint32
}

func (d *DirEntry) Encode() []byte {
	bufLen := 88 + len(d.FileName)
	bufLen = (bufLen + 7) & ^7
	buf := make([]byte, bufLen)
	binary.BigEndian.PutUint32(buf[0:], d.Mode)
	binary.BigEndian.PutUint32(buf[4:], d.Uid)
	binary.BigEndian.PutUint32(buf[8:], d.Gid)
	binary.BigEndian.PutUint32(buf[12:], uint32(len(d.FileName)))
	binary.BigEndian.PutUint64(buf[16:], d.Dev)
	binary.BigEndian.PutUint64(buf[24:], d.Atim)
	binary.BigEndian.PutUint64(buf[32:], d.Mtim)
	binary.BigEndian.PutUint64(buf[40:], d.Ctim)
	binary.BigEndian.PutUint64(buf[48:], d.Size)
	copy(buf[56:], d.ObjName[:])
	copy(buf[88:], d.FileName)
	return buf
}

func validatePathName(name string) bool {
	if len(name) > unix.NAME_MAX {
		return false
	}
	if name == "" {
		return false
	}
	for _, ch := range name {
		if ch == 0 || ch == '/' {
			return false
		}
	}
	return true
}

func nullTerminatedString(data []byte) string {
	for i, ch := range data {
		if ch == 0 {
			return string(data[:i])
		}
	}
	return string(data)
}

func importRegular(hs hcas.Session, fd int) ([]byte, uint64, error) {
	buf := make([]byte, 1<<16)

	writer, err := hs.StreamObject()
	if err != nil {
		return nil, 0, err
	}

	var totalBytesRead uint64
	for {
		bytesRead, err := unix.Read(fd, buf[:])
		if err != nil {
			return nil, 0, err
		}
		if bytesRead == 0 {
			break
		}
		totalBytesRead += uint64(bytesRead)

		bufRead := buf[:bytesRead]
		for total := 0; total < bytesRead; {
			bytesWritten, err := writer.Write(bufRead[total:])
			if err != nil {
				return nil, 0, err
			}
			total += bytesWritten
		}
	}

	err = writer.Close()
	if err != nil {
		return nil, 0, err
	}

	return writer.Name(), totalBytesRead, nil
}

func importLink(hs hcas.Session, fd int) ([]byte, uint64, error) {
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

func importDirectory(hs hcas.Session, fd int) ([]byte, uint64, error) {
	buf := make([]byte, 1<<16)
	var totalChildDeps uint64
	dirEntries := make([]DirEntry, 0, 16)
	var directDeps [][]byte
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

			fmt.Printf("Got file %s\n", fileName)

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

			var childObjName []byte
			var childSize uint64
			var childDeps uint64

			if tp == unix.DT_REG {
				childObjName, childSize, err = importRegular(hs, childFd)
			} else if tp == unix.DT_DIR {
				childObjName, childDeps, err = importDirectory(hs, childFd)
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
				return nil, 0, errors.New("Unexpected file size")
			}

			dirEntry := DirEntry{
				Mode:      childSt.Mode,
				Uid:       childSt.Uid,
				Gid:       childSt.Gid,
				Dev:       0,
				Atim:      uint64(childSt.Atim.Nano()),
				Mtim:      uint64(childSt.Mtim.Nano()),
				Ctim:      uint64(childSt.Ctim.Nano()),
				Size:      uint64(childSt.Size),
				FileName:  fileName,
				ChildDeps: childDeps,
			}
			copy(dirEntry.ObjName[:], childObjName)
			totalChildDeps += childDeps
			if childObjName != nil {
				directDeps = append(directDeps, childObjName)
			}

			if tp == unix.DT_CHR || tp == unix.DT_BLK {
				dirEntry.Dev = childSt.Rdev
			}
			dirEntries = append(dirEntries, dirEntry)
		}
	}

	for i := range dirEntries {
		dirEntries[i].FileNameChecksum = crc32.ChecksumIEEE([]byte(dirEntries[i].FileName))
	}
	sort.Slice(dirEntries, func(i, j int) bool {
		return dirEntries[i].FileNameChecksum < dirEntries[j].FileNameChecksum
	})

	headerSize := 16 + len(dirEntries)*8
	dataOut := make([]byte, headerSize, headerSize+len(dirEntries)*88)

	recordPositions := make([]uint32, 0, len(dirEntries))
	for i := range dirEntries {
		recordPositions = append(recordPositions, uint32(len(dataOut)))
		dataOut = append(dataOut, dirEntries[i].Encode()...)
	}

	var flags uint32 = 0
	binary.BigEndian.PutUint32(dataOut[0:], flags)
	binary.BigEndian.PutUint32(dataOut[4:], uint32(len(dirEntries)))
	binary.BigEndian.PutUint64(dataOut[8:], totalChildDeps)
	for ind := range dirEntries {
		binary.BigEndian.PutUint32(dataOut[16+8*ind:], recordPositions[ind])
		binary.BigEndian.PutUint32(dataOut[20+8*ind:], dirEntries[ind].FileNameChecksum)
	}

	name, err := hs.CreateObject(dataOut, directDeps...)
	if err != nil {
		return nil, 0, err
	}
	return name, totalChildDeps, nil
}

func ImportPath(hs hcas.Session, path string) ([]byte, error) {
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
