package hcasfs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"io"
	"sort"

	"github.com/go-errors/errors"

	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/unix"
)

type InodeData struct {
	Mode    uint32
	Uid     uint32
	Gid     uint32
	Dev     uint64
	Atim    uint64
	Mtim    uint64
	Ctim    uint64
	Size    uint64
	ObjName [32]byte
}

type DirEntry struct {
	Inode						 InodeData
	FileName         string
	ChildDeps        uint64
	FileNameChecksum uint32
	ParentDepIndex uint64
}

func readAll(stream io.Reader, buf []byte) error {
	for len(buf) > 0 {
		amt, err := stream.Read(buf)
		if err != nil {
			return err
		}
		buf = buf[amt:]
	}
	return nil
}

func (d *DirEntry) Encode() []byte {
	bufLen := 96 + len(d.FileName)
	bufLen = (bufLen + 7) & ^7
	buf := make([]byte, bufLen)
	binary.BigEndian.PutUint32(buf[0:], d.Inode.Mode)
	binary.BigEndian.PutUint32(buf[4:], d.Inode.Uid)
	binary.BigEndian.PutUint32(buf[8:], d.Inode.Gid)
	binary.BigEndian.PutUint64(buf[12:], d.Inode.Dev)
	binary.BigEndian.PutUint64(buf[20:], d.Inode.Atim)
	binary.BigEndian.PutUint64(buf[28:], d.Inode.Mtim)
	binary.BigEndian.PutUint64(buf[36:], d.Inode.Ctim)
	binary.BigEndian.PutUint64(buf[44:], d.Inode.Size)
	copy(buf[52:], d.Inode.ObjName[:])
	binary.BigEndian.PutUint64(buf[84:], d.ParentDepIndex)
	binary.BigEndian.PutUint32(buf[92:], uint32(len(d.FileName)))
	copy(buf[96:], d.FileName)
	return buf
}

func (d *DirEntry) DecodeStream(stream io.Reader) error {
	var buf [96]byte
	err := readAll(stream, buf[:])
	if err != nil {
		return err
	}

	d.Inode.Mode = binary.BigEndian.Uint32(buf[0:])
	d.Inode.Uid = binary.BigEndian.Uint32(buf[4:])
	d.Inode.Gid = binary.BigEndian.Uint32(buf[8:])
	d.Inode.Dev = binary.BigEndian.Uint64(buf[12:])
	d.Inode.Atim = binary.BigEndian.Uint64(buf[20:])
	d.Inode.Mtim = binary.BigEndian.Uint64(buf[28:])
	d.Inode.Ctim = binary.BigEndian.Uint64(buf[36:])
	d.Inode.Size = binary.BigEndian.Uint64(buf[44:])
	copy(d.Inode.ObjName[:], buf[52:])
	d.ParentDepIndex = binary.BigEndian.Uint64(buf[84:])
	fileNameLen := binary.BigEndian.Uint32(buf[92:])

	fileName := make([]byte, fileNameLen)
	err = readAll(stream, fileName)
	if err != nil {
		return err
	}
	d.FileName = string(fileName)
	return nil
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
				Inode: InodeData {
					Mode:      childSt.Mode,
					Uid:       childSt.Uid,
					Gid:       childSt.Gid,
					Dev:       0,
					Atim:      uint64(childSt.Atim.Nano()),
					Mtim:      uint64(childSt.Mtim.Nano()),
					Ctim:      uint64(childSt.Ctim.Nano()),
					Size:      uint64(childSt.Size),
				},
				FileName:  fileName,
				ChildDeps: childDeps,
			}
			copy(dirEntry.Inode.ObjName[:], childObjName)
			totalChildDeps += childDeps + 1
			if childObjName != nil {
				directDeps = append(directDeps, childObjName)
			}

			if tp == unix.DT_CHR || tp == unix.DT_BLK {
				dirEntry.Inode.Dev = childSt.Rdev
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

	var parentOffset uint64 = 1
	for i := range dirEntries {
		dirEntries[i].ParentDepIndex = parentOffset
		parentOffset += dirEntries[i].ChildDeps + 1
	}

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

func LookupChild(dirData io.ReadSeeker, name string) (dirEntry *DirEntry, err error) {
	/* Return the DirEntry associated with the given name if it exists. If no
   * entry with matching name is found the returned *DirEntry will be nil as
   * well as the error.
   *
	 * A lot of potential optimization to make here. Caching, better collision
	 * behavior, better hashing (e.g. use dynamic salt), directory could indicate
	 * if there _are_ collisions, etc...
   */

	var header [16]byte
	err = readAll(dirData, header[:])
	if err != nil {
		return
	}

	flags := binary.BigEndian.Uint32(header[0:])
	if flags != 0 {
		err = errors.New("unexpected flags")
		return
	}
	childCount := binary.BigEndian.Uint32(header[4:])
	// totalChildDeps := binary.BigEndian.Uint64(header[8:])

	var headerOffset uint32 = 16

	crc := crc32.ChecksumIEEE([]byte(name))

	var lo uint32 = 0
	var hi uint32 = childCount
	var loCrc uint32 = 0x00000000
	var hiCrc uint32 = 0xFFFFFFFF
	var ind uint32
	var recordPosition uint32

	for {
		if lo == hi {
			return
		}

		ind = lo + uint32(1.0 * (crc - loCrc) / (hiCrc - loCrc) * (hi - lo))
		if ind == hi {
			ind -= 1
		}

		_, err = dirData.Seek(int64(headerOffset + 8 *	ind), 0)
		if err != nil {
			return
		}

		var crcEntry [8]byte
		err = readAll(dirData, crcEntry[:])
		if err != nil {
			return
		}

		recordChecksum := binary.BigEndian.Uint32(crcEntry[4:])
		if recordChecksum < crc {
			lo = ind + 1
			loCrc = recordChecksum
		} else if recordChecksum > crc {
			hi = ind
			hiCrc = recordChecksum
		} else {
			recordPosition = binary.BigEndian.Uint32(crcEntry[0:])
			break
		}
	}

	extractDirEntry := func() (*DirEntry, error) {
		_, err := dirData.Seek(int64(recordPosition), 0)
		if err != nil {
			return nil, err
		}

		var de DirEntry
		err = de.DecodeStream(dirData)
		if err != nil {
			return nil, err
		}

		if de.FileName == name {
			return &de, nil
		}
		return nil, nil
	}

	crcMatch := func(index uint32, needSeek bool) (bool, error) {
		if needSeek {
			_, err := dirData.Seek(int64(headerOffset + 8 * index), 0)
			if err != nil {
				return false, err
			}
		}

		var crcEntry [8]byte
		err := readAll(dirData, crcEntry[:])
		if err != nil {
			return false, err
		}

		recordChecksum := binary.BigEndian.Uint32(crcEntry[4:])
		recordPosition = binary.BigEndian.Uint32(crcEntry[0:])
		return recordChecksum != crc, nil
	}

	dirEntry, err = extractDirEntry()
	if dirEntry != nil || err != nil {
		return
	}

	for testInd := ind + 1; testInd < hi; testInd++ {
		var match bool
		match, err = crcMatch(testInd, testInd == ind + 1)
		if err != nil {
			return
		} else if !match {
			break
		}
		dirEntry, err = extractDirEntry()
		if dirEntry != nil || err != nil {
			return
		}
	}
	for testInd := ind - 1; testInd >= lo; testInd-- {
		var match bool
		match, err = crcMatch(testInd, true)
		if err != nil {
			return
		} else if !match {
			break
		}
		dirEntry, err = extractDirEntry()
		if dirEntry != nil || err != nil {
			return
		}
	}

	return
}
