package hcasfs

import (
	"encoding/binary"
	"hash/crc32"
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
	ObjName *hcas.Name
}

func InodeFromStat(st unix.Stat_t, objName *hcas.Name) *InodeData {
	inode := &InodeData{
		Mode:    st.Mode,
		Uid:     st.Uid,
		Gid:     st.Gid,
		Dev:     0,
		Atim:    uint64(st.Atim.Nano()),
		Mtim:    uint64(st.Mtim.Nano()),
		Ctim:    uint64(st.Ctim.Nano()),
		Size:    uint64(st.Size),
		ObjName: objName,
	}
	if unix.S_ISCHR(st.Mode) || unix.S_ISBLK(st.Mode) {
		inode.Dev = st.Rdev
	}
	return inode
}

type DirEntry struct {
	Inode            InodeData
	FileName         string
	TreeSize         uint64
	FileNameChecksum uint32
	ParentDepIndex   uint64
}

type dirBuilder struct {
	DirEntries    []DirEntry
	DepNames      []hcas.Name
	TotalTreeSize uint64
}

func CreateDirBuilder() *dirBuilder {
	return &dirBuilder{
		DirEntries:    make([]DirEntry, 0, 16),
		DepNames:      make([]hcas.Name, 0, 16),
		TotalTreeSize: 1,
	}
}

// Insert a child object into this directory. treeSize should be the total tree
// size of the child including itself (and should be exactly 1 if the child is
// not a directory).
func (d *dirBuilder) Insert(fileName string, inode *InodeData, treeSize uint64) {
	if treeSize == 0 {
		panic("treeSize cannot be 0")
	}
	if treeSize > 1 && !unix.S_ISDIR(inode.Mode) {
		panic("treeSize must be 1 for non-directories")
	}

	dirEntry := DirEntry{
		Inode:            *inode,
		FileName:         fileName,
		TreeSize:         treeSize,
		FileNameChecksum: crc32.ChecksumIEEE([]byte(fileName)),
	}
	d.DirEntries = append(d.DirEntries, dirEntry)

	if fileModeHasObjectData(inode.Mode) != (inode.ObjName != nil) {
		panic("object data state unexpected for file type")
	}
	if inode.ObjName != nil {
		d.DepNames = append(d.DepNames, *inode.ObjName)
	}
	d.TotalTreeSize += treeSize
}

func (d *dirBuilder) Build() []byte {
	sort.Slice(d.DirEntries, func(i, j int) bool {
		return d.DirEntries[i].FileNameChecksum < d.DirEntries[j].FileNameChecksum
	})

	var parentOffset uint64 = 1
	for i := range d.DirEntries {
		d.DirEntries[i].ParentDepIndex = parentOffset
		parentOffset += d.DirEntries[i].TreeSize
	}

	headerSize := 16 + len(d.DirEntries)*8
	dataOut := make([]byte, headerSize)

	recordPositions := make([]uint32, 0, len(d.DirEntries))
	for i := range d.DirEntries {
		recordPositions = append(recordPositions, uint32(len(dataOut)))
		dataOut = append(dataOut, d.DirEntries[i].Encode()...)
	}

	var flags uint32 = 0
	binary.BigEndian.PutUint32(dataOut[0:], flags)
	binary.BigEndian.PutUint32(dataOut[4:], uint32(len(d.DirEntries)))
	binary.BigEndian.PutUint64(dataOut[8:], d.TotalTreeSize)
	for ind := range d.DirEntries {
		binary.BigEndian.PutUint32(dataOut[16+8*ind:], recordPositions[ind])
		binary.BigEndian.PutUint32(dataOut[20+8*ind:], d.DirEntries[ind].FileNameChecksum)
	}

	return dataOut
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

func fileModeHasObjectData(mode uint32) bool {
	return unix.S_ISREG(mode) || unix.S_ISDIR(mode) || unix.S_ISLNK(mode)
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
	if d.Inode.ObjName != nil {
		// TODO: Don't need to reserve space for modes that don't have objects
		// associated.
		copy(buf[52:], d.Inode.ObjName.Name())
	}
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
	if fileModeHasObjectData(d.Inode.Mode) {
		objName := hcas.NewName(string(buf[52:84]))
		d.Inode.ObjName = &objName
	}
	d.ParentDepIndex = binary.BigEndian.Uint64(buf[84:])
	fileNameLen := binary.BigEndian.Uint32(buf[92:])

	recordLen := len(buf) + int(fileNameLen)
	recordLen = (recordLen + 7) & ^7
	fileName := make([]byte, recordLen-len(buf))
	err = readAll(stream, fileName)
	if err != nil {
		return err
	}
	d.FileName = string(fileName[:fileNameLen])
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

func importRegular(hs hcas.Session, fd int) (*hcas.Name, uint64, error) {
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

		ind = lo + uint32(1.0*(crc-loCrc)/(hiCrc-loCrc)*(hi-lo))
		if ind == hi {
			ind -= 1
		}

		_, err = dirData.Seek(int64(headerOffset+8*ind), 0)
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
			_, err := dirData.Seek(int64(headerOffset+8*index), 0)
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
		match, err = crcMatch(testInd, testInd == ind+1)
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
