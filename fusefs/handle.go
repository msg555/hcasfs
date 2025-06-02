package fusefs

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

	"github.com/msg555/hcas/hcasfs"
	"github.com/msg555/hcas/unix"
)

type FileHandle interface {
	Read(*fuse.ReadRequest) error
	Release(*fuse.ReleaseRequest) error
}

type FileHandleDir struct {
	nodeFile      *os.File
	inodeId       uint64
	dirEntryCount uint32
	currentSeek   uint32
}

type FileHandleReg struct {
	nodeFile *os.File
	inodeId  uint64
}

func (hm *HcasMount) openHandle(handle FileHandle) fuse.HandleID {
	hm.handleLock.Lock()
	hm.lastHandleID++
	handleID := hm.lastHandleID
	hm.handleMap[handleID] = handle
	hm.handleLock.Unlock()
	return handleID
}

func (hm *HcasMount) handleOpenRequest(req *fuse.OpenRequest) error {
	inode, err := hm.getInode(req.Node)
	if err != nil {
		return err
	}

	var handleID fuse.HandleID
	switch inode.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		handle, err := hm.CreateFileHandleDir(uint64(req.Node), inode.ObjName[:])
		if err != nil {
			return err
		}

		handleID = hm.openHandle(handle)
	case unix.S_IFREG:
		handle, err := hm.CreateFileHandleReg(uint64(req.Node), inode.ObjName[:])
		if err != nil {
			return err
		}

		handleID = hm.openHandle(handle)
	default:
		return errors.New("not implemented")
	}

	req.Respond(&fuse.OpenResponse{
		Handle: handleID,
		Flags:  fuse.OpenKeepCache, // What does this mean?
	})
	return nil
}

func (hm *HcasMount) CreateFileHandleDir(inodeId uint64, objName []byte) (*FileHandleDir, error) {
	f, err := hm.openFileByName(objName)
	if err != nil {
		return nil, err
	}

	var header [16]byte
	err = readAll(f, header[:])
	if err != nil {
		f.Close()
		return nil, err
	}

	flags := binary.BigEndian.Uint32(header[0:])
	if flags != 0 {
		return nil, errors.New("Unknown directory format")
	}
	dirEntries := binary.BigEndian.Uint32(header[4:])

	_, err = f.Seek(int64(16+8*dirEntries), 0)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &FileHandleDir{
		nodeFile:      f,
		inodeId:       inodeId,
		dirEntryCount: dirEntries,
		currentSeek:   0,
	}, nil
}

func (fhd *FileHandleDir) Release(req *fuse.ReleaseRequest) error {
	return fhd.nodeFile.Close()
}

func (h *FileHandleDir) Read(req *fuse.ReadRequest) error {
	if !req.Dir {
		return unix.EISDIR
	}

	if uint64(req.Offset) >= uint64(h.dirEntryCount) {
		req.Respond(&fuse.ReadResponse{})
		return nil
	}

	fmt.Printf("Read seek %d %d\n", req.Offset, h.currentSeek)

	// Someone seek'ed our handle.
	if uint64(req.Offset) != uint64(h.currentSeek) {
		_, err := h.nodeFile.Seek(16+8*req.Offset, 0)
		if err != nil {
			return err
		}

		var offsetBuf [4]byte
		err = readAll(h.nodeFile, offsetBuf[:])
		if err != nil {
			return err
		}

		_, err = h.nodeFile.Seek(int64(binary.BigEndian.Uint32(offsetBuf[:])), 0)
		if err != nil {
			return err
		}

		h.currentSeek = uint32(req.Offset)
	}

	bufOffset := 0
	buf := make([]byte, req.Size)
	for h.currentSeek < h.dirEntryCount {
		var dirEntry hcasfs.DirEntry
		dirEntry.DecodeStream(h.nodeFile)

		inodeId := h.inodeId + dirEntry.ParentDepIndex
		size := addDirEntry(
			buf[bufOffset:],
			dirEntry.FileName,
			inodeId,
			uint64(h.currentSeek+1),
			dirEntry.Inode.Mode,
		)
		if size == 0 {
			break
		}
		h.currentSeek++
		bufOffset += size
	}

	req.Respond(&fuse.ReadResponse{
		Data: buf[:bufOffset],
	})
	return nil
}

func (hm *HcasMount) CreateFileHandleReg(inodeId uint64, objName []byte) (*FileHandleReg, error) {
	f, err := hm.openFileByName(objName)
	if err != nil {
		return nil, err
	}

	return &FileHandleReg{
		nodeFile: f,
		inodeId:  inodeId,
	}, nil
}

func (fhr *FileHandleReg) Release(req *fuse.ReleaseRequest) error {
	return fhr.nodeFile.Close()
}

func (fhr *FileHandleReg) Read(req *fuse.ReadRequest) error {
	buf := make([]byte, req.Size)
	bytesRead := 0
	fmt.Printf("Got read %d %d\n", req.Offset, req.Size)
	for bytesRead < req.Size {
		amt, err := fhr.nodeFile.ReadAt(buf[bytesRead:], req.Offset+int64(bytesRead))
		bytesRead += amt
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	req.Respond(&fuse.ReadResponse{Data: buf[:bytesRead]})
	return nil
}

func (hm *HcasMount) handleReleaseRequest(req *fuse.ReleaseRequest) error {
	hm.handleLock.Lock()
	handle, ok := hm.handleMap[req.Handle]
	delete(hm.handleMap, req.Handle)
	hm.handleLock.Unlock()

	if !ok {
		return FuseError{
			source: errors.New("invalid file handle"),
			errno:  unix.EBADF,
		}
	}
	return handle.Release(req)
}

func (hm *HcasMount) handleFlushRequest(req *fuse.FlushRequest) error {
	/* Nothing to do */
	req.Respond()
	return nil
}

func (hm *HcasMount) handleGetattrRequest(req *fuse.GetattrRequest) error {
	inode, err := hm.getInode(req.Node)
	if err != nil {
		return err
	}

	req.Respond(&fuse.GetattrResponse{
		Attr: inodeAttr(req.Node, inode),
	})
	return nil
}

func (hm *HcasMount) handleReadRequest(req *fuse.ReadRequest) error {
	hm.handleLock.RLock()
	handle, ok := hm.handleMap[req.Handle]
	hm.handleLock.RUnlock()

	if !ok {
		return FuseError{
			source: errors.New("invalid file handle"),
			errno:  unix.EBADF,
		}
	}
	return handle.Read(req)
}

func (hm *HcasMount) handleReadlinkRequest(req *fuse.ReadlinkRequest) error {
	inode, err := hm.getInode(req.Node)
	if err != nil {
		return err
	}

	f, err := hm.openFileByName(inode.ObjName[:])
	if err != nil {
		return err
	}

	buf := make([]byte, unix.PATH_MAX+1)
	bytesRead := 0
	for bytesRead < len(buf) {
		amt, err := f.Read(buf[bytesRead:])
		bytesRead += amt
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	req.Respond(string(buf[:bytesRead]))
	return nil
}
