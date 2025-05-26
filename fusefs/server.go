package fusefs

import (
	"encoding/binary"
	"path/filepath"
	"io"
	"log"
	"fmt"
	"time"
	"os"
	"sync"

  "github.com/go-errors/errors"
	"bazil.org/fuse"

	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/hcasfs"
	"github.com/msg555/hcas/unix"
)

const DURATION_DEFAULT time.Duration = time.Duration(1000000000 * 60 * 60)

func nsTimestampToTime(nsTimestamp uint64) time.Time {
  return time.Unix(int64(nsTimestamp/1000000000), int64(nsTimestamp%1000000000))
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

type FileHandle interface {
	Read(*fuse.ReadRequest) error
	Release(*fuse.ReleaseRequest) error
}

type FileHandleDir struct {
	nodeFile *os.File
	inodeId uint64
	dirEntryCount uint32
	currentSeek uint32
}

type InodeReference struct {
	Inode hcasfs.InodeData
	RefCount int64
}

type HcasMount struct {
	conn *fuse.Conn
	mountPoint string
	hcasDataDir string
	rootInode hcasfs.InodeData

  inodeLock   sync.RWMutex
	inodeMap   map[fuse.NodeID]*InodeReference

  handleLock   sync.RWMutex
  handleMap    map[fuse.HandleID]FileHandle
  lastHandleID fuse.HandleID
}

func CreateServer(
	mountPoint string,
	hcasRootDir string,
	rootName []byte,
	options ...fuse.MountOption,
) (*HcasMount, error) {
	options = append(options, fuse.Subtype("hcasfs"), fuse.DefaultPermissions())
	options = append(options, fuse.Subtype("hcasfs"), fuse.ReadOnly())
	options = append(options, fuse.Subtype("hcasfs"), fuse.CacheSymlinks())
	options = append(options, fuse.Subtype("hcasfs"), fuse.Subtype("hcasfs"))

	// Want to enable kernel_cache but there's no option defined in fuse package

	// Not sure exactly what this is but sounds relevant
	// options = append(options, fuse.Subtype("hcasfs"), fuse.ExplicitInvalidateData())

	conn, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		return nil, err
	}

	hcasMount := &HcasMount{
		conn: conn,
		mountPoint: mountPoint,
		hcasDataDir: filepath.Join(hcasRootDir, hcas.DataPath),
		inodeMap: make(map[fuse.NodeID]*InodeReference),
		handleMap: make(map[fuse.HandleID]FileHandle),
	}
	rootNode := InodeReference{
		Inode: hcasfs.InodeData{
			Mode: unix.S_IFDIR | 0o777,
		},
		RefCount: 1,
	}
	copy(rootNode.Inode.ObjName[:], rootName)
	hcasMount.inodeMap[1] = &rootNode

	go func() {
		err := hcasMount.serve()
    if err == io.EOF {
      log.Printf("Connection unmounted at '%s'", mountPoint)
    } else {
      log.Printf("Connection '%s' shutting down do to '%s'", mountPoint, err)
    }
	}()

	return hcasMount, nil
}

func (hm *HcasMount) Close() error {
	return fuse.Unmount(hm.mountPoint)
}

func (hm *HcasMount) serve() error {
  for {
    req, err := hm.conn.ReadRequest()
    if err != nil {
      return err
    }
    go hm.handleRequest(req)
  }
}

func (hm *HcasMount) handleRequest(req fuse.Request) {
  var err error

  // fmt.Println("REQUEST:", req)
  switch req.(type) {
  case *fuse.StatfsRequest:
    err = hm.handleStatfsRequest(req.(*fuse.StatfsRequest))

  // Node methods
  case *fuse.ForgetRequest:
    err = hm.handleForgetRequest(req.(*fuse.ForgetRequest))
  case *fuse.BatchForgetRequest:
    err = hm.handleBatchForgetRequest(req.(*fuse.BatchForgetRequest))
  case *fuse.AccessRequest:
    err = hm.handleAccessRequest(req.(*fuse.AccessRequest))
  case *fuse.LookupRequest:
    err = hm.handleLookupRequest(req.(*fuse.LookupRequest))
  case *fuse.OpenRequest:
    err = hm.handleOpenRequest(req.(*fuse.OpenRequest))
  case *fuse.GetattrRequest:
    err = hm.handleGetattrRequest(req.(*fuse.GetattrRequest))
  case *fuse.ReadlinkRequest:
    err = hm.handleReadlinkRequest(req.(*fuse.ReadlinkRequest))
/*
  case *fuse.ListxattrRequest:
    err = hm.handleListxattrRequest(req.(*fuse.ListxattrRequest))
  case *fuse.GetxattrRequest:
    err = hm.handleGetxattrRequest(req.(*fuse.GetxattrRequest))
*/
    /*
       case *fuse.SetattrRequest:
         nd.handleSetattrRequest(req.(*fuse.SetattrRequest))
       case *fuse.CreateRequest:
         nd.handleCreateRequest(req.(*fuse.CreateRequest))
       case *fuse.RemoveRequest:
         nd.handleRemoveRequest(req.(*fuse.RemoveRequest))
    */
  // Handle methods
  case *fuse.ReleaseRequest:
    err = hm.handleReleaseRequest(req.(*fuse.ReleaseRequest))
  case *fuse.ReadRequest:
    err = hm.handleReadRequest(req.(*fuse.ReadRequest))
/*
  case *fuse.WriteRequest:
    err = hm.handleWriteRequest(req.(*fuse.WriteRequest))
  case *fuse.FlushRequest:
    err = hm.handleFlushRequest(req.(*fuse.FlushRequest))
*/
    /*
       case *fuse.WriteRequest:
         nd.handleWriteRequest(req.(*fuse.WriteRequest))
       case *fuse.IoctlRequest:
         nd.handleIoctlRequest(req.(*fuse.IoctlRequest))
    */
  // Not implemented/rely on default kernel level behavior. These failures are
  // cached by the fuse-driver and future calls will be automatically skipped.
  case *fuse.PollRequest:
    err = FuseError{
      source: errors.New("not implemented"),
      errno:  unix.ENOSYS,
    }

/*
  case *fuse.DestroyRequest:
    fmt.Println("TODO: Got destroy request")
*/

  default:
    fmt.Println("WARNING NOT IMPLEMENTED:", req)
    err = errors.New("not implemented")
  }

  if err != nil {
    req.RespondError(WrapIOError(err))
  }
}

func (hm *HcasMount) handleStatfsRequest(req *fuse.StatfsRequest) error {
  var stfs unix.Statfs_t
	err := unix.Statfs(hm.hcasDataDir, &stfs)
  if err != nil {
    return err
  }

  req.Respond(&fuse.StatfsResponse{
    Blocks:  stfs.Blocks,
    Bfree:   stfs.Bfree,
    Bavail:  stfs.Bavail,
    Files:   stfs.Files,
    Ffree:   stfs.Ffree,
    Bsize:   uint32(stfs.Bsize),
    Namelen: uint32(stfs.Namelen),
    Frsize:  uint32(stfs.Frsize),
  })
  return nil
}

func (hm *HcasMount) openFileByName(name []byte) (*os.File, error) {
  nameHex := hcas.NameHex(name)
  return os.Open(filepath.Join(
		hm.hcasDataDir,
    nameHex[:2],
    nameHex[2:],
  ))
}

func (hm *HcasMount) getInode(inode fuse.NodeID) (*hcasfs.InodeData, error) {
	hm.inodeLock.RLock()
	defer hm.inodeLock.RUnlock()

	nod, ok := hm.inodeMap[inode]
	if !ok {
		return nil, errors.New("Unknown inode")
	}

	return &nod.Inode, nil
}

func (hm *HcasMount) trackInode(inodeId fuse.NodeID, inodeData *hcasfs.InodeData) {
	hm.inodeLock.Lock()
	defer hm.inodeLock.Unlock()

	nod, ok := hm.inodeMap[inodeId]
	if ok {
		nod.RefCount += 1
	} else {
		hm.inodeMap[inodeId] = &InodeReference{
			Inode: *inodeData,
			RefCount: 1,
		}
	}
}

func (hm *HcasMount) handleAccessRequest(req *fuse.AccessRequest) error {
	inode, err := hm.getInode(req.Node)
	if err != nil {
		return err
	}

  if !unix.TestAccess(req.Uid == inode.Uid, req.Gid == inode.Gid, inode.Mode, req.Mask) {
    return FuseError{
      source: errors.New("permission denied"),
      errno:  unix.EACCES,
    }
  }

  req.Respond()
  return nil
}

func inodeAttr(inodeId fuse.NodeID, inode *hcasfs.InodeData) fuse.Attr {
  size := inode.Size
  if unix.S_ISDIR(inode.Mode) {
    size = 1024
  }
	return fuse.Attr{
    Valid:     DURATION_DEFAULT, // Check this out
    Inode:     uint64(inodeId),
    Size:      size,
    Blocks:    (size + 511) >> 9, // This looks wrong? Was there a reason this is not 1024 alignted?
    Atime:     nsTimestampToTime(inode.Atim),
    Mtime:     nsTimestampToTime(inode.Mtim),
    Ctime:     nsTimestampToTime(inode.Ctim),
    Mode:      unix.UnixToFileStatMode(inode.Mode),
    Nlink:     1,
    Uid:       inode.Uid,
    Gid:       inode.Gid,
    Rdev:      uint32(inode.Dev),
    BlockSize: 1024,
  }
}

func (hm *HcasMount) handleForgetRequest(req *fuse.ForgetRequest) error {
	hm.inodeLock.Lock()
	nod, ok := hm.inodeMap[req.Node]
	if !ok {
		fmt.Printf("Batch forget on unknown inode\n")
		return nil
	}
	nod.RefCount -= int64(req.N)
	if nod.RefCount < 0 {
		fmt.Printf("Negative inode ref count\n")
	}
	if nod.RefCount <= 0 {
		delete(hm.inodeMap, req.Node)
	}
	hm.inodeLock.Unlock()

	req.Respond()
	return nil
}

func (hm *HcasMount) handleBatchForgetRequest(req *fuse.BatchForgetRequest) error {
	hm.inodeLock.Lock()
	for _, forget := range req.Forget {
		nod, ok := hm.inodeMap[forget.NodeID]
		if ! ok {
			fmt.Printf("Batch forget on unknown inode\n")
			continue
		}
		nod.RefCount -= int64(forget.N)
		if nod.RefCount < 0 {
			fmt.Printf("Negative inode ref count\n")
		}
		if nod.RefCount <= 0 {
			delete(hm.inodeMap, forget.NodeID)
		}
	}
	hm.inodeLock.Unlock()

	req.Respond()
	return nil
}

func (hm *HcasMount) handleLookupRequest(req *fuse.LookupRequest) error {
	inode, err := hm.getInode(req.Node)
	if err != nil {
		return err
	}

	nodeFile, err := hm.openFileByName(inode.ObjName[:])
	if err != nil {
		return err
	}

	dirEntry, err := hcasfs.LookupChild(nodeFile, req.Name)
	if err != nil {
		return err
	}

  if dirEntry == nil {
    return FuseError{
      source: errors.New("file not found"),
      errno:  unix.ENOENT,
    }
  }

	inodeId := fuse.NodeID(uint64(req.Node) + dirEntry.ParentDepIndex)
	fmt.Printf("Looking up %s %d %d\n", req.Name, inodeId, dirEntry.ParentDepIndex)
  req.Respond(&fuse.LookupResponse{
    Node:       inodeId,
    Generation: 1, // What is this?
    EntryValid: DURATION_DEFAULT, // Check this out, too
    Attr:       inodeAttr(inodeId, &dirEntry.Inode),
  })
	hm.trackInode(inodeId, &dirEntry.Inode)

  return nil
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
  // case unix.S_IFREG:
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

	_, err = f.Seek(int64(16 + 8 * dirEntries), 0)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &FileHandleDir{
		nodeFile: f,
		inodeId: inodeId,
		dirEntryCount: dirEntries,
		currentSeek: 0,
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

  // Someone seek'ed our handle.
  if uint64(req.Offset) != uint64(h.currentSeek) {
		_, err := h.nodeFile.Seek(16 + 8 * req.Offset, 0)
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
			uint64(h.currentSeek + 1),
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
	fmt.Printf("got readlink 0 %d!\n", uint64(req.Node))
  inode, err := hm.getInode(req.Node)
  if err != nil {
    return err
  }
	fmt.Printf("got readlink 1!\n")

	f, err := hm.openFileByName(inode.ObjName[:])
	if err != nil {
		return err
	}
	fmt.Printf("got readlink 2!\n")

	buf := make([]byte, unix.PATH_MAX + 1)
	bytesRead := 0
	for bytesRead < len(buf) {
		amt, err := f.Read(buf[bytesRead:])
		bytesRead += amt
		if err == io.EOF {
			break;
		}
		if err != nil {
			return err
		}
	}
	fmt.Printf("readlink %s\n", string(buf[:bytesRead]))

	req.Respond(string(buf[:bytesRead]))
	return nil
}
