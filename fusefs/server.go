package fusefs

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

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

type HcasMount struct {
	conn        *fuse.Conn
	mountPoint  string
	hcasDataDir string
	rootInode   hcasfs.InodeData

	inodeLock sync.RWMutex
	inodeMap  map[fuse.NodeID]*InodeReference

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
		conn:        conn,
		mountPoint:  mountPoint,
		hcasDataDir: filepath.Join(hcasRootDir, hcas.DataPath),
		inodeMap:    make(map[fuse.NodeID]*InodeReference),
		handleMap:   make(map[fuse.HandleID]FileHandle),
	}
	rootNode := InodeReference{
		Inode: hcasfs.InodeData{
			Mode: unix.S_IFDIR | 0o777,
		},
		RefCount: 1,
	}
	rootNodeName := hcas.NewName(string(rootName))
	rootNode.Inode.ObjName = &rootNodeName
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
	case *fuse.GetxattrRequest:
		err = hm.handleGetxattrRequest(req.(*fuse.GetxattrRequest))
	case *fuse.ListxattrRequest:
		err = hm.handleListxattrRequest(req.(*fuse.ListxattrRequest))
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
	case *fuse.FlushRequest:
		err = hm.handleFlushRequest(req.(*fuse.FlushRequest))
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

func (hm *HcasMount) openFileByName(name *hcas.Name) (*os.File, error) {
	nameHex := name.HexName()
	return os.Open(filepath.Join(
		hm.hcasDataDir,
		nameHex[:2],
		nameHex[2:],
	))
}
