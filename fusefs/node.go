package fusefs

import (
	"fmt"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

	"github.com/msg555/hcas/hcasfs"
	"github.com/msg555/hcas/unix"
)

type InodeReference struct {
	Inode    hcasfs.InodeData
	RefCount int64
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
			Inode:    *inodeData,
			RefCount: 1,
		}
	}
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
		if !ok {
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

	nodeFile, err := hm.openFileByName(inode.ObjName)
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
		Generation: 1,                // What is this?
		EntryValid: DURATION_DEFAULT, // Check this out, too
		Attr:       inodeAttr(inodeId, &dirEntry.Inode),
	})
	hm.trackInode(inodeId, &dirEntry.Inode)

	return nil
}

func (hm *HcasMount) handleAccessRequest(req *fuse.AccessRequest) error {
	inode, err := hm.getInode(req.Node)
	if err != nil {
		return err
	}

	fmt.Println("Not expecting this call")

	if !unix.TestAccess(req.Uid == inode.Uid, req.Gid == inode.Gid, inode.Mode, req.Mask) {
		return FuseError{
			source: errors.New("permission denied"),
			errno:  unix.EACCES,
		}
	}

	req.Respond()
	return nil
}

func (hm *HcasMount) handleGetxattrRequest(req *fuse.GetxattrRequest) error {
	/* Xattrs are not supported */
	req.Respond(&fuse.GetxattrResponse{
		Xattr: nil,
	})
	return nil
}

func (hm *HcasMount) handleListxattrRequest(req *fuse.ListxattrRequest) error {
	/* Xattrs are not supported */
	req.Respond(&fuse.ListxattrResponse{
		Xattr: nil,
	})
	return nil
}
