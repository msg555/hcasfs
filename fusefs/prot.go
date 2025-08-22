package fusefs

import (
	"fmt"

	"github.com/msg555/hcas/unix"
)

const DIRENT_OFFSET_EOF = 0xFFFFFFFFFFFFFFFF

/*
TODOLook at fill_dir at https://github.com/libfuse/libfuse/blob/master/lib/fuse.c
to see how to add entries to the directory listing buffer.

Also see
https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
for further notes about how fill_dir is meant to be used.

https://libfuse.github.io/doxygen/fuse__lowlevel_8h.html#ad1957bcc8ece8c90f16c42c4daf3053f
*/
func direntAlign(x int) int {
	return (x + 7) &^ 7
}

func addDirEntry(buf []byte, name string, inodeId uint64, offset uint64, inodeMode uint32) int {
	/*
	   define FUSE_DIRENT_ALIGN(x) (((x) + sizeof(__u64) - 1) & ~(sizeof(__u64) - 1))

	   struct fuse_dirent {
	     u64   ino;
	     u64   off;
	     u32   namelen;
	     u32   type;
	     char name[];
	   };
	*/

	entryBaseLen := 24 + len(name)
	entryPadLen := direntAlign(entryBaseLen)
	if len(buf) < entryPadLen {
		return 0
	}

	fmt.Printf("Yielding %s %d %d\n", name, inodeId, (inodeMode&unix.S_IFMT) >> 12)
	unix.Hbo.PutUint64(buf[0:], inodeId)
	unix.Hbo.PutUint64(buf[8:], offset)
	unix.Hbo.PutUint32(buf[16:], uint32(len(name)))
	unix.Hbo.PutUint32(buf[20:], uint32(inodeMode&unix.S_IFMT)>>12)

	copy(buf[24:], name)
	for i := entryBaseLen; i < entryPadLen; i++ {
		buf[i] = 0
	}

	return entryPadLen
}
