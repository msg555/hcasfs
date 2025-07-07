package hcas

import (
	"os"

	"github.com/msg555/hcas/unix"
)

func lockFile(f *os.File) error {
	lock := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: int16(os.SEEK_SET),
		Start:  0,
		Len:    0,
	}

	return unix.FcntlFlock(f.Fd(), unix.F_SETLKW, &lock)
}
