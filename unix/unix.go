package unix

import (
	"os"
	"unsafe"

	"github.com/go-errors/errors"

	"golang.org/x/sys/unix"
)

const (
	NAME_MAX       = 255
	PATH_MAX       = 4096
	PATH_MAX_LIMIT = 1 << 16

	O_NOFOLLOW  = unix.O_NOFOLLOW
	O_PATH      = unix.O_PATH
	O_RDONLY    = unix.O_RDONLY
	O_RDWR      = unix.O_RDWR
	O_WRONLY    = unix.O_WRONLY
	O_DIRECTORY = unix.O_DIRECTORY

	S_IFMT   = unix.S_IFMT
	S_IFBLK  = unix.S_IFBLK
	S_IFCHR  = unix.S_IFCHR
	S_IFDIR  = unix.S_IFDIR
	S_IFIFO  = unix.S_IFIFO
	S_IFLNK  = unix.S_IFLNK
	S_IFREG  = unix.S_IFREG
	S_IFSOCK = unix.S_IFSOCK

	S_ISGID = unix.S_ISGID
	S_ISUID = unix.S_ISUID
	S_ISVTX = unix.S_ISVTX

	EACCES  = unix.EACCES
	EBADF   = unix.EBADF
	EINVAL  = unix.EINVAL
	EIO     = unix.EIO
	EISDIR  = unix.EISDIR
	ENODATA = unix.ENODATA
	ENOENT  = unix.ENOENT
	ENOSYS  = unix.ENOSYS
	ENOTDIR = unix.ENOTDIR
	ENOTSUP = unix.ENOTSUP
	EROFS   = unix.EROFS

	DT_UNKNOWN = 0
	DT_FIFO    = S_IFIFO >> 12
	DT_CHR     = S_IFCHR >> 12
	DT_DIR     = S_IFDIR >> 12
	DT_BLK     = S_IFBLK >> 12
	DT_REG     = S_IFREG >> 12
	DT_LNK     = S_IFLNK >> 12
	DT_SOCK    = S_IFSOCK >> 12

	AT_SYMLINK_NOFOLLOW = 0x100
)

type Stat_t = unix.Stat_t
type Statfs_t = unix.Statfs_t
type Errno = unix.Errno

// Supports basic makedev implementation. Most kernels support major/minors
// larger than 255 however how this is encoded varies between kernels therefore
// we only support 8 bit major/minors which is consistently represented.
func Makedev(major, minor uint64) (uint64, error) {
	if 255 < major {
		return 0, errors.New("major number too large")
	}
	if 255 < minor {
		return 0, errors.New("minor number too large")
	}
	return major<<8 | minor, nil
}

func S_ISDIR(mode uint32) bool {
	return ((mode & S_IFMT) == S_IFDIR)
}

func S_ISREG(mode uint32) bool {
	return ((mode & S_IFMT) == S_IFREG)
}

func S_ISLNK(mode uint32) bool {
	return ((mode & S_IFMT) == S_IFLNK)
}

func S_ISBLK(mode uint32) bool {
	return ((mode & S_IFMT) == S_IFBLK)
}

func S_ISCHR(mode uint32) bool {
	return ((mode & S_IFMT) == S_IFCHR)
}

func UnixToFileStatMode(unixMode uint32) os.FileMode {
	fsMode := os.FileMode(unixMode & 0777)
	switch unixMode & S_IFMT {
	case S_IFBLK:
		fsMode |= os.ModeDevice
	case S_IFCHR:
		fsMode |= os.ModeDevice | os.ModeCharDevice
	case S_IFDIR:
		fsMode |= os.ModeDir
	case S_IFIFO:
		fsMode |= os.ModeNamedPipe
	case S_IFLNK:
		fsMode |= os.ModeSymlink
	case S_IFREG:
		// nothing to do
	case S_IFSOCK:
		fsMode |= os.ModeSocket
	}
	if (unixMode & S_ISGID) != 0 {
		fsMode |= os.ModeSetgid
	}
	if (unixMode & S_ISUID) != 0 {
		fsMode |= os.ModeSetuid
	}
	if (unixMode & S_ISVTX) != 0 {
		fsMode |= os.ModeSticky
	}
	return fsMode
}

func FileStatToUnixMode(fsMode os.FileMode) uint32 {
	unixMode := uint32(fsMode & 0777)
	if (fsMode & os.ModeCharDevice) != 0 {
		unixMode |= S_IFCHR
	} else if (fsMode & os.ModeDevice) != 0 {
		unixMode |= S_IFBLK
	} else if (fsMode & os.ModeDir) != 0 {
		unixMode |= S_IFDIR
	} else if (fsMode & os.ModeNamedPipe) != 0 {
		unixMode |= S_IFIFO
	} else if (fsMode & os.ModeSymlink) != 0 {
		unixMode |= S_IFLNK
	} else if (fsMode & os.ModeSocket) != 0 {
		unixMode |= S_IFSOCK
	} else {
		unixMode |= S_IFREG
	}
	if (fsMode & os.ModeSetgid) != 0 {
		unixMode |= S_ISGID
	}
	if (fsMode & os.ModeSetuid) != 0 {
		unixMode |= S_ISUID
	}
	if (fsMode & os.ModeSticky) != 0 {
		unixMode |= S_ISVTX
	}
	return unixMode
}

func TestAccess(user, group bool, mode, mask uint32) bool {
	modeEffective := mode & 07
	if user {
		modeEffective |= (mode >> 6) & 07
	}
	if group {
		modeEffective |= (mode >> 6) & 07
	}
	return (mask & modeEffective) == mask
}

// Invoke a syscall that returns just an error, retrying on EINTR
func RetrySyscallE(callSyscallE func() error) error {
	for {
		err := callSyscallE()
		if err == unix.EINTR {
			continue
		}
		if err == nil || err == Errno(0) {
			return nil
		}
		return errors.New(err)
	}
}

// Invoke a syscall that returns an int and an error, retrying on EINTR
func RetrySyscallIE(callSyscallIE func() (int, error)) (int, error) {
	for {
		n, err := callSyscallIE()
		if err == unix.EINTR {
			continue
		}
		if err == nil {
			return n, nil
		}
		return n, errors.New(err)
	}
}

// Invoke a syscall that returns an int and an error, retrying on EINTR
func RetrySyscall6(trap, a1, a2, a3, a4, a5, a6 uintptr) (uintptr, uintptr, error) {
	for {
		r1, r2, err := unix.Syscall6(trap, a1, a2, a3, a4, a5, a6)
		if err == unix.EINTR {
			continue
		}
		if err == 0 {
			return r1, r2, nil
		}
		return r1, r2, errors.New(err)
	}
}

func Openat(dirfd int, path string, flags int, mode uint32) (int, error) {
	return RetrySyscallIE(func() (int, error) {
		return unix.Openat(dirfd, path, flags, mode)
	})
}

func Getdents(fd int, buf []byte) (int, error) {
	return RetrySyscallIE(func() (int, error) {
		return unix.Getdents(fd, buf)
	})
}

func Read(fd int, p []byte) (int, error) {
	return RetrySyscallIE(func() (int, error) {
		return unix.Read(fd, p)
	})
}

func Pread(fd int, p []byte, offset int64) (int, error) {
	return RetrySyscallIE(func() (int, error) {
		return unix.Pread(fd, p, offset)
	})
}

func Readlinkat(dirfd int, path string, buf []byte) (int, error) {
	return RetrySyscallIE(func() (int, error) {
		return unix.Readlinkat(dirfd, path, buf)
	})
}

func Open(path string, mode int, perm uint32) (int, error) {
	return RetrySyscallIE(func() (int, error) {
		return unix.Open(path, mode, perm)
	})
}

func Close(fd int) error {
	return RetrySyscallE(func() error {
		return unix.Close(fd)
	})
}

func Stat(path string, stat *Stat_t) error {
	return RetrySyscallE(func() error {
		return unix.Stat(path, stat)
	})
}

func Fstat(fd int, stat *Stat_t) error {
	return RetrySyscallE(func() error {
		return unix.Fstat(fd, stat)
	})
}

func Fstatat(dirfd int, pathname string, stat *unix.Stat_t, flags int) error {
	var p *byte
	p, err := unix.BytePtrFromString(pathname)
	if err != nil {
		return err
	}

	_, _, err = RetrySyscall6(unix.SYS_NEWFSTATAT, uintptr(dirfd), uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(stat)), uintptr(flags), 0, 0)
	return err
}

func Statfs(path string, buf *Statfs_t) error {
	return RetrySyscallE(func() error {
		return unix.Statfs(path, buf)
	})
}
