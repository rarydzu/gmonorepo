package file

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type FsFile struct {
	fsName       string
	inode        fuseops.InodeID
	handle       fuseops.HandleID
	offset       uint64
	data         []byte
	hash         string
	dataBasePath string
}

// New creates new FsFile object
func New(name string, inode fuseops.InodeID, hash string, dataBasePath string) *FsFile {
	return &FsFile{
		fsName:       name,
		inode:        inode,
		hash:         hash,
		dataBasePath: dataBasePath,
	}
}

func (file *FsFile) Read(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	// Read the requested data.
	n := copy(op.Dst, file.data[op.Offset:])
	op.BytesRead = n
	return
}

func (file *FsFile) Write(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {
	// Extend the file if necessary.
	if uint64(len(file.data)) < uint64(op.Offset)+uint64(len(op.Data)) {
		file.data = append(file.data, make([]byte, uint64(op.Offset)+uint64(len(op.Data))-uint64(len(file.data)))...)
	}

	// Write the data.
	copy(file.data[op.Offset:], op.Data)
	return
}

func (file *FsFile) Flush(
	ctx context.Context,
	op *fuseops.FlushFileOp) error {
	return nil
}

func (file *FsFile) Release(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) error {
	return nil
}

func (file *FsFile) GetInode() fuseops.InodeID {
	return file.inode
}

func (file *FsFile) GetHandle() fuseops.HandleID {
	return file.handle
}

func (file *FsFile) GetOffset() uint64 {
	return file.offset
}

func (file *FsFile) SetInode(inode fuseops.InodeID) {
	file.inode = inode
}

func (file *FsFile) SetHandle(handle fuseops.HandleID) {
	file.handle = handle
}

func (file *FsFile) SetOffset(offset uint64) {
	file.offset = offset
}

func (file *FsFile) GetData() []byte {
	return file.data
}

func (file *FsFile) SetData(data []byte) {
	file.data = data
}

func (file *FsFile) GetSize() uint64 {
	return uint64(len(file.data))
}

func (file *FsFile) SetSize(size uint64) {
	file.data = file.data[:size]
}

func (file *FsFile) GetCapacity() uint64 {
	return uint64(cap(file.data))
}

func (file *FsFile) SetCapacity(capacity uint64) {
	file.data = file.data[:capacity]
}

func (file *FsFile) ReadAt(b []byte, off int64) (n int, err error) {
	n = copy(b, file.data[off:])
	return
}

func (file *FsFile) WriteAt(b []byte, off int64) (n int, err error) {
	// Extend the file if necessary.
	if uint64(len(file.data)) < uint64(off)+uint64(len(b)) {
		file.data = append(file.data, make([]byte, uint64(off)+uint64(len(b))-uint64(len(file.data)))...)
	}

	// Write the data.
	n = copy(file.data[off:], b)
	return
}

func (file *FsFile) Truncate(size int64) (err error) {
	file.data = file.data[:size]
	return
}

func (file *FsFile) Close() (err error) {
	return
}

func (file *FsFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		file.offset = uint64(offset)
	case 1:
		file.offset += uint64(offset)
	case 2:
		file.offset = uint64(len(file.data)) + uint64(offset)
	}
	return int64(file.offset), nil
}

func (file *FsFile) ReadDir(n int) (fi []fuseutil.Dirent, err error) {
	return
}

func (file *FsFile) Stat() (fi os.FileInfo, err error) {
	return
}

func (file *FsFile) Readdir(n int) (fi []os.FileInfo, err error) {
	return
}

func (file *FsFile) Readdirnames(n int) (names []string, err error) {
	return
}

func (file *FsFile) Name() string { return "" }

func (file *FsFile) Mode() os.FileMode { return 0 }

func (file *FsFile) ModTime() time.Time { return time.Time{} }

func (file *FsFile) IsDir() bool { return false }

func (file *FsFile) Sys() interface{} { return nil }

func (file *FsFile) ReadFrom(r io.Reader) (n int64, err error) {
	return
}

func (file *FsFile) WriteTo(w io.Writer) (n int64, err error) {
	return
}
func (file *FsFile) Sync() (err error) { return }
