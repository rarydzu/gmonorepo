package dir

import (
	"github.com/jacobsa/fuse/fuseops"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
)

type FsDir struct {
	db     *fsdb.Fsdb
	inode  fuseops.InodeID
	offset int
	name   string
}

// New creates new DirFile object
func New(db *fsdb.Fsdb, inode fuseops.InodeID) *FsDir {
	return &FsDir{
		db:     db,
		inode:  inode,
		offset: 0,
		name:   "",
	}
}

// Entries returns directory entries
func (dir *FsDir) Entries(offset fuseops.DirOffset, size int) ([]*fsdb.Inode, error) {
	entries, currentOffset, err := dir.db.GetChildren(uint64(dir.inode), dir.offset, size)
	if err != nil {
		return nil, err
	}
	dir.offset += currentOffset
	return entries, nil
}

// UpdateOffset updates offset
func (dir *FsDir) UpdateOffset(offset int) {
	dir.offset = offset
}

// GetInodeID returns inode id
func (dir *FsDir) GetInodeID() fuseops.InodeID {
	return dir.inode
}
