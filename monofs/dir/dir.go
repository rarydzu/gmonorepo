package dir

import (
	"github.com/jacobsa/fuse/fuseops"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
	"go.uber.org/zap"
)

type FsDir struct {
	db        *fsdb.Fsdb
	inode     fuseops.InodeID
	offset    uint64
	lastEntry string
	name      string
	log       *zap.SugaredLogger
}

// New creates new DirFile object
func New(db *fsdb.Fsdb, inode fuseops.InodeID, log *zap.SugaredLogger) *FsDir {
	return &FsDir{
		db:        db,
		inode:     inode,
		offset:    0,
		lastEntry: "",
		name:      "",
	}
}

// Entries returns directory entries
func (dir *FsDir) Entries(location fuseops.DirOffset, size int) []*fsdb.Inode {
	entries, err := dir.db.GetChildren(uint64(dir.inode), dir.lastEntry, size)
	if err != nil {
		dir.log.Errorf("Error while reading directory %d: %+v", dir.inode, err)
		return nil
	}
	return entries
}

// UpdateLast update last prefix for directory entries
func (dir *FsDir) UpdateLast(name string) {
	dir.lastEntry = name
}

// GetInodeID returns inode id
func (dir *FsDir) GetInodeID() fuseops.InodeID {
	return dir.inode
}
