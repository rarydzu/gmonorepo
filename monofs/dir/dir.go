package dir

import (
	"github.com/jacobsa/fuse/fuseops"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
)

type FsDir struct {
	db       *fsdb.Fsdb
	inode    fuseops.InodeID
	offset   int
	name     string
	dentries []*fsdb.Inode
}

// New creates new DirFile object
func New(db *fsdb.Fsdb, inode fuseops.InodeID) *FsDir {
	return &FsDir{
		db:       db,
		inode:    inode,
		offset:   0,
		name:     "",
		dentries: []*fsdb.Inode{},
	}
}

// GetDentries returns dentries
func (dir *FsDir) GetDentries(offset fuseops.DirOffset, size int) ([]*fsdb.Inode, error) {
	if offset == 0 && dir.offset > 0 {
		return dir.Revind(offset, size)
	} else if offset >= fuseops.DirOffset(size) || dir.CacheSize() == 0 {
		return dir.DbEntries(offset, size)
	}
	return dir.CacheEntries(offset)
}

// Revind compare entries from and cache and compare changes
func (dir *FsDir) Revind(offset fuseops.DirOffset, size int) ([]*fsdb.Inode, error) {
	i, err := dir.DbEntries(offset, size)
	if err != nil {
		return nil, err
	}
	if len(i) != len(dir.dentries) {
		return i, nil
	}
	// this could be faster. In majory of cases cache and db entries will be the same
	// so we could create hash for db entres and compare it with cache hash
	// if they are the same we could return empty ( [] *fsdb.Inode{})

	for x, inode := range i {
		if inode.Name != dir.dentries[x].Name {
			return i, nil
		}
	}
	return []*fsdb.Inode{}, nil
}

// DbEntries returns directory entries1
func (dir *FsDir) DbEntries(offset fuseops.DirOffset, size int) ([]*fsdb.Inode, error) {
	entries, currentOffset, err := dir.db.GetChildren(uint64(dir.inode), dir.offset, size, []byte(dir.name))
	if err != nil {
		return nil, err
	}
	dir.offset += currentOffset
	if len(entries) > 0 {
		dir.name = entries[len(entries)-1].Name
	}
	if offset < fuseops.DirOffset(size) {
		dir.dentries = entries
	}
	return entries, nil
}

// UpdateOffset updates offset
func (dir *FsDir) UpdateOffset(offset int) {
	dir.offset = offset
}

// GetOffset returns offset
func (dir *FsDir) GetOffset() int {
	return dir.offset
}

// UpdateName updates name
func (dir *FsDir) UpdateName(name string) {
	dir.name = name
}

// GetInodeID returns inode id
func (dir *FsDir) GetInodeID() fuseops.InodeID {
	return dir.inode
}

// CacheSize returns cache size
func (dir *FsDir) CacheSize() int {
	return len(dir.dentries)
}

// CacheEntries returns entries from cache
func (dir *FsDir) CacheEntries(offset fuseops.DirOffset) ([]*fsdb.Inode, error) {
	return dir.dentries[offset:], nil
}
