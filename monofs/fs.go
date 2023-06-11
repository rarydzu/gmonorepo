package monofs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rarydzu/gmonorepo/monofs/fsdb"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

const (
	StatFsDurationDeadline = 200 * time.Millisecond
)

// NewMonoFuseFS Create a new file system backed by the given directory.
func NewMonoFuseFS(fs *Monofs) (fuse.Server, error) {
	// don't need to lock it here, because it's not used yet
	fs.nextInode = fuseops.RootInodeID + 1
	rootInode, err := fs.GetInode(fuseops.RootInodeID-1, "", true)
	if err != nil {
		if !errors.Is(err, fsdb.ErrNoSuchInode) {
			return nil, fmt.Errorf("failed to get root inode: %v", err)
		}
		// Create the root directory.
		t := fs.Clock.Now()
		rootInode = fsdb.NewInode(fuseops.RootInodeID, 0, "", fsdb.InodeAttributes{
			Hash: "",
			InodeAttributes: fuseops.InodeAttributes{
				Size:   4096,
				Nlink:  1,
				Mode:   os.ModeDir | 0755,
				Rdev:   0,
				Atime:  t,
				Mtime:  t,
				Ctime:  t,
				Crtime: t,
				Uid:    fs.uid,
				Gid:    fs.gid,
			},
		})
		if err = fs.AddInode(rootInode, true); err != nil {
			return nil, err
		}
	}
	if err = fs.lastInodeEngine.Init(); err != nil {
		return nil, err
	}
	fs.GetLastInode()
	fs.lockInode.RLock()
	fs.log.Debugf("Last inode: %v root Inode: %d snapshot: %s", fs.nextInode, rootInode.ID(), fs.CurrentSnapshot)
	fs.lockInode.RUnlock()
	// check if snaphosts have errors
	return fuseutil.NewFileSystemServer(fs), nil
}

// StatFS Get file system attributes.
func (fs *Monofs) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	nctx, cancel := context.WithTimeout(ctx, StatFsDurationDeadline)
	defer cancel()

	ret, err := fs.metadb.StatClient.Stat(nctx, fs.Name)
	if err != nil {
		fs.log.Debugf("StatFS not recieved: %v", err)
		op.BlockSize = 1024
		op.Blocks = 1024 * 1024 * 1024
		op.BlocksFree = 1024 * 1024 * 1024
		op.BlocksAvailable = 1024 * 1024 * 1024
		return nil
	}
	op.BlockSize = ret.BlockSize
	op.Blocks = ret.Blocks
	op.BlocksFree = ret.BlocksFree
	op.BlocksAvailable = uint64(ret.BlocksAvailable)
	return nil
}

// NextInode returns the next available inode ID.
func (fs *Monofs) NextInode() fuseops.InodeID {
	fs.lockInode.Lock()
	fs.nextInode++
	fs.lastInodeEngine.StoreLastInode(fs.nextInode)
	fs.lockInode.Unlock()
	return fs.nextInode
}

// GetLastInode get last inode from lastinode engine
func (fs *Monofs) GetLastInode() {
	fs.lockInode.RLock()
	defer fs.lockInode.RUnlock()
	fs.nextInode = fs.lastInodeEngine.GetLastInode()
}

// FindNextDirHandle find unused dir handle
func (fs *Monofs) FindNextDirHandle() fuseops.HandleID {
	fs.lockHandle.Lock()
	defer fs.lockHandle.Unlock()
	handle := fs.nextHandle
	for _, ok := fs.fileHandles[handle]; ok; _, ok = fs.fileHandles[handle] {
		handle++
	}
	fs.nextHandle = handle + 1
	return handle
}

// DeleteDirHandle delete dir handle
func (fs *Monofs) DeleteDirHandle(handle fuseops.HandleID) {
	fs.lockHandle.Lock()
	defer fs.lockHandle.Unlock()
	delete(fs.dirHandles, handle)
}

// NewInode Create a new inode.
func (fs *Monofs) NewInode(parent fuseops.InodeID, name string, attr fsdb.InodeAttributes) *fsdb.Inode {
	ID := fs.NextInode()
	return fsdb.NewInode(uint64(ID), uint64(parent), name, attr)
}

// AddInode Add a new inode to the inode database.
func (fs *Monofs) AddInode(inode *fsdb.Inode, attr bool) error {
	return fs.metadb.AddInode(inode, attr)
}

// GetInode Get an inode from the inode database.
func (fs *Monofs) GetInode(inode fuseops.InodeID, name string, attr bool) (*fsdb.Inode, error) {
	return fs.metadb.GetInode(uint64(inode), name, attr)
}

// DeleteInode Delete an inode from the inode database.
func (fs *Monofs) DeleteInode(inode *fsdb.Inode, attr bool) error {
	return fs.metadb.DeleteInode(inode, attr)
}

// CreateInodeAttrs Create inode attributes.
func (fs *Monofs) CreateInodeAttrs(inode *fsdb.Inode) error {
	return fs.metadb.CreateInodeAttrs(inode)
}

// GetInodeAttrs Get inode attributes.
func (fs *Monofs) GetInodeAttrs(inode fuseops.InodeID) (fuseops.InodeAttributes, error) {
	return fs.metadb.GetInodeAttrs(uint64(inode))
}

// DeleteInodeAttrs Delete inode attributes.
func (fs *Monofs) DeleteInodeAttrs(inode fuseops.InodeID) error {
	return fs.metadb.DeleteInodeAttrs(uint64(inode))
}

// UpdateInodeAttrs Update inode attributes.
func (fs *Monofs) UpdateInodeAttrs(inode fuseops.InodeID, attr fuseops.InodeAttributes) error {
	return fs.metadb.UpdateInodeAttrs(uint64(inode), attr)
}

// patchTime Update the inode attributes (not in db) with the current time.
func (fs *Monofs) patchTime(attr *fuseops.InodeAttributes) time.Time {
	now := fs.Clock.Now()
	attr.Atime = now
	return time.Now()
}

// Destroy Stop the filesystem.
func (fs *Monofs) Destroy() {
	if err := fs.metadb.Close(); err != nil {
		fs.log.Errorf("Error closing metadb: %v", err)
	}
	if err := fs.lastInodeEngine.Close(); err != nil {
		fs.log.Errorf("Error closing lastInodeEngine: %v", err)
	}
}
