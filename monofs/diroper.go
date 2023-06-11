package monofs

import (
	"context"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	monodir "github.com/rarydzu/gmonorepo/monofs/dir"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
)

// MkDir creates a new directory.
func (fs *Monofs) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) error {
	fs.fsHashLock.Lock(op.Parent)
	defer fs.fsHashLock.Unlock(op.Parent)
	_, err := fs.GetInode(op.Parent, op.Name, false)
	if err == nil {
		fs.log.Infof("MkDir(%d:%s): already exists", op.Parent, op.Name)
		return fuse.EEXIST
	}
	t := fs.Clock.Now()
	inode := fs.NewInode(op.Parent, op.Name,
		fsdb.InodeAttributes{
			Hash: "", // TODO FIXME
			InodeAttributes: fuseops.InodeAttributes{
				Size:  4096,
				Nlink: 1,
				Mode:  op.Mode,
				Rdev:  0,
				Uid:   fs.uid,
				Gid:   fs.gid,
				Atime: t,
				Mtime: t,
				Ctime: t,
			},
		},
	)
	if err = fs.AddInode(inode, true); err != nil {
		return err
	}
	// Report the inode's attributes.
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// OpenDir opens a directory for reading.
func (fs *Monofs) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	fs.fsHashLock.RLock(op.Inode)
	defer fs.fsHashLock.RUnlock(op.Inode)
	// Open the directory.
	attr, err := fs.GetInodeAttrs(op.Inode)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			err = fuse.ENOENT
		} else {
			fs.log.Errorf("OpenFile(GetInodeAttrs)(%d): %v", op.Inode, err)
			err = fuse.EIO
		}
		return err
	}
	if fsdb.InodeDirentType(attr.Mode) != fuseutil.DT_Directory {
		return fuse.ENOTDIR
	}
	op.Handle = fs.FindNextDirHandle()
	fs.dirHandles[op.Handle] = monodir.New(fs.metadb, op.Inode)
	return nil
}

// ReadDir reads a directory.
func (fs *Monofs) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	var (
		inodeEntries []*fsdb.Inode
		err          error
	)
	fs.fsHashLock.RLock(op.Inode)
	defer fs.fsHashLock.RUnlock(op.Inode)
	// Look up the directory.
	dir, ok := fs.dirHandles[op.Handle]
	if !ok {
		return fuse.ENOTDIR
	}
	if op.Inode != dir.GetInodeID() {
		fs.log.Errorf("ReadDir(%d): wrong inode %d", op.Inode, dir.GetInodeID())
		return fuse.EINVAL
	}
	inodeEntries, err = dir.GetDentries(op.Offset, len(op.Dst))
	if err != nil {
		fs.log.Errorf("ReadDir(%d): %v", op.Inode, err)
		return fuse.EIO
	}
	for x, inode := range inodeEntries {
		// Report the entry.
		dirent := fuseutil.Dirent{
			Offset: op.Offset + fuseops.DirOffset(x+1), // TODO FIXME
			Inode:  inode.ID(),
			Name:   inode.Name,
			Type:   fsdb.InodeDirentType(inode.Attrs.Mode),
		}
		op.BytesRead += fuseutil.WriteDirent(
			op.Dst[op.BytesRead:],
			dirent,
		)
		// Stop if we've filled the buffer.
		if op.BytesRead == len(op.Dst) {
			op.Offset = fuseops.DirOffset(x + 1)
			offset := int(op.Offset)
			dir.UpdateOffset(offset)
			dir.UpdateName(inode.Name)
			return nil
		}
	}
	// Advance the offset.
	op.Offset = fuseops.DirOffset(len(inodeEntries))
	return nil
}

// RmDir removes a directory.
func (fs *Monofs) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	fs.fsHashLock.Lock(op.Parent)
	defer fs.fsHashLock.Unlock(op.Parent)
	inode, err := fs.GetInode(op.Parent, op.Name, true)
	if err != nil {
		if err != fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("RmDir(GetInode)(%d, %s): %v", op.Parent, op.Name, err)
		return fuse.EIO
	}
	if fsdb.InodeDirentType(inode.Attrs.Mode) != fuseutil.DT_Directory {
		return fuse.ENOTDIR
	}
	children, err := fs.metadb.GetChildrenCount(inode.InodeID)
	if err != nil {
		if err != fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("RmDir(GetChildrenCount)(%d): %v", inode.ID(), err)
		return fuse.EIO
	}
	if children > 0 {
		return fuse.ENOTEMPTY
	}
	if err = fs.metadb.DeleteInode(inode, true); err != nil {
		fs.log.Errorf("RmDir(RemoveInode)(%d): %v", inode.ID(), err)
		return fuse.EIO
	}
	return nil
}

// ReleaseDirHandle releases a directory handle.
func (fs *Monofs) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) error {
	// Release the directory.
	fs.DeleteDirHandle(op.Handle)
	return nil
}
