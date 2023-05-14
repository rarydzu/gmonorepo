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
	fs.log.Debugf("MkDir(%d:%d:%s): %v", inode.InodeID, op.Parent, op.Name, err)
	// Report the inode's attributes.
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// OpenDir opens a directory for reading.
func (fs *Monofs) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
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
	fs.dirHandles[op.Handle] = monodir.New(fs.metadb, op.Inode, fs.log)
	return nil
}

// ReadDir reads a directory.
func (fs *Monofs) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	// Look up the directory.
	dir, ok := fs.dirHandles[op.Handle]
	if !ok {
		return fuse.ENOTDIR
	}
	if op.Inode != dir.GetInodeID() {
		fs.log.Errorf("ReadDir(%d): wrong inode %d", op.Inode, dir.GetInodeID())
		return fuse.EINVAL
	}
	// Read the directory entries.
	for _, inode := range dir.Entries(op.Offset, len(op.Dst)) {
		// Report the entry.
		dirent := fuseutil.Dirent{
			Offset: op.Offset,
			Inode:  inode.ID(),
			Name:   inode.Name,
			Type:   fsdb.InodeDirentType(inode.Attrs.Mode),
		}
		op.BytesRead += fuseutil.WriteDirent(
			op.Dst[op.BytesRead:],
			dirent,
		)
		// Advance the offset.
		dir.UpdateLast(inode.Name)
		op.Offset++
		// Stop if we've filled the buffer.
		if op.BytesRead == len(op.Dst) {
			break
		}
	}
	return nil
}

// RmDir removes a directory.
func (fs *Monofs) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
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
	fs.log.Debugf("RmDir(%d:%d:%s)", inode.InodeID, op.Parent, op.Name)
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
	fs.log.Debugf("RmDir(%d): removing inode", inode.ID())
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
	_, ok := fs.dirHandles[op.Handle]
	if ok {
		delete(fs.dirHandles, op.Handle)
	}
	return nil
}
