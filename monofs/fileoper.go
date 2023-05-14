package monofs

import (
	"context"
	"os"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	monofile "github.com/rarydzu/gmonorepo/monofs/file"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
)

// CreateFile Create a new file.
func (fs *Monofs) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	fs.locker.Lock(uint64(op.Parent))
	defer fs.locker.Unlock(uint64(op.Parent))
	// Create a new inode.
	i, err := fs.GetInode(op.Parent, op.Name, true)
	if err == nil {
		op.Entry.Child = i.ID()
		op.Entry.Attributes = i.Attrs.InodeAttributes
		return nil
	}
	t := fs.Clock.Now()
	inode := fs.NewInode(op.Parent, op.Name,
		fsdb.InodeAttributes{
			Hash: "", // TODO FIXME
			InodeAttributes: fuseops.InodeAttributes{
				Size:  0,
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
	if err := fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("CreateFile(%d:%s): %v", op.Parent, op.Name, err)
		return fuse.EIO
	}
	fs.fileHandles[op.Handle] = monofile.New(inode.ID())
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// CreateLink Create a new link.
func (fs *Monofs) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	fs.locker.Lock(uint64(op.Parent))
	defer fs.locker.Unlock(uint64(op.Parent))
	attr, err := fs.GetInodeAttrs(op.Target)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("CreateLink(GetInodeAttr) %d: %v", op.Target, err)
		return fuse.EIO
	}
	inode := fsdb.NewInode(uint64(op.Target), uint64(op.Parent), op.Name,
		fsdb.InodeAttributes{
			Hash:            "", // TODO FIXME
			InodeAttributes: attr,
		})
	inode.Attrs.Nlink++
	if err = fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("CreateLink(AddInode)(%d:%s): %v", op.Target, op.Name, err)
		return fuse.EIO
	}
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// CreateSymlink Create a new symlink.
func (fs *Monofs) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	fs.locker.Lock(uint64(op.Parent))
	defer fs.locker.Unlock(uint64(op.Parent))
	t := fs.Clock.Now()
	inode := fs.NewInode(op.Parent, op.Name,
		fsdb.InodeAttributes{
			Hash: op.Target,
			InodeAttributes: fuseops.InodeAttributes{
				Size:  0,
				Nlink: 1,
				Mode:  0777 | os.ModeSymlink,
				Rdev:  0,
				Uid:   fs.uid,
				Gid:   fs.gid,
				Ctime: t,
				Mtime: t,
				Atime: t,
			},
		})
	if err := fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("CreateSymlink(AddInode)(%s:%s): %v", op.Target, op.Name, err)
		return fuse.EIO
	}
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// TODO ReadSymlink
func (fs *Monofs) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	fs.locker.RLock(uint64(op.Inode))
	defer fs.locker.RUnlock(uint64(op.Inode))
	attr, err := fs.metadb.GetFsdbInodeAttributes(uint64(op.Inode))
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("ReadSymlink(GetInode): %v", err)
		return fuse.EIO
	}
	if fsdb.InodeDirentType(attr.InodeAttributes.Mode) == fuseutil.DT_Link {
		//TODO check if hash is not a sha256 hash
		op.Target = attr.Hash
	}
	return nil
}

// Rename rename a file or directory
func (fs *Monofs) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	fs.locker.Lock(uint64(op.OldParent))
	defer fs.locker.Unlock(uint64(op.OldParent))
	// Look up the source inode.
	inode, err := fs.GetInode(op.OldParent, op.OldName, true)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("Rename(GetInode) (%s:%d): %v", op.OldParent, op.OldName, err)
		return fuse.EIO
	}
	// Remove it from the source directory.
	err = fs.DeleteInode(inode, false)
	if err != nil {
		fs.log.Errorf("Rename(DeleteInode)(%d:%s): %v", inode.ParentID, inode.Name, err)
		return fuse.EIO
	}

	// Add it to the target directory.
	inode.SetParent(op.NewParent)
	inode.SetName(op.NewName)
	inode.SetAttrsParent(op.NewParent)
	t := fs.Clock.Now()
	inode.Attrs.Mtime = t
	inode.Attrs.Atime = t
	if err = fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("Rename(AddInode)(%d:%s): %v", inode.ParentID, inode.Name, err)
		return fuse.EIO
	}
	return nil
}

// Unlink remove a file or directory
func (fs *Monofs) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	fs.locker.Lock(uint64(op.Parent))
	defer fs.locker.Unlock(uint64(op.Parent))
	// Look up the source inode.
	inode, err := fs.GetInode(op.Parent, op.Name, true)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("Unlink(GetInode)(%d:%s): %v", op.Parent, op.Name, err)
		return fuse.EIO
	}
	err = fs.DeleteInode(inode, false)
	if err != nil {
		fs.log.Errorf("Unlink(DeleteInode)(%d:%s): %v", inode.ParentID, inode.Name, err)
		return fuse.EIO
	}
	if inode.Attrs.Mode&os.ModeSymlink != os.ModeSymlink {
		if inode.Attrs.Nlink > 1 {
			inode.Attrs.Nlink--
			if err = fs.CreateInodeAttrs(inode); err != nil {
				fs.log.Errorf("Unlink(CreateInodeAttrs)(%d): %v", inode.ID(), err)
				return fuse.EIO
			}
		} else {
			if err = fs.DeleteInodeAttrs(inode.ID()); err != nil {
				fs.log.Errorf("Unlink(DeleteInodeAttrs)(%d): %v", inode.ID(), err)
				return fuse.EIO
			}
		}
	}
	return nil
}

// OpenFile open a file
func (fs *Monofs) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	fs.locker.Lock(uint64(op.Inode))
	defer fs.locker.Unlock(uint64(op.Inode))
	_, err := fs.GetInodeAttrs(op.Inode)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("OpenFile(GetInodeAttrs)(%d): %v", op.Inode, err)
		return fuse.EIO
	}
	fs.fileHandles[op.Handle] = monofile.New(op.Inode)
	return nil
}

// ReadFile read a file
func (fs *Monofs) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	var err error
	// probably global lock is sufficient
	fs.locker.RLock(uint64(op.Inode))
	defer fs.locker.RUnlock(uint64(op.Inode))
	// Look up the file.
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Read the file.
	op.BytesRead, err = handle.ReadAt(op.Dst, int64(op.Offset))
	return err
}

// WriteFile write a file
func (fs *Monofs) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	// probably global lock is sufficient
	fs.locker.Lock(uint64(op.Inode))
	defer fs.locker.Unlock(uint64(op.Inode))
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Write the file.
	_, err := handle.WriteAt(op.Data, int64(op.Offset))
	return err
}

// FlushFile flush a file
func (fs *Monofs) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) error {
	fs.locker.Lock(uint64(op.Inode))
	defer fs.locker.Unlock(uint64(op.Inode))
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Flush the file.
	return handle.Sync()
}

// ReleaseFileHandle release a file handle
func (fs *Monofs) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) error {
	// Release the file.
	delete(fs.fileHandles, op.Handle)
	return nil
}

// SyncFile sync a file
func (fs *Monofs) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) error {
	fs.locker.Lock(uint64(op.Inode))
	defer fs.locker.Unlock(uint64(op.Inode))
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Flush the file.
	return handle.Sync()
}
