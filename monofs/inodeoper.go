package monofs

import (
	"context"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
)

// MkNode - Create a new inode.
func (fs *Monofs) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	t := fs.Clock.Now()
	inode := fs.NewInode(op.Parent, op.Name, fsdb.InodeAttributes{
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
	})
	if err := fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("MkNode(%d:%s): %v", op.Parent, op.Name, err)
		return fuse.EIO
	}
	// Report the inode's attributes.
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// LookUpInode looks up a child inode by name and reports its attributes.
func (fs *Monofs) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {
	// Look up the requested inode.
	inode, err := fs.GetInode(op.Parent, op.Name, true)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("LookUpInode: %v", err)
		return fuse.EIO
	}
	// Report the inode's attributes.
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// GetInodeAttributes looks up an inode and reports its attributes.
func (fs *Monofs) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	attrs, err := fs.GetInodeAttrs(op.Inode)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		return fuse.EIO
	}
	op.Attributes = attrs
	fs.patchTime(&attrs)
	return nil
}

// SetInodeAttributes sets the attributes of an inode.
func (fs *Monofs) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	attrs, err := fs.GetInodeAttrs(op.Inode)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			fs.log.Debugf("SetInodeAttributes(%d): %v", op.Inode, err)
			return nil
		}
		fs.log.Errorf("SetInodeAttributes(%d): %v", op.Inode, err)
		return fuse.EIO
	}
	if op.Size != nil {
		attrs.Size = *op.Size
	}
	if op.Mode != nil {
		attrs.Mode = *op.Mode
	}
	if op.Uid != nil {
		attrs.Uid = *op.Uid
	}
	if op.Gid != nil {
		attrs.Gid = *op.Gid
	}
	if op.Atime != nil {
		attrs.Atime = *op.Atime
	}
	if op.Mtime != nil {
		attrs.Mtime = *op.Mtime
	}
	if err := fs.UpdateInodeAttrs(op.Inode, attrs); err != nil {
		return fuse.EIO
	}
	return nil
}

// ForgetInode - Forget about an inode.
func (fs *Monofs) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) error {
	//TODO integrate with last inode
	return nil
}
