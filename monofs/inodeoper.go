package monofs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
	"github.com/rarydzu/gmonorepo/utils"
)

// MkNode - Create a new inode.
func (fs *Monofs) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	t := fs.Clock.Now()
	fs.fsHashLock.Lock(op.Parent)
	defer fs.fsHashLock.Unlock(op.Parent)
	// add to sha256 name of file current time and some random string
	sha256 := sha256.New()
	sha256.Write([]byte(op.Name))
	sha256.Write([]byte(t.String()))
	sha256.Write([]byte(utils.RandString(32)))
	inode := fs.NewInode(op.Parent, op.Name, fsdb.InodeAttributes{
		Hash: fmt.Sprintf("%x.%s", sha256.Sum(nil), fs.CurrentSnapshot),
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
	fs.fsHashLock.RLock(op.Parent)
	defer fs.fsHashLock.RUnlock(op.Parent)
	// Look up the requested inode.
	inode, err := fs.GetInode(op.Parent, op.Name, true)
	if err != nil {
		if errors.Is(err, fsdb.ErrNoSuchInode) {
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
			fs.log.Debugf("GetInodeAttributes NOT FOUND(%d): %v", op.Inode, err)
			return fuse.ENOENT
		}
		fs.log.Errorf("GetInodeAttributes: %v", err)
		return fuse.EIO
	}
	fs.patchTime(&attrs)
	op.Attributes = attrs
	return nil
}

// SetInodeAttributes sets the attributes of an inode.
func (fs *Monofs) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	attrs, err := fs.GetInodeAttrs(op.Inode)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			op.AttributesExpiration = fs.Clock.Now()
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
	op.Attributes = attrs
	return nil
}

// ForgetInode - Forget about an inode.
func (fs *Monofs) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) error {
	// TODO FIX THIS and check if op.N actually == inode access count
	if op.N > 0 {
		if err := fs.DeleteInodeAttrs(op.Inode); err != nil {
			if err != fsdb.ErrNoSuchInode {
				return fuse.ENOENT
			}
			fs.log.Errorf("ForgetInode(%d): %v", op.Inode, err)
			return fuse.EIO
		}
	}
	return nil
}
