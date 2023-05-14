package monofs

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
	"syscall"
	"time"

	monohash "github.com/rarydzu/gmonorepo/hash"
	"github.com/rarydzu/gmonorepo/monofs/config"
	monodir "github.com/rarydzu/gmonorepo/monofs/dir"
	monofile "github.com/rarydzu/gmonorepo/monofs/file"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
	"github.com/rarydzu/gmonorepo/monofs/lastinode"
	"go.uber.org/zap"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/timeutil"
)

const (
	StatFsDurationDeadline = 200 * time.Millisecond
)

type Monofs struct {
	fuseutil.NotImplementedFileSystem
	Name            string
	log             *zap.SugaredLogger
	metadb          *fsdb.Fsdb
	nextInode       fuseops.InodeID
	files           map[fuseops.InodeID]*monofile.FsFile
	fileHandles     map[fuseops.HandleID]*monofile.FsFile
	dirHandles      map[fuseops.HandleID]*monodir.FsDir
	nextHandle      fuseops.HandleID
	lock            sync.RWMutex
	Clock           timeutil.Clock
	uid             uint32
	gid             uint32
	lastInodeEngine *lastinode.LastInodeEngine
	locker          *monohash.Hash
}

// NewMonoFS Create a new file system backed by the given directory.
func NewMonoFS(config *config.Config, logger *zap.SugaredLogger) (fuse.Server, error) {
	var limit syscall.Rlimit
	metadb, err := fsdb.New(config)
	if err != nil {
		return nil, err
	}
	user, err := user.Current()
	if err != nil {
		return nil, err
	}
	uid, err := strconv.ParseUint(user.Uid, 10, 32)
	if err != nil {
		return nil, err
	}
	gid, err := strconv.ParseUint(user.Gid, 10, 32)
	if err != nil {
		return nil, err
	}
	lastInodeEngine := lastinode.New(config.Path, metadb.GetIStoreHandler())
	if err := lastInodeEngine.Init(); err != nil {
		return nil, err
	}
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return nil, fmt.Errorf("getrlimit: %v", err)
	}
	fs := &Monofs{
		Name:            config.FilesystemName,
		metadb:          metadb,
		log:             logger,
		Clock:           timeutil.RealClock(),
		files:           make(map[fuseops.InodeID]*monofile.FsFile),
		fileHandles:     make(map[fuseops.HandleID]*monofile.FsFile),
		dirHandles:      make(map[fuseops.HandleID]*monodir.FsDir),
		uid:             uint32(uid),
		gid:             uint32(gid),
		lastInodeEngine: lastInodeEngine,
		locker:          monohash.New(uint64(limit.Cur)),
	}
	_, err = fs.GetInode(fuseops.RootInodeID, "", true)
	if err != nil {
		if err != fsdb.ErrNoSuchInode {
			return nil, err
		}
		// Create the root directory.
		t := fs.Clock.Now()
		rootInode := fs.NextInode()
		dbRoot := fsdb.NewInode(uint64(rootInode), fuseops.RootInodeID, "", fsdb.InodeAttributes{
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
		if err := fs.AddInode(dbRoot, true); err != nil {
			return nil, err
		}
	}
	fs.GetLastInode()
	fs.lock.RLock()
	fs.log.Debugf("Last inode: %v", fs.nextInode)
	fs.lock.RUnlock()
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
	fs.lock.Lock()
	fs.nextInode++
	fs.lastInodeEngine.StoreLastInode(fs.nextInode)
	fs.lock.Unlock()
	return fs.nextInode
}

// GetLastInode get last inode from lastinode engine
func (fs *Monofs) GetLastInode() {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	fs.nextInode = fs.lastInodeEngine.GetLastInode()
}

// NextHandle returns the next available handle ID.
func (fs *Monofs) NextHandle() fuseops.HandleID {
	fs.lock.Lock()
	fs.nextHandle++
	fs.lock.Unlock()
	return fs.nextHandle
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
