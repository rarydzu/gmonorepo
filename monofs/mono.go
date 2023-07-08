package monofs

import (
	"fmt"
	"net"
	"os/user"
	"strconv"
	"sync"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/timeutil"
	"github.com/rarydzu/gmonorepo/hash"
	"github.com/rarydzu/gmonorepo/monofs/config"
	monodir "github.com/rarydzu/gmonorepo/monofs/dir"
	monofile "github.com/rarydzu/gmonorepo/monofs/file"
	"github.com/rarydzu/gmonorepo/monofs/fsdb"
	"github.com/rarydzu/gmonorepo/monofs/lastinode"
	"github.com/rarydzu/gmonorepo/monoserver/manager"
	pb "github.com/rarydzu/gmonorepo/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Monofs struct {
	fuseutil.NotImplementedFileSystem
	Name              string
	log               *zap.SugaredLogger
	metadb            *fsdb.Fsdb
	nextInode         fuseops.InodeID
	files             map[fuseops.InodeID]*monofile.FsFile
	fileHandles       map[fuseops.HandleID]*monofile.FsFile
	dirHandles        map[fuseops.HandleID]*monodir.FsDir
	nextHandle        fuseops.HandleID
	lockInode         sync.RWMutex
	lockHandle        sync.Mutex
	Clock             timeutil.Clock
	uid               uint32
	gid               uint32
	lastInodeEngine   *lastinode.LastInodeEngine
	fsHashLock        *hash.Hash
	CurrentSnapshot   string
	stopSnapshotCheck chan bool
	manager           *manager.Manager
	grpcManager       *grpc.Server
	localDataPath     string
}

func NewMonoFS(cfg *config.Config, log *zap.SugaredLogger) (*Monofs, error) {
	var limit syscall.Rlimit
	metadb, err := fsdb.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("metadb: %v", err)
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
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return nil, err
	}
	lastInodeEngine := lastinode.New(cfg.Path, metadb.GetIStoreHandler())
	s, err := metadb.StartSyncSnapshot()
	if err != nil {
		return nil, fmt.Errorf("StartSyncSnapshot: %v", err)
	}
	manager := manager.New(cfg.FilesystemName, metadb.Snapshot, cfg.ManagerPort)
	manager.Start()

	fs := &Monofs{
		Name:              cfg.FilesystemName,
		metadb:            metadb,
		log:               log,
		Clock:             timeutil.RealClock(),
		files:             make(map[fuseops.InodeID]*monofile.FsFile),
		fileHandles:       make(map[fuseops.HandleID]*monofile.FsFile),
		dirHandles:        make(map[fuseops.HandleID]*monodir.FsDir),
		uid:               uint32(uid),
		gid:               uint32(gid),
		lastInodeEngine:   lastInodeEngine,
		fsHashLock:        hash.New(limit.Cur),
		CurrentSnapshot:   s,
		stopSnapshotCheck: make(chan bool),
		manager:           manager,
		grpcManager:       grpc.NewServer(),
		localDataPath:     cfg.LocalDataPath,
	}
	return fs, nil
}

func (fs *Monofs) PostInitStart() error {
	lis, err := net.Listen("tcp", fs.manager.Port)
	if err != nil {
		return err
	}
	pb.RegisterMonofsManagerServer(fs.grpcManager, fs.manager)
	go func() {
		if err := fs.grpcManager.Serve(lis); err != nil {
			fs.log.Errorf("failed to serve: %v", err)
		}
	}()
	return nil
}

func (fs *Monofs) Stop() error {
	fs.grpcManager.Stop()
	fs.manager.Stop()
	return nil
}
