package lastinode

import (
	"fmt"
	"os"
	"path"
	"sync"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/nutsdb/nutsdb"
	"github.com/rarydzu/gmonorepo/utils"
)

const bucket = "inodes"

type LastInodeEngine struct {
	Path       string
	LastInode  fuseops.InodeID
	InodeQueue chan fuseops.InodeID
	//wg         sync.WaitGroup
	shutdownWait sync.WaitGroup
	shutdown     chan bool
	lastFile     *os.File
	rlock        sync.RWMutex
	db           *nutsdb.DB
}

func New(path string, db *nutsdb.DB) *LastInodeEngine {
	return &LastInodeEngine{
		LastInode: 0,
		Path:      path,
		shutdown:  make(chan bool),
		db:        db,
	}
}

// Init  initialize last inode engine
func (l *LastInodeEngine) Init() error {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return fmt.Errorf("getrlimit: %v", err)
	}
	l.InodeQueue = make(chan fuseops.InodeID, limit.Cur)
	if err := l.readLastInode(); err != nil {
		return err
	}
	l.shutdownWait.Add(1)
	go l.worker()
	return l.lock(true)
}

// create or remove lock file depends on flag
func (l *LastInodeEngine) lock(flag bool) error {
	if flag {
		f, err := os.Create(path.Join(l.Path, "lastinode.lock"))
		if err != nil {
			return err
		}
		defer f.Close()
	} else {
		if _, err := os.Stat(path.Join(l.Path, "lastinode.lock")); err == nil {
			if err := os.Remove(path.Join(l.Path, "lastinode.lock")); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *LastInodeEngine) addLastInode(inode fuseops.InodeID) error {
	if l.lastFile == nil {
		f, err := os.Create(path.Join(l.Path, "lastinode"))
		if err != nil {
			return err
		}
		l.lastFile = f
	}
	if _, err := l.lastFile.WriteString(fmt.Sprintf("%d", inode)); err != nil {
		return err
	}
	_, err := l.lastFile.Seek(0, 0)
	return err
}

func (l *LastInodeEngine) getInodeFromDb() error {
	maxInode := uint64(0)
	tx, err := l.db.Begin(false)
	if err != nil {
		return err
	}
	iterator := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{Reverse: false})
	ok, err := iterator.SetNext()
	if err != nil {
		if nutsdb.IsBucketNotFound(err) {
			return nil
		}
		return err
	}
	for ok {
		inode := utils.BytesToUint64(iterator.Entry().Value)
		if inode > maxInode {
			maxInode = inode
		}
		ok, err = iterator.SetNext()
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	l.rlock.Lock()
	l.LastInode = fuseops.InodeID(maxInode)
	l.rlock.Unlock()
	return nil
}

func (l *LastInodeEngine) readLastInode() error {
	if _, err := os.Stat(path.Join(l.Path, "lastinode.lock")); err != nil {
		if os.IsNotExist(err) {
			f, err := os.Open(path.Join(l.Path, "lastinode"))
			if err != nil {
				if os.IsNotExist(err) {
					return l.getInodeFromDb()
				}
				return err
			}
			defer f.Close()
			var inode fuseops.InodeID
			if _, err := fmt.Fscanf(f, "%d", &inode); err != nil {
				return err
			}
			l.rlock.Lock()
			l.LastInode = inode
			l.rlock.Unlock()
		} else {
			return fmt.Errorf("lastinode stat: %v", err)
		}
	} else {
		return l.getInodeFromDb()
	}
	return nil
}

func (l *LastInodeEngine) worker() {
	defer l.shutdownWait.Done()
	for {
		select {
		case inode := <-l.InodeQueue:
			l.rlock.Lock()
			l.LastInode = inode
			l.rlock.Unlock()
			l.addLastInode(inode)
		case <-l.shutdown:
			if len(l.InodeQueue) == 0 {
				return
			}
		}
	}
}

// StoreLastInode store last inode
func (l *LastInodeEngine) StoreLastInode(lastInode fuseops.InodeID) {
	l.InodeQueue <- lastInode
}

// GetLastInode get last inode
func (l *LastInodeEngine) GetLastInode() fuseops.InodeID {
	l.rlock.RLock()
	inode := l.LastInode
	l.rlock.RUnlock()
	return inode
}

// Close close last inode engine
func (l *LastInodeEngine) Close() error {
	close(l.shutdown)
	l.shutdownWait.Wait()
	if l.lastFile != nil {
		l.lastFile.Sync()
		l.lastFile.Close()
	}
	return l.lock(false)
}

// ForgetInode forget inode
func (l *LastInodeEngine) ForgetInode(inode fuseops.InodeID) error {
	//TODO
	return nil
}
