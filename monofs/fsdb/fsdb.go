// Create database for inodes
package fsdb

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	monostat "github.com/rarydzu/gmonorepo/monoclient/stat"
	"github.com/rarydzu/gmonorepo/monofs/config"
	"github.com/rarydzu/gmonorepo/monofs/monocache"
	"github.com/rarydzu/gmonorepo/monofs/wal"
	msnapshot "github.com/rarydzu/gmonorepo/snapshot"
	"github.com/rarydzu/gmonorepo/utils"
	"github.com/syndtr/goleveldb/leveldb"
	lfilter "github.com/syndtr/goleveldb/leveldb/filter"
	lopt "github.com/syndtr/goleveldb/leveldb/opt"
	lutil "github.com/syndtr/goleveldb/leveldb/util"

	"github.com/ztrue/tracerr"
)

// TODO add caching layer for parent,names pairs with ttl whcih can speed up dir lookups

var ErrNoSuchInode = errors.New("not such inode")

type Fsdb struct {
	istore     *leveldb.DB
	astore     *leveldb.DB
	Quit       chan bool
	path       string
	failedFile string
	aCache     *monocache.CacheTable
	Wal        *wal.WAL
	StatClient *monostat.Client
	Snapshot   *msnapshot.Snapshot
}

// New creates a new fsdb
func New(config *config.Config) (*Fsdb, error) {
	var err error
	var istore *leveldb.DB
	ipath := fmt.Sprintf("%s/inodes", config.Path)
	apath := fmt.Sprintf("%s/attrs", config.Path)
	wpath := fmt.Sprintf("%s/wal", config.Path)
	err = os.MkdirAll(ipath, 0755)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(apath, 0755)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(wpath, 0755)
	if err != nil {
		return nil, err
	}
	io := &lopt.Options{
		Filter: lfilter.NewBloomFilter(1000),
	}
	istore, err = leveldb.OpenFile(ipath, io)
	if err != nil {
		return nil, err
	}
	ao := &lopt.Options{
		Filter: lfilter.NewBloomFilter(1000),
	}
	astore, err := leveldb.OpenFile(apath, ao)
	if err != nil {
		istore.Close()
		return nil, err
	}
	w, err := wal.New(wpath, astore)
	if err != nil {
		istore.Close()
		astore.Close()
		return nil, err
	}
	s, err := msnapshot.New(config.Path, istore, astore, w)
	if err != nil {
		istore.Close()
		astore.Close()
		w.Close()
		return nil, err
	}
	fsdb := &Fsdb{
		istore:     istore,
		astore:     astore,
		Quit:       make(chan bool),
		path:       config.Path,
		failedFile: fmt.Sprintf("%s/broken.marker", config.Path),
		aCache:     monocache.NewCacheTable(config.CacheSize),
		Wal:        w,
		StatClient: config.StatClient,
		Snapshot:   s,
	}
	if fsdb.CheckIfFailed() {
		if err := fsdb.Fsck(); err != nil {
			fsdb.Close()
			return nil, err
		}
	}
	entries, err := fsdb.Wal.Reply()
	if err != nil {
		fsdb.Close()
		return nil, tracerr.Errorf("replying WAL entries failed: %w", err)
	}
	for _, entry := range entries {
		cacheItem := &monocache.CacheItem{}
		if err := cacheItem.Unmarshall(entry.Value); err != nil {
			return nil, tracerr.Errorf("unmarshalling cache entry failed: %w", err)
		}
		fsdb.aCache.Set(cacheItem)
	}
	fsdb.aCache.SetAddCallback(func(key uint64, value []byte) error {
		entry := &wal.Entry{
			Key:        utils.Uint64ToBytes(key),
			Value:      value,
			Tombstoned: false,
		}
		return fsdb.Wal.AddEntry(entry)
	})
	fsdb.aCache.SetDelCallback(func(key uint64, value []byte) error {
		entry := &wal.Entry{
			Key:        utils.Uint64ToBytes(key),
			Value:      value,
			Tombstoned: true,
		}
		return fsdb.Wal.AddEntry(entry)
	})
	fsdb.aCache.SetCacheFullCallback(func(output chan string) error {
		// dump must include generation and cache so it can set item.processed
		return w.Dump(output)
	})
	return fsdb, nil
}

// StartSyncSnaphost returns and creates if nessesary current Snapshot name
func (db *Fsdb) StartSyncSnapshot() (string, error) {
	snap, err := db.Snapshot.GetCurrentSnapshot()
	if err != nil {
		return "", err
	}
	if snap != "" {
		return snap, nil
	}
	snapName := fmt.Sprintf("%d", time.Now().UnixNano())
	hash, err := db.Snapshot.CreateSyncSnapshot(snapName)
	if err != nil {
		return "", err
	}
	return hash, nil
}

// GetIStoreHandler returns a handler to the istore
func (db *Fsdb) GetIStoreHandler() *leveldb.DB {
	return db.istore
}

// Close closes the fsdb
func (db *Fsdb) Close() error {
	close(db.Quit)
	ierr := db.istore.Close()
	aerr := db.astore.Close()
	if ierr != nil {
		return ierr
	}
	if err := db.Wal.Close(); err != nil {
		return err
	}
	return aerr
}

// AddInode stores an inode
func (db *Fsdb) AddInode(inode *Inode, attr bool) error {
	inodeKey := DbInodeKey(inode.ParentID, inode.Name)

	if err := db.istore.Put(inodeKey, inode.DbID(), nil); err != nil {
		return db.MarkAsFailed(err)
	}
	if attr {
		buf, err := inode.Attrs.Marshall()
		if err != nil {
			return err
		}
		err = db.aCache.Add(inode.InodeID, buf, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetInode gets an inode
func (db *Fsdb) GetInode(parent uint64, name string, attr bool) (*Inode, error) {
	var inode Inode
	inodeKey := DbInodeKey(parent, name)
	val, err := db.istore.Get(inodeKey, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil, ErrNoSuchInode
		}
		return nil, db.MarkAsFailed(err)
	}
	inode.InodeID = utils.BytesToUint64(val)
	inode.ParentID = parent
	inode.Name = name
	if attr {
		val, err := db.aCache.Get(inode.InodeID)
		if err == nil {
			return &inode, inode.Attrs.Unmarshall(val)
		}
		if err == monocache.ErrKeyDeleted {
			return nil, ErrNoSuchInode
		}
		if err != monocache.ErrKeyNotFound {
			return nil, err
		}
		v, err := db.astore.Get(inode.DbID(), nil)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				return nil, ErrNoSuchInode
			}
			return nil, db.MarkAsFailed(err)
		}
		if err := inode.Attrs.Unmarshall(v); err != nil {
			return nil, err
		}
	}
	return &inode, err
}

// DeleteInode deletes an inode
func (db *Fsdb) DeleteInode(inode *Inode, attr bool) error {
	inodeKey := DbInodeKey(inode.ParentID, inode.Name)
	if err := db.istore.Delete(inodeKey, nil); err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return ErrNoSuchInode
		}
		return err
	}
	if attr {
		err := db.aCache.Del(inode.InodeID)
		if err == nil {
			return nil
		}
		if err := db.astore.Delete(inode.DbID(), nil); err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				return ErrNoSuchInode
			}
			return db.MarkAsFailed(err)
		}
	}
	return nil
}

// CreateInodeAttrs stores an inode's attributes
func (db *Fsdb) CreateInodeAttrs(inode *Inode) error {
	buf, err := inode.Attrs.Marshall()
	if err != nil {
		return err
	}
	return db.aCache.Add(inode.InodeID, buf, 0)
}

// GetInodeAttrs gets an inode's fuseops attributes
func (db *Fsdb) GetInodeAttrs(ID uint64) (fuseops.InodeAttributes, error) {
	i, err := db.GetFsdbInodeAttributes(ID)
	return i.InodeAttributes, err
}

// GetFsdbInodeAttributes gets an inode's attributes
func (db *Fsdb) GetFsdbInodeAttributes(ID uint64) (InodeAttributes, error) {
	var iattrs InodeAttributes
	val, err := db.aCache.Get(ID)
	if err == nil {
		return iattrs, iattrs.Unmarshall(val)
	}
	if err == monocache.ErrKeyDeleted {
		return iattrs, ErrNoSuchInode
	}
	v, err := db.astore.Get(utils.Uint64ToBytes(ID), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return iattrs, ErrNoSuchInode
		}
		return iattrs, db.MarkAsFailed(err)
	}
	return iattrs, iattrs.Unmarshall(v)
}

// UpdateInodeAttrs sets an inode's attributes
func (db *Fsdb) UpdateInodeAttrs(ID uint64, attr fuseops.InodeAttributes) error {
	//TODO: this should be better optimised global change fuseops.InodeAttributes to InodeAttributes
	var iattr InodeAttributes
	val, err := db.aCache.Get(ID)
	if err == nil {
		if err := iattr.Unmarshall(val); err != nil {
			return err
		}
	} else if err != monocache.ErrKeyNotFound {
		return err
	} else if err == monocache.ErrKeyDeleted {
		return ErrNoSuchInode
	} else {
		v, err := db.astore.Get(utils.Uint64ToBytes(ID), nil)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				return ErrNoSuchInode
			}
			return db.MarkAsFailed(err)
		}
		if err := iattr.Unmarshall(v); err != nil {
			return db.MarkAsFailed(err)
		}
	}
	//TODO update only the changed fields
	iattr.Size = attr.Size
	iattr.Mode = attr.Mode
	iattr.Nlink = attr.Nlink
	iattr.Uid = attr.Uid
	iattr.Gid = attr.Gid
	iattr.Atime = attr.Atime
	iattr.Mtime = attr.Mtime
	buf, err := iattr.Marshall()
	if err != nil {
		return err
	}
	return db.aCache.Add(ID, buf, 0)
}

// DeleteInodeAttrs deletes an inode's attributes
func (db *Fsdb) DeleteInodeAttrs(inodeID uint64) error {
	if err := db.aCache.Del(inodeID); err == nil {
		return nil
	}
	return db.astore.Delete(utils.Uint64ToBytes(inodeID), nil)
}

// MarkAsFailed marks database as bad and force check
func (db *Fsdb) MarkAsFailed(err error) error {
	if err == nil {
		return nil
	}
	// create failed file
	f, oserr := os.Create(db.failedFile)
	if oserr != nil {
		return tracerr.Errorf("(%w). Failed to mark db as failed: %w", err, oserr)
	}
	defer f.Close()
	f.WriteString(fmt.Sprintf("Error: %v", err))
	return tracerr.Wrap(err)
}

// CheckIfFailed checks if database is marked as failed
func (db *Fsdb) CheckIfFailed() bool {
	_, err := os.Stat(db.failedFile)
	return err == nil
}

// GetChildren gets the children of an inode
func (db *Fsdb) GetChildren(inodeID uint64, offset int, limit int, key []byte) ([]*Inode, int, error) {
	values := []*Inode{}
	tmpValues := []*Inode{}
	prefix := []byte(fmt.Sprintf("%d:", inodeID))
	iter := db.istore.NewIterator(lutil.BytesPrefix(prefix), nil)
	if offset > 0 {
		iter.Seek(key)
	}
	for iter.Next() {
		kSlice := strings.Split(string(iter.Key()), ":")
		name := kSlice[1]
		if len(name) == 0 {
			continue
		}
		iid := utils.BytesToUint64(iter.Value())
		inode := &Inode{
			InodeID:  iid,
			ParentID: inodeID,
			Name:     name,
			Attrs:    InodeAttributes{},
		}
		tmpValues = append(tmpValues, inode)
		if len(tmpValues) >= limit {
			break
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return values, len(values), err
	}
	for _, inode := range tmpValues {
		val, err := db.aCache.Get(inode.InodeID)
		if err == nil {
			if err := inode.Attrs.Unmarshall(val); err != nil {
				return values, len(values), err
			}
			values = append(values, inode)
			continue
		}
		if errors.Is(err, monocache.ErrKeyNotFound) {
			v, err := db.astore.Get(inode.DbID(), nil)
			if err != nil {
				if errors.Is(err, leveldb.ErrNotFound) {
					continue
				}
				return values, len(values), err
			}
			if err := inode.Attrs.Unmarshall(v); err != nil {
				return values, len(values), err
			}
			values = append(values, inode)
		} else if errors.Is(err, monocache.ErrKeyDeleted) {
			continue
		} else {
			break
		}
	}
	return values, len(values), nil
}

// GetChildrenCount gets the number of children of an inode
func (db *Fsdb) GetChildrenCount(inodeID uint64) (int, error) {
	c := 0
	prefix := []byte(fmt.Sprintf("%d:", inodeID))
	iter := db.istore.NewIterator(lutil.BytesPrefix(prefix), nil)
	for iter.Next() {
		c++
	}
	iter.Release()
	return c, iter.Error()
}

// Fsck check databse for errors
func (db *Fsdb) Fsck() error {
	return fmt.Errorf("FSCK not implemented")
}

func InodeDirentType(mode os.FileMode) fuseutil.DirentType {
	if mode.IsDir() {
		return fuseutil.DT_Directory
	}
	if mode.IsRegular() {
		return fuseutil.DT_File
	}
	if mode&os.ModeSymlink != 0 {
		return fuseutil.DT_Link
	}
	return fuseutil.DT_Unknown
}
