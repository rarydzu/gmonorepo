// Create database for inodes
package fsdb

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	monostat "github.com/rarydzu/gmonorepo/monoclient/stat"
	"github.com/rarydzu/gmonorepo/monofs/config"
	"github.com/rarydzu/gmonorepo/monofs/monocache"
	"github.com/rarydzu/gmonorepo/monofs/wal"
	"github.com/rarydzu/gmonorepo/utils"
	"github.com/ztrue/tracerr"
)

//TODO add caching layer for parent,names pairs with ttl whcih can speed up dir lookups

var ErrNoSuchInode = errors.New("not such inode")

type Fsdb struct {
	istore     *badger.DB
	astore     *badger.DB
	Quit       chan bool
	path       string
	failedFile string
	iCache     *monocache.CacheTable
	Wal        *wal.WAL
	StatClient *monostat.Client
}

// New creates a new fsdb
func New(config *config.Config) (*Fsdb, error) {
	var err error
	var istore *badger.DB
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
	istore, err = badger.Open(badger.DefaultOptions(ipath).WithLogger(nil))
	if err != nil {
		return nil, err
	}
	astore, err := badger.Open(badger.DefaultOptions(apath).WithLogger(nil))
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

	fsdb := &Fsdb{
		istore:     istore,
		astore:     astore,
		Quit:       make(chan bool),
		path:       config.Path,
		failedFile: fmt.Sprintf("%s/broken.marker", config.Path),
		iCache:     monocache.NewCacheTable(config.CacheSize),
		Wal:        w,
		StatClient: config.StatClient,
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
		fsdb.iCache.Set(cacheItem)
	}
	fsdb.iCache.SetAddCallback(func(key uint64, value []byte) error {
		entry := &wal.Entry{
			Key:        utils.Uint64ToBytes(key),
			Value:      value,
			Tombstoned: false,
		}
		return fsdb.Wal.AddEntry(entry)
	})
	fsdb.iCache.SetDelCallback(func(key uint64, value []byte) error {
		entry := &wal.Entry{
			Key:        utils.Uint64ToBytes(key),
			Value:      value,
			Tombstoned: true,
		}
		return fsdb.Wal.AddEntry(entry)
	})
	fsdb.iCache.SetCacheFullCallback(func(output chan string) error {
		// dump must include generation and cache so it can set item.processed
		return w.Dump(output)
	})
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := istore.RunValueLogGC(0.7)
				if err != nil && !errors.Is(err, badger.ErrNoRewrite) {
					log.Printf("GC: %v", err)
				}
				err = astore.RunValueLogGC(0.7)
				if err != nil && !errors.Is(err, badger.ErrNoRewrite) {
					log.Printf("GC: %v", err)
				}
			case <-fsdb.Quit:
				ticker.Stop()
				return
			}
		}
	}()
	return fsdb, nil
}

// GetIStoreHandler returns a handler to the istore
func (db *Fsdb) GetIStoreHandler() *badger.DB {
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
	err := db.istore.Update(func(txn *badger.Txn) error {
		inodeKey := DbInodeKey(inode.ParentID, inode.Name)
		if err := txn.Set(inodeKey, inode.DbID()); err != nil {
			return err
		}
		if attr {
			buf, err := inode.Attrs.Marshall()
			if err != nil {
				return err
			}
			err = db.iCache.Add(inode.InodeID, buf, 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return db.MarkAsFailed(err)
}

// GetInode gets an inode
func (db *Fsdb) GetInode(parent uint64, name string, attr bool) (*Inode, error) {
	var inode Inode
	itxn := db.istore.NewTransaction(false)
	defer itxn.Discard()
	inodeKey := DbInodeKey(parent, name)
	item, err := itxn.Get(inodeKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrNoSuchInode
		}
		return nil, db.MarkAsFailed(err)
	}
	_ = item.Value(func(val []byte) error {
		inode.InodeID = utils.BytesToUint64(val)
		return nil
	})
	inode.ParentID = parent
	inode.Name = name
	if attr {
		val, err := db.iCache.Get(inode.InodeID)
		if err == nil {
			return &inode, inode.Attrs.Unmarshall(val)
		}
		if err == monocache.ErrKeyDeleted {
			return nil, ErrNoSuchInode
		}
		if err != monocache.ErrKeyNotFound {
			return nil, err
		}
		atxn := db.astore.NewTransaction(false)
		defer atxn.Discard()
		item, err := atxn.Get(inode.DbID())
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil, ErrNoSuchInode
			}
			return nil, db.MarkAsFailed(err)
		}
		if err := item.Value(func(val []byte) error {
			return inode.Attrs.Unmarshall(val)
		}); err != nil {
			return nil, db.MarkAsFailed(err)
		}
	}
	return &inode, err
}

// DeleteInode deletes an inode
func (db *Fsdb) DeleteInode(inode *Inode, attr bool) error {
	itxn := db.istore.NewTransaction(true)
	defer itxn.Discard()
	inodeKey := DbInodeKey(inode.ParentID, inode.Name)
	if err := itxn.Delete(inodeKey); err != nil {
		return db.MarkAsFailed(err)
	}
	if attr {
		err := db.iCache.Del(inode.InodeID)
		if err == nil {
			return db.MarkAsFailed(itxn.Commit())
		}
		atxn := db.astore.NewTransaction(true)
		defer atxn.Discard()
		if err := atxn.Delete(inode.DbID()); err != nil {
			return db.MarkAsFailed(err)
		}
		if err := atxn.Commit(); err != nil {
			return db.MarkAsFailed(err)
		}
	}
	return db.MarkAsFailed(itxn.Commit())
}

// CreateInodeAttrs stores an inode's attributes
func (db *Fsdb) CreateInodeAttrs(inode *Inode) error {
	buf, err := inode.Attrs.Marshall()
	if err != nil {
		return err
	}
	return db.iCache.Add(inode.InodeID, buf, 0)
}

// GetInodeAttrs gets an inode's fuseops attributes
func (db *Fsdb) GetInodeAttrs(ID uint64) (fuseops.InodeAttributes, error) {
	i, err := db.GetFsdbInodeAttributes(ID)
	return i.InodeAttributes, err
}

// GetFsdbInodeAttributes gets an inode's attributes
func (db *Fsdb) GetFsdbInodeAttributes(ID uint64) (InodeAttributes, error) {
	var iattrs InodeAttributes
	val, err := db.iCache.Get(ID)
	if err == nil {
		return iattrs, iattrs.Unmarshall(val)
	}
	if err == monocache.ErrKeyDeleted {
		return iattrs, ErrNoSuchInode
	}
	err = db.astore.View(func(txn *badger.Txn) error {
		item, err := txn.Get(utils.Uint64ToBytes(ID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return iattrs.Unmarshall(val)
		})
	})
	return iattrs, err
}

// UpdateInodeAttrs sets an inode's attributes
func (db *Fsdb) UpdateInodeAttrs(ID uint64, attr fuseops.InodeAttributes) error {
	//TODO: this should be better optimised global change fuseops.InodeAttributes to InodeAttributes
	var iattr InodeAttributes
	val, err := db.iCache.Get(ID)
	if err == nil {
		if err := iattr.Unmarshall(val); err != nil {
			return err
		}
	} else if err != monocache.ErrKeyNotFound {
		return err
	} else if err == monocache.ErrKeyDeleted {
		return ErrNoSuchInode
	} else {
		err = db.astore.View(func(txn *badger.Txn) error {
			item, err := txn.Get(utils.Uint64ToBytes(ID))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				return iattr.Unmarshall(val)
			})
		})
		if err != nil {
			return err
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
	return db.iCache.Add(ID, buf, 0)
}

// DeleteInodeAttrs deletes an inode's attributes
func (db *Fsdb) DeleteInodeAttrs(inodeID uint64) error {
	if err := db.iCache.Del(inodeID); err == nil {
		return nil
	}
	return db.astore.Update(func(txn *badger.Txn) error {
		return txn.Delete(utils.Uint64ToBytes(inodeID))
	})
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
func (db *Fsdb) GetChildren(inodeID uint64, lastEntry string, limit int) ([]*Inode, error) {
	values := []*Inode{}
	tmpValues := []*Inode{}
	limitCounter := 0
	// add . and .. dir entries if name is ""
	if lastEntry == "" {
		inode := &Inode{
			InodeID: inodeID,
			Name:    ".",
			Attrs:   InodeAttributes{},
		}
		i, err := db.GetFsdbInodeAttributes(inode.InodeID)
		if err != nil {
			return values, fmt.Errorf("failed to get inode %d attributes: %w", inode.InodeID, err)
		}
		inode.Attrs = i
		values = append(values, inode)
		parenoInodeId := inode.ParentID
		if parenoInodeId == 0 {
			parenoInodeId = inode.InodeID
		}
		parentInode := &Inode{
			InodeID: parenoInodeId,
			Name:    "..",
			Attrs:   InodeAttributes{},
		}
		pi, err := db.GetFsdbInodeAttributes(parentInode.InodeID)
		if err != nil {
			return values, fmt.Errorf("failed to get inode %d attributes: %w", parentInode.InodeID, err)
		}
		parentInode.Attrs = pi
		values = append(values, parentInode)
		limitCounter = 2
	}
	err := db.istore.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		seekPrefix := []byte(fmt.Sprintf("#%d:%s~", inodeID, lastEntry))
		if lastEntry == "" {
			seekPrefix = []byte(fmt.Sprintf("#%d:", inodeID))
		}
		validPrefix := []byte(fmt.Sprintf("#%d:", inodeID))
		for it.Seek(seekPrefix); it.ValidForPrefix(validPrefix); it.Next() {
			if limitCounter >= limit {
				break
			}
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(v []byte) error {
				keySlice := strings.Split(string(k), ":")
				//extract name from key
				name := keySlice[1]
				if len(name) == 0 {
					return nil
				}
				// extract inodeID from value
				iID := utils.BytesToUint64(v)
				// add to tmpValues
				inode := &Inode{
					InodeID:  iID,
					ParentID: inodeID,
					Name:     name,
					Attrs:    InodeAttributes{},
				}
				tmpValues = append(tmpValues, inode)
				limitCounter++
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	for _, inode := range tmpValues {
		val, err := db.iCache.Get(inode.InodeID)
		if err == nil {
			if err := inode.Attrs.Unmarshall(val); err != nil {
				return values, err
			}
			values = append(values, inode)
			continue
		}
		err = db.astore.View(func(txn *badger.Txn) error {
			item, err := txn.Get(inode.DbID())
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				return inode.Attrs.Unmarshall(val)
			})
		})
		if err == nil {
			values = append(values, inode)
		}
	}
	return values, err
}

// GetChildrenCount gets the number of children of an inode
func (db *Fsdb) GetChildrenCount(inodeID uint64) (int, error) {
	count := 0
	err := db.istore.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		seekPrefix := []byte(fmt.Sprintf("#%d:", inodeID))
		validPrefix := []byte(fmt.Sprintf("#%d:", inodeID))
		for it.Seek(seekPrefix); it.ValidForPrefix(validPrefix); it.Next() {
			count++
		}
		return nil
	})
	return count, err
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
