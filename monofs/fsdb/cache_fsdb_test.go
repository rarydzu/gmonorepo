package fsdb

import (
	"fmt"
	"os"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/rarydzu/gmonorepo/monofs/config"
	"github.com/rarydzu/gmonorepo/monofs/monocache"
	"github.com/rarydzu/gmonorepo/utils"
)

func TestCacheStore(t *testing.T) {
	os.Setenv("MONOFS_DEV_RUN", "testing")
	config := &config.Config{
		Path:           t.TempDir(),
		FilesystemName: "test",
		CacheSize:      10,
	}
	db, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	maxInodes := 22
	// add items and check if they only in cache
	// force cache to dump
	// check if only if db
	for i := 0; i < 10; i++ {
		inode := &Inode{
			InodeID:  uint64(i),
			ParentID: 1,
			Name:     fmt.Sprintf("inode-%d", i),
			Attrs: InodeAttributes{
				InodeAttributes: fuseops.InodeAttributes{
					Size:  4096,
					Nlink: 1,
					Mode:  0755 | os.ModeDir,
					Mtime: TestInode.Attrs.Mtime,
					Uid:   0,
					Gid:   0,
				},
			},
		}
		if err := db.AddInode(inode, true); err != nil {
			t.Fatal(err)
		}
	}
	// in cache not in db
	for i := 0; i < 10; i++ {
		if _, err := db.iCache.Get(uint64(i)); err != nil {
			t.Errorf("item not in cache: %d", i)
		}
		// get item from badger
		err := db.astore.View(func(txn *badger.Txn) error {
			_, err := txn.Get(utils.Uint64ToBytes(uint64(i)))
			return err
		})
		if err == nil {
			t.Errorf("item %d in db", i)
		}
	}
	for i := 10; i < maxInodes; i++ {
		inode := &Inode{
			InodeID:  uint64(i),
			ParentID: 1,
			Name:     fmt.Sprintf("inode-%d", i),
			Attrs: InodeAttributes{
				InodeAttributes: fuseops.InodeAttributes{
					Size:  4096,
					Nlink: 1,
					Mode:  0755 | os.ModeDir,
					Mtime: TestInode.Attrs.Mtime,
					Uid:   0,
					Gid:   0,
				},
			},
		}
		if err := db.AddInode(inode, true); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * time.Second)
	notFoundInodes := 0
	for i := 0; i < maxInodes; i++ {
		if _, err := db.iCache.Get(uint64(i)); err != nil {
			if err != monocache.ErrKeyNotFound {
				t.Errorf("failed item %d: %v", i, err)
			} else {
				notFoundInodes++
			}
		}
		// get item from badger
		err := db.astore.View(func(txn *badger.Txn) error {
			_, err := txn.Get(utils.Uint64ToBytes(uint64(i)))
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1 * time.Second)
	if db.iCache.Len()+notFoundInodes != maxInodes {
		t.Errorf("cache size: %d, not found inodes: %d", db.iCache.Len(), notFoundInodes)
	}
}
