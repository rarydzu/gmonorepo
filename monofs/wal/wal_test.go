package wal

import (
	"embed"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/rarydzu/gmonorepo/monofs/monocache"
	"github.com/rarydzu/gmonorepo/utils"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	expectedDbEntries = 20
)

//go:embed testdata/*.wal
var walFiles embed.FS

// generateRandom generates random slice if bytes for testing
func generateRandom() []byte {
	size := rand.Intn(1124)
	if size < 100 {
		size += 100
	}
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(rand.Intn(255))
	}
	return b
}

func TestWalReply(t *testing.T) {
	db, err := leveldb.OpenFile(path.Join(t.TempDir(), "testWalReply"), nil)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := New(t.TempDir(), db)
	if err != nil {
		t.Fatal(err)
	}
	tombstoned := true
	rand.Seed(time.Now().UnixNano())
	nrOfItems := 1020
	for i := 0; i < 1020; i++ {
		kmod := i % expectedDbEntries
		if kmod == 0 {
			tombstoned = !tombstoned
		}
		e := &Entry{
			Key:        []byte(fmt.Sprintf("test%d", kmod)),
			Value:      generateRandom(),
			Tombstoned: tombstoned,
		}
		if err := wal.AddEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	wal.Close()
	if err := wal.OpenLastWALFile(); err != nil {
		t.Fatal(err)
	}
	entries, err := wal.Reply()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, nrOfItems, len(entries))
}

func TestWAL(t *testing.T) {
	db, err := leveldb.OpenFile(path.Join(t.TempDir(), "walTest"), nil)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := New(t.TempDir(), db)
	if err != nil {
		t.Fatal(err)
	}
	tombstoned := true
	for i := 0; i < 1020; i++ {
		kmod := i % expectedDbEntries
		if kmod == 0 {
			tombstoned = !tombstoned
		}
		e := &Entry{
			Key:        []byte(fmt.Sprintf("test%d", kmod)),
			Value:      []byte("test"),
			Tombstoned: tombstoned,
		}
		if err := wal.AddEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	o := make(chan string, 1020)
	if _, err := wal.Dump(o, nil); err != nil {
		t.Fatal(err)
	}
	if err := wal.Wait(); err != nil {
		t.Fatal(err)
	}
	numberOfKeys := 0
	iterator := db.NewIterator(nil, nil)
	for iterator.Next() {
		numberOfKeys++
	}
	iterator.Release()
	if err := iterator.Error(); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectedDbEntries, numberOfKeys)
}

func TestWALReplyLong(t *testing.T) {
	data, _ := walFiles.ReadFile("testdata/0.wal")
	f, err := os.Create(t.TempDir() + "/test_0.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = f.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	f.Seek(0, 0)
	db, err := leveldb.OpenFile(path.Join(t.TempDir(), "testWalReplyLong"), nil)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := New(t.TempDir(), db)
	if err != nil {
		t.Fatal(err)
	}
	wal.file.Close()
	wal.file = f
	entries, err := wal.Reply()
	if err != nil {
		t.Fatal(err)
	}
	cacheTable := make(map[uint64]*monocache.CacheItem)
	for _, value := range entries {
		item := &monocache.CacheItem{}
		if err := item.Unmarshall(value.Value); err != nil {
			t.Fatal(err)
		}
		cacheTable[utils.BytesToUint64(value.Key)] = item
	}
	tombstoned := 0
	live := 0
	for _, v := range cacheTable {
		if v.Tombstoned {
			tombstoned++
		} else {
			live++
		}
	}
	assert.Equal(t, 17, tombstoned)
	assert.Equal(t, 79, live)
}
