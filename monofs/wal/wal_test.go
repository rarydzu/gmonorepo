package wal

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

const (
	expectedDbEntries = 20
)

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
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
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
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
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
	if err := wal.Dump(o); err != nil {
		t.Fatal(err)
	}
	if err := wal.Wait(); err != nil {
		t.Fatal(err)
	}
	numberOfKeys := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if len(k) != 0 {
				numberOfKeys++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectedDbEntries, numberOfKeys)
}
