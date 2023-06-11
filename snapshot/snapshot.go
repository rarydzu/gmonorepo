package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/rarydzu/gmonorepo/monofs/wal"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	// CurrentSnapshotName 	current snapshot name
	CurrentSnapshotName = "current"
	//SnapshostsDataPath 	snapshots data path
	SnapshostsDataPath = "data"
)

type SnapshotResult struct {
	Name     string
	Id       string
	Message  string
	Error    error
	Creation time.Time
}

type Snapshot struct {
	SnapshotPath string
	Name         string
	db           *leveldb.DB
	inodeDB      *leveldb.DB
	attrDB       *leveldb.DB
	w            *wal.WAL
}

// New Create a new snapshot
func New(spath string, inodeDB, attrDB *leveldb.DB, w *wal.WAL) (*Snapshot, error) {
	if spath == "" {
		return nil, errors.New("path cannot be empty")
	}
	db, err := leveldb.OpenFile(path.Join(spath, "db"), nil)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(path.Join(spath, SnapshostsDataPath), 0755)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		SnapshotPath: spath,
		db:           db,
		inodeDB:      inodeDB,
		attrDB:       attrDB,
		w:            w,
	}, nil
}

// newSnapshot Create a new snapshot
func (s *Snapshot) newSnapshot(name string) (string, error) {
	if name == "" {
		return "", errors.New("name cannot be empty")
	}

	// create Snapshot hash
	sha256 := sha256.New()
	sha256.Write([]byte(name))
	hash := fmt.Sprintf("%x", sha256.Sum(nil))

	ok := false
	cSnapshot, err := s.db.Get([]byte(CurrentSnapshotName), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			ok = true
		} else {
			return "", err
		}
	}
	if string(cSnapshot) != hash {
		ok = true
	}
	if !ok {
		return "", fmt.Errorf("snapshot %q already exists", name)
	}

	inodeSnapshot, err := s.inodeDB.GetSnapshot()
	if err != nil {
		return "", err
	}
	attrSnapshot, err := s.attrDB.GetSnapshot()
	if err != nil {
		return "", err
	}
	inodeSnapshotDB, err := leveldb.OpenFile(path.Join(s.SnapshotPath, SnapshostsDataPath, string(cSnapshot), "inode"), nil)
	if err != nil {
		return "", err
	}
	attrSnapshotDB, err := leveldb.OpenFile(path.Join(s.SnapshotPath, SnapshostsDataPath, string(cSnapshot), "attrs"), nil)
	if err != nil {
		return "", err
	}
	s.Name = name
	//TODO SORT OUT WAL
	defer inodeSnapshotDB.Close()
	defer attrSnapshotDB.Close()
	is := inodeSnapshot.NewIterator(nil, nil)
	batch := new(leveldb.Batch)
	for is.Next() {
		batch.Put(is.Key(), is.Value())
	}
	is.Release()
	err = inodeSnapshotDB.Write(batch, nil)
	if err != nil {
		return "", err
	}
	as := attrSnapshot.NewIterator(nil, nil)
	batch = new(leveldb.Batch)
	for as.Next() {
		batch.Put(as.Key(), as.Value())
	}
	as.Release()
	err = attrSnapshotDB.Write(batch, nil)
	if err != nil {
		return "", err
	}
	err = s.db.Put([]byte(CurrentSnapshotName), []byte(hash), nil)
	if err != nil {
		return "", err
	}
	err = s.db.Put([]byte(name), []byte(hash), nil)
	if err != nil {
		return "", err
	}
	s.Name = name
	return hash, nil
}

// CreateSyncSnapshot Create a new snapshot
func (s *Snapshot) CreateSyncSnapshot(name string) (string, error) {
	return s.newSnapshot(name)
}

// GetCurrentSnapshot get snapshot
func (s *Snapshot) GetCurrentSnapshot() (string, error) {
	if s.Name != "" {
		return s.Name, nil
	}
	n, err := s.db.Get([]byte(CurrentSnapshotName), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return "", nil
		}
		return "", err
	}
	s.Name = string(n)
	return s.Name, nil
}

// DeleteSnapshot delete snapshot
func (s *Snapshot) DeleteSnapshot(ctx context.Context, name string) error {
	return fmt.Errorf("not implemented")
}

// ListSnapshots list snapshots
func (s *Snapshot) ListSnapshots(ctx context.Context) ([]string, error) {
	l := []string{}
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		l = append(l, string(iter.Key()))
	}
	iter.Release()
	return l, nil
}
