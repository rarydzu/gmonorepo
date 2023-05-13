package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"golang.org/x/sync/errgroup"
)

type Entry struct {
	Key        []byte
	Value      []byte
	Tombstoned bool
}

//TODO convert to base64

type WAL struct {
	// path is path to WAL directory
	path string
	// file is current WAL file
	file *os.File
	// fileCounter	 is counter of WAL files
	fileCounter int
	// db is database for WAL
	db *badger.DB
	sync.RWMutex
	// encoder is encoder for WAL
	encoder *Encoder
	//errs is channel for errors
	g *errgroup.Group
}

// New creates or opens new WAL object
func New(path string, db *badger.DB) (*WAL, error) {
	w := &WAL{
		path:        path,
		fileCounter: 0,
		db:          db,
		file:        nil,
		g:           &errgroup.Group{},
	}
	w.Lock()
	defer w.Unlock()
	return w, w.OpenLastWALFile()
}

// OpenLastWALFile opens last WAL file
func (w *WAL) OpenLastWALFile() error {
	err := filepath.Walk(w.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Size() > 0 {
			if strings.HasSuffix(path, ".wal") {
				fileName := path
				parts := strings.Split(path, "/")
				if len(parts) > 0 {
					fileName = parts[len(parts)-1]
				}
				fc, err := strconv.Atoi(strings.TrimSuffix(fileName, ".wal"))
				if err != nil {
					return err
				}
				if fc > w.fileCounter {
					w.fileCounter = fc
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	fileName := fmt.Sprintf("%s/%d.wal", w.path, w.fileCounter)
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		return err
	}
	w.file = f
	w.encoder = NewEncoder(f)
	return nil
}

// CreateNewWALFile creates new WAL file
func (w *WAL) CreateNewWALFile() error {
	if w.file != nil {
		w.file.Close()
		w.file = nil
		w.fileCounter++
	}
	f, err := os.Create(fmt.Sprintf("%s/%d.wal", w.path, w.fileCounter))
	if err != nil {
		return err
	}
	w.file = f
	w.encoder = NewEncoder(f)
	w.fileCounter++
	return nil
}

// AddEntry adds entry to WAL
func (w *WAL) AddEntry(entry *Entry) error {
	w.Lock()
	defer w.Unlock()
	if err := w.encoder.Encode(entry); err != nil {
		return err
	}
	return nil
}

// Close closes WAL
func (w *WAL) Close() error {
	w.Lock()
	defer w.Unlock()
	w.file.Sync()
	return w.file.Close()
}

// Dump  dumps WAL to database and creates new WAL file
func (w *WAL) Dump(output chan string) error {
	w.Lock()
	defer w.Unlock()
	previousFileName := fmt.Sprintf("%s/%d.wal", w.path, w.fileCounter)
	if err := w.CreateNewWALFile(); err != nil {
		return err
	}
	w.g.Go(func() error {
		return w.internalDBDump(previousFileName, output)
	})
	return nil
}

// Wait waits for all WAL dumps to finish
func (w *WAL) Wait() error {
	return w.g.Wait()
}

// internalDump dumps WAL to database
func (w *WAL) internalDBDump(fileName string, output chan string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := NewDecoder(f)
	wb := w.db.NewWriteBatch()
	defer wb.Cancel()
	for {
		var entry Entry
		if err := decoder.Decode(&entry); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		if entry.Tombstoned {
			if err := wb.Delete(entry.Key); err != nil {
				return err
			}
			output <- string(entry.Key)
		} else {
			if err := wb.Set(entry.Key, entry.Value); err != nil {
				return err
			}
			output <- string(entry.Key)
		}
	}
	f.Close()
	err = wb.Flush()
	if err != nil {
		return err
	}
	return os.Remove(fileName)
}

// Reply reads content of WAL file and return list of entries
func (w *WAL) Reply() ([]Entry, error) {
	decoder := NewDecoder(w.file)
	var entries []Entry
	counter := 0
	for {
		counter += 1
		var entry Entry
		if err := decoder.Decode(&entry); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("error while decoding WAL file entry %d : %w", counter, err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
