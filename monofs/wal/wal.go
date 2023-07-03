package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/sync/errgroup"
)

const (
	// WALBatchMaxSize is max size of WAL batch size
	WALBatchMaxSize = 500
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
	db *leveldb.DB
	sync.RWMutex
	// encoder is encoder for WAL
	encoder *Encoder
	//errs is channel for errors
	g *errgroup.Group
}

// New creates or opens new WAL object
func New(path string, db *leveldb.DB) (*WAL, error) {
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
		return fmt.Errorf("OpenLastWALFile: %v", err)
	}
	w.file = f
	w.encoder = NewEncoder(f)
	return nil
}

// CreateNewWALFile creates new WAL file
func (w *WAL) CreateNewWALFile() error {
	cnt := w.fileCounter
	if w.file != nil {
		w.file.Close()
		w.file = nil
		cnt++
	}
	f, err := os.Create(fmt.Sprintf("%s/%d.wal", w.path, cnt))
	if err != nil {
		return err
	}
	w.file = f
	w.encoder = NewEncoder(f)
	w.fileCounter++
	return nil
}

// CheckFileSize return size of current WAL file
func (w *WAL) CheckFileSize() (int64, error) {
	if w.file != nil {
		fileInfo, err := w.file.Stat()
		if err != nil {
			return 0, err
		}
		return fileInfo.Size(), nil
	}
	return 0, fmt.Errorf("WAL file is not opened")
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
func (w *WAL) Dump(output chan string, snapshotDB *leveldb.DB) (string, error) {
	w.Lock()
	defer w.Unlock()
	size, err := w.CheckFileSize()
	if err != nil {
		return "", err
	}
	if size == 0 {
		return "", nil
	}
	previousFileName := fmt.Sprintf("%s/%d.wal", w.path, w.fileCounter)
	if err := w.CreateNewWALFile(); err != nil {
		return "", err
	}
	if snapshotDB == nil {
		w.g.Go(func() error {
			if err := w.DBDump(previousFileName, output, nil); err != nil {
				return err
			}
			return os.Remove(previousFileName)
		})
		go func() {
			if err := w.g.Wait(); err != nil {
				fmt.Println(err) // TODO CHANGE ME
			}
		}()
	}
	return previousFileName, nil
}

// Wait waits for all WAL dumps to finish
func (w *WAL) Wait() error {
	return w.g.Wait()
}

// BDump dumps WAL to database
func (w *WAL) DBDump(fileName string, output chan string, db *leveldb.DB) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := NewDecoder(f)

	wb := new(leveldb.Batch)
	entryCounter := 0
	outputData := []string{}
	for {
		var entry Entry
		if err := decoder.Decode(&entry); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		entryCounter++
		if entry.Tombstoned {
			wb.Delete(entry.Key)
			if db != nil {
				outputData = append(outputData, string(entry.Key))
			}
		} else {
			wb.Put(entry.Key, entry.Value)
			if db != nil {
				outputData = append(outputData, string(entry.Key))
			}
		}
		if entryCounter%WALBatchMaxSize == 0 && len(outputData) > 0 {
			err = w.db.Write(wb, nil)
			if err != nil {
				return err
			}
			if db != nil {
				err = db.Write(wb, nil)
				if err != nil {
					return err
				}
			}
			wb.Reset()
			for _, key := range outputData {
				output <- key
			}
			outputData = []string{}
		}
	}
	f.Close()
	if len(outputData) > 0 {
		err = w.db.Write(wb, nil)
		if err != nil {
			return err
		}
		if db != nil {
			err = db.Write(wb, nil)
			if err != nil {
				return err
			}
		}
		for _, key := range outputData {
			output <- key
		}
	}
	return nil
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

// WalFilename returns current WAL filename
func (w *WAL) WalFilename() string {
	return fmt.Sprintf("%s/%d.wal", w.path, w.fileCounter)
}
