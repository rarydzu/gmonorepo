package wal

import (
	"encoding/base64"
	"os"
)

// Encoder is a encoder for WAL stores data in WAL

type Encoder struct {
	file *os.File
}

// NewEncoder creates new encoder for WAL
func NewEncoder(file *os.File) *Encoder {
	return &Encoder{
		file: file,
	}
}

// Encode encodes entry to WAL
func (e *Encoder) Encode(entry *Entry) error {
	// converts entry to byte array
	// writes byte array to file
	// returns error if any
	encKey := base64.StdEncoding.EncodeToString(entry.Key)
	encValue := base64.StdEncoding.EncodeToString(entry.Value)
	encTombstone := "0"
	if entry.Tombstoned {
		encTombstone = "1"
	}
	_, err := e.file.WriteString(encKey + "#" + encValue + "#" + encTombstone + "\n")
	return err
}
