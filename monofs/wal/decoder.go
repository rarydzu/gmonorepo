package wal

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
)

// Decoder is a decoder for WAL retrieves data from WAL and convert them to Entry
type Decoder struct {
	reader   *bufio.Reader
	position int64
}

// NewDecoder creates new decoder for WAL
func NewDecoder(file *os.File) *Decoder {
	return &Decoder{
		reader:   bufio.NewReader(file),
		position: 0,
	}
}

// Decode decodes entry from WAL
func (d *Decoder) Decode(e *Entry) error {
	// reads line from file
	l, err := d.reader.ReadString('\n')
	if err != nil {
		return err
	}
	d.position += 1
	rline := strings.Split(strings.TrimRight(l, "\n"), "#")
	if len(rline) == 3 {
		// decodes line to entry
		decKey, err := base64.StdEncoding.DecodeString(rline[0])
		if err != nil {
			return fmt.Errorf("unable decode Entry key in line %d: %w", d.position, err)
		}
		decValue, err := base64.StdEncoding.DecodeString(rline[1])
		if err != nil {
			return fmt.Errorf("unable decode Entry value in line %d: %w", d.position, err)
		}
		decTombstone := false
		if rline[2] == "1" {
			decTombstone = true
		}
		e.Key = decKey
		e.Value = decValue
		e.Tombstoned = decTombstone
		return nil
	}
	return fmt.Errorf("error while decoding WAL file entry %d", d.position)
}
