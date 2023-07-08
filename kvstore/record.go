package kvstore

import (
	"fmt"

	"hash/crc32"

	"github.com/rarydzu/gmonorepo/utils"
)

const (
	Tombstoned = iota + 1
	headerSize = 13
	metaSize   = 17
)

type Record struct {
	Flags int8
	Key   uint64
	Value []byte
}

func setBit(n int8, pos uint) int8 {
	n |= (1 << pos)
	return n
}

func clearBit(n int8, pos uint) int8 {
	mask := int8(^(1 << pos))
	n &= mask
	return n
}

func hasBit(n int8, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

func NewRecord(key uint64, value []byte) *Record {
	return &Record{Key: key, Value: value}
}

func (r *Record) IsTombstoned() bool {
	return hasBit(r.Flags, Tombstoned)
}

func (r *Record) Tombstone() {
	r.Flags = setBit(r.Flags, Tombstoned)
}

func (r *Record) Untombstone() {
	r.Flags = clearBit(r.Flags, Tombstoned)
}

func (r *Record) CalculateCRC(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}

func (r *Record) Encode() []byte {
	buf := make([]byte, metaSize+len(r.Value)) // 1 + 8 + 4 + len(value) + 4
	buf[0] = byte(r.Flags)
	valueLen := uint32(len(r.Value))
	copy(buf[1:9], utils.Uint64ToBytes(r.Key))
	copy(buf[9:13], utils.Uint32ToBytes(valueLen))
	valEndPos := headerSize + valueLen
	copy(buf[headerSize:valEndPos], r.Value)
	crc := r.CalculateCRC(buf[0:valEndPos])
	copy(buf[valEndPos:], utils.Uint32ToBytes(crc))
	return buf
}

func (r *Record) Decode(data []byte) error {
	r.Flags = int8(data[0])
	r.Key = utils.BytesToUint64(data[1:9])
	valueLen := utils.BytesToUint32(data[9:13])
	valEndPos := headerSize + valueLen
	r.Value = data[headerSize:valEndPos]
	crc32 := utils.BytesToUint32(data[valEndPos : valEndPos+4])
	crc := r.CalculateCRC(data[0:valEndPos])
	if crc != crc32 {
		return fmt.Errorf("CRC check failed %d != %d", crc, crc32)
	}
	return nil
}
