package utils

import (
	"encoding/binary"
	"math/rand"
)

func Uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Uint64ToUint40 converts uint64 to uint40
func Uint64ToUint40(i uint64) uint64 {
	return i & 0xFFFFFFFFFF
}

// Uint40ToUint64 converts uint40 to uint64
func Uint40ToUint64(i uint64) uint64 {
	return i & 0xFFFFFFFFFF
}

// UInt64ToUint20 converts uint64 to uint20
func Uint64ToUint20(i uint64) uint64 {
	return i & 0xFFFFF
}

// Uint20ToUint64 converts uint20 to uint64
func Uint20ToUint64(i uint64) uint64 {
	return i & 0xFFFFF
}

// MaxUint40	returns max uint40 value
func MaxUint40() uint64 {
	return 0xFFFFFFFFFF
}

// MaxUint20	returns max uint20 value
func MaxUint20() uint64 {
	return 0xFFFFF
}

// RandStting returns random string with length n
func RandString(n int) string {
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.!?/\\-_=+<>")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}
