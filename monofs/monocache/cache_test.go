package monocache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// testing cache

func TestCacheTable(t *testing.T) {
	counter := 0
	cache := NewCacheTable(1000)
	cache.SetAddCallback(func(key uint64, data []byte) error {
		counter += 1
		return nil
	})
	cache.SetDelCallback(func(key uint64, data []byte) error {
		counter -= 1
		return nil
	})
	cache.Add(uint64(1), []byte("data1"), 0)
	cache.Add(uint64(2), []byte("data2"), 0)
	cache.Add(uint64(3), []byte("data3"), 0)
	assert.Equal(t, counter, 3)
	cache.Del(uint64(1))
	assert.Equal(t, counter, 2)
	cache.Stop()
}

func TestCacheTableSizeTTL(t *testing.T) {
	cache := NewCacheTable(10)
	cache.SetMinSize(10)
	cache.Add(uint64(1), []byte("data1"), 10*time.Second)
	cache.Add(uint64(2), []byte("data2"), 10*time.Second)
	cache.Add(uint64(3), []byte("data3"), 10*time.Millisecond)
	time.Sleep(1100 * time.Millisecond)
	assert.Equal(t, cache.Len(), 2)
	cache.Stop()
}

func TestCacheTableFull(t *testing.T) {
	cache := NewCacheTable(2)
	cache.SetMinSize(2)
	cache.Add(uint64(1), []byte("data1"), 0, WithProcessed(true))
	cache.Add(uint64(2), []byte("data2"), 0, WithProcessed(true))
	cache.Add(uint64(3), []byte("data3"), 0, WithProcessed(true))
	time.Sleep(1100 * time.Millisecond)
	assert.Equal(t, cache.Len(), 2)
	cache.Stop()
}

func TestAddGet(t *testing.T) {
	cache := NewCacheTable(10005)
	for a := 0; a < 1000; a++ {
		if err := cache.Add(uint64(a), []byte("foo"), 0); err != nil {
			t.Errorf("cache.Add() failed: %v", err)
		}
	}
	for a := 0; a < 1000; a++ {
		if _, err := cache.Get(uint64(a)); err != nil {
			t.Errorf("cache.Get() failed: %v", err)
		}
	}
	cache.Stop()
}
