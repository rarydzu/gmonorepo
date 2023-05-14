package hash

import "sync"

type Hash struct {
	// create map of mutexes
	hashMap map[uint64]*sync.RWMutex
	size    uint64
}

func New(size uint64) *Hash {
	h := &Hash{
		hashMap: make(map[uint64]*sync.RWMutex, size),
		size:    size,
	}
	for i := uint64(0); i < size; i++ {
		h.hashMap[i] = &sync.RWMutex{}
	}
	return h
}

func (h *Hash) Lock(key uint64) {
	h.hashMap[key%h.size].Lock()
}

func (h *Hash) Unlock(key uint64) {
	h.hashMap[key%h.size].Unlock()
}

func (h *Hash) RLock(key uint64) {
	h.hashMap[key%h.size].RLock()
}

func (h *Hash) RUnlock(key uint64) {
	h.hashMap[key%h.size].RUnlock()
}
