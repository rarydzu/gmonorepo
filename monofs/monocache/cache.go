package monocache

import (
	"errors"
	"sync"
	"time"

	"github.com/rarydzu/gmonorepo/utils"
)

var (
	ErrKeyNotFound = errors.New("no such key")
	ErrKeyDeleted  = errors.New("key deleted")
)

type CacheTable struct {
	// contains filtered or unexported fields
	table map[interface{}]*CacheItem
	sync.RWMutex
	threshold           int
	addCallback         func(key uint64, data []byte) error
	delCallback         func(key uint64, data []byte) error
	cacheFullCallback   func(output chan string) error
	ticker              *time.Ticker
	stop                chan bool
	cacheMutations      int
	minSize             int
	cacheGeneration     uint64
	fullCallbackRunning bool
	currentSize         int
}

// NewCacheTable creates new cache table
func NewCacheTable(threshold int) *CacheTable {
	ct := &CacheTable{
		table:               make(map[interface{}]*CacheItem),
		stop:                make(chan bool),
		ticker:              time.NewTicker(1 * time.Second),
		cacheMutations:      0,
		minSize:             int(float64(threshold) * 0.5),
		cacheGeneration:     0,
		fullCallbackRunning: false,
		threshold:           threshold,
	}
	// add error handling in callbacks and log them
	go func() {
		for {
			select {
			case <-ct.ticker.C:
				meanAccess := float64(0)
				ct.Lock()
				// if cache is larger than utilization size
				if len(ct.table) > ct.threshold {
					if ct.fullCallbackRunning {
						ct.Unlock()
						continue
					}
					// switch generation
					ct.cacheGeneration++
					// run fullCallback
					ct.fullCallbackRunning = true
					ct.Unlock()
					if ct.cacheFullCallback != nil {
						processedElements := make(chan string, len(ct.table))
						oldCacheGeneration := ct.cacheGeneration - 1
						err := ct.cacheFullCallback(processedElements)
						ct.fullCallbackRunning = false
						if err != nil {
							//TODO LOG ERROR
							continue
						}
						go func() {
							for strKey := range processedElements {
								ct.Lock()
								key := utils.BytesToUint64([]byte(strKey))
								if item, ok := ct.table[key]; ok {
									if item.GetGeneration() == oldCacheGeneration {
										ct.table[key].SetProcessed(true)
									}
								}
								ct.Unlock()
							}
						}()
					} else {
						ct.fullCallbackRunning = false
					}
				} else {
					ct.Unlock()
				}
				ct.Lock()
				oldGenerationCount := 0
				for key, item := range ct.table {
					if item.GetTTL() > 0 && time.Since(item.GetLastAccess()) > item.GetTTL() {
						ct.currentSize--
						delete(ct.table, key)
					} else {
						if item.GetGeneration() != ct.cacheGeneration {
							meanAccess += float64(item.GetAccessCount())
							oldGenerationCount++
						}
					}
				}
				if oldGenerationCount == 0 {
					ct.Unlock()
					continue
				}
				meanAccess /= float64(oldGenerationCount)
				if ct.cacheMutations > ct.threshold {
					for key, item := range ct.table {
						//this should only include keys which are already deployed in db
						if item.IsProcessed() && item.GetAccessCount() <= uint64(meanAccess) && item.GetGeneration() != ct.cacheGeneration {
							delete(ct.table, key)
							ct.cacheMutations--
							ct.currentSize--
							if ct.cacheMutations <= ct.minSize {
								break
							}
						}
					}
				}
				ct.Unlock()
			case <-ct.stop:
				return
			}
		}
	}()
	return ct
}

// Add adds new item to cache
func (t *CacheTable) Add(key uint64, data []byte, ttl time.Duration, opts ...Option) error {
	t.Lock()
	defer t.Unlock()
	t.currentSize++
	t.table[key] = NewCacheItem(key, data, t.cacheGeneration, ttl, opts...)
	t.cacheMutations++
	if t.addCallback != nil {
		return t.addCallback(key, data)
	}
	return nil
}

// Set sets item in cache
func (t *CacheTable) Set(item *CacheItem) {
	t.Lock()
	defer t.Unlock()
	t.currentSize++
	t.table[item.Key] = item
	t.cacheMutations++
}

// Del deletes item from cache
func (t *CacheTable) Del(key uint64) error {
	t.Lock()
	defer t.Unlock()
	if _, ok := t.table[key]; !ok {
		return ErrKeyNotFound
	}
	t.table[key].SetTombstoned(true)
	t.cacheMutations++
	if t.delCallback != nil {
		bData, err := t.table[key].Marshall()
		if err != nil {
			return err
		}
		return t.delCallback(key, bData)
	}
	return nil
}

// Get returns item from cache
func (t *CacheTable) Get(key uint64) ([]byte, error) {
	t.RLock()
	defer t.RUnlock()
	item, ok := t.table[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	if item.GetTTL() > 0 && time.Since(item.GetLastAccess()) > item.GetTTL() {
		delete(t.table, key)
		return nil, ErrKeyNotFound
	}
	if item.IsTomstoned() {
		return nil, ErrKeyDeleted
	}
	return item.GetData(), nil
}

// Len returns number of items in cache
func (t *CacheTable) Len() int {
	t.RLock()
	defer t.RUnlock()
	return t.currentSize
}

// SetAddCallback sets callback function which is called when item is added to cache
func (t *CacheTable) SetAddCallback(cb func(key uint64, data []byte) error) {
	t.Lock()
	defer t.Unlock()
	t.addCallback = cb
}

// SetDelCallback sets callback function which is called when item is deleted from cache
func (t *CacheTable) SetDelCallback(cb func(key uint64, data []byte) error) {
	t.Lock()
	defer t.Unlock()
	t.delCallback = cb
}

// SetCacheFullCallback sets callback function which is called when cache is full
func (t *CacheTable) SetCacheFullCallback(cb func(output chan string) error) {
	t.Lock()
	defer t.Unlock()
	t.cacheFullCallback = cb
}

// Stop stops cache table
func (t *CacheTable) Stop() {
	t.ticker.Stop()
	t.stop <- true
}

// SetCacheTreshold sets cache size
func (t *CacheTable) SetCacheTreshold(size int) {
	t.Lock()
	defer t.Unlock()
	t.threshold = size
}

func (t *CacheTable) GetCacheGeneration() uint64 {
	t.RLock()
	defer t.RUnlock()
	return t.cacheGeneration
}

func (t *CacheTable) SetMinSize(size int) {
	t.Lock()
	defer t.Unlock()
	t.minSize = size
}
