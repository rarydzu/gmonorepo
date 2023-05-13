package monocache

import (
	"encoding/json"
	"sync"
	"time"
)

type CacheItem struct {
	sync.RWMutex `json:"-"` // lock for item
	// key is used to identify item in cache
	Key uint64 `json:"key"`
	// data is stored in cache
	Data []byte `json:"data"`
	// ttl is time to live for item in cache
	Ttl time.Duration `json:"ttl"`
	// lastAccess is time when item was last accessed
	LastAccess time.Time `json:"lastAccess"`
	// lastUpdate is time when item was last updated
	LastUpdate time.Time `json:"lastUpdate"`
	// accessCount is number of times item was accessed
	AccessCount uint64 `json:"accessCount"`
	// generation is a generation of item
	Generation uint64 `json:"generation"`
	// tombstoned is a flag that indicates that item was deleted
	Tombstoned bool `json:"tombstoned"`
	//processed us a flag which indicates that item was processed by any of callback functions
	Processed bool `json:"processed"`
}

type Option func(*CacheItem)

func WithProcessed(processed bool) Option {
	return func(item *CacheItem) {
		item.Processed = processed
	}
}

// NewCacheItem creates new cache item
func NewCacheItem(key uint64, data []byte, generation uint64, ttl time.Duration, opts ...Option) *CacheItem {
	ci := &CacheItem{
		Key:         key,
		Data:        data,
		Ttl:         ttl,
		LastAccess:  time.Now(),
		LastUpdate:  time.Now(),
		AccessCount: 0,
		Generation:  generation,
		Tombstoned:  false,
		Processed:   false,
	}
	for _, opt := range opts {
		opt(ci)
	}
	return ci
}

// key is used to identify item in cache
func (item *CacheItem) GetKey() uint64 {
	item.RLock()
	item.LastAccess = time.Now()
	item.AccessCount++
	defer item.RUnlock()
	return item.Key
}

// GetData returns data stored in cache
func (item *CacheItem) GetData() []byte {
	item.RLock()
	item.LastAccess = time.Now()
	item.AccessCount++
	defer item.RUnlock()
	return item.Data
}

// GetTTL returns time to live for item in cache
func (item *CacheItem) GetTTL() time.Duration {
	item.RLock()
	defer item.RUnlock()
	return item.Ttl
}

// GetLastAccess returns time when item was last accessed
func (item *CacheItem) GetLastAccess() time.Time {
	item.RLock()
	defer item.RUnlock()
	return item.LastAccess
}

// GetLastUpdate returns time when item was last updated
func (item *CacheItem) GetLastUpdate() time.Time {
	item.RLock()
	defer item.RUnlock()
	return item.LastUpdate
}

// GetAccessCount returns number of times item was accessed
func (item *CacheItem) GetAccessCount() uint64 {
	item.RLock()
	defer item.RUnlock()
	return item.AccessCount
}

// SetData sets data for item in cache
func (item *CacheItem) SetData(data []byte) {
	item.Lock()
	defer item.Unlock()
	item.Data = data
	item.LastUpdate = time.Now()
}

// SetTTL sets time to live for item in cache
func (item *CacheItem) SetTTL(ttl time.Duration) {
	item.Lock()
	defer item.Unlock()
	item.Ttl = ttl
	item.LastUpdate = time.Now()
}

// GetGeneration returns generation of item
func (item *CacheItem) GetGeneration() uint64 {
	item.RLock()
	defer item.RUnlock()
	return item.Generation
}

// SetTombstoned sets tombstoned flag for item
func (item *CacheItem) SetTombstoned(tombstoned bool) {
	item.Lock()
	defer item.Unlock()
	item.Tombstoned = tombstoned
}

// IsTomstoned returns tombstoned flag for item
func (item *CacheItem) IsTomstoned() bool {
	item.RLock()
	defer item.RUnlock()
	return item.Tombstoned
}

// SetProcessed sets processed flag for item
func (item *CacheItem) SetProcessed(processed bool) {
	item.Lock()
	defer item.Unlock()
	item.Processed = processed
}

// IsProcessed returns processed flag for item
func (item *CacheItem) IsProcessed() bool {
	item.RLock()
	defer item.RUnlock()
	return item.Processed
}

// Marshall marshalls item to json
func (item *CacheItem) Marshall() ([]byte, error) {
	item.RLock()
	defer item.RUnlock()
	return json.Marshal(item)
}

// Unmarshall unmarshalls item from json
func (item *CacheItem) Unmarshall(data []byte) error {
	item.Lock()
	defer item.Unlock()
	return json.Unmarshal(data, item)
}
