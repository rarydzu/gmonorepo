package monocache

import (
	"testing"
	"time"
)

// test cache item
func TestCacheItem(t *testing.T) {
	item := NewCacheItem(1, []byte("data"), 0, 1*time.Second)
	if item.GetKey() != 1 {
		t.Errorf("item.GetKey() != \"1\"")
	}
	if string(item.GetData()) != "data" {
		t.Errorf("item.GetData() != \"data\"")
	}
	if item.GetTTL() != 1*time.Second {
		t.Errorf("item.GetTTL() != 1 * time.Second")
	}
}

// test tomnbstoned
func TestCacheItemTombstoned(t *testing.T) {
	item := NewCacheItem(1, []byte("data"), 0, 1*time.Second)
	item.SetTombstoned(true)
	if !item.IsTomstoned() {
		t.Errorf("item.IsTomstoned() != true")
	}
}
