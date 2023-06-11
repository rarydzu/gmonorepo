package hash

import (
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseops"
)

func sleep(h *Hash, seconds int, endSignal chan<- bool) {
	h.Lock(fuseops.InodeID(1))
	defer h.Unlock(1)
	h.Lock(fuseops.InodeID(2))
	defer h.Unlock(2)
	time.Sleep(time.Duration(seconds) * time.Second)
	endSignal <- true
}

func TestHash(t *testing.T) {
	var end bool
	hash := New(10)
	endSignal := make(chan bool, 1)
	go sleep(hash, 1, endSignal)
	for !end {
		select {
		case end = <-endSignal:
		case <-time.After(2 * time.Second):
			t.Error("TestHash failed")
			end = true
		}
	}
}
