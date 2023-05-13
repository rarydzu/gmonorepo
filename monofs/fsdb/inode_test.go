package fsdb

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseops"
)

const TestInodeHash = "e5ea704c794879536f4a325d35147d5fd4107ae1728c98f43e0ceeb5709f8f73"

var TestInode = &Inode{
	InodeID:  2,
	ParentID: 1,
	Name:     "",
	Attrs: InodeAttributes{
		InodeAttributes: fuseops.InodeAttributes{
			Size:  4096,
			Nlink: 1,
			Mode:  0755 | os.ModeDir,
			Mtime: time.Date(2000, 0, 0, 0, 0, 0, 0, time.UTC),
			Uid:   0,
			Gid:   0,
		},
	},
}

func TestInodeAttributes(t *testing.T) {

	buf, err := TestInode.Marshall()
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(buf)
	if fmt.Sprintf("%x", h) != TestInodeHash {
		t.Fatalf("Hash mismatch %x != %s", h, TestInodeHash)
	}
}
