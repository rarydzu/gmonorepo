package fsdb

import (
	"os"
	"testing"

	"github.com/rarydzu/gmonorepo/monofs/config"
)

func TestNodeStore(t *testing.T) {
	os.Setenv("MONOFS_DEV_RUN", "testing")
	config := &config.Config{
		Path:           t.TempDir(),
		FilesystemName: "test",
		CacheSize:      10000,
	}
	db, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddInode(TestInode, true); err != nil {
		t.Fatal(err)
	}
	inode, err := db.GetInode(TestInode.ParentID, TestInode.Name, true)
	if err != nil {
		t.Fatal(err)
	}
	if inode.InodeID != TestInode.InodeID {
		t.Fatalf("inode.InodeID != TestInode.InodeID")
	}
	if inode.ParentID != TestInode.ParentID {
		t.Fatalf("inode.ParentID != TestInode.ParentID")
	}
	if inode.Attrs.Size != TestInode.Attrs.Size {
		t.Fatalf("inode.Attrs.Size != TestInode.Attrs.Size")
	}
	if !inode.Attrs.Mtime.Equal(TestInode.Attrs.Mtime) {
		t.Fatalf("%v != %v", inode.Attrs.Mtime, TestInode.Attrs.Mtime)
	}
}
