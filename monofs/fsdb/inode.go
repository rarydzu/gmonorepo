package fsdb

import (
	"encoding/json"
	"fmt"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/rarydzu/gmonorepo/utils"
)

type InodeAttributes struct {
	Hash     string
	ParentID uint64
	fuseops.InodeAttributes
}

func DbInodeKey(parent uint64, name string) []byte {
	if len(name) == 0 {
		return []byte(fmt.Sprintf("%d:", parent))
	}
	return []byte(fmt.Sprintf("%d:%s", parent, name))
}

type Inode struct {
	InodeID  uint64
	Name     string
	ParentID uint64
	Attrs    InodeAttributes
}

func NewInode(id, parent uint64, name string, attrs InodeAttributes) *Inode {
	attrs.ParentID = parent
	return &Inode{
		InodeID:  id,
		Name:     name,
		ParentID: parent,
		Attrs:    attrs,
	}
}

func (i *Inode) ID() fuseops.InodeID {
	return fuseops.InodeID(i.InodeID)
}

func (i *Inode) Parent() fuseops.InodeID {
	return fuseops.InodeID(i.ParentID)
}
func (i *Inode) SetID(id fuseops.InodeID) {
	i.InodeID = uint64(id)
}

func (i *Inode) SetParent(parent fuseops.InodeID) {
	i.ParentID = uint64(parent)
}

func (i *Inode) SetName(name string) {
	i.Name = name
}

func (i *Inode) DbID() []byte {
	return utils.Uint64ToBytes(i.InodeID)
}

func (i *Inode) SetInodeAttributes(attr fuseops.InodeAttributes) {
	i.Attrs.InodeAttributes = attr
}

func (i *Inode) SetHash(hash string) {
	i.Attrs.Hash = hash
}

func (i *Inode) SetAttrsParent(parent fuseops.InodeID) {
	i.Attrs.ParentID = uint64(parent)
}

func (i *Inode) Marshall() ([]byte, error) {
	return json.Marshal(i)
}

func (i *Inode) Unmarshall(data []byte) error {
	return json.Unmarshal(data, i)
}

func (ia *InodeAttributes) Marshall() ([]byte, error) {
	return json.Marshal(ia)
}

func (ia *InodeAttributes) Unmarshall(data []byte) error {
	return json.Unmarshal(data, ia)
}

func (ia *InodeAttributes) GetHash() string {
	return ia.Hash
}
