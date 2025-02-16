package skiplist

import (
	"github.com/cerdore/cdb/memtable/interfaces"
	"github.com/cerdore/cdb/storage"
	log "github.com/sirupsen/logrus"
)

type Iterator struct {
	list    *SkipList
	pointer *Node
}

func NewIterator(list *SkipList) interfaces.InternalIterator {
	return &Iterator{list: list, pointer: list.head}
}

func (i *Iterator) HasNext() bool {
	return i.pointer.next[0] != nil
}

func (i *Iterator) Next() *storage.Record {
	if !i.HasNext() {
		log.Panic("iterator has no next element")
	}

	node := i.pointer.next[0]
	i.pointer = node

	return storage.NewRecord(node.key, node.value, node.deleted)
}

var _ interfaces.InternalIterator = &Iterator{}
