package memtable

import (
	"time"

	"github.com/cerdore/cdb/memtable/interfaces"

	"github.com/cerdore/cdb/memtable/skiplist"
)

type MemTable struct {
	memStore interfaces.InMemoryStore
}

func New() *MemTable {
	return &MemTable{memStore: skiplist.New(time.Now().UnixNano())}
}

func (m *MemTable) Get(key []byte) (bool, []byte) {
	if found, val := m.memStore.Get(key); found {
		if val == nil {
			return true, nil
		}
		return false, val
	} else {
		return false, nil
	}
}

func (m *MemTable) Put(key []byte, value []byte) {
	m.memStore.Put(key, value)
}

func (m *MemTable) Delete(key []byte) {
	m.memStore.Delete(key)
}

func (m *MemTable) InternalIterator() interfaces.InternalIterator {
	return m.memStore.InternalIterator()
}

func (m *MemTable) Size() uint32 {
	return m.memStore.Size()
}

func (m *MemTable) Num() uint32 {
	return m.memStore.Num()
}
