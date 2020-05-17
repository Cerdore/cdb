package sstable

import (
	"bytes"
)

type Metadata struct {
	Level    uint8
	Filename string
	StartKey []byte
	EndKey   []byte
	Bits     []byte
}

// ContainsKey returns true if the metadata key range contains the specified key
func (m *Metadata) ContainsKey(key []byte) bool {
	// bloom := bloom.RecoverBloom(m.Bits)

	// if !bloom.Check(key) {
	// 	return false
	// }

	// startKey <= key <= endKey
	return bytes.Compare(m.StartKey, key) <= 0 && bytes.Compare(key, m.EndKey) <= 0

}
