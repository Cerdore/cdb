package manifest

import (
	"encoding/binary"
	"testing"

	"github.com/cerdore/cdb/sstable"
	"github.com/stretchr/testify/assert"
)

func TestCodec_RoundTrip(t *testing.T) {
	entry := NewEntry(&sstable.Metadata{
		Level:    3,
		Filename: "foo",
		StartKey: []byte("foo"),
		EndKey:   []byte("bar"),
	}, false)

	codec := Codec{}

	eBytes, err := codec.EncodeEntry(entry)
	assert.NoError(t, err)

	totalLen := binary.BigEndian.Uint32(eBytes[0:4])
	assert.Equal(t, totalLen, uint32(len(eBytes)-4))

	actual, err := codec.DecodeEntry(eBytes[4:])
	actual.metadata.Bits = nil
	assert.NoError(t, err)

	assert.Equal(t, entry, actual)
}
