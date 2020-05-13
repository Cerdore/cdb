package memtable

import (
	"fmt"
	"testing"
)

func BenchmarkPut(t *testing.B) {
	mem := New()

	for i := 0; i < t.N; i++ {
		mem.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	for i := 0; i < t.N; i++ {
		byte := mem.Get([]byte(fmt.Sprintf("mykey%7d", i)))
		if byte != nil {
			t.Log(string(byte))
		}
	}
}
