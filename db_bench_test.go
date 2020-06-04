package cdb

import (
	"fmt"
	"os"
	"path"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	db, err := New("chen4", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 1; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	db.Close()
	defer os.RemoveAll(path.Join(DataDir, "chen4"))
}
