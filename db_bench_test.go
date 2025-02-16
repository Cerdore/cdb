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
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}
	b.StopTimer()
	db.Close()
	defer os.RemoveAll(path.Join(DataDir, "chen4"))
}

// func Benchmark1wPut(b *testing.B) {
// 	db, err := New("chen4", DBOpts{DataDir: "", MtSizeLimit: 0})
// 	if err != nil {
// 		panic(err)
// 	}
// 	for i := 0; i < 1000000; i++ {
// 		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
// 	}

// 	db.Close()
// 	defer os.RemoveAll(path.Join(DataDir, "chen4"))
// }
