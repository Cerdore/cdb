package cdb

import (
	"fmt"
	"runtime"
	"testing"
)

func TestOpen1(t *testing.T) {
	// db, err := New("chen3", DBOpts{dataDir: "", mtSizeLimit: 0})
	// if err != nil {
	// 	panic(err)
	// }
	// for i := 0; i < 400; i++ {
	// 	db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	// 	if db.memTable.Size() > db.mtSizeLimit {
	// 		fmt.Printf("MTSize oversize")
	// 	}
	// }
	// db.Close()

	db1, err := Open("chen3", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	if value1, err := db1.Get([]byte("key1")); err != nil {
		t.Log(err)
	} else if err == nil {
		t.Log(string(value1))
	}

	if value2, err := db1.Get([]byte(fmt.Sprintf("mykey%7d", 399))); err != nil {
		t.Log(err)
	} else if err == nil {
		t.Log(string(value2))
	}

	db1.Close()
	//os.RemoveAll(path.Join(datadir, "chen3"))
}

func TestLargePut(t *testing.T) {
	runtime.GOMAXPROCS(4)
	db1, err := New("chen4", DBOpts{dataDir: "", mtSizeLimit: 0})
	// if err != nil {
	// 	panic(err)
	// }
	// for i := 0; i < 400; i++ {
	// 	db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	// 	if db.memTable.Size() > db.mtSizeLimit {
	// 		fmt.Printf("MTSize oversize")
	// 	}
	// }
	// db.Close()

	//db1, err := Open("chen3", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000; i++ {
		db1.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	db1.Close()
	//os.RemoveAll(path.Join(datadir, "chen3"))
}

func TestTimeof(t *testing.T) {
	//	runtime.GOMAXPROCS(4)
	d, err := New("chen17", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 500000; i++ {
		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	d.Close()
}

func TestRestore(t *testing.T) {
	db1, err := Open("chen17", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		t.Log(err)
	}
	for i := 0; i < 1000; i++ {
		ans, err := db1.Get([]byte(fmt.Sprintf("mykey%7d", i)))
		if err != nil {
			t.Log(err)
		}
		if ans != nil {
			fmt.Println(string(ans))
		}
	}

	db1.Close()
}
