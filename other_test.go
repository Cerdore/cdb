package cdb

import (
	"fmt"
	"runtime"
	"testing"
	"time"
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
	d, err := New("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000000; i++ {
		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	d.Close()
}

func TestRestore(t *testing.T) {
	db1, err := Open("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		t.Log(err)
	}
	for i := 999900; i < 1000000; i++ {
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

func TestPutAGet(t *testing.T) {
	db, err := Open("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		t.Log(err)
	}
	db.Put([]byte("kk1"), []byte("kv23"))

	ans, err := db.Get([]byte("kk1"))
	if err != nil {
		t.Log(err)
	}
	if ans != nil {
		fmt.Println(string(ans))
	}

	ans, err = db.Get([]byte(fmt.Sprintf("mykey%7d", 990000)))
	if err != nil {
		t.Log(err)
	}
	if ans != nil {
		fmt.Println(fmt.Sprintf("mykey%7d", 990000), string(ans))
	}
	db.Close()
}

func TestOpenAndDel(t *testing.T) {
	db, err := Open("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		t.Log(err)
	}
	//	db.Put([]byte("kk1"), []byte("kv23"))
	db.Delete([]byte("kk1"))
	ans, err := db.Get([]byte("kk1"))
	if err != nil {
		t.Log(err)
	}
	if ans != nil {
		fmt.Println(string(ans))
	} else {
		fmt.Println("not found")
	}
	db.Close()
}

func TestTimeUnix(t *testing.T) {
	fmt.Println(time.Now().UnixNano() / 1000000000)

	fmt.Println(time.Now().UnixNano() / 1000000)

	fmt.Println(time.Now().UnixNano() / 1000)

	fmt.Println(time.Now().UnixNano())

}

func TestDataPut(t *testing.T) {
	d, err := New("chen49", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000000; i++ {
		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	d.Close()
}

func TestDataPut1(t *testing.T) {
	d, err := Open("chen49", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	// for i := 0; i < 1000000; i++ {
	// 	d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	// }
	d.Put([]byte("kk1"), []byte("kkv"))
	d.Close()
}
