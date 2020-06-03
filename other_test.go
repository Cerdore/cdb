package cdb

import (
	"fmt"
	"testing"
)

// func TestOpen1(t *testing.T) {
// 	// db, err := New("chen3", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	// if err != nil {
// 	// 	panic(err)
// 	// }
// 	// for i := 0; i < 400; i++ {
// 	// 	db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)),false)
// 	// 	if db.memTable.Size() > db.mtSizeLimit {
// 	// 		fmt.Printf("MTSize oversize")
// 	// 	}
// 	// }
// 	// db.Close()

// 	db1, err := Open("chen3", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	if err != nil {
// 		panic(err)
// 	}
// 	if value1, err := db1.Get([]byte("key1")); err != nil {
// 		t.Log(err)
// 	} else if err == nil {
// 		t.Log(string(value1))
// 	}

// 	if value2, err := db1.Get([]byte(fmt.Sprintf("mykey%7d", 399))); err != nil {
// 		t.Log(err)
// 	} else if err == nil {
// 		t.Log(string(value2))
// 	}

// 	db1.Close()
// 	//os.RemoveAll(path.Join(datadir, "chen3"))
// }

// func TestLargePut(t *testing.T) {
// 	runtime.GOMAXPROCS(4)
// 	db1, err := New("chen4", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	// if err != nil {
// 	// 	panic(err)
// 	// }
// 	// for i := 0; i < 400; i++ {
// 	// 	db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)),false)
// 	// 	if db.memTable.Size() > db.mtSizeLimit {
// 	// 		fmt.Printf("MTSize oversize")
// 	// 	}
// 	// }
// 	// db.Close()

// 	//db1, err := Open("chen3", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	if err != nil {
// 		panic(err)
// 	}
// 	for i := 0; i < 1000; i++ {
// 		db1.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)),false)
// 	}

// 	db1.Close()
// 	//os.RemoveAll(path.Join(datadir, "chen3"))
// }

// func TestTimeof(t *testing.T) {
// 	//	runtime.GOMAXPROCS(4)
// 	d, err := New("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	if err != nil {
// 		panic(err)
// 	}
// 	for i := 0; i < 1000000; i++ {
// 		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)),false)
// 	}

// 	d.Close()
// }

// func TestRestore(t *testing.T) {
// 	db1, err := Open("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	if err != nil {
// 		t.Log(err)
// 	}
// 	for i := 999900; i < 1000000; i++ {
// 		ans, err := db1.Get([]byte(fmt.Sprintf("mykey%7d", i)))
// 		if err != nil {
// 			t.Log(err)
// 		}
// 		if ans != nil {
// 			fmt.Println(string(ans))
// 		}
// 	}

// 	db1.Close()
// }

// func TestPutAGet(t *testing.T) {
// 	db, err := Open("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	if err != nil {
// 		t.Log(err)
// 	}
// 	db.Put([]byte("kk1"), []byte("kv23"),false)

// 	ans, err := db.Get([]byte("kk1"))
// 	if err != nil {
// 		t.Log(err)
// 	}
// 	if ans != nil {
// 		fmt.Println(string(ans))
// 	}

// 	ans, err = db.Get([]byte(fmt.Sprintf("mykey%7d", 990000)))
// 	if err != nil {
// 		t.Log(err)
// 	}
// 	if ans != nil {
// 		fmt.Println(fmt.Sprintf("mykey%7d", 990000), string(ans))
// 	}
// 	db.Close()
// }

// func TestOpenAndDel(t *testing.T) {
// 	db, err := Open("chen2", DBOpts{dataDir: "", mtSizeLimit: 0})
// 	if err != nil {
// 		t.Log(err)
// 	}
// 	//	db.Put([]byte("kk1"), []byte("kv23"),false)
// 	db.Delete([]byte("kk1"),false)
// 	ans, err := db.Get([]byte("kk1"))
// 	if err != nil {
// 		t.Log(err)
// 	}
// 	if ans != nil {
// 		fmt.Println(string(ans))
// 	} else {
// 		fmt.Println("not found")
// 	}
// 	db.Close()
// }

// func TestTimeUnix(t *testing.T) {
// 	fmt.Println(time.Now().UnixNano() / 1000000000)

// 	fmt.Println(time.Now().UnixNano() / 1000000)

// 	fmt.Println(time.Now().UnixNano() / 1000)

// 	fmt.Println(time.Now().UnixNano())

// }

func TestDataExists1(t *testing.T) {
	db, err := New("chen34", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 1000; i < 200000; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	db.Close()

	// db1, err := Open("chen24", DBOpts{dataDir: "", mtSizeLimit: 0})
	// if err != nil {
	// 	panic(err)
	// }
	// ans, err := db1.Get([]byte(fmt.Sprintf("mykey%7d", 200000)))
	// if err != nil {
	// 	t.Log(err)
	// }
	// if ans != nil {
	// 	fmt.Println(string(ans))
	// } else {
	// 	fmt.Println("not found")
	// }

	// ans1, err1 := db1.Get([]byte(fmt.Sprintf("mykey%7d", 1000)))
	// if err1 != nil {
	// 	t.Log(err)
	// }
	// if ans1 != nil {
	// 	fmt.Println(string(ans))
	// } else {
	// 	fmt.Println("not found")
	// }
	// db1.Close()
}

func TestDataExists2(t *testing.T) {
	db, err := New("chen99", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 1; i < 200000; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	db.Close()

}

func TestDataLaExists(t *testing.T) {
	db, err := New("chen50", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 1; i < 1000000; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	db.Close()

}

func TestDataPut(t *testing.T) {
	d, err := New("chen49", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000000; i++ {
		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}
	d.Close()
}

func TestDataPut1(t *testing.T) {
	d, err := Open("chen50", DBOpts{dataDir: "", mtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	// for i := 0; i < 1000000; i++ {
	// 	d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)),false)
	// }
	//d.Put([]byte("kk1"), []byte("kkv"),false)
	ans, err1 := d.Get([]byte(fmt.Sprintf("mykey%7dx", 720016)))
	if err1 != nil {
		t.Log(err)
	}
	if ans != nil {
		fmt.Println(string(ans))
	} else {
		fmt.Println("not found")
	}

	d.Close()
}
