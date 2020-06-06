package cdb

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpen1(t *testing.T) {
	db, err := New("chen1", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 400; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
		// if db.memTable.Size() > db.MtSizeLimit {
		// 	fmt.Printf("MTSize oversize")
		// }
	}
	db.Close()

	db1, err := Open("chen1", DBOpts{DataDir: "", MtSizeLimit: 0})
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
	defer os.RemoveAll(path.Join(DataDir, "chen1"))
}

func TestLargePut(t *testing.T) {
	runtime.GOMAXPROCS(4)
	db1, err := New("chen2", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 400; i++ {
		db1.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
		// if db1.memTable.Size() > db1.MtSizeLimit {
		// 	fmt.Printf("MTSize oversize")
		// }
	}
	db1.Close()

	db1, err = Open("chen2", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000; i++ {
		db1.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	db1.Close()
	defer os.RemoveAll(path.Join(DataDir, "chen2"))
}

func TestTimeof(t *testing.T) {
	//	runtime.GOMAXPROCS(4)
	d, err := New("chen3", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000000; i++ {
		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	d.Close()
	defer os.RemoveAll(path.Join(DataDir, "chen3"))
}

func TestRestore(t *testing.T) {
	d, err := New("chen3", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000000; i++ {
		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	d.Close()

	db1, err := Open("chen3", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		t.Log(err)
	}
	sum := 0
	for i := 999900; i < 1000000; i++ {
		ans, err := db1.Get([]byte(fmt.Sprintf("mykey%7d", i)))
		if err != nil {
			t.Log(err)
		}
		if ans != nil {
			//fmt.Println(string(ans))
			sum++
		}
	}
	db1.Close()
	assert.Equal(t, sum, 1000000-999900)

	defer os.RemoveAll(path.Join(DataDir, "chen3"))
}

func TestPutAGet(t *testing.T) {
	d, err := New("chen3", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000000; i++ {
		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	d.Close()

	db, err := Open("chen3", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		t.Log(err)
	}
	db.Put([]byte("kk1"), []byte("kv23"), false)

	ans, err := db.Get([]byte("kk1"))
	if err != nil {
		t.Log(err)
	}

	assert.Equal(t, "kv23", string(ans))

	ans, err = db.Get([]byte(fmt.Sprintf("mykey%7d", 990000)))
	if err != nil {
		t.Log(err)
	}
	// if ans != nil {
	// 	fmt.Println(fmt.Sprintf("mykey%7d", 990000), string(ans))
	// }
	db.Close()

	assert.Equal(t, "myvalue990000", string(ans))

	defer os.RemoveAll(path.Join(DataDir, "chen3"))
}

func TestOpenAndDel(t *testing.T) {

	d, err := New("chen3", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	d.Put([]byte("kk1"), []byte("kv23"), false)

	d.Close()

	db, err := Open("chen3", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		t.Log(err)
	}
	//	db.Put([]byte("kk1"), []byte("kv23"),false)
	db.Delete([]byte("kk1"), false)
	ans, err := db.Get([]byte("kk1"))
	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, []byte(nil), ans)
	db.Close()

	defer os.RemoveAll(path.Join(DataDir, "chen3"))
}

func TestDataExists2(t *testing.T) {
	db, err := New("chen4", DBOpts{DataDir: "", MtSizeLimit: 0})
	if err != nil {
		panic(err)
	}
	for i := 1; i < 1000000; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
	}

	db.Close()

}

// func TestDataLaExists(t *testing.T) {
// 	db, err := New("chen50", DBOpts{DataDir: "", MtSizeLimit: 0})
// 	if err != nil {
// 		panic(err)
// 	}
// 	for i := 1; i < 1000000; i++ {
// 		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
// 	}

// 	db.Close()

// }

// func TestDataPut(t *testing.T) {
// 	d, err := New("chen49", DBOpts{DataDir: "", MtSizeLimit: 0})
// 	if err != nil {
// 		panic(err)
// 	}
// 	for i := 0; i < 1000000; i++ {
// 		d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)), false)
// 	}
// 	d.Close()
// }

// func TestDataPut1(t *testing.T) {
// 	d, err := Open("chen50", DBOpts{DataDir: "", MtSizeLimit: 0})
// 	if err != nil {
// 		panic(err)
// 	}
// 	// for i := 0; i < 1000000; i++ {
// 	// 	d.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)),false)
// 	// }
// 	//d.Put([]byte("kk1"), []byte("kkv"),false)
// 	ans, err1 := d.Get([]byte(fmt.Sprintf("mykey%7dx", 720016)))
// 	if err1 != nil {
// 		t.Log(err)
// 	}
// 	if ans != nil {
// 		fmt.Println(string(ans))
// 	} else {
// 		fmt.Println("not found")
// 	}

// 	d.Close()
// }
