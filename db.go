package cdb

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cerdore/cdb/bloom"

	"github.com/cerdore/cdb/compaction"
	"github.com/cerdore/cdb/manifest"
	"github.com/cerdore/cdb/memtable"
	"github.com/cerdore/cdb/sstable"
	"github.com/cerdore/cdb/storage"
	"github.com/cerdore/cdb/wal"

	log "github.com/sirupsen/logrus"
)

// TODO: copy keys and values passed as arguments
// TODO: check key and value size and fail if > threshold

// DB represents the API for database access
// One process can have a database open at a time
// Calls to Get, Put, Delete are thread-safe
type DB struct {
	name    string
	DataDir string

	mutex     sync.RWMutex
	memTable  *memtable.MemTable
	walog     *wal.WAL
	manifest  *manifest.Manifest
	compactor *compaction.Compactor

	compactingMemTable *memtable.MemTable
	compactingWAL      *wal.WAL
	compact            chan bool
	stopWatching       chan bool
	MtSizeLimit        uint32
	wg                 sync.WaitGroup
}

// TODO: allow configuration via options provided to constructor
const (
	// Linux
	DataDir  = "/home/cerdore/cdb"
	lockFile = "__DB_LOCK__"
	// Limit memtable to 4 MBs before flushing
	//MtSizeLimit = uint32(4194304)
	MtSizeLimit = uint32(4 * 1024 * 1024)
	//MtSizeLimit = uint32(4096)
)

type DBOpts struct {
	DataDir     string
	MtSizeLimit uint32
}

func (o *DBOpts) applyDefaults() {
	if o.DataDir == "" {
		o.DataDir = DataDir
	}

	if o.MtSizeLimit == 0 {
		o.MtSizeLimit = MtSizeLimit
	}
}

// New creates a new database if it's not exists
func New(name string, opts DBOpts) (*DB, error) {
	opts.applyDefaults()

	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create data dir %s: %w", opts.DataDir, err)
	}

	dbPath := path.Join(opts.DataDir, name)

	if exists, err := exists(name, opts.DataDir); !exists {
		if err := os.Mkdir(dbPath, 0755); err != nil {
			return nil, fmt.Errorf("failed creating data directory for database %s: %w", name, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("could not create new database: %w", err)
	} else {
		return nil, fmt.Errorf("database %s already exists. use DB#Open instead", name)
	}

	return Open(name, opts)
}

func lock(name string, DataDir string) error {
	pid := os.Getpid()
	lockPath := path.Join(DataDir, name, lockFile)

	lock, err := os.Open(lockPath)
	// not locked
	if os.IsNotExist(err) {
		if lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666); os.IsExist(err) {
			return fmt.Errorf("cannot lock database. already locked by another process")
		} else if err != nil {
			return fmt.Errorf("failure attempting to lock database: %w", err)
		} else {
			pidBytes := []byte(strconv.Itoa(pid))
			if n, err := lockFile.Write(pidBytes); n < len(pidBytes) {
				return fmt.Errorf("failure writing owner pid to lock file. wrote %d bytes, expected %d",
					n, len(pidBytes))
			} else if err != nil {
				return fmt.Errorf("failure writing owner pid to lock file: %w", err)
			}
			return nil
		}
	} else if err != nil {
		return fmt.Errorf("failure attempting to lock database: %w", err)
	} else {
		// locked
		scanner := bufio.NewScanner(lock)
		scanner.Scan()
		lockPid, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return fmt.Errorf("failed attempting to read lockfile: %w", err)
		}

		if lockPid == pid {
			return nil
		} else {
			return fmt.Errorf("cannot lock database. already locked by another process (%d)", lockPid)
		}
	}
}

// Open opens a database
func Open(name string, opts DBOpts) (*DB, error) {
	opts.applyDefaults()
	log.Info("open database: ", name)
	if exists, err := exists(name, opts.DataDir); !exists {
		if err == nil {
			return nil, fmt.Errorf("failed opening database %s. does not exist", name)
		} else {
			return nil, fmt.Errorf("failed opening database %s: %v", name, err)
		}
	}

	if err := lock(name, opts.DataDir); err != nil {
		return nil, fmt.Errorf("could not lock database: %w", err)
	}

	mem := memtable.New()

	// load WAL if exists
	found, walog, err := wal.FindExisting(name, opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed attempting to look for existing WAL file: %w", err)
	}

	if !found {
		waf, err := wal.CreateFile(name, opts.DataDir)
		if err != nil {
			return nil, fmt.Errorf("could not create WAL file: %w", err)
		}
		walog = wal.New(waf)
	} else {
		if err = walog.Restore(mem); err != nil {
			return nil, fmt.Errorf("failed attempting to restore WAL: %w", err)
		}
	}

	found, man, err := manifest.LoadLatest(name, opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed attempting to load manifest file: %w", err)
	} else if !found {
		maf, err := manifest.CreateManifestFile(name, opts.DataDir)
		if err != nil {
			return nil, fmt.Errorf("could not create manifest file: %w", err)
		}
		man = manifest.NewManifest(maf)
	}

	db := &DB{
		memTable:     mem,
		walog:        walog,
		manifest:     man,
		compactor:    compaction.New(man, opts.DataDir, name),
		name:         name,
		DataDir:      opts.DataDir,
		compact:      make(chan bool, 1),
		stopWatching: make(chan bool),
		MtSizeLimit:  opts.MtSizeLimit,
	}
	db.wg.Add(1)
	go db.compactionWatcher()
	return db, nil
}

func exists(name string, DataDir string) (bool, error) {
	dbPath := path.Join(DataDir, name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failure checking to see if database already exists: %w", err)
	} else {
		return true, nil
	}
}

// OpenOrNew opens the DB if it exists or creates it if it doesn't
func OpenOrNew(name string, opts DBOpts) (*DB, error) {
	opts.applyDefaults()

	dbExists, err := exists(name, opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed checking if database %s already exists: %v", name, err)
	}

	if dbExists {
		return Open(name, opts)
	} else {
		return New(name, opts)
	}
}

// Close ensures that any resources used by the DB are tidied up
func (d *DB) Close() error {
	// TODO:
	// flush to memtable here
	close(d.stopWatching)
	d.wg.Wait()
	log.Info("Already closed db: ", d.name)
	return d.unlock()
}

func (d *DB) unlock() error {
	lockPath := path.Join(d.DataDir, d.name, lockFile)
	return os.Remove(lockPath)
}

// Get returns the value associated with the key. If key is not found then
// the value returned is nil
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	val := d.memTable.Get(key)
	if val == nil && d.compactingMemTable != nil {
		val = d.compactingMemTable.Get(key)
	}

	// TODO: add a bloom filter to reduce need to potentially check every level
	// TODO: can we unlock during this search? issue to solve is sstables getting compacted while searching
	if val == nil {
		// 255 == uint8 max == max number of levels based on value used for encoding level information on disk
	levelTraversal:
		for i := 0; i < 255; i++ {
			for _, meta := range d.manifest.MetadataForLevel(i) {
				if meta == nil {
					break levelTraversal
				}
				if meta.ContainsKey(key) {
					bloom := bloom.RecoverBloom(meta.Bits)
					if !bloom.Check(key) {
						return nil, nil
					}
					val, err := d.searchSSTable(key, meta)
					if err != nil {
						return nil, fmt.Errorf("failed attempting to scan sstable for key %s: %w", string(key), err)
					}

					if val != nil {
						return val, nil
					}
				}
			}
		}
	}

	return val, nil
}

func (d *DB) searchSSTable(key []byte, meta *sstable.Metadata) ([]byte, error) {
	// TODO: cache this instead of opening and closing every time
	sstHandle, err := os.Open(path.Join(d.DataDir, d.name, meta.Filename))
	if err != nil {
		return nil, fmt.Errorf("failed attempting to open sstable for reading: %w", err)
	}
	defer sstHandle.Close()

	return sstable.Search(key, sstHandle)
}

// Put inserts or updates the value if the key already exists
func (d *DB) Put(key []byte, value []byte, syncW bool) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if err := d.walog.Write(storage.NewRecord(key, value, false), syncW); err != nil {
		return fmt.Errorf("failed attempting write put to WAL: %w", err)
	}

	d.memTable.Put(key, value)

	// compactingMemTable not being nil indicating that a compaction is already underway
	if d.memTable.Size() > d.MtSizeLimit && d.compactingMemTable == nil {
		//fmt.Println("begin to Compact ")
		d.compactingMemTable = d.memTable
		d.compactingWAL = d.walog

		d.memTable = memtable.New()

		waf, err := wal.CreateFile(d.name, d.DataDir)
		if err != nil {
			// Abort compaction attempt
			d.memTable = d.compactingMemTable
			d.walog = d.compactingWAL

			// if err := d.compactingWAL.Close(); err != nil {
			// 	return fmt.Errorf("compactingWAL closed error : %w", err)
			// }

			d.compactingMemTable = nil
			d.compactingWAL = nil

			return fmt.Errorf("could not create WAL file: %w", err)
		}
		d.walog = wal.New(waf)

		d.compact <- true
	}

	return nil
}

// Deletes the specified key from the data store
func (d *DB) Delete(key []byte, syncW bool) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if err := d.walog.Write(storage.NewRecord(key, nil, true), syncW); err != nil {
		return fmt.Errorf("failed attempting write delete to WAL: %w", err)
	}
	d.memTable.Delete(key)

	return nil
}

func (d *DB) compactionWatcher() {
	defer d.wg.Done()
	for {
		select {
		case <-d.compact:
			//fmt.Println("watcher found chanel changed!!")
			d.wg.Add(1)
			if err := d.doCompaction(); err != nil {
				log.Errorf("error performing compaction: %v", err)
			}
		case <-d.stopWatching:
			return
		}
	}
}

func (d *DB) flushMemTable(tableName string, writer *os.File, memNum uint32) error {
	iter := d.compactingMemTable.InternalIterator()

	//fmt.Println(d.compactingMemTable.Num())

	builder := sstable.NewBuilder(tableName, iter, 0, writer)
	metadata, err := builder.WriteTable(memNum)
	if err != nil {
		return fmt.Errorf("could not write memtable to level 0 sstable: %w", err)
	}

	return d.manifest.AddEntry(manifest.NewEntry(metadata, false))
}

func (d *DB) doCompaction() error {
	defer d.wg.Done()
	if d.compactingMemTable != nil {
		file, err := sstable.CreateFile(d.name, d.DataDir)
		if err != nil {
			return fmt.Errorf("failed attempt to create new sstable file: %w", err)
		}
		defer file.Close()

		err = d.flushMemTable(filepath.Base(file.Name()), file, d.compactingMemTable.Num())

		if err == nil {
			if err = file.Sync(); err != nil {
				return fmt.Errorf("error flushing sstable to disk: %w", err)
			}

			d.mutex.Lock()
			defer d.mutex.Unlock()

			if err = d.compactingWAL.Close(); err != nil {
				return fmt.Errorf("failed attempt to close WAL: %w", err)
			}

			d.compactingMemTable = nil
			d.compactingWAL = nil
		} else {
			fmt.Println("Closed error %w: ", err)
		}
	}

	if err := d.compactor.Compact(); err != nil {
		return fmt.Errorf("failed attempting to compact: %w", err)
	}

	return nil
}
