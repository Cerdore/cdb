package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/cerdore/cdb/memtable"
	"github.com/cerdore/cdb/storage"
	"github.com/cerdore/cdb/util"
)

// WAL is the structure representing the writeahead log. All updates (incl. deletes)
// are first written to the writeahead log before being stored anywhere else (i.e. memtable,
//  sstables). This ensures that upon crash, memtable that was in memory can be regenerated
// from the writeahead log
type WAL struct {
	codec   storage.Codec
	logFile *os.File
	size    uint32
	writer  *bufio.Writer
}

const (
	walPrefix  = "wal"
	uint32size = 4
)

// New creates a new writeahead log and returns a reference to it
func New(file *os.File) *WAL {
	return &WAL{codec: storage.Codec{}, logFile: file, size: 0, writer: bufio.NewWriter(file)}
}

func CreateFile(dbName string, dataDir string) (*os.File, error) {
	return util.CreateFile(fmt.Sprintf("%s_%s_%d", walPrefix, dbName, time.Now().UnixNano()/1_000_000_000),
		dbName, dataDir)
}

// FindExisting returns true and the WAL filename if an existing WAL is fine. Otherwise, returns false
func FindExisting(dbName string, dataDir string) (bool, *WAL, error) {
	search := path.Join(dataDir, dbName, fmt.Sprintf("%s_%s_*", walPrefix, dbName))
	matches, err := filepath.Glob(search)
	if err != nil {
		return false, nil, fmt.Errorf("error loading WAL file: %w", err)
	} else if len(matches) == 0 {
		return false, nil, nil
	} else if len(matches) > 1 {
		return false, nil, fmt.Errorf("multiple WAL files detected: %v", matches)
	}

	file, err := os.OpenFile(matches[0], os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return false, nil, fmt.Errorf("error opening existing WAL file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return false, nil, fmt.Errorf("error retrieving file info for WAL: %w", err)
	}

	wal := New(file)
	wal.size = uint32(info.Size())

	return true, wal, nil
}

// Write writes the record to the writeahead log
func (wlog *WAL) WriteSync(record *storage.Record) error {
	data, err := wlog.codec.Encode(record)
	if err != nil {
		return fmt.Errorf("failed encoding data to write to log: %w", err)
	}

	if n, err := wlog.logFile.Write(data); n != len(data) {
		return fmt.Errorf("failed to write entirety of data to log, bytes written=%d, expected=%d, err=%w",
			n, len(data), err)
	} else if err != nil {
		return fmt.Errorf("failed to write data to log: %w", err)
	}

	// update current size of WAL
	wlog.size += uint32(len(data))

	// if syncW == true {
	// 	if err := wlog.logFile.Sync(); err != nil {
	// 		return fmt.Errorf("failed syncing data to disk: %w", err)
	// 	}
	// }

	return nil
}

func (wlog *WAL) Write(record *storage.Record, syncW bool) error {
	//t1 := time.Now()
	data, err := wlog.codec.Encode(record)
	if err != nil {
		return fmt.Errorf("failed encoding data to write to log: %w", err)
	}
	//el1 := time.Since(t1)
	//t2 := time.Now()

	// if n, err := wlog.logFile.Write(data); n != len(data) {
	// 	return fmt.Errorf("failed to write entirety of data to log, bytes written=%d, expected=%d, err=%w",
	// 		n, len(data), err)
	// } else if err != nil {
	// 	return fmt.Errorf("failed to write data to log: %w", err)
	// }

	if n, err := wlog.writer.Write(data); n != len(data) {
		return fmt.Errorf("failed to write entirety of data to log, bytes written=%d, expected=%d, err=%w",
			n, len(data), err)
	} else if err != nil {
		return fmt.Errorf("failed to write data to log: %w", err)
	}

	// el2 := time.Since(t2)
	// t3 := time.Now()

	// update current size of WAL
	wlog.size += uint32(len(data))

	// el3 := time.Since(t3)
	// fmt.Println(el2, el3)

	if syncW == true {
		if err := wlog.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush into log: %w", err)
		}
		if err := wlog.logFile.Sync(); err != nil {
			return fmt.Errorf("failed syncing data to disk: %w", err)
		}
	}

	return nil
}

func (wlog *WAL) Size() uint32 {
	return wlog.size
}

func (wlog *WAL) Restore(mem *memtable.MemTable) error {
	for {
		data := make([]byte, uint32size)
		if n, err := wlog.logFile.Read(data); err == io.EOF {
			break
		} else if n != len(data) {
			return fmt.Errorf("failed to read expected amount of data from WAL."+
				" read=%d, expected=%d", n, len(data))
		} else if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}

		rLen := binary.BigEndian.Uint32(data)

		recBytes := make([]byte, rLen)
		if n, err := wlog.logFile.Read(recBytes); uint32(n) != rLen {
			return fmt.Errorf("failed to read expected amount of record data from WAL."+
				" read=%d, expected=%d", n, rLen)
		} else if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}

		record, err := wlog.codec.Decode(recBytes)
		if err != nil {
			return fmt.Errorf("failed to decoding record: %w", err)
		}

		if record.Type == storage.RecordUpdate {
			mem.Put(record.Key, record.Value)
		} else {
			mem.Delete(record.Key)
		}
	}

	return nil
}

//TODO: old wal files need to remove
func (wlog *WAL) Close() error {
	wlog.logFile.Sync()
	filename := wlog.logFile.Name()
	if err := wlog.logFile.Close(); err != nil {
		return fmt.Errorf("failed attempting to close WAL log file: %w", err)
	}

	// TODO: if this fails, the log file is closed and future calls to Close will error
	// on the os.File#Close call. Could leave an old WAL around
	if err := os.Remove(filename); err != nil {
		wlog.logFile.Close()
		os.Remove(filename)
		return fmt.Errorf("failed attempting to remove WAL file: %w", err)
	}
	return nil
}
