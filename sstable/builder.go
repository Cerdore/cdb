package sstable

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/cerdore/cdb/bloom"
	"github.com/cerdore/cdb/memtable/interfaces"
	"github.com/cerdore/cdb/storage"
	"github.com/cerdore/cdb/util"
)

// Builder is a structure that can take an iterator from a memtable data structure and use
// that to create an SSTable
type Builder struct {
	name  string
	iter  interfaces.InternalIterator
	codec *storage.Codec
	//writer         io.Writer
	writer         *bufio.Writer
	indexPerRecord int
	level          int
}

const (
	// TODO: Make this configurable as option? Don't make configurable without removing assumtion in sstable.Search
	// and sstable.Merger
	// that this won't change -- will need to encode in the metadata
	indexCount = 1000
	sstPrefix  = "sstable"
	footerLen  = 12
)

func CreateFile(dbName string, DataDir string) (*os.File, error) {
	// TODO: needs a nonce to prevent potential collusions of really quickly created sstable files
	return util.CreateFile(fmt.Sprintf("%s_%s_%d", sstPrefix, dbName, time.Now().UnixNano()/1_000_000),
		dbName, DataDir)
}

func NewBuilder(name string, iter interfaces.InternalIterator, level int, writer *os.File) *Builder {
	return newBuilder(name, iter, level, writer, indexCount)
}

func newBuilder(name string, iter interfaces.InternalIterator, level int, writer *os.File, indexPerRecord int) *Builder {
	return &Builder{
		name:           name,
		iter:           iter,
		codec:          &storage.Codec{},
		writer:         bufio.NewWriter(writer),
		indexPerRecord: indexPerRecord,
		level:          level}
}

// TODO: crashing while writing -- what to do?
// WriteTable writes data from memtable iterator to an sstable file.
/*
	record
	...
	record
	//[filter block]
	index_block

	footer
*/
func (s *Builder) WriteTable(memNum uint32) (*Metadata, error) {
	recWritten := 0 //已写数据条数
	bytesWritten := uint32(0)

	bloom := bloom.NewBloom(int(memNum))

	indices := make(map[string]storage.RecordPointer) //rec的索引信息

	var order []string

	var firstKey []byte
	var lastKey []byte

	// Write actual key-values to disk
	for ; s.iter.HasNext(); recWritten++ {
		//fmt.Println(recWritten)
		rec := s.iter.Next()
		if rec == nil {
			fmt.Println("error in loop because iter")
			break
		}
		if firstKey == nil {
			firstKey = rec.Key
		}

		bloom.Insert(rec.Key)

		bytes, err := s.codec.Encode(rec)
		if err != nil {
			return nil, fmt.Errorf("could not encode record: %w", err)
		}

		//if err = write(s.writer, bytes); err != nil {
		if n, err := s.writer.Write(bytes); err != nil || n != len(bytes) {
			return nil, fmt.Errorf("failed attempting to write to level 0 sstable: %w", err)
		}

		// Create index entry if reached threshold for number of written records
		if recWritten%s.indexPerRecord == 0 {
			indices[string(rec.Key)] = storage.RecordPointer{Key: rec.Key, StartByte: bytesWritten, Length: uint32(len(bytes))}
			order = append(order, string(rec.Key))
		}

		lastKey = rec.Key
		bytesWritten += uint32(len(bytes))
	}

	// bloom Here
	// bloomStart := bytesWritten
	// bLen := len(bloom.Bytes())
	// //if err := write(s.writer, bloom.Bytes()); err != nil {
	// if n, err := s.writer.Write(bloom.Bytes()); err != nil || n != len(bloom.Bytes()) {
	// 	return nil, fmt.Errorf("failed attempting to write the bloom to level 0 sstable: %w", err)
	// }
	// bytesWritten += uint32(bLen)

	indexStart := bytesWritten
	firstLen := 0
	// Write index blocks in correct order
	for _, key := range order {
		ptr := indices[key]
		bytes, err := s.codec.EncodePointer(&ptr)
		if err != nil {
			return nil, fmt.Errorf("could not encode index pointer record: %w", err)
		}

		//if err = write(s.writer, bytes); err != nil {
		if n, err := s.writer.Write(bytes); err != nil || n != len(bytes) {
			return nil, fmt.Errorf("failed attempting to write to level 0 sstable: %w", err)
		}

		// Keep length of first index block written for use in footer pointer
		if firstLen == 0 {
			firstLen += len(bytes)
		}
	}

	// Write footer
	bytes, err := s.codec.EncodeFooter(&storage.Footer{
		IndexStartByte: indexStart,
		Length:         uint32(firstLen),
		IndexEntries:   uint32(len(indices)),
		// BloomStartByte: uint32(bloomStart),
		// BLength:        uint32(bLen),
	})
	if err != nil {
		return nil, fmt.Errorf("could not encode footer pointer record: %w", err)
	}

	//if err = write(s.writer, bytes); err != nil {
	if n, err := s.writer.Write(bytes); err != nil || n != len(bytes) {
		return nil, fmt.Errorf("failed attempting to write to level 0 sstable: %w", err)
	}

	if err := s.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush into disk: %w", err)
	}
	return &Metadata{
		Level:    uint8(s.level),
		Filename: s.name,
		StartKey: firstKey,
		EndKey:   lastKey,
		Bits:     bloom.Bytes(),
	}, nil
}
