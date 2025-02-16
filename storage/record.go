package storage

type RecordType int8

const (
	RecordUpdate RecordType = iota // indicates that this record was an update
	RecordDelete                   // indicates that this record was a delete
)

/*
	|Record|
	|......|
	|Record|
	|index block|
	|footer|

*/
// Record is an in-memory representation of an update on the datastore
type Record struct {
	Key   []byte
	Value []byte
	Type  RecordType
}

// RecordPointer is a pointer to a Record on disk
type RecordPointer struct {
	Key       []byte
	StartByte uint32
	Length    uint32
}

// Footer is the last entry in an sstable. It points to the first index in the list
// of indices within the file. Length is the length of the index entry in bytes
type Footer struct {
	IndexStartByte uint32
	Length         uint32 //第一个索引的len
	IndexEntries   uint32 //索引数目
	// BloomStartByte uint32
	// BLength        uint32
}

func NewRecord(key []byte, value []byte, delete bool) *Record {
	var rType RecordType
	if delete {
		rType = RecordDelete
	} else {
		rType = RecordUpdate
	}

	return &Record{
		Key:   key,
		Value: value,
		Type:  rType,
	}
}
