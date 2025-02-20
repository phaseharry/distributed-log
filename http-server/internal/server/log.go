package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mutex   sync.Mutex
	records []Record
}

type Record struct {
	Value  []byte `json: "value`
	Offset uint64 `json: "offset"`
}

func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(record Record) (uint64, error) {
	l.mutex.Lock()
	// assigning an index to the record that's being appended to our log
	record.Offset = uint64(len(l.records))
	l.records = append(l.records, record)
	l.mutex.Unlock()

	return record.Offset, nil
}

func (l *Log) Read(offset uint64) (Record, error) {
	l.mutex.Lock()
	// deferring the mutex.Unlock call so it will call after we've returned out of this function
	defer l.mutex.Unlock()
	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return l.records[offset], nil
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
