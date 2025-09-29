package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/phaseharry/distributed-log/write-a-log-package/api/v1"
	"google.golang.org/protobuf/proto"
)

/*
segment wraps around index and the actual record store to coordinate operations between the two.
ex. when log has to write data to the active segment, the segment needs to write the actual record
to storage as well as to the index file that points to the actual record

need the baseOffset and nextOffset to know what offsets to append new records under and to use to
calculate the relative offsets for index entries. adding these values to store so we know when a segment
is maxed out based on the config and a new segment will be created as the current segment for new records
*/
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

/*
called to create a new segment when a new segment is needed by the log.
ex. when the current active segment hit its max size as configured by the config
opening the store & index files with the OS._CREATE flag to create the file if it doesn't exist
and os.O_APPEND to append to the files on updates and not overwrite
*/
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error

	// opening up store file that is associated with this baseOffset segment.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d,%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// opening up index file that is associated with this baseOffset segment.
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".omdex")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	/*
	   reading the latest offset where the next record entry should be placed.
	   - if there is no error then the offset that the next record will be placed at is where the offset of the index is current at plus 1
	   because that is where the next record's index iwll be placed
	   - if there is an error when reading in -1 (where the last added entry is) then that means the file was empty and there is nothing to
	   read, indicating that the offset where the next entry should be placed is the baseOffset
	*/
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	/*
	   assigning the nextOffset value to the currently appended record and marshaling it (turning it into binary)
	   to prep it for saving it in store file
	*/
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		/*
			index offsets are relative to base offset.
			ex. 0, 1, 2, etc. will be used for the same index file for each entry to an index and it will map that offset to the actual position of the record within a store file
		*/
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	/*
		1. given an absolute offset value, use it to get the position of the index entry by subtracting	the baseOffset to get the position of the index entry for offset (relative offset).
		2. use the position value that the index points to to get the actual binary of the record
		3. unmarshal the binary to get the actual record of the log
	*/
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}
