package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

/*
BigEndian & LittleEndian depicts how bytes are ordered.

Big Endian order is where the most significant byte (MSB) is stored at the smallest address, while the least significant byte (LSB) is
stored at the largest address. This means the byte ordering is from left to right, with the most significant byte first.

ex) for data 0x12345678 (32 bit value / 4 bytes), 0x12 is the MSB and 0x78 is the LSB.

	Address: [0x100, 0x101, 0x102, 0x103]
	Data:    [0x12,  0x34, 0x56, 0x78]

	This is how we normally see numbers. 0x12 would be the MSB because it has the highest power.

Little Endian order is where the MSB is stored at the largest address, while the LSB is stored at the smallest address. This means the ordering
is from right to left, with the LSB first.

ex) for data 0x12345678, 0x12 is would be in the largest address allocated to the data and 0x78 would be in the smallest address

	Address: [0x100, 0x101, 0x102, 0x103]
	Data:    [0x78, 0x56, 0x34, 0x12]
*/
var (
	enc = binary.BigEndian // encoding used to persist record sizes & index entries in
)

const (
	lenWidth = 8 // 8 for 8 bytes used to store the record's length
)

/*
wrapper around a file that appends and read bytes
from a file
*/
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (uint64, uint64, error) {
	// making sure that we have exclusive write access when append a record
	s.mu.Lock()
	defer s.mu.Unlock()

	// where the current record will be stored.
	position := s.size

	/*
		Writes the size of the incoming record to store file, so when we read the record, we know how many bytes to read.
		ex) Since we're writing a []byte slice, we know if there's 8 elements then the total size of our record is 8 bytes (64 bits).
		If is 100 elements then it is 100 bytes, etc. We get the len of the slice to get the size of the record we're saving and prefixing it
		in our store before we store our data. Since we're using uint64 to save the length of the byte array, we use 8 bytes just to save the size of the record.
		see: https://go.dev/ref/spec#Size_and_alignment_guarantees
	*/
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	/*
		Writes the actual record to the file. Writing through buffer instead of directly to file to reduce sys-calls and improve performance.
		ex) if a user were to write a lot of small records, they would all be stored in buffer and get written to the file when resources are available.

		Returns the number of bytes written to the buffer so we can use it to update our store's size. This size value will be used to calculate
		the position of the next record & decide where it will be stored. Will also be used when we're done writing to this file as it has maxxed out its
		stated max size.
	*/
	bytesWritten, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	/*
		Every time a record is written to the store file, we write the {recordSize}{recordDate} to the file in that order.
		So to correctly account for the record size data, we have to add 8 to our bytesWritten variable and then add it to size,
		so the next record can be appended in the next free available slot adjacent to this record.
	*/
	bytesWritten += lenWidth
	// this will be where the next record is stored
	s.size += uint64(bytesWritten)
	/*
		return
		1. the number of bytes written to file (Go APIs conventionally do this),
		2. position where the store holds the record in file
		3. error if this is
	*/
	return uint64(bytesWritten), position, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	/*
		Write anything that's still within buffers to the actual store file incase we're trying
		to read a file that hasn't been flushed to disk (file) yet.
	*/
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	/*
	 Make an 8 byte slice to contain the size value of the record that's stored within the store file as a prefix.
	 Will use this value to create a []byte that has the exact size needed to hold the record
	*/
	size := make([]byte, lenWidth)

	/*
		Using the passed in position (offset that's a size value of where to start looking), read in exactly len(size) (lenWidth) bytes into
		the size slice.
	*/
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	/*
		creates byte slice that's exactly ${size} bytes using the bigEndian encoding format so the data will store the MSB in the smallest address and the LSB
		in the largest address. Then using the initial position size offset and adding lengthWidth (8) to offset the size value, we read in the number of bytes
		that's the size of our record.
	*/
	record := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(record, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

/*
The below method will just read the len(p) bytes starting at the offset size.
There is no additional logic to get size and using that size to read in the exact record, (nothing more, nothing less).
This will just read whatever size is the byte slice starting at the offset value.
*/
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flushing buffer to disk. If error, stop processing
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

/*
Closing the current file connection to the store.
1. flush any existing bytes within buffer to file (persist any buffered data before closing file)
2. close the file connection
*/
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
