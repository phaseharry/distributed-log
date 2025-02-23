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
	enc = binary.BigEndian
)

const (
	lenWidth = 8 // 8 bytes
)

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
