package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// number of bytes allocated to the index offset (the position of where the index entry is stored in the index file)
	offWidth uint64 = 4 // 4 bytes / 32 bits
	// number of bytes allocated to store the position / offset of the record within the store file. (the byte position of where the log entry is stored)
	posWidth uint64 = 8 // 8 bytes / 64 bits
	/*
		Each index entry will be a combination of {indexOffsetPosition}{recordPositionWithStoreFile} with the
		index's offset (where the index is stored in the file) is 4 bytes and the recordPositionWithStoreFile is 8 bytes.
		We use the entWidth value teo jump to the position of an index entry since the position of the index entry
		is offset * entWidth.
		ex.
			- the first index entry will have offset 0 and 0 * 12 = 0 so the first index entry will be at 0 bytes in the index file.
		  - the second index entry will have the offset 1 and 1 * 12 = 12 so the 2nd index entry will be at 12 bytes in the index file.
		  - etc...
	*/
	entWidth uint64 = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}
