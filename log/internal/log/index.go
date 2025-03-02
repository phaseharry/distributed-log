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
	// create a new index that holds the persisted file
	idx := &index{
		file: f,
	}

	/*
		getting the file stats we get the value of the current size of the file.
		this will let us know where our next entry will be appended within the next file.
		- if the file used is a brand new empty file, then it will be 0
		- if it's an existing file then it will be the byte position of where the next record is appended
	*/
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	/*
		need to check:
		assuming that if the file is not the size of "MaxIndexBytes" yet, truncate will
		actually increase the file size by adding spaces. this seems to be needed to give
		the gommap memory map implementation the max size it's memory mapped file should have.
		as part of this truncating process to add white space, the last index entry will not
		be within the actual last byte within the file. To remedy this, when we close the file,
		we have to truncate to remove any white spaces so if it reopens, it will point to the correct position for the next appended record
	*/
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	/*
		initializing our memory mapped file by linking it to the passed in index file
		and giving it read and write permissions.
		using this memory map messes up the sizing of the file as it grows.
		reads in the file and uses its size as the max length of the memory map.
		that's why we have to truncate it in the above step to the "MaxIndexBytes" size.

		ex. if MaxIndexBytes was 10, then there will be a list of 10 bytes where each idx correlates to the
		position of the file offset.
	*/
	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

/*
Read takes in an offset value ("in" variable) and returns the associated record's position in the store.
The given offset is relative to the segment's base offset. Using relative offsets to reduce
the size of the indexes by storing it as uint32s (4 bytes). If we were to use absolute values
then we would need to store the values as uint64s (8 bytes).
*/
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	/*
	   Trying to read while our index size is 0, means we don't have
	   any records yet so return an EOF error.
	*/
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	/*
		Supports reading the latest element of the index by accepting -1 value
		and calculating the last element's offset give the current size.

		Converting to int32 because the index's offset value is 4 bytes
	*/
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	/*
	   using the offset to calculate the actual byte position of where it would be stored within the file.
	   ex.
	   out = 0 (first entry in index)
	   pos = 0 * 12 bytes = 0

	   out = 1 (second entry in index)
	   pos = 1 * 12 bytes = 12
	*/
	pos = uint64(out) * entWidth
	// throw error if the position calculated is greater than our actual size (out of bounds / EOF error)
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	/*
	   Reading the data using pos as the initial offset.
	   out = pos : pos + offWidth (not inclusive)
	   pos = pos + offWidth : pos + endWidth (not inclusive)
	*/
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	/*
	   checking the memory maps max size in bytes.
	   if currentSize + the size of a new entry is greater than the max memory map size,
	   then return an EOF err
	*/
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	/*
	   store the index entries offset using the current [idx.size : idx.size + offWidth ([0:4]) (4 bytes)
	   store the position of the actual log entry using [idz.size + offWidth : to i.size + entWidth] [4: 12] (8 bytes)
	*/
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// incrementing the size by endWidth so the next index entry will be at the next offset
	i.size += uint64(entWidth)
	return nil
}

// method to return the index file path
func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	/*
		When .Close() is called, we want to make sure our memory mapped file
		gets synced with the actual index file. This will force the sync and not wait until
		resources are freed up.
		When the sync is done then we make sure the persisted file's buffers are flushed to disk
		to make sure all changes are saved.
	*/

	// syncing memory map to persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// making sure persisted file flushes its buffers to save contents to disk
	if err := i.file.Sync(); err != nil {
		return err
	}

	/*
		truncating the file to only contain the data we actually have.
		this will ensure that if this file hasn't reached the "MaxIndexBytes"
		and this file gets reopened as an index, the "size" value will be the correct
		position for where the next log entry is stored.
	*/
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}
