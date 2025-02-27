package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	/*
	 Creating a temporary file that gets passed to our store struct for testing.
	 Testing that there's no errors with creating the file and deferring the
	 file's deletion using the os.Remove method`
	*/
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// Creates new store connection using the created temp file
	s, err := newStore(f)
	require.NoError(t, err)

	// testing various store operations
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	/*
	 Create another store reference with the same file to test that it can
	 read from the same file again
	*/
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	/*
	  appending 3 entries to store and checking that the
	  - its written to the expected position, given a start of an empty file
	*/
	for i := uint64(1); i < 4; i++ {
		bytesWritten, recordPosition, err := s.Append(write)
		require.NoError(t, err)

		/*
			record i = 1
			initial recordPosition will be 0 since we start with an empty file.
			since we're storing each entry as ${sizeOfRecord}${record}, the bytesWritten
			will equal to 8 bytes (the amount of space we allocate to store size of record)
			plus the actual number of bytes for the record.

			since we're writing the same record each time, we have our width value as the expected size offset.

			width = size of record + the 8 bytes we allocate for record size

			so we're testing width * i as our expected value with i being each record we insert into store.

			recordPosition will the start of where the current record is stored
		*/
		require.Equal(t, recordPosition+bytesWritten, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	pos := uint64(0)
	/*
		test case to read the records we stored as part of the testAppend function.
		starting at position 0, we read the first record & test that the returned byte record is
		the same as the one we wrote earlier "write".
		We need increment our position variable by adding the width (size of each record entry) so
		we can read the next entry.
	*/
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	off := int64(0)

	// testing to read the same 3 records we initially created in the testAppend function.

	for i := uint64(1); i < 4; i++ {
		/*
		  reading the first 8 bytes to get the size of the record
		  using the initial offset of 0 to get the first record's size
		*/
		recordSize := make([]byte, lenWidth)
		recordSizeByteCount, err := s.ReadAt(recordSize, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, recordSizeByteCount)

		/*
		   adding 8 bytes to the offset so the next set of bytes
		   we read in will be the actual record entry
		*/
		off += int64(recordSizeByteCount)

		// reading the bytes in BigEndian order since it's stored in BigEndian order and save it as an int64
		size := enc.Uint64(recordSize)
		// making a slice of bytes big enough just to hold the actual record using the recordSize (size due to type changing)
		record := make([]byte, size)
		recordBytes, err := s.ReadAt(record, off)
		require.NoError(t, err)
		require.Equal(t, write, record)
		require.Equal(t, int(size), recordBytes)

		// appending the number records read into record slice to offset so next read can start at the next record
		off += int64(recordBytes)
	}
}

func TestStoreClose(t *testing.T) {
	/*
	   creating temp file to test close functionality
	   and add clean up to delete the temp file.
	*/
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// get size of the file before we appended the entry
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	/*
	   1. create new store with that temp file
	   2. append the test "write" record entry
	*/
	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	// closing the store connection
	err = s.Close()
	require.NoError(t, err)

	// opening the file again to make sure it is larger after we inserted an entry
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

// test util to open file and get size
func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
