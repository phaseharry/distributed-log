package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/phaseharry/distributed-log/serve-requests-with-grpc/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment   // points to the current active segment that's being active written to
	segments      []*segment // points to a list of segments that's still cataloged on disk and hasn't been fully processed yet. (used and then tossed)
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	/*
		reading segment directories on disk into memory and initializing
		the index and store models. sorting it by offset so the oldest offsets
		are at the front of the slice and the newest is at the back
	*/
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}
	/*
		if there were no segments from a previous state, initialize a segment
		so any new records to the log can be assigned to that segment
	*/
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return nil
		}
	}
	return nil
}

/*
- creates a new segment with the passed-in offset value as its loweest offset
- append the newly created segment to the segments slice
- set the newly created segment as the new activeSegment
*/
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	/*
		add new record to current segment and if it has hit maxSize
		after this insert, create a new segment and assign it as the activeSegment
		if the current activeSegment is maxed out
	*/
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	/*
		given an offset, find the segment that the offset belongs in.
		ie. it must be less than or equal to a segments baseOffset but
		less than its nextOffset value
	*/
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	// throw error if we can't find the segment based on the offset
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	/*
	   the offset is the index's location. Segment will get the index entry
	   for that offset to get the location of the actual record and use that location
	   to look the record up in the store
	*/
	return s.Read(off)
}

// closes all segments, but its data is still stored on disk
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// closes all segments and remove all of its data from disk. Assuming that it will be called when all data is processed
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// closes all segments, removes all its data from disk and reinitialize a new log
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

/*
These methods will tell us the offset range for the log and be used for
coordinating services with consensus when the log will be replicated in a cluster.
will be needed to know which nodes have the latest and oldest data and which node is falling
behind and need to replicate
*/
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

/*
truncate removes all segments whose highestOffset value is lower
than the passed in lowest value. It will delete the segments disk files.
This is done because we don't have infinite disk space and hopefully by
the time of Truncate, the records in those segments were processed already
*/
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

/*
originReader satisfies the io.Reader interface
- returns a reader to read the entire log (combines all segments into 1 interface)
- will need for implementation of coodinate consensus, support snapshots, and restoring a log
- each segment is turned into a io.Reader interface compatibile struct and then merged into the io.MultiReader interface for the log as a whole
- io.Reader -> io.MultiReader interface usage to ensure we start reading with the lowest offset segment to the highest offset segment (ordered) and
that the whole file is read
*/
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store // deconstructing the store attributes and adding it directly to originReader struct
	off    int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
