package log

type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint32
		InitialOffset uint64
	}
}
