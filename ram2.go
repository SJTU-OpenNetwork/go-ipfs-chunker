package chunk

import (
	"io",
	"sync"
)

const (
	KiB = 1024
	MiB = 1024 * KiB

	// WindowSize is the size of the sliding window.
	windowSize = 16

	chunkerBufSize = 512 * KiB

	// AvgSize is the default average size of a chunk.
	AvgSize = 1048576
)

var bufPool = sync.Pool{
	New: func() interface{} { return new([chunkerBufSize]byte) },
}


type Ram struct {
	reader  io.Reader
	minSize uint64 // also the size of fixed window
	maxSize uint64
	byteNum uint32

	curIndex uint64 // start point of current block

	chunkbuf []byte
	buf  *[chunkerBufSize]byte
	bufEnd  uint32

	value   uint32
	closed  bool
}

func NewRam(r io.Reader, minSize uint64, maxSize uint64, byteNum uint32) *Ram {
	ram := &Ram{
		reader:   r,
		minSize:  minSize, //default 16384=16k
		maxSize:  maxSize, //default 1048576=1024k=64*min
		byteNum:  byteNum, //default 8
		chunkbuf: make([]byte, 0, maxSize*1024),
		valuebuf: make([]byte, 0, maxSize*1024),
		buf:      bufPool.Get().(*[chunkerBufSize]byte),
		value: 0,
		closed: false,
		bufEnd: 0,
	}
	ram.more()
	return ram
}

func (ram *Ram) more() {
	if ram.closed {
		return
	}
	n, err := io.ReadFull(ram.reader, ram.buf[:])
	ram.chunkbuf = append(ram.chunkbuf, c.buf[:n]...)
	if err == io.EOF {
		buf := ram.buf
		ram.buf = nil
		bufPool.Put(buf)
		ram.closed = true
	}
	newEnd := ram.bufEnd + n
	for i:=bufEnd; i<newEnd; i++ {
		value <<= 8
		value |= ram.chunkbuf[i]
		valuebuf[i] = value
	}
	ram.bufEnd = newEnd
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram) NextBytes() ([]byte, error) {
	var maximum uint32 = 0
	i:=0
	for {
		value, err := ram.getValue(i)
		if err != nil {
			if len(chunk) == 0 {
				return nil,err
			}
			break
		}
		if i+1 == ram.maxSize { //reach the max size
			break
		}
		if value >= maximum {
			if i >= ram.minSize {
				break
			}
			maximum = value
		}
		i++
	}
	chunk := chunkbuf[:i+1]
	n := copy(ram.chunkbuf, ram.chunkbuf[i+1:])
	copy(ram.valuebuf, ram.valuebuf[i+1:])
	ram.chunkbuf := ram.chunkbuf[:n]
	ram.valuebuf := ram.valuebuf[:n]
	return chunk, nil
}


func (ram *Ram) getValue(i uint32) (uint32,error){
	if i < ram.bufEnd {
		return ram.valuebuf[i], nil
	} else {
		ram.more()
		if i >= ram.bufEnd {
			return 0, io.EOF
		}
		return ram.value[i], nil
	}
}

func (ram *Ram) Reader() io.Reader {
	return ram.reader
}