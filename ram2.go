package chunk

import (
	"io"
	"sync"
)

const (
	KiB = 1024
	MiB = 1024 * KiB

	// WindowSize is the size of the sliding window.
	windowSize = 16

	chunkerBufSize = 16 * KiB

	// AvgSize is the default average size of a chunk.
	AvgSize = 1048576
)

var bufPool = sync.Pool{
	New: func() interface{} { return new([chunkerBufSize]byte) },
}


type Ram2 struct {
	reader  io.Reader
	minSize int // also the size of fixed window
	maxSize int
	byteNum int

	chunkbuf []byte
	valuebuf []uint32
	buf  *[chunkerBufSize]byte
	bufEnd  int



	value   uint32
	closed  bool
}

func NewRam2(r io.Reader, minSize int, maxSize int, byteNum int) *Ram2 {
	ram := &Ram2{
		reader:   r,
		minSize:  minSize, //default 16384=16k
		maxSize:  maxSize, //default 1048576=1024k=64*min
		byteNum:  byteNum, //default 8
		chunkbuf: make([]byte, 0, maxSize*1024),
		//valuebuf: make([]uint32, maxSize*1024),
		buf:      bufPool.Get().(*[chunkerBufSize]byte),
		value: 0,
		closed: false,
		bufEnd: 0,
	}
	ram.more()
	return ram
}

func (ram *Ram2) more() {
	if ram.closed {
		return
	}
	n, err := io.ReadFull(ram.reader, ram.buf[:])
	ram.chunkbuf = append(ram.chunkbuf, ram.buf[:n]...)
	if err == io.EOF {
		buf := ram.buf
		ram.buf = nil
		bufPool.Put(buf)
		ram.closed = true
	}
	//fmt.Println("len(chunkBuf):",len(ram.chunkbuf),"   bufEnd:",ram.bufEnd)
	newEnd := ram.bufEnd + n
	//for i:=ram.bufEnd; i<newEnd; i++ {
	//	ram.value = (ram.value<<8) | uint32(ram.chunkbuf[i])
	//	ram.valuebuf[i] = ram.value
	//}
	ram.bufEnd = newEnd
	//fmt.Println("====len(valuebuf):",len(ram.valuebuf),"   bufEnd:",ram.bufEnd)
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram2) NextBytes() ([]byte, error) {
	var maximum uint32 = 0
	i:=0
	for ;;i++{
		value, err := ram.getValue(i)
		if err != nil {
			if i == 0 {
				return nil,err
			}
			chunk := ram.chunkbuf[:]
			ram.chunkbuf = ram.chunkbuf[:0]
			ram.bufEnd = 0
			return chunk, nil
		}
		if i+1 == ram.maxSize { //reach the max size
			break
		}
		if value >= maximum {
			if i >= ram.minSize {
				//fmt.Printf("i:%d,   len(chunkbuf):%d,   len(valuebuf):%d \n",i,len(ram.chunkbuf),len(ram.valuebuf))
				break
			}
			maximum = value
		}
	}
	chunk := ram.chunkbuf[:i+1]
	n := copy(ram.chunkbuf, ram.chunkbuf[i+1:])
	ram.chunkbuf = ram.chunkbuf[:n]
	ram.bufEnd = n
	return chunk, nil
}


func (ram *Ram2) getValue(i int) (uint32,error){

	if i < ram.bufEnd {
		ram.value = (ram.value<<8) | uint32(ram.chunkbuf[i])
		return ram.value, nil
	} else {
		ram.more()
		if i >= ram.bufEnd {
			return 0, io.EOF
		}
		ram.value = (ram.value<<8) | uint32(ram.chunkbuf[i])
		return ram.value, nil
	}
}

func (ram *Ram2) Reader() io.Reader {
	return ram.reader
}