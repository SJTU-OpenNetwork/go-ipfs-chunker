package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Ram struct {
	reader  io.Reader
	minSize int // also the size of fixed window
	maxSize int
	byteNum int

	buf      []byte // buffer
	byteNumBuf []byte // save the end bytes of last buf
	curIndex int    // the start point in cur chunk
	endIndex int
}

func NewRam(r io.Reader, minSize int, maxSize int, byteNum int) *Ram {
	return &Ram{
		reader:   r,
		minSize:  minSize, //default 16384=16k
		maxSize:  maxSize, //default 1048576=1024k=64*min
		byteNum:  byteNum, //default 8
		buf:      nil,
		byteNumBuf: make([]byte, byteNum-1),
		curIndex: 0,
		endIndex: 0, // if buf is full , endIndex is minSize
	}
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram) NextBytes() ([]byte, error) {
	if ram.endIndex > 0 && ram.endIndex < ram.minSize {
		var chunk = make([]byte, ram.endIndex-ram.curIndex)
		copy(chunk, ram.buf[ram.curIndex:ram.endIndex])
		return chunk, nil
	}
	// endIndex == minSize, buf is full
	chunk := make([]byte, 0)
	index := 0
	if ram.buf == nil { // first chunk
		ram.buf = make([]byte, ram.minSize)
		n, err := io.ReadFull(ram.reader, ram.buf)
		if err == io.ErrUnexpectedEOF { // return the bytes directly
			small := make([]byte, n)
			copy(small, ram.buf)
			return small, io.ErrUnexpectedEOF
		}
		chunk = ram.buf // read a buf
	} else { // buf not nil
		chunk = append(chunk, ram.buf[ram.curIndex:ram.endIndex]...)
		index += ram.endIndex - ram.curIndex
		ram.readNewBuf()
		var need = ram.minSize - len(chunk)
		if ram.endIndex <= need {
			chunk = append(chunk, ram.buf[ram.curIndex:ram.endIndex]...)
			return chunk, nil
		}
		chunk = append(chunk, ram.buf[0:need]...)
		ram.curIndex += need
		index += need
	}
	//now chunk is the fixed window, index is minSize
	var maximum uint32 = 0
	for i := ram.byteNum; i <= ram.minSize; i++ {
		var value = binary.BigEndian.Uint32(chunk[(i - ram.byteNum):i])
		if value > maximum {
			maximum = value
		}
	}
	fmt.Println("maximum: ", maximum)

	// move forward to get a value larger than maximum
	for ; index < ram.maxSize; index++ { // the count of operation
		tmpBytes, _ := ram.getByteNum()                  // get the next byte window
		if binary.BigEndian.Uint32(tmpBytes) > maximum { // index is the cut point
			
		}
	}

	return nil, nil
}

var ErrBufOver = errors.New("buf full")
var ErrBufEnd = errors.New("buf end")

func (ram *Ram) getByteNum() ([]byte, error) {
	var bytes = make([]byte,0)
	if ram.curIndex < ram.byteNum-1 { // need get the byteNumBuf
		bytes= append(bytes, ram.byteNumBuf[ram.curIndex:]...)
		bytes=append(bytes,ram.buf[0:ram.curIndex+1]...)
		ram.curIndex++
		return bytes, nil
	}
	if ram.curIndex == ram.minSize {
		ram.readNewBuf()
		ram.curIndex+=ram.byteNum
		return ram.buf[0:ram.curIndex], ErrBufOver
	}
	if ram.curIndex == ram.endIndex { // at this line, end must be less than minSize
		return ram.buf[ram.endIndex-ram.byteNum:ram.endIndex], ErrBufEnd
	}
}

func (ram *Ram) readNewBuf() {
	copy(ram.byteNumBuf, ram.buf[len(ram.buf)-ram.byteNum+1:])
	n, _ := io.ReadFull(ram.reader, ram.buf)
	ram.endIndex = n
	ram.curIndex = 0
}

func (ram *Ram) Reader() io.Reader {
	return ram.reader
}
