package chunk

import (
	"io"
)

type Ram struct {
	reader  io.Reader
	minSize int // also the size of fixed window
	maxSize int
	byteNum uint32

	curIndex int // start point of current block

	buf      []byte // buffer
	bufStart int
	bufEnd  int

	value uint32

	chunkarr []byte
}

func NewRam(r io.Reader, minSize int, maxSize int, byteNum uint32) *Ram {
	return &Ram{
		reader:   r,
		minSize:  minSize, //default 16384=16k
		maxSize:  maxSize, //default 1048576=1024k=64*min
		byteNum:  byteNum, //default 8
		curIndex: 0,
		buf:      make([]byte, minSize*20),
		bufStart: 0,
		bufEnd:   0,
		value: 0,
		chunkarr: make([]byte,maxSize),
	}
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram) NextBytes() ([]byte, error) {
	var maximum uint32 = 0
	maxsizeMinusOne := ram.maxSize-1
	i:=ram.curIndex
	curByteIndex:=i-ram.bufStart
	chunkSize:=0
	for {
		if i>=ram.bufEnd {
			//fmt.Printf("i:%d   bufStart:%d   bufEnd:%d   chunkSize:%d   curIndex:%d   curByteIndex:%d\n",i,ram.bufStart,ram.bufEnd,chunkSize,ram.curIndex,curByteIndex)
			n,_ := io.ReadFull(ram.reader, ram.buf)
			if n == 0 {
				if chunkSize == 0 {
					ram.curIndex = ram.bufEnd
					return nil,io.EOF
				}
				break
			}
			if i!=0 {
				ram.bufStart += len(ram.buf)
			}
			ram.bufEnd += n
			curByteIndex=0
		}
		//fmt.Printf("==========i:%d   bufStart:%d   bufEnd:%d   chunkSize:%d   curIndex:%d   curByteIndex:%d\n",i,ram.bufStart,ram.bufEnd,chunkSize,ram.curIndex,curByteIndex)
		ram.chunkarr[chunkSize]=ram.buf[curByteIndex]
		ram.value = (ram.value<<8) | uint32(ram.chunkarr[chunkSize])
		if chunkSize == maxsizeMinusOne { //reach the max size
			break
		}
		if ram.value >= maximum {
			if chunkSize > ram.minSize {
				break
			}
			maximum = ram.value
		}
		i++
		chunkSize++
		curByteIndex++
	}
	var x = ram.curIndex
	ram.curIndex = i+1
	//fmt.Printf("***get a chunk,i:%d   bufStart:%d   bufEnd:%d   chunkSize:%d   curIndex:%d\n",i,ram.bufStart,ram.bufEnd,chunkSize,ram.curIndex)
	return ram.chunkarr[:(i+1-x)], nil
}

func (ram *Ram) Reader() io.Reader {
	return ram.reader
}