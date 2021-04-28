package chunk

import (
	"encoding/binary"
	"github.com/cespare/xxhash"
	"io"
)

const (
	AVARAGE_SIZE = 3072
	MIN_SIZE = 1024
	MAX_SIZE= 6144
)



type Hram struct {
	reader io.Reader
	minSize int // also the size of fixed window
	maxSize int
	modD uint64
	byteNum uint32

	curIndex int // start point of current block

	buf      []byte // buffer
	bufStart int
	bufEnd  int

	value uint64

	observe uint64
	observeArr []byte

	chunNum uint64
}

func NewHram(r io.Reader, minSize int ,avrgSize int, maxSize int, byteNum uint32) *Hram {
	return &Hram{
		reader:   r,
		minSize:  MIN_SIZE,
		maxSize:  MAX_SIZE,
		modD: AVARAGE_SIZE/20,
		byteNum:  byteNum,
		curIndex: 0,
		buf:      make([]byte, minSize*40),
		bufStart: 0,
		bufEnd:   0,
		value: 0,
		observeArr: make([]byte,8),
		chunNum: 0,
	}
}

func (ram *Hram) NextBytes() ([]byte, error) {
	var maximum uint64 = 0
	maxsizeMinusOne := ram.maxSize-1
	i:=ram.curIndex
	curByteIndex:=i-ram.bufStart
	chunkSize:=0
	chunkarr := make([]byte,ram.maxSize)
	for {
		if i>=ram.bufEnd {
			n,_ := io.ReadFull(ram.reader, ram.buf)
			if n == 0 {
				if chunkSize == 0 {
					ram.curIndex = ram.bufEnd
					return nil,io.EOF
				}
				i--
				break
			}
			if i!=0 {
				ram.bufStart += len(ram.buf)
			}
			ram.bufEnd += n
			curByteIndex=0
		}
		chunkarr[chunkSize]=ram.buf[curByteIndex]
		if chunkSize == maxsizeMinusOne { //reach the max size
			break
		}
		ram.value = (ram.value<<8) | uint64(chunkarr[chunkSize])
		if ram.value >= (maximum/100*95) {
			if chunkSize > ram.minSize {
				ram.observe = (ram.observe<<8) | uint64(chunkarr[chunkSize])
				binary.BigEndian.PutUint64(ram.observeArr,ram.observe)
				var hashVal = xxhash.Sum64(ram.observeArr)
				if hashVal % ram.modD == uint64(1) {
					break
				}
			}
			if ram.value > maximum {
				maximum = ram.value
			}
		}
		i++
		chunkSize++
		curByteIndex++
	}
	ram.chunNum++
	var x =ram.curIndex
	ram.curIndex = i+1

	return chunkarr[:(i+1-x)], nil
}

func (ram *Hram) Reader() io.Reader {
	return ram.reader
}
