package chunk

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
)

type Hram struct {
	reader io.Reader
	minSize int // also the size of fixed window
	maxSize int
	byteNum uint32

	curIndex int // start point of current block

	buf      []byte // buffer
	bufStart int
	bufEnd  int

	value uint32

	observe uint64
	observeArr []byte

	chunkarr []byte
}

func NewHram(r io.Reader, minSize int, maxSize int, byteNum uint32) *Hram {
	return &Hram{
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
		observeArr: make([]byte,8),
	}
}

func (ram *Hram) NextBytes() ([]byte, error) {
	var maximum uint32 = 0
	maxsizeMinueOne := ram.maxSize-1
	i:=ram.curIndex
	curByteIndex:=i-ram.bufStart
	chunkSize:=0
	for {
		if i>=ram.bufEnd {
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
		ram.chunkarr[chunkSize]=ram.buf[curByteIndex]
		ram.value = (ram.value<<8) | uint32(ram.chunkarr[chunkSize])
		if chunkSize == maxsizeMinueOne { //reach the max size
			break
		}
		if ram.value >= maximum {
			if chunkSize > ram.minSize {
				ram.observe = (ram.observe<<8) | uint64(ram.chunkarr[chunkSize])
				binary.BigEndian.PutUint64(ram.observeArr,ram.observe)
				var hashByte = md5.Sum(ram.observeArr)
				if 2*binary.BigEndian.Uint32(hashByte[:])< ram.value {
					fmt.Printf("get an cut point, hashvale:%d,   value:%d\n",2*binary.BigEndian.Uint32(hashByte[:]),ram.value)
					break
				}
			}
			maximum = ram.value
		}
		i++
		chunkSize++
		curByteIndex++
	}
	var x =ram.curIndex
	ram.curIndex = i+1
	return ram.chunkarr[:(i+1-x)], nil
}

func (ram *Hram) Reader() io.Reader {
	return ram.reader
}