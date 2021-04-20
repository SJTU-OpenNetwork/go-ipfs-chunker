package chunk

import (
	"crypto/md5"
	"encoding/binary"
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

	value uint64

	observe uint64
	observeArr []byte

	chunkarr []byte
}

const MaxUint64 = ^uint64(0)

func NewHram(r io.Reader, minSize int, maxSize int, byteNum uint32) *Hram {
	return &Hram{
		reader:   r,
		minSize:  minSize, //default 16384=16k
		maxSize:  maxSize, //default 1048576=1024k=64*min
		byteNum:  byteNum, //default 8
		curIndex: 0,
		buf:      make([]byte, minSize*40),
		bufStart: 0,
		bufEnd:   0,
		value: 0,
		chunkarr: make([]byte,maxSize),
		observeArr: make([]byte,8),
	}
}

func (ram *Hram) NextBytes() ([]byte, error) {
	var maximum uint64 = 0
	maxsizeMinueOne := ram.maxSize-1
	i:=ram.curIndex
	curByteIndex:=i-ram.bufStart
	chunkSize:=0
	//toMaxUint64:=MaxUint64-maximum
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
		ram.value = (ram.value<<8) | uint64(ram.chunkarr[chunkSize])
		if chunkSize == maxsizeMinueOne { //reach the max size
			break
		}
		if ram.value >= (maximum/100*95) {
			if chunkSize > ram.minSize {
				ram.observe = (ram.observe<<8) | uint64(ram.chunkarr[chunkSize])
				binary.BigEndian.PutUint64(ram.observeArr,ram.observe)
				var hashByte = md5.Sum(ram.observeArr)
				if binary.BigEndian.Uint64(hashByte[:]) % 10000 == uint64(5337) {
				//bigger := ram.value-maximum
				//fmt.Printf("======get an cut point, hashvale:%d,   value-max:%d,   value:%d,   max:%d\n",2*binary.BigEndian.Uint64(hashByte[:]), bigger,ram.value, maximum)
				//if binary.BigEndian.Uint64(hashByte[:]) % toMaxUint64 < bigger {
				//	fmt.Printf("get an cut point, hashvale:%d,   value:%d,   max:%d\n",2*binary.BigEndian.Uint64(hashByte[:]),ram.value, maximum)
					break
				}
			}
			if ram.value > maximum {
				maximum = ram.value
			}
			//toMaxUint64 = MaxUint64-maximum
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