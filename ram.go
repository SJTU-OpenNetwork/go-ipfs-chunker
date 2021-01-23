package chunk

import (
	"io"
)

type Ram struct {
	reader  io.Reader
	minSize int // also the size of fixed window
	maxSize int
	byteNum uint32

	curIndex uint64 // start point of current block

	buf      []byte // buffer
	bufStart uint64
	bufEnd  uint64

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
		buf:      make([]byte, minSize),
		bufStart: 0,
		bufEnd:   0,
		value: 0,
		chunkarr: make([]byte,maxSize),
	}
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram) NextBytes() ([]byte, error) {
	var maximum uint32 = 0
	i:=ram.curIndex
	maxsizeMinueOne := ram.maxSize-1

	for chunkSize:=0;;chunkSize++ {
		curByte, value, err := ram.getByteAndValue(i)
		if err != nil {
			if chunkSize == 0 {
				return nil,err
			}
			break
		}
		ram.chunkarr[chunkSize]=curByte
		if chunkSize == maxsizeMinueOne { //reach the max size
			break
		}
		if value >= maximum {
			if chunkSize > ram.minSize {
				break
			}
			maximum = value
		}
		i++
	}
	var x =ram.curIndex
	ram.curIndex = i+1
	//fmt.Println("break, bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   cut point:",i,"   maximum:",maximum,"   value:",ram.value,"   len:",len(chunk))
	return ram.chunkarr[:(i+1-x)], nil
}

//var ErrFileEnd = errors.New("file end============")

func (ram *Ram) getByteAndValue(i uint64) (byte, uint32,error){
	if i==0 {
		//fmt.Println("0===.bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
		n,_ := io.ReadFull(ram.reader, ram.buf)
		if n == 0 {
			return 0, 0, io.EOF
		}
		ram.bufEnd += uint64(n)
		ram.value = uint32(ram.buf[0])
		return ram.buf[0], ram.value, nil
	}
	if i < ram.bufEnd {
		curByte := ram.buf[i-ram.bufStart]
		ram.value = (ram.value<<8) | uint32(curByte)
		return curByte, ram.value, nil
	} else {
		ram.bufStart += uint64(len(ram.buf))
		n,_ := io.ReadFull(ram.reader, ram.buf)
		if n == 0 {
			ram.curIndex = ram.bufEnd
			return 0, 0, io.EOF
		}
		ram.bufEnd += uint64(n)

		curByte := ram.buf[i-ram.bufStart]
		ram.value = (ram.value<<8) | uint32(curByte)
		return curByte, ram.value, nil
	}
}

func (ram *Ram) Reader() io.Reader {
	return ram.reader
}