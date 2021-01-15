package chunk

import (
	"errors"
	"fmt"
	"io"
)

type Ram struct {
	reader  io.Reader
	minSize uint64 // also the size of fixed window
	maxSize uint64
	byteNum uint64

	curIndex uint64 // start point of current block

	buf      []byte // buffer
	bufStart uint64
	bufEnd  uint64
	value uint64
}

func NewRam(r io.Reader, minSize uint64, maxSize uint64, byteNum uint64) *Ram {
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
	}
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram) NextBytes() ([]byte, error) {
	chunk:=make([]byte,0)
	var maximum uint64 = 0
	i:=ram.curIndex
	for {
		curByte, value, err := ram.getByteAndValue(i)
		if err != nil {
			fmt.Println("get chunk, len:",err,len(chunk))
			if len(chunk) == 0 {
				return nil,err
			}
			break
		}
		chunk=append(chunk,curByte)
		if i-ram.curIndex == ram.maxSize { //reach the max size
			break
		}
		if value >= maximum {
			if i-ram.curIndex > ram.minSize {
				ram.curIndex = i+1
				break
			}
			maximum = value
		}
		i++
	}
	fmt.Println("break, bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
	return chunk, nil
}

var ErrFileEnd = errors.New("file end============")

func (ram *Ram) getByteAndValue(i uint64) (byte, uint64,error){
	if i==0 {
		fmt.Println("0===.bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
		n,_ := io.ReadFull(ram.reader, ram.buf)
		if n == 0 {
			return 0, 0, io.EOF
		}
		ram.bufEnd += uint64(n)
		curByte := ram.buf[0]
		ram.value = (ram.value<<4) & uint64(curByte)
		fmt.Println("01===.bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
		return curByte, ram.value, nil
	}
	if i < ram.bufEnd {
		//if i%50==0 {
		//	fmt.Println("22.bufStart:", ram.bufStart, "   bufEnd:", ram.bufEnd, "   i:", i)
		//}
		curByte := ram.buf[i-ram.bufStart]
		ram.value = (ram.value<<4) & uint64(curByte)
		return curByte, ram.value, nil
	} else {
		fmt.Println("1------.bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
		//buftmp := ram.buf[uint64(len(ram.buf))-ram.byteNum:]
		buftmp := ram.buf[uint64(len(ram.buf))-ram.byteNum:]
		n,_ := io.ReadFull(ram.reader, ram.buf)
		fmt.Println("read full: ",n)
		if n == 0 {
			ram.curIndex = ram.bufEnd
			return 0, 0, io.EOF
		}
		ram.bufStart += uint64(len(ram.buf))-ram.byteNum
		ram.bufEnd += uint64(n)

		fmt.Println("2------.bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
		ram.buf = append(buftmp,ram.buf...)

		curByte := ram.buf[i-ram.bufStart]
		ram.value = (ram.value<<4) & uint64(curByte)
		return curByte, ram.value, nil
	}
}

func (ram *Ram) Reader() io.Reader {
	return ram.reader
}
