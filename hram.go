package chunk

import (
	"fmt"
	"io"
)

type Hram struct {
	reader io.Reader
	minSize uint64 // also the size of fixed window
	maxSize uint64
	byteNum uint64

	buf      []byte // buffer
	curIndex uint64 // start point of current block
	bufStart uint64
	bufEnd  uint64
	value uint64
}

func (ram *Hram) NextBytes() ([]byte, error){

}

func (ram *Hram) getByteAndValue(i uint64) (byte, uint64,error){
	if i==0 {
		fmt.Println("0===.bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
		n,_ := io.ReadFull(ram.reader, ram.buf)
		if n == 0 {
			return 0, 0, io.EOF
		}
		ram.bufEnd += uint64(n)
		ram.value = uint64(ram.buf[0])
		fmt.Println("01===.bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   i:",i)
		return ram.buf[0], ram.value, nil
	}
	if i < ram.bufEnd {
		curByte := ram.buf[i-ram.bufStart]
		ram.value = (ram.value<<8) & uint64(curByte)
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
		ram.value = (ram.value<<8) & uint64(curByte)
		return curByte, ram.value, nil
	}
}

func (ram *Hram) Reader() io.Reader {
	return ram.reader
}