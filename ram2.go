package chunk

import (
	"io"
)

type Ram2 struct {
	reader  io.Reader
	minSize uint64
	maxSize uint64
	byteNum uint64
	curIndex uint64 // start point of current block
	buf      []byte // buffer
	bufStart uint64
	bufEnd  uint64
	value uint64
}

func NewRam2(r io.Reader, minSize uint64, maxSize uint64, byteNum uint64) *Ram2 {
	return &Ram2{
		reader:   r,
		minSize:  minSize,
		maxSize:  maxSize,
		byteNum:  byteNum,
		curIndex: 0,
		buf:      make([]byte, minSize),
		bufStart: 0,
		bufEnd:   0,
		value:    0,
	}
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram2) NextBytes() ([]byte, error) {
	chunk:=make([]byte,0)
	var maximum uint64 = 0
	i:=ram.curIndex
	for {
		curByte, value, err := ram.getByteAndValue(i)
		if err != nil {
			if len(chunk) == 0 {
				return nil,err
			}
			break
		}
		//fmt.Printf("==========i:%d   bufStart:%d   bufEnd:%d   chunkSize:%d   curIndex:%d   curByteIndex:%d\n",i,ram.bufStart,ram.bufEnd,len(chunk),ram.curIndex,i-ram.bufStart)
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
	//fmt.Printf("***get a chunk,i:%d   bufStart:%d   bufEnd:%d   chunkSize:%d   curIndex:%d   curByteIndex:%d\n",i,ram.bufStart,ram.bufEnd,i-ram.curIndex,ram.curIndex,i-ram.bufStart)
	return chunk, nil
}

func (ram *Ram2) getByteAndValue(i uint64) (byte, uint64,error){
	if i==0 {
		//fmt.Printf("i:%d   bufStart:%d   bufEnd:%d   chunkSize:%d   curIndex:%d   curByteIndex:%d\n",i,ram.bufStart,ram.bufEnd,i-ram.curIndex,ram.curIndex,i-ram.bufStart)
		n,_ := io.ReadFull(ram.reader, ram.buf)
		if n == 0 {
			return 0, 0, io.EOF
		}
		ram.bufEnd += uint64(n)
		curByte := ram.buf[0]
		ram.value = (ram.value<<4) & uint64(curByte)
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
		//fmt.Printf("i:%d   bufStart:%d   bufEnd:%d   chunkSize:%d   curIndex:%d   curByteIndex:%d\n",i,ram.bufStart,ram.bufEnd,i-ram.curIndex,ram.curIndex,i-ram.bufStart)
		//buftmp := ram.buf[uint64(len(ram.buf))-ram.byteNum:]
		buftmp := ram.buf[uint64(len(ram.buf))-ram.byteNum:]
		n,_ := io.ReadFull(ram.reader, ram.buf)
		if n == 0 {
			ram.curIndex = ram.bufEnd
			return 0, 0, io.EOF
		}
		ram.bufStart += uint64(len(ram.buf))-ram.byteNum
		ram.bufEnd += uint64(n)

		ram.buf = append(buftmp,ram.buf...)

		curByte := ram.buf[i-ram.bufStart]
		ram.value = (ram.value<<4) & uint64(curByte)
		return curByte, ram.value, nil
	}
}

func (ram *Ram2) Reader() io.Reader {
	return ram.reader
}
