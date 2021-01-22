package chunk

import (
	"crypto/md5"
	"encoding/binary"
	"io"
)

const MaxUint32 = uint64(^uint32(0))

type Hram struct {
	reader io.Reader
	minSize uint64 // also the size of fixed window
	maxSize uint64
	byteNum uint32

	buf      []byte // buffer
	curIndex uint64 // start point of current block
	bufStart uint64
	bufEnd  uint64
	value uint32

	s uint32
}

func NewHram(r io.Reader, minSize uint64, maxSize uint64, byteNum uint32) *Hram {
	return &Hram{
		reader:   r,
		minSize:  minSize, //default 16384=16k
		maxSize:  maxSize, //default 1048576=1024k=64*min
		byteNum:  byteNum, //default 8
		curIndex: 0,
		buf:      make([]byte, minSize),
		bufStart: 0,
		bufEnd:   0,
		value: 0,
		s: 1,
	}
}

func (ram *Hram) NextBytes() ([]byte, error) {
	chunk:=make([]byte,0)
	var maximum uint32 = 0
	i:=ram.curIndex
	for {
		curByte, value, err := ram.getByteAndValue(i)
		if err != nil {
			//fmt.Println("get chunk, len:",err,len(chunk))
			if len(chunk) == 0 {
				return nil,err
			}
			break
		}
		chunk=append(chunk,curByte)
		if i-ram.curIndex == ram.maxSize-1 { //reach the max size
			break
		}
		if value >= maximum {
			if i-ram.curIndex > ram.minSize {
				if ram.getHashMod() < value {
					break
				}
			}
			maximum = value
		}
		i++
	}
	ram.curIndex = i + 1
	//fmt.Println("break, bufStart:",ram.bufStart, "   bufEnd:",ram.bufEnd, "   cut point:",i,"   maximum:",maximum,"   value:",ram.value,"   len:",len(chunk))
	return chunk, nil
}


func (ram *Hram) getByteAndValue(i uint64) (byte, uint32,error){
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


func (ram *Hram) getHashMod() uint32 {
	var tmp = make([]byte,4)
	binary.BigEndian.PutUint32(tmp,ram.value)
	var hashByte = md5.Sum(tmp)
	var res = binary.BigEndian.Uint64(hashByte[:]) % MaxUint32
	return ram.s * uint32(res)
}

func (ram *Hram) Reader() io.Reader {
	return ram.reader
}