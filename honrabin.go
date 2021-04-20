package chunk

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
)

type HonRabin struct {
	reader io.Reader
	minSize uint64
	maxSize uint64
	windowSize uint32

	curIndex uint64 // start point of current block

	buf      []byte // buffer
	bufStart uint64
	bufEnd  uint64

	observeH uint64
	observeHarr []byte
	observeL uint64
	observeLarr []byte
}

func NewHonRabin(r io.Reader, minSize uint64, maxSize uint64, windowSize uint32) *HonRabin {
	return &HonRabin{
		reader:     r,
		minSize:    minSize,
		maxSize:    maxSize,
		windowSize: windowSize,
		curIndex:   0,
		buf:        make([]byte, minSize*1024),
		bufStart:   0,
		bufEnd:     0,
		observeH: 0,
		observeL: 0,
		observeHarr: make([]byte, 8),
		observeLarr: make([]byte, 8),
	}
}

func (h *HonRabin) NextBytes() ([]byte,error) {
	chunk := make([]byte,0)
	i:=h.curIndex
	for{
		curByte,hashValue1,hashValue2,err := h.getByteAndHash(i)
		if err != nil {
			if len(chunk) == 0 {
				return nil,err
			}
			break
		}
		chunk=append(chunk,curByte)
		if i-h.curIndex == h.maxSize-1 { //reach the max size
			break
		}
		if (hashValue1 % 1000 == 351) && (hashValue2 % 500 == 277 ) {
			if i-h.curIndex > h.minSize {
				fmt.Printf("==============get an cut point at %d,  hashValue:%d, %d \n",i,hashValue1,hashValue2)
				break
			}
		}
		i++
	}
	h.curIndex=i+1
	//fmt.Println("break, bufStart:",h.bufStart, "   bufEnd:",h.bufEnd, "   cut point:",i,"   len:",len(chunk))
	return chunk,nil
}

func (h *HonRabin) getByteAndHash(i uint64) (byte, uint64, uint64, error) {
	if i==0 {
		n,_ := io.ReadFull(h.reader, h.buf)
		if n == 0 {
			return 0, 0,0, io.EOF
		}
		h.bufEnd += uint64(n)
		curByte := h.buf[i-h.bufStart]
		h.observeH = (h.observeH<<8) | (h.observeL >> 24)
		binary.BigEndian.PutUint64(h.observeHarr,h.observeH)
		h.observeL = (h.observeL<<8) | uint64(curByte)
		binary.BigEndian.PutUint64(h.observeLarr,h.observeL)
		res1:=md5.Sum(h.observeHarr)
		res2:=md5.Sum(h.observeLarr)
		return curByte,binary.BigEndian.Uint64(res1[:8]),binary.BigEndian.Uint64(res2[:8]),nil
	}
	if i<h.bufEnd {
		curByte := h.buf[i-h.bufStart]
		h.observeH = (h.observeH<<8) | (h.observeL >> 24)
		binary.BigEndian.PutUint64(h.observeHarr,h.observeH)
		h.observeL = (h.observeL<<8) | uint64(curByte)
		binary.BigEndian.PutUint64(h.observeLarr,h.observeL)
		res1:=md5.Sum(h.observeHarr)
		res2:=md5.Sum(h.observeLarr)
		return curByte,binary.BigEndian.Uint64(res1[:8]),binary.BigEndian.Uint64(res2[:8]),nil
	}else{
		h.bufStart += uint64(len(h.buf))
		n,_ := io.ReadFull(h.reader, h.buf)
		if n == 0 {
			h.curIndex = h.bufEnd
			return 0, 0, 0, io.EOF
		}
		h.bufEnd += uint64(n)

		curByte := h.buf[i-h.bufStart]
		h.observeH = (h.observeH<<8) | (h.observeL >> 24)
		binary.BigEndian.PutUint64(h.observeHarr,h.observeH)
		h.observeL = (h.observeL<<8) | uint64(curByte)
		binary.BigEndian.PutUint64(h.observeLarr,h.observeL)
		res1:=md5.Sum(h.observeHarr)
		res2:=md5.Sum(h.observeLarr)
		return curByte,binary.BigEndian.Uint64(res1[:8]),binary.BigEndian.Uint64(res2[:8]),nil
	}
}

func (h *HonRabin) Reader() io.Reader {
	return h.reader
}