package chunk

import (
	"encoding/binary"
	"io"
)

type Ram struct {
	reader    io.Reader
	minSize   int // also the size of fixed window
	maxSize   int
	byteNum   int
	leftBytes []byte
	leftNum   int
}

func NewRam(r io.Reader, minSize int, maxSize int, byteNum int) *Ram {
	return &Ram{
		reader:    r,
		minSize:   minSize, //default 16384=16k
		maxSize:   maxSize, //default 1048576=1024k=64*min
		byteNum:   byteNum, //default 8
		leftBytes: make([]byte, 0, minSize),
		leftNum:   0,
	}
}

// NextBytes get a maximum in the fixed windows, and move to the next byte where the value is larger than the maximum.
func (ram *Ram) NextBytes() ([]byte, error) {
	// get the maximum in the fixed window
	full := make([]byte, 0, ram.minSize)
	if ram.leftNum > 0 {
		tmp := make([]byte, ram.leftNum)
		copy(tmp, ram.leftBytes)
		full = append(full, tmp...)
	}
	newFull := make([]byte, ram.minSize-ram.leftNum) // need more bytes to fill the fixed window
	n, err := io.ReadFull(ram.reader, newFull)
	if err == io.ErrUnexpectedEOF { // return the bytes directly
		small := make([]byte, n)
		copy(small, newFull)
		full = append(full, small...)
		return full, nil
	}
	var maximum uint32 = 0
	for i := ram.byteNum; i <= ram.minSize; i++ {
		var value = binary.BigEndian.Uint32(full[(i - ram.byteNum):i])
		if value > maximum {
			maximum = value
		}
	}

	// move forward to get a value larger than maximum
	var batchNum = ram.maxSize/ram.minSize - 1 // default is 63
	var i = 0
	for i < batchNum {
		batch := make([]byte, ram.minSize)
		n, err := io.ReadFull(ram.reader, batch) // n is the real length of data
		if err == io.ErrUnexpectedEOF {          //reach the end of the file, append the batch and return
			full = append(full, batch[0:n]...)
			return full, nil
		}

		// batch is for testing the value
		// get the last bytes of full
		var j = 0
		for j < ram.minSize { // byteNum-1 times use full
			var tmp []byte
			if j <= ram.byteNum-2 {
				tmp = append(full[len(full)-1-j:], batch[0:j+1]...)
			} else {
				tmp = batch[j-ram.byteNum+1 : j+1]
			}
			if binary.BigEndian.Uint32(tmp) > maximum { //j is the cut point
				tmpBatch := make([]byte, j+1)
				copy(tmpBatch, batch)
				full = append(full, tmpBatch...)
				ram.leftBytes = batch[j+1:]
				ram.leftNum = ram.minSize - j - 1
				return full, nil
			}
			j++
		}
		if j == ram.minSize { // no cut point
			full = append(full, batch...)
			ram.leftNum = 0
			ram.leftBytes = ram.leftBytes[0:0]
		}
		i++
	}
	if i == batchNum { // reach the max size
		return full,nil
	}
	return nil, nil
}

func (ram *Ram) Reader() io.Reader {
	return ram.reader
}
