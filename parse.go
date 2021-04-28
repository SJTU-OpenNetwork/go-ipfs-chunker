package chunk

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	// DefaultBlockSize is the chunk size that splitters produce (or aim to).
	DefaultBlockSize int64 = 1024 * 256

	// No leaf block should contain more than 1MiB of payload data ( wrapping overhead aside )
	// This effectively mandates the maximum chunk size
	// See discussion at https://github.com/ipfs/go-ipfs-chunker/pull/21#discussion_r369124879 for background
	ChunkSizeLimit int = 1048576
)

var (
	ErrRabinMin = errors.New("rabin min must be greater than 16")
	ErrSize     = errors.New("chunker size must be greater than 0")
	ErrSizeMax  = fmt.Errorf("chunker parameters may not exceed the maximum chunk size of %d", ChunkSizeLimit)
)

// FromString returns a Splitter depending on the given string:
// it supports "default" (""), "size-{size}", "rabin", "rabin-{blocksize}",
// "rabin-{min}-{avg}-{max}" and "buzhash".
func FromString(r io.Reader, chunker string) (Splitter, error) {
	fmt.Println(chunker)
	switch {
	case chunker == "" || chunker == "default":
		return DefaultSplitter(r), nil

	case strings.HasPrefix(chunker,"honrabin"):
		return parseHonRabin(r,chunker)

	case strings.HasPrefix(chunker, "hram"):
		return parseHramString(r,chunker)


	case strings.HasPrefix(chunker,"newram2"):
		return parseRam2String(r,chunker)

	case strings.HasPrefix(chunker,"ram"):
		return parseRamString(r,chunker)


	case strings.HasPrefix(chunker, "size-"):
		sizeStr := strings.Split(chunker, "-")[1]
		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			return nil, err
		} else if size <= 0 {
			return nil, ErrSize
		} else if size > ChunkSizeLimit {
			return nil, ErrSizeMax
		}
		return NewSizeSplitter(r, int64(size)), nil

	case strings.HasPrefix(chunker, "rabin"):
		return parseRabinString(r, chunker)

	case chunker == "buzhash":
		return NewBuzhash(r), nil

	default:
		return nil, fmt.Errorf("unrecognized chunker================= option: %s", chunker)
	}
}

func parseRam2String(r io.Reader, chunker string) (Splitter, error) {
	parts := strings.Split(chunker, "-")
	fmt.Println("ram2=========: ",parts[1],parts[2],parts[3])
	minSize,err:=strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	maxSize,err:=strconv.Atoi(parts[2])
	if err != nil {
		return nil, err
	}
	byteNum,err:=strconv.Atoi(parts[3])
	if err != nil {
		return nil, err
	}
	return NewRam2(r,uint64(minSize),uint64(maxSize),uint64(byteNum)), nil
}

func parseHonRabin(r io.Reader, chunker string)(Splitter, error) {
	parts := strings.Split(chunker, "-")
	fmt.Println("honrabin: ",parts[1],parts[2])
	minSize,err:=strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	maxSize,err:=strconv.Atoi(parts[2])
	if err != nil {
		return nil, err
	}
	return NewHonRabin(r,uint64(minSize),uint64(maxSize),16), nil
}

func parseHramString(r io.Reader, chunker string) (Splitter, error) {
	parts := strings.Split(chunker, "-")
	fmt.Println("hram===================: ",parts[1],parts[2],parts[3],parts[4])
	minSize,err:=strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	avrgSize,err:=strconv.Atoi(parts[2])
	if err != nil {
		return nil, err
	}
	maxSize,err:=strconv.Atoi(parts[3])
	if err != nil {
		return nil, err
	}
	byteNum,err:=strconv.Atoi(parts[4])
	if err != nil {
		return nil, err
	}
	//return NewHram(r,262144), nil
	return NewHram(r,minSize,avrgSize,maxSize,uint32(byteNum)), nil
}

func parseRamString(r io.Reader, chunker string) (Splitter, error) {
	parts := strings.Split(chunker, "-")
	fmt.Println("ram: ",parts[1],parts[2],parts[3])
	minSize,err:=strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	maxSize,err:=strconv.Atoi(parts[2])
	if err != nil {
		return nil, err
	}
	byteNum,err:=strconv.Atoi(parts[3])
	if err != nil {
		return nil, err
	}
	return NewRam(r,minSize,maxSize,uint32(byteNum)), nil
}

func parseRabinString(r io.Reader, chunker string) (Splitter, error) {
	parts := strings.Split(chunker, "-")
	switch len(parts) {
	case 1:
		return NewRabin(r, uint64(DefaultBlockSize)), nil
	case 2:
		size, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		} else if int(float32(size)*1.5) > ChunkSizeLimit { // FIXME - this will be addressed in a subsequent PR
			return nil, ErrSizeMax
		}
		return NewRabin(r, uint64(size)), nil
	case 4:
		sub := strings.Split(parts[1], ":")
		if len(sub) > 1 && sub[0] != "min" {
			return nil, errors.New("first label must be min")
		}
		min, err := strconv.Atoi(sub[len(sub)-1])
		if err != nil {
			return nil, err
		}
		if min < 16 {
			return nil, ErrRabinMin
		}
		sub = strings.Split(parts[2], ":")
		if len(sub) > 1 && sub[0] != "avg" {
			log.Error("sub == ", sub)
			return nil, errors.New("second label must be avg")
		}
		avg, err := strconv.Atoi(sub[len(sub)-1])
		if err != nil {
			return nil, err
		}

		sub = strings.Split(parts[3], ":")
		if len(sub) > 1 && sub[0] != "max" {
			return nil, errors.New("final label must be max")
		}
		max, err := strconv.Atoi(sub[len(sub)-1])
		if err != nil {
			return nil, err
		}

		if min >= avg {
			return nil, errors.New("incorrect format: rabin-min must be smaller than rabin-avg")
		} else if avg >= max {
			return nil, errors.New("incorrect format: rabin-avg must be smaller than rabin-max")
		} else if max > ChunkSizeLimit {
			return nil, ErrSizeMax
		}

		return NewRabinMinMax(r, uint64(min), uint64(avg), uint64(max)), nil
	default:
		return nil, errors.New("incorrect format (expected 'rabin' 'rabin-[avg]' or 'rabin-[min]-[avg]-[max]'")
	}
}
