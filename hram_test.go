package chunk


import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	util "github.com/ipfs/go-ipfs-util"
)

func TestHramChunking(t *testing.T) {
	data := make([]byte, 1024*1024*64)
	util.NewTimeSeededRand().Read(data)

	chunks := make([][]byte, 10)
	startTime := time.Now()
	r := NewHram(bytes.NewReader(data), 1024,16384, 32768,4)
	for {
		chunk, err := r.NextBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		chunks = append(chunks, chunk)
	}
	fmt.Println("======time: ",time.Since(startTime),"    ,num:",len(chunks))
	unchunked := bytes.Join(chunks, nil)

	fmt.Printf("\n---a: ")
	fmt.Println(data[0:10])

	fmt.Printf("---b: ")
	fmt.Println(unchunked[0:10])

	if !bytes.Equal(unchunked[:], data) {
		fmt.Printf("%d %d\n", len(unchunked), len(data))
		t.Fatal("data was chunked incorrectly")
	}
}

func chunkData1(t *testing.T, newC newSplitter, data []byte) map[string]blocks.Block {
	r := newC(bytes.NewReader(data))

	blkmap := make(map[string]blocks.Block)

	for {
		blk, err := r.NextBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}

		b := blocks.NewBlock(blk)
		blkmap[b.Cid().KeyString()] = b
	}

	return blkmap
}

func testReuse1(t *testing.T, cr newSplitter) {
	data := make([]byte, 1024*1024*16)
	util.NewTimeSeededRand().Read(data)

	ch1 := chunkData1(t, cr, data[1000:])
	ch2 := chunkData1(t, cr, data)

	var extra int
	for k := range ch2 {
		_, ok := ch1[k]
		if !ok {
			extra++
		}
	}

	if extra > 2 {
		t.Logf("too many spare chunks made: %d", extra)
	}
}

func TestHramChunkReuse(t *testing.T) {
	newRabin := func(r io.Reader) Splitter {
		return NewHram(r, 1024,16384, 32768,4)
	}
	testReuse1(t, newRabin)
}

var Res1 uint64

func BenchmarkHram(b *testing.B) {
	benchmarkChunker(b, func(r io.Reader) Splitter {
		return NewHram(r, 1024,16384, 32768,4)
	})
}
