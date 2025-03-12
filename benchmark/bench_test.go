package benchmark

import (
	bitcaskkv "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"
	"time"

	"math/rand"

	"github.com/stretchr/testify/assert"
)

var db *bitcaskkv.DB

func init() {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-benchPUT")
	opts.DirPath = dir

	var err error
	db, err = bitcaskkv.Open(opts)

	if err != nil {
		panic(err)
	}

}

func Benchmark_Put1KB(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024)) // 1kb 大小 可以定量分析
		assert.Nil(b, err)
	}

}

// func Benchmark_Put1MB(b *testing.B) {

// 	b.ResetTimer()
// 	b.ReportAllocs()

// 	for i := 0; i < b.N; i++ {
// 		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024*1024)) // 1kb 大小 可以定量分析
// 		assert.Nil(b, err)
// 	}

// }

// func Benchmark_Put1GB(b *testing.B) {

// 	b.ResetTimer()
// 	b.ReportAllocs()

// 	for i := 0; i < b.N; i++ {
// 		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024*1024*1024)) // 1kb 大小 可以定量分析
// 		assert.Nil(b, err)
// 	}

// }

func Benchmark_Get1KB(b *testing.B) {

	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024)) // 1kb 大小 可以定量分析
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcaskkv.ErrKeyNotFound {
			b.Fatal(err)
		}
	}

}

func Benchmark_Delete1KB(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}

}
