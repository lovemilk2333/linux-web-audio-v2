package packages

import (
	"math"
	"strconv"
)

const COMPRESSION_SEP = ":"
const UNPROVIDED_COMPRESSION_LEVEL = math.MinInt

type Compressor interface {
	Ident() string
	Name() string
	Level() int
	Compress(data []byte) ([]byte, error)
	Decompress(compressed []byte) ([]byte, error)
}

type NoneCompressor struct{}

func (this *NoneCompressor) Ident() string {
	return this.Name() + COMPRESSION_SEP + strconv.Itoa(this.Level())
}
func (this *NoneCompressor) Name() string {
	return "none"
}
func (this *NoneCompressor) Level() int {
	return 0
}
func (this *NoneCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}
func (this *NoneCompressor) Decompress(compressed []byte) ([]byte, error) {
	return compressed, nil
}

var NONE_COMPRESSOR = &NoneCompressor{}
