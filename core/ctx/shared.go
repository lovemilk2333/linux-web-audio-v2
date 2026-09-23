package ctx

import (
	"math"
)

const COMPRESSION_SEP = ":"
const UNPROVIDED_COMPRESSION_LEVEL = math.MinInt32

type Compressor interface {
	Ident() string
	Name() string
	Level() int32
	Compress(data []byte) ([]byte, error)
	Decompress(compressed []byte) ([]byte, error)
}
