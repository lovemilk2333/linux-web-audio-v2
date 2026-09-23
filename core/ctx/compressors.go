package ctx

import (
	"bytes"
	"compress/gzip"
	"io"
	"runtime"
	"strconv"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap"
)

func CompressorIdent(name string, level int32) string {
	if level > UNPROVIDED_COMPRESSION_LEVEL {
		return name + COMPRESSION_SEP + strconv.Itoa(int(level))
	} else {
		return name
	}
}

type NoneCompressor struct{}

func (this *NoneCompressor) Ident() string {
	return CompressorIdent(this.Name(), this.Level())
}
func (this *NoneCompressor) Name() string {
	return "none"
}
func (this *NoneCompressor) Level() int32 {
	return UNPROVIDED_COMPRESSION_LEVEL
}
func (this *NoneCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}
func (this *NoneCompressor) Decompress(compressed []byte) ([]byte, error) {
	return compressed, nil
}

var NONE_COMPRESSOR = &NoneCompressor{}

type GzipCompressor struct {
	level       int32
	writer_pool sync.Pool
	reader_pool sync.Pool
}

func (this *GzipCompressor) Ident() string {
	return CompressorIdent(this.Name(), this.Level())
}

func (this *GzipCompressor) Name() string {
	return "gzip"
}

func (this *GzipCompressor) Level() int32 {
	return this.level
}

func (this *GzipCompressor) Compress(data []byte) ([]byte, error) {
	zw := this.writer_pool.Get().(*gzip.Writer)
	defer this.writer_pool.Put(zw)

	var buf bytes.Buffer
	zw.Reset(&buf)

	if _, err := zw.Write(data); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (this *GzipCompressor) Decompress(compressed []byte) ([]byte, error) {
	zr := this.reader_pool.Get().(*gzip.Reader)
	defer this.reader_pool.Put(zr)

	if err := zr.Reset(bytes.NewReader(compressed)); err != nil {
		return nil, err
	}
	defer zr.Close()

	return io.ReadAll(zr)
}

func NewGzipCompressor(ctx *ClientContext, level int32) (*GzipCompressor, error) {
	// logger := ctx.Logger
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		level = gzip.DefaultCompression
	}

	compressor := &GzipCompressor{
		level: level,
	}

	compressor.writer_pool = sync.Pool{
		New: func() interface{} {
			zw, _ := gzip.NewWriterLevel(io.Discard, int(level))
			return zw
		},
	}

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_ = w.Close()
	header := buf.Bytes()

	compressor.reader_pool = sync.Pool{
		New: func() interface{} {
			zr, _ := gzip.NewReader(bytes.NewReader(header))
			return zr
		},
	}

	return compressor, nil
}

type ZstdCompressor struct {
	level       int32
	writer_pool sync.Pool
	reader_pool sync.Pool
}

func (this *ZstdCompressor) Ident() string {
	return CompressorIdent(this.Name(), this.Level())
}

func (this *ZstdCompressor) Name() string {
	return "zstd"
}

func (this *ZstdCompressor) Level() int32 {
	return this.level
}

func (this *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	zw := this.writer_pool.Get().(*zstd.Encoder)
	defer this.writer_pool.Put(zw)

	var buf bytes.Buffer
	zw.Reset(&buf)

	if _, err := zw.Write(data); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (this *ZstdCompressor) Decompress(compressed []byte) ([]byte, error) {
	zr := this.reader_pool.Get().(*zstd.Decoder)
	defer this.reader_pool.Put(zr)

	var buf bytes.Buffer
	if err := zr.Reset(bytes.NewReader(compressed)); err != nil {
		return nil, err
	}

	if _, err := io.Copy(&buf, zr); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func NewZstdCompressor(ctx *ClientContext, level int32) (*ZstdCompressor, error) {
	logger := ctx.Logger

	zstd_level := zstd.EncoderLevelFromZstd(int(level))

	compressor := &ZstdCompressor{
		level: level,
	}

	compressor.writer_pool = sync.Pool{
		New: func() interface{} {
			zw, err := zstd.NewWriter(io.Discard,
				zstd.WithEncoderLevel(zstd_level),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				logger.Error("cannot create zstd writer", zap.Error(err))
				return nil
			}

			runtime.SetFinalizer(zw, func(w *zstd.Encoder) {
				_ = w.Close()
			})

			return zw
		},
	}

	compressor.reader_pool = sync.Pool{
		New: func() interface{} {
			zr, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			if err != nil {
				logger.Error("cannot create zstd reader", zap.Error(err))
				return nil
			}

			runtime.SetFinalizer(zr, func(r *zstd.Decoder) {
				r.Close()
			})

			return zr
		},
	}

	return compressor, nil
}

type LZ4Compressor struct {
	level       int32
	writer_pool sync.Pool
	reader_pool sync.Pool
}

func (this *LZ4Compressor) Ident() string {
	return CompressorIdent(this.Name(), this.Level())
}

func (this *LZ4Compressor) Name() string {
	return "lz4"
}

func (this *LZ4Compressor) Level() int32 {
	return this.level
}

func (this *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	zw := this.writer_pool.Get().(*lz4.Writer)
	defer this.writer_pool.Put(zw)

	var buf bytes.Buffer
	zw.Reset(&buf)

	if _, err := zw.Write(data); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (this *LZ4Compressor) Decompress(compressed []byte) ([]byte, error) {
	zr := this.reader_pool.Get().(*lz4.Reader)
	defer this.reader_pool.Put(zr)

	var buf bytes.Buffer
	zr.Reset(bytes.NewReader(compressed))

	if _, err := io.Copy(&buf, zr); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func NewLZ4Compressor(ctx *ClientContext, level int32) (*LZ4Compressor, error) {
	logger := ctx.Logger
	lz4_level := lz4.CompressionLevel(level)

	compressor := &LZ4Compressor{
		level: level,
	}

	compressor.writer_pool = sync.Pool{
		New: func() interface{} {
			zw := lz4.NewWriter(io.Discard)
			if err := zw.Apply(lz4.CompressionLevelOption(lz4_level)); err != nil {
				logger.Error("cannot create lz4 writer option", zap.Error(err))
			}

			runtime.SetFinalizer(zw, func(w *lz4.Writer) {
				_ = w.Close()
			})

			return zw
		},
	}

	compressor.reader_pool = sync.Pool{
		New: func() interface{} {
			zr := lz4.NewReader(nil)

			runtime.SetFinalizer(zr, func(r *lz4.Reader) {
				r.Reset(nil)
			})

			return zr
		},
	}

	return compressor, nil
}
