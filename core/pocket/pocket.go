package pocket

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lovemilk2333/linux-web-audio-v2/core/ctx"
)

type PocketType uint16

const (
	// client handshake
	POCKET_C_HANDSHAKE PocketType = iota
	// server resp handshake
	POCKET_S_R_HANDSHAKE
	// close
	POCKET_CLOSE
	// server latency response
	POCKET_S_LATENCY
	// opus data send
	POCKET_S_OPUS
	// buffer too low to request server send more faster or buffer status upload
	POCKET_C_BUFFER
)

const RAW_PAYLOAD_MASK = uint16(0x8000)

/*
| payload | type        |
| dynamic | uint16(BE)  |

the highest bit of type is 1 which means the payload is uncompressed raw data
*/
type Pocket interface {
	GetType() PocketType
	GetCtx() *ctx.ClientContext
}

type PocketBase struct {
	Type PocketType
	Ctx  *ctx.ClientContext
}

func (this *PocketBase) GetType() PocketType {
	return this.Type
}

func (this *PocketBase) GetCtx() *ctx.ClientContext {
	return this.Ctx
}

type Handshake struct {
	PocketBase `bson:"-"`
	// like `zstd:3`, `gzip`
	Compression string `bson:"c"`
	// rate of `FRAME_DURATION`: 1 -> 2.5ms, 2 -> 5ms
	TargetBuffer uint16 `bson:"tb"`
}

func (this *Handshake) GetCompressor() (ctx.Compressor, error) {
	logger := this.Ctx.Logger
	clean_compression := strings.TrimSpace(this.Compression)

	if clean_compression == "" {
		return ctx.NONE_COMPRESSOR, nil
	}

	var (
		name  string
		level int32 = ctx.UNPROVIDED_COMPRESSION_LEVEL
	)

	before, after, found := strings.Cut(clean_compression, ctx.COMPRESSION_SEP)
	name = strings.TrimSpace(before)
	if found {
		if lvl, err := strconv.Atoi(strings.TrimSpace(after)); err == nil {
			level = int32(lvl)
		}
	}

	switch name {
	case "none":
		return ctx.NONE_COMPRESSOR, nil
	case "zstd":
		compressor, err := ctx.NewZstdCompressor(this.Ctx, level)
		if err != nil {
			logger.Warnw("cannot create zstd compressor", "error", err)
			return ctx.NONE_COMPRESSOR, err
		}

		return compressor, nil
	case "gz", "gzip":
		compressor, err := ctx.NewGzipCompressor(this.Ctx, level)
		if err != nil {
			logger.Warnw("cannot create gzip compressor", "error", err)
			return ctx.NONE_COMPRESSOR, err
		}

		return compressor, nil
	case "lz4":
		compressor, err := ctx.NewLZ4Compressor(this.Ctx, level)
		if err != nil {
			logger.Warnw("cannot create lz4 compressor", "error", err)
			return ctx.NONE_COMPRESSOR, nil
		}

		return compressor, nil
	default:
		logger.Warnf("unknown compressor: %s", ctx.CompressorIdent(name, level))
		return ctx.NONE_COMPRESSOR, fmt.Errorf("unknown compressor")
	}
}

func (this *Handshake) Check() error {
	return nil
}

type Close struct {
	PocketBase `bson:"-"`
	Reason     string `bson:"r"`
	Source     string `bson:"-"`
}

type Latency struct {
	PocketBase    `bson:"-"`
	Opus          int64 `bson:"o"`
	AudioBuffer   int64 `bson:"ab"`
	WsSend        int64 `bson:"ws"`
	Compression   int64 `bson:"c"`
	Decompression int64 `bson:"d"`
}

/*
NOTE: Opus pocket is not BSON payload
*/
type Opus struct {
	PocketBase
	/*
		| data0 length | data0 seq  | data0 | data1 length | data1 seq  | data1 | ...
		| uint8        | uint16(BE) | -     | uint8        | uint16(BE) | -     | ...
	*/
	Payload []byte
}

type Buffer struct {
	PocketBase
	CurrentBuffer uint16 `bson:"cb"`
	CurrentSeq    uint16 `bson:"cs"`
	Resync        bool   `bson:"rs,omitempty"`
	RequestBuffer uint16 `bson:"rb,omitempty"`
}
