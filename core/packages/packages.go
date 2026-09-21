package packages

import (
	"strconv"
	"strings"
)

type PackageType uint16

const (
	// client handshake
	PACKAGE_C_HANDSHAKE PackageType = iota
	// server resp handshake
	PACKAGE_S_R_HANDSHAKE
)

const RAW_PAYLOAD_MASK = uint16(0x8000)

/*
| payload | type |
| dynamic | 2 B  |

the highest bit of type is 1 which means the payload is uncompressed raw data
*/
type PackageBase interface {
	GetType() PackageType
}

type Handshake struct {
	Type PackageType
	// like `zstd:3`, `gzip`
	Compression string `bson:"c"`
	// rate of `FRAME_DURATION`: 1 -> 2.5ms, 2 -> 5ms
	TargetBuffer uint32 `bson:"tb"`
}

func (this *Handshake) GetType() PackageType {
	return this.Type
}

func (this *Handshake) GetCompressor() Compressor {
	clean_compression := strings.TrimSpace(this.Compression)

	if clean_compression == "" {
		return nil
	}

	var (
		algorithm string
		level     int = UNPROVIDED_COMPRESSION_LEVEL
	)

	before, after, found := strings.Cut(clean_compression, COMPRESSION_SEP)
	algorithm = strings.TrimSpace(before)
	if found {
		if lvl, err := strconv.Atoi(strings.TrimSpace(after)); err == nil {
			level = lvl
		}
	}

	// TODO init compressor
	switch algorithm {
	case "none":
		return NONE_COMPRESSOR
	case "zstd":
		// TODO
	case "gz", "gzip":
		// TODO
	case "lz4":
		// TODO
	default:
		return NONE_COMPRESSOR
	}
}

func (this *Handshake) Check() error {
	return nil
}
