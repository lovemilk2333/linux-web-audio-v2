//go:build js && wasm

package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"syscall/js"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

func bytesFromJS(value js.Value) []byte {
	data := make([]byte, value.Get("byteLength").Int())
	js.CopyBytesToGo(data, value)
	return data
}

func bytesToJS(data []byte) js.Value {
	result := js.Global().Get("Uint8Array").New(len(data))
	js.CopyBytesToJS(result, data)
	return result
}

func gzipCompress(data []byte) ([]byte, error) {
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func zstdCompress(data []byte, level int) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func zstdDecompress(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return decoder.DecodeAll(data, nil)
}

func lz4Compress(data []byte, level int) ([]byte, error) {
	var output bytes.Buffer
	writer := lz4.NewWriter(&output)
	if err := writer.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(level))); err != nil {
		return nil, err
	}
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func lz4Decompress(data []byte) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(data))
	return io.ReadAll(reader)
}

func transform(this js.Value, args []js.Value) interface{} {
	if len(args) < 3 {
		panic("compressor: expected operation, algorithm and bytes")
	}
	operation, algorithm := args[0].String(), args[1].String()
	data := bytesFromJS(args[2])
	level := 3
	if len(args) > 3 {
		level = args[3].Int()
	}
	var result []byte
	var err error
	switch algorithm {
	case "gzip":
		if operation == "compress" {
			result, err = gzipCompress(data)
		} else {
			result, err = gzipDecompress(data)
		}
	case "zstd":
		if operation == "compress" {
			result, err = zstdCompress(data, level)
		} else {
			result, err = zstdDecompress(data)
		}
	case "lz4":
		if operation == "compress" {
			result, err = lz4Compress(data, level)
		} else {
			result, err = lz4Decompress(data)
		}
	default:
		panic("compressor: unsupported algorithm")
	}
	if err != nil {
		panic(err.Error())
	}
	return bytesToJS(result)
}

func main() {
	js.Global().Set("linuxWebAudioCompress", js.FuncOf(transform))
	select {}
}
