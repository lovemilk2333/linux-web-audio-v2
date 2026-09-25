package ctx

import (
	"testing"

	"github.com/pierrec/lz4/v4"
)

func TestNormalizeLZ4CompressionLevel(t *testing.T) {
	tests := []struct {
		name  string
		input int32
		want  lz4.CompressionLevel
	}{
		{name: "unprovided", input: UNPROVIDED_COMPRESSION_LEVEL, want: lz4.Fast},
		{name: "negative", input: -1, want: lz4.Fast},
		{name: "fast", input: 0, want: lz4.Fast},
		{name: "level one", input: 1, want: lz4.Level1},
		{name: "level nine", input: 9, want: lz4.Level9},
		{name: "too high", input: 10, want: lz4.Level9},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizeLZ4CompressionLevel(test.input); got != test.want {
				t.Fatalf("normalizeLZ4CompressionLevel(%d) = %d, want %d", test.input, got, test.want)
			}
		})
	}
}
