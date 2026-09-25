package core

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/hraban/opus"
	"github.com/lovemilk2333/linux-web-audio-v2/core/ctx"
	"github.com/lovemilk2333/linux-web-audio-v2/core/pocket"
	"go.uber.org/zap"
)

func TestOpusFrameDuration(t *testing.T) {
	sizer, err := NewOpusSizer(SAMPLE_RATE, CHANNELS, OPUS_BITRATE, DURATION_RATE)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sizer.PCMFrameLength(), uint(240); got != want {
		t.Fatalf("PCM frame has %d float32 samples, want %d", got, want)
	}

	encoder, err := opus.NewEncoder(SAMPLE_RATE, CHANNELS, opus.AppAudio)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := opus.NewDecoder(SAMPLE_RATE, CHANNELS)
	if err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, 1024)
	length, err := encoder.EncodeFloat32(make([]float32, sizer.PCMFrameLength()), encoded)
	if err != nil {
		t.Fatal(err)
	}
	samples, err := decoder.DecodeFloat32(encoded[:length], make([]float32, 5760*CHANNELS))
	if err != nil {
		t.Fatal(err)
	}
	if samples != SAMPLE_RATE/OPUS_DURATION_BASE {
		t.Fatalf("decoded %d samples per channel, want %d", samples, SAMPLE_RATE/OPUS_DURATION_BASE)
	}
}

func TestBufferWatermarks(t *testing.T) {
	tests := []struct {
		target uint16
		lower  uint16
		upper  uint16
	}{
		{target: 0, lower: 2, upper: 2},
		{target: 1, lower: 2, upper: 3},
		{target: 2, lower: 2, upper: 4},
		{target: 3, lower: 1, upper: 4},
		{target: 4, lower: 2, upper: 6},
		{target: 5, lower: 2, upper: 7},
		{target: 8, lower: 4, upper: 12},
		{target: 20, lower: 10, upper: 30},
		{target: 21, lower: 10, upper: 31},
		{target: 400, lower: 200, upper: 600},
		{target: ^uint16(0), lower: ^uint16(0) / 2, upper: ^uint16(0)},
	}
	for _, test := range tests {
		lower, upper := buffer_watermarks(test.target)
		if lower != test.lower || upper != test.upper {
			t.Errorf("bufferWatermarks(%d) = (%d, %d), want (%d, %d)", test.target, lower, upper, test.lower, test.upper)
		}
	}
}

func TestIsNewerSequence(t *testing.T) {
	tests := []struct {
		sequence uint16
		previous uint16
		want     bool
	}{
		{sequence: 11, previous: 10, want: true},
		{sequence: 0, previous: ^uint16(0), want: true},
		{sequence: 10, previous: 10, want: false},
		{sequence: 9, previous: 10, want: false},
		{sequence: ^uint16(0), previous: 0, want: false},
	}
	for _, test := range tests {
		if got := isNewerSequence(test.sequence, test.previous); got != test.want {
			t.Errorf("isNewerSequence(%d, %d) = %t, want %t", test.sequence, test.previous, got, test.want)
		}
	}
}

func TestContinuousOpusAudio(t *testing.T) {
	sizer, err := NewOpusSizer(SAMPLE_RATE, CHANNELS, OPUS_BITRATE, DURATION_RATE)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := opus.NewEncoder(SAMPLE_RATE, CHANNELS, opus.AppAudio)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := opus.NewDecoder(SAMPLE_RATE, CHANNELS)
	if err != nil {
		t.Fatal(err)
	}

	pcm := make([]float32, sizer.PCMFrameLength())
	encoded := make([]byte, 1024)
	decoded := make([]float32, 5760*CHANNELS)
	previous := float32(0)
	maximumStep := float32(0)
	maximumAmplitude := float32(0)
	for frame := 0; frame < 400; frame++ {
		for sample := 0; sample < len(pcm)/CHANNELS; sample++ {
			value := float32(0.25 * math.Sin(2*math.Pi*440*float64(frame*len(pcm)/CHANNELS+sample)/SAMPLE_RATE))
			pcm[sample*CHANNELS] = value
			pcm[sample*CHANNELS+1] = value
		}
		length, err := encoder.EncodeFloat32(pcm, encoded)
		if err != nil {
			t.Fatal(err)
		}
		samples, err := decoder.DecodeFloat32(encoded[:length], decoded)
		if err != nil || samples != len(pcm)/CHANNELS {
			t.Fatalf("frame %d decoded %d samples: %v", frame, samples, err)
		}
		for sample := 0; sample < samples; sample++ {
			value := decoded[sample*CHANNELS]
			maximumStep = max(maximumStep, float32(math.Abs(float64(value-previous))))
			maximumAmplitude = max(maximumAmplitude, float32(math.Abs(float64(value))))
			previous = value
		}
	}
	if maximumAmplitude < 0.1 || maximumStep > 0.25 {
		t.Fatalf("decoded audio peak %.3f, max adjacent sample step %.3f", maximumAmplitude, maximumStep)
	}
}

func TestOpusPocketPayload(t *testing.T) {
	service := &Service{}
	client := &ctx.ClientContext{Logger: zap.NewNop().Sugar(), Compressor: ctx.NONE_COMPRESSOR}
	frames := []*OpusFrame{{seq: 65535, data: []byte{1, 2, 3}}, {seq: 0, data: []byte{4, 5}}}
	packet := service.ws_build_opus(client, frames)
	if packet.GetType() != pocket.POCKET_S_OPUS {
		t.Fatalf("unexpected pocket type: %d", packet.GetType())
	}
	want := []byte{3, 255, 255, 1, 2, 3, 2, 0, 0, 4, 5}
	if len(packet.Payload) != len(want) {
		t.Fatalf("payload has %d bytes, want %d", len(packet.Payload), len(want))
	}
	for index := range want {
		if packet.Payload[index] != want[index] {
			t.Fatalf("payload byte %d is %d, want %d", index, packet.Payload[index], want[index])
		}
	}
	wire, err := service.ws_encode_pocket(client, packet)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint16(wire[len(wire)-2:]); got != uint16(pocket.POCKET_S_OPUS)|pocket.RAW_PAYLOAD_MASK {
		t.Fatalf("unexpected wire type: %d", got)
	}
}
