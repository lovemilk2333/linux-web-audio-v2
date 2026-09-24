package core

import "testing"

func TestRingBufferGetGreater(t *testing.T) {
	buffer := NewRingBuffer[uint16, *OpusFrame](3)
	buffer.Append(&OpusFrame{seq: 65534}, &OpusFrame{seq: 65535}, &OpusFrame{seq: 0})

	for _, testcase := range []struct {
		cursor uint16
		want   []uint16
		full   bool
	}{
		{65532, []uint16{65534, 65535, 0}, false},
		{65533, []uint16{65534, 65535, 0}, false},
		{65534, []uint16{65535, 0}, true},
		{65535, []uint16{0}, true},
		{0, nil, true},
		{1, nil, true},
	} {
		frames, full := buffer.GetGreater(testcase.cursor)
		if full != testcase.full || len(frames) != len(testcase.want) {
			t.Fatalf("cursor %d: got %d frames (full=%t), want %d (full=%t)", testcase.cursor, len(frames), full, len(testcase.want), testcase.full)
		}
		for index, frame := range frames {
			if frame.seq != testcase.want[index] {
				t.Fatalf("cursor %d: frame %d has seq %d, want %d", testcase.cursor, index, frame.seq, testcase.want[index])
			}
		}
	}

}

func TestRingBufferRangesAcrossWrap(t *testing.T) {
	buffer := NewRingBuffer[uint16, *OpusFrame](4)
	if frames, full := buffer.GetRange(65535, 0); len(frames) != 0 || full {
		t.Fatalf("empty buffer: got %d frames (full=%t)", len(frames), full)
	}
	if frames, full := buffer.GetGreater(65535); len(frames) != 0 || !full {
		t.Fatalf("empty buffer GetGreater: got %d frames (full=%t)", len(frames), full)
	}

	buffer.Append(&OpusFrame{seq: 65534}, &OpusFrame{seq: 65535}, &OpusFrame{seq: 0}, &OpusFrame{seq: 1})
	for _, testcase := range []struct {
		name string
		read func() ([]*OpusFrame, bool)
		want []uint16
		full bool
	}{
		{"full wrap", func() ([]*OpusFrame, bool) { return buffer.GetRange(65534, 1) }, []uint16{65534, 65535, 0, 1}, true},
		{"partial wrap", func() ([]*OpusFrame, bool) { return buffer.GetRange(65533, 0) }, []uint16{65534, 65535, 0}, false},
		{"past newest", func() ([]*OpusFrame, bool) { return buffer.GetRange(65535, 2) }, []uint16{65535, 0, 1}, false},
		{"before oldest", func() ([]*OpusFrame, bool) { return buffer.GetRange(65532, 65533) }, nil, false},
		{"after newest", func() ([]*OpusFrame, bool) { return buffer.GetRange(2, 3) }, nil, false},
		{"reversed", func() ([]*OpusFrame, bool) { return buffer.GetRange(1, 0) }, nil, false},
		{"below wrap", func() ([]*OpusFrame, bool) { return buffer.GetBelow(0) }, []uint16{65534, 65535, 0}, true},
		{"below oldest", func() ([]*OpusFrame, bool) { return buffer.GetBelow(65533) }, nil, false},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			frames, full := testcase.read()
			if full != testcase.full || len(frames) != len(testcase.want) {
				t.Fatalf("got %d frames (full=%t), want %d (full=%t)", len(frames), full, len(testcase.want), testcase.full)
			}
			for index, frame := range frames {
				if frame.seq != testcase.want[index] {
					t.Fatalf("frame %d has seq %d, want %d", index, frame.seq, testcase.want[index])
				}
			}
		})
	}
}
