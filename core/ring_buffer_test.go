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

	last, _ := buffer.Last(2)
	buffer.Append(&OpusFrame{seq: 1}, &OpusFrame{seq: 2})
	if last[0].seq != 65535 || last[1].seq != 0 {
		t.Fatalf("Last result changed after append: %d, %d", last[0].seq, last[1].seq)
	}
}
