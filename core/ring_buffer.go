package core

import (
	"sync"
)

type UnsignedSeq interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type Seqable[S UnsignedSeq] interface {
	Seq() S
}

type RingBuffer[S UnsignedSeq, D Seqable[S]] struct {
	min_seq S
	max_seq S
	data    []D

	data_cap    S
	data_length S
	seq_half    S

	mu sync.RWMutex
}

func (this *RingBuffer[S, D]) GetLock() *sync.RWMutex {
	return &this.mu
}

func (this *RingBuffer[S, D]) IsEmpty() bool {
	this.mu.RLock()
	defer this.mu.RUnlock()

	return this.data_length == 0
}

func (this *RingBuffer[S, D]) updateRange() {
	if this.data_length == 0 {
		this.min_seq = 0
		this.max_seq = 0
	} else {
		this.min_seq = this.data[0].Seq()
		this.max_seq = this.data[this.data_length-1].Seq()
	}
}

func (this *RingBuffer[S, D]) UpdateRange() {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.updateRange()
}

/*
NOTE: you should use lock manually

@returns last n items appended of data
*/
func (this *RingBuffer[S, D]) Append(data ...D) int {
	length := len(data)
	if length == 0 {
		return 0
	}

	if length > int(this.data_cap) {
		data = data[length-int(this.data_cap):]
		length = int(this.data_cap)
	}

	length_diff := int(this.data_cap) - (int(this.data_length) + length)

	// can store directly
	if length_diff >= 0 {
		copy(this.data[this.data_length:], data)
		this.data_length += S(length)
	} else {
		overflow := -length_diff
		copy(this.data, this.data[overflow:this.data_length])
		base := int(this.data_length) - overflow
		copy(this.data[base:], data)
		this.data_length = this.data_cap
	}

	this.updateRange()

	return length
}

func (this *RingBuffer[S, D]) getRange(min_seq S, max_seq S) ([]D, bool) {
	if this.data_length == 0 {
		return nil, false
	}

	request_offset := S(max_seq - min_seq)
	if request_offset > this.seq_half {
		return nil, false
	}

	start_offset := S(min_seq - this.min_seq)
	end_offset := S(max_seq - this.min_seq)
	full_range := true

	if end_offset > this.seq_half {
		return nil, false
	}

	start := int(start_offset)
	if start_offset > this.seq_half {
		full_range = false
		start = 0
	}

	end := int(end_offset) + 1
	if end > int(this.data_length) {
		full_range = false
		end = int(this.data_length)
	}

	if start >= int(this.data_length) || start >= end {
		return nil, false
	}

	return this.data[start:end], full_range
}

func (this *RingBuffer[S, D]) GetRange(min_seq S, max_seq S) ([]D, bool) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	return this.getRange(min_seq, max_seq)
}

func (this *RingBuffer[S, D]) GetBelow(max_seq S) ([]D, bool) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	return this.getRange(this.min_seq, max_seq)
}

func (this *RingBuffer[S, D]) GetGreater(min_seq S) ([]D, bool) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if this.data_length == 0 {
		return nil, true
	}

	index := S(min_seq - this.min_seq)
	if index > this.seq_half {
		return this.data[:this.data_length], false
	}

	if index >= this.data_length-1 {
		return nil, true
	}

	return this.getRange(min_seq+1, this.max_seq)
}

func (this *RingBuffer[S, D]) First(n S) ([]D, bool) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if this.data_length < n {
		return this.data[:this.data_length], false
	}

	return this.data[:n], true
}

func (this *RingBuffer[S, D]) Last(n S) ([]D, bool) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if this.data_length < n {
		return this.data[:this.data_length], false
	}

	return this.data[this.data_length-n : this.data_length], true
}

func NewRingBuffer[S UnsignedSeq, T Seqable[S]](max_data_length S) *RingBuffer[S, T] {
	buffer := &RingBuffer[S, T]{
		data:     make([]T, max_data_length),
		data_cap: max_data_length,
		seq_half: ^S(0) >> 1,
	}

	return buffer
}
