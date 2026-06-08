package tools

import "sync"

// EventRing is a fixed-capacity ring buffer for agent events.
// When full, Push evicts the oldest entry to make room.
type EventRing struct {
	mu   sync.Mutex
	buf  [][]byte
	cap  int
	head int // index of oldest entry
	size int
}

func NewEventRing(cap int) *EventRing {
	return &EventRing{
		buf: make([][]byte, cap),
		cap: cap,
	}
}

// Push adds an event. If the ring is full the oldest entry is overwritten.
func (r *EventRing) Push(event []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cap == 0 {
		return
	}
	tail := (r.head + r.size) % r.cap
	r.buf[tail] = event
	if r.size < r.cap {
		r.size++
	} else {
		// overwrite oldest: advance head
		r.head = (r.head + 1) % r.cap
	}
}

// Snapshot returns a copy of all buffered events in insertion order.
func (r *EventRing) Snapshot() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == 0 {
		return [][]byte{}
	}
	out := make([][]byte, r.size)
	for i := 0; i < r.size; i++ {
		out[i] = r.buf[(r.head+i)%r.cap]
	}
	return out
}
