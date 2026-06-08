package tools

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventRing_NewEventRing(t *testing.T) {
	t.Parallel()

	ring := NewEventRing(10)
	require.NotNil(t, ring)

	snapshot := ring.Snapshot()
	assert.Equal(t, 0, len(snapshot))
}

func TestEventRing_Push(t *testing.T) {
	t.Parallel()

	ring := NewEventRing(10)

	event := map[string]interface{}{
		"type": "test",
		"seq":  1,
	}
	eventJSON, _ := json.Marshal(event)

	ring.Push(eventJSON)

	snapshot := ring.Snapshot()
	assert.Equal(t, 1, len(snapshot))
	assert.Equal(t, eventJSON, snapshot[0])
}

func TestEventRing_MultipleEvents(t *testing.T) {
	t.Parallel()

	ring := NewEventRing(10)

	// Push 3 events
	for i := 1; i <= 3; i++ {
		event := map[string]interface{}{
			"type": "test",
			"seq":  i,
		}
		eventJSON, _ := json.Marshal(event)
		ring.Push(eventJSON)
	}

	snapshot := ring.Snapshot()
	assert.Equal(t, 3, len(snapshot))

	// Verify they're in order
	for i := 0; i < 3; i++ {
		var event map[string]interface{}
		json.Unmarshal(snapshot[i], &event)
		assert.Equal(t, float64(i+1), event["seq"])
	}
}

func TestEventRing_Eviction(t *testing.T) {
	t.Parallel()

	capacity := 5
	ring := NewEventRing(capacity)

	// Push capacity+1 events
	for i := 1; i <= capacity+1; i++ {
		event := map[string]interface{}{
			"type": "test",
			"seq":  i,
		}
		eventJSON, _ := json.Marshal(event)
		ring.Push(eventJSON)
	}

	snapshot := ring.Snapshot()
	// Should have exactly capacity events, with the oldest evicted
	assert.Equal(t, capacity, len(snapshot))

	// Verify the first event (seq=1) was evicted
	var firstEvent map[string]interface{}
	json.Unmarshal(snapshot[0], &firstEvent)
	assert.Equal(t, float64(2), firstEvent["seq"])
}

func TestEventRing_SnapshotConsistency(t *testing.T) {
	t.Parallel()

	ring := NewEventRing(10)

	event1 := json.RawMessage(`{"seq":1}`)
	event2 := json.RawMessage(`{"seq":2}`)

	ring.Push(event1)
	ring.Push(event2)

	snapshot1 := ring.Snapshot()
	snapshot2 := ring.Snapshot()

	// Both snapshots should be identical
	assert.Equal(t, snapshot1, snapshot2)
}

func TestEventRing_EmptySnapshot(t *testing.T) {
	t.Parallel()

	ring := NewEventRing(10)
	snapshot := ring.Snapshot()

	require.NotNil(t, snapshot)
	assert.Equal(t, 0, len(snapshot))
}

func TestEventRing_LargeCapacity(t *testing.T) {
	t.Parallel()

	capacity := 1000
	ring := NewEventRing(capacity)

	// Push capacity events
	for i := 1; i <= capacity; i++ {
		event := map[string]interface{}{
			"seq": i,
		}
		eventJSON, _ := json.Marshal(event)
		ring.Push(eventJSON)
	}

	snapshot := ring.Snapshot()
	assert.Equal(t, capacity, len(snapshot))

	// Push one more to trigger eviction
	event := map[string]interface{}{
		"seq": capacity + 1,
	}
	eventJSON, _ := json.Marshal(event)
	ring.Push(eventJSON)

	snapshot = ring.Snapshot()
	assert.Equal(t, capacity, len(snapshot))
}
