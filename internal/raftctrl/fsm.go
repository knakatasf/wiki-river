package raftctrl

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type EntryType string

// defines types of logs raft will replicate
const (
	EntryRegister EntryType = "REGISTER"
	EntryAck      EntryType = "ACK"
)

// worker nodes' registration info
type RegisterV struct{ Kind, ID, Addr string }

// worker nodes' acknowledgements info for barriers
type AckV struct {
	Kind, ID string
	Epoch    int64
}

// store actual log entries
type LogEntry struct {
	T EntryType   `json:"t"`
	V interface{} `json:"v"`
}

// replicated state
// represents what controller holds
// this will be fsm (finite state machine)
type ControllerState struct {
	Registry map[string]string             // "KIND:ID" -> addr ("STAGE_KIND_FILTER:F1" → "127.0.0.1:7102")
	WaitAcks map[int64]map[string]struct{} // epoch -> set("KIND:ID")
}

// WaitAcks looks like
// WaitAcks[5] = {
//  "STAGE_KIND_SOURCE:S1": struct{}{},
//  "STAGE_KIND_FILTER:F1": struct{}{},
// }

type FSM struct {
	mu    sync.RWMutex
	State *ControllerState
}

func NewFSM() *FSM {
	return &FSM{
		State: &ControllerState{
			Registry: make(map[string]string),
			WaitAcks: make(map[int64]map[string]struct{}),
		},
	}
}

// once a log is committed, Raft calls Apply on every node
// This is where we update the replicated controller state.
func (f *FSM) Apply(logEntry *raft.Log) interface{} {
	var e LogEntry
	_ = json.Unmarshal(logEntry.Data, &e)

	f.mu.Lock()
	defer f.mu.Unlock()

	switch e.T {
	case EntryRegister:
		var v RegisterV
		_ = decodeInto(e.V, &v) // struct{ Kind, ID, Addr string }
		key := v.Kind + ":" + v.ID
		f.State.Registry[key] = v.Addr

	case EntryAck:
		var v AckV
		_ = decodeInto(e.V, &v) // type AckV struct { Kind, ID string; Epoch int64 }
		key := v.Kind + ":" + v.ID
		set := f.State.WaitAcks[v.Epoch]
		if set == nil {
			set = make(map[string]struct{})
			f.State.WaitAcks[v.Epoch] = set
		}
		set[key] = struct{}{}
	}
	return nil
}

// RegistrySnapshot returns a shallow copy of the registry so callers can
// safely iterate without holding the FSM lock.
func (f *FSM) RegistrySnapshot() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out := make(map[string]string, len(f.State.Registry))
	for k, v := range f.State.Registry {
		out[k] = v
	}
	return out
}

// AckCount returns how many workers have acked the given epoch so far.
func (f *FSM) AckCount(epoch int64) int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	set := f.State.WaitAcks[epoch]
	return len(set)
}

// No snapshots (keep it simple)
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return emptySnap{}, nil }
func (f *FSM) Restore(io.ReadCloser) error         { return nil }

type emptySnap struct{}

func (emptySnap) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (emptySnap) Release()                             {}

func decodeInto(src interface{}, dst interface{}) error {
	b, _ := json.Marshal(src)
	return json.Unmarshal(b, dst)
}
