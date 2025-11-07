package raftctrl

import (
	"encoding/json"
	"io"

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
	Registry map[string]string             // "KIND:ID" -> addr ("FILTER:F1" → "127.0.0.1:7102")
	WaitAcks map[int64]map[string]struct{} // epoch -> set("KIND:ID")
}

// WaitAcks looks like
// WaitAcks[5] = {
//  "SOURCE:S1": struct{}{},
//  "FILTER:F1": struct{}{},
// }

type FSM struct {
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

// once the leader gets majority of acks, change the fsm.State
func (f *FSM) Apply(log *raft.Log) interface{} {
	var e LogEntry
	_ = json.Unmarshal(log.Data, &e)
	switch e.T {
	case EntryRegister:
		var v RegisterV
		_ = decodeInto(e.V, &v) // struct{ Kind, ID, Addr string }
		key := v.Kind + ":" + v.ID
		f.State.Registry[key] = v.Addr
	case EntryAck:
		var v AckV
		_ = decodeInto(e.V, &v) // type AckV struct { Kind, ID string, Epoch int64 }
		key := v.Kind + ":" + v.ID
		set := f.State.WaitAcks[v.Epoch]
		if set == nil { // create a new epoch set
			set = make(map[string]struct{})
			f.State.WaitAcks[v.Epoch] = set
		}
		set[key] = struct{}{}
	}
	return nil
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
