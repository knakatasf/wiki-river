package raftctrl

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type Opts struct {
	NodeID    string // e.g. "ctrl-1"
	RaftBind  string // e.g. "127.0.0.1:9001" (Raft transport port)
	DataDir   string // e.g. "./data/ctrl-1"
	Bootstrap bool   // true for the first node only
}

type Node struct {
	Raft *raft.Raft
	FSM  *FSM
}

func NewNode(o Opts) (*Node, error) {
	if err := os.MkdirAll(o.DataDir, 0o755); err != nil {
		return nil, err
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(o.NodeID)
	cfg.SnapshotInterval = 0 // no snapshots

	addr, err := net.ResolveTCPAddr("tcp", o.RaftBind)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(o.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(o.DataDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(o.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}

	fsm := NewFSM()
	snapStore := raft.NewDiscardSnapshotStore() // no snapshots

	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, err
	}

	if o.Bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{ID: raft.ServerID(o.NodeID), Address: transport.LocalAddr()},
			},
		}
		r.BootstrapCluster(cfg)
	}

	return &Node{Raft: r, FSM: fsm}, nil
}

// share a new registration log with other controllers
func (n *Node) ApplyRegister(kind, id, addr string) error {
	e := LogEntry{T: EntryRegister, V: RegisterV{Kind: kind, ID: id, Addr: addr}}
	b, _ := json.Marshal(e)
	f := n.Raft.Apply(b, 5*time.Second)
	return f.Error()
}

// share a new ack log with other controllers
func (n *Node) ApplyAck(kind, id string, epoch int64) error {
	e := LogEntry{T: EntryAck, V: AckV{Kind: kind, ID: id, Epoch: epoch}}
	b, _ := json.Marshal(e)
	f := n.Raft.Apply(b, 5*time.Second)
	return f.Error()
}
