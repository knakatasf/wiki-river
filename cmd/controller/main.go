package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	controlpb "github.com/knakatasf/wiki-river/internal/proto/control"
	raftctrl "github.com/knakatasf/wiki-river/internal/raftctrl"
)

var epoch int64

type server struct {
	controlpb.UnimplementedRegistryServer

	node *raftctrl.Node

	// hard-coded pipeline for now
	sourceNext   string // -> filter
	filterNext   string // -> tokenize
	tokenizeNext string // -> wcount
	wcountNext   string // -> sink
}

func newServer(node *raftctrl.Node, sourceNext, filterNext, tokenizeNext, wcountNext string) *server {
	return &server{
		node:         node,
		sourceNext:   sourceNext,
		filterNext:   filterNext,
		tokenizeNext: tokenizeNext,
		wcountNext:   wcountNext,
	}
}

// each worker node as client calls this method indirectly
func (s *server) Register(ctx context.Context, hello *controlpb.Hello) (*controlpb.Config, error) {
	// Only the Raft leader should accept state mutations
	if s.node.Raft.State() != raft.Leader {
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}

	kindStr := hello.GetKind().String()
	key := fmt.Sprintf("%s:%s", kindStr, hello.GetId())

	// Replicate registration via Raft
	if err := s.node.ApplyRegister(kindStr, hello.GetId(), hello.GetAddr()); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply register for %s: %v", key, err)
	}

	var downstream string
	switch hello.GetKind() {
	case controlpb.StageKind_STAGE_KIND_SOURCE:
		downstream = s.sourceNext
	case controlpb.StageKind_STAGE_KIND_FILTER:
		downstream = s.filterNext
	case controlpb.StageKind_STAGE_KIND_TOKENIZE:
		downstream = s.tokenizeNext
	case controlpb.StageKind_STAGE_KIND_WCOUNT:
		downstream = s.wcountNext
	case controlpb.StageKind_STAGE_KIND_SINK:
		downstream = "" // terminal
	default:
		downstream = ""
	}

	log.Printf("[REG   ] %s id=%s addr=%s -> downstream=%s",
		hello.GetKind().String(), hello.GetId(), hello.GetAddr(), downstream)

	return &controlpb.Config{DownstreamAddr: downstream}, nil
}

// each node except source will call this service method
func (s *server) AckBarrier(ctx context.Context, in *controlpb.AckBarrierReq) (*controlpb.AckBarrierResp, error) {
	// Only leader should take writes
	if s.node.Raft.State() != raft.Leader {
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}

	kindStr := in.GetKind().String()
	key := fmt.Sprintf("%s:%s", kindStr, in.GetId())

	// replicate ACK via Raft
	if err := s.node.ApplyAck(kindStr, in.GetId(), in.GetEpoch()); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply ack for %s: %v", key, err)
	}

	// Compute how many workers have acked this epoch, and how many workers exist.
	got := s.node.FSM.AckCount(in.GetEpoch())

	regSnap := s.node.FSM.RegistrySnapshot()
	need := 0
	for k := range regSnap {
		if strings.HasPrefix(k, controlpb.StageKind_STAGE_KIND_SOURCE.String()+":") {
			continue
		}
		need++
	}

	log.Printf("[CTRL  ] ack epoch=%d %s (%d/%d)", in.GetEpoch(), key, got, need)
	if need > 0 && got >= need {
		log.Printf("[CTRL  ] epoch=%d COMPLETE (%d/%d)", in.GetEpoch(), got, need)
	}

	return &controlpb.AckBarrierResp{Ok: true}, nil
}

// leader-only: issues barriers every 30s based on the FSM registry
func (s *server) startBarrierTicker() {
	t := time.NewTicker(30 * time.Second)
	go func() {
		defer t.Stop()
		for range t.C {
			// only leader issues barriers
			if s.node.Raft.State() != raft.Leader {
				continue
			}

			k := atomic.AddInt64(&epoch, 1)

			// snapshot registry from FSM
			regSnap := s.node.FSM.RegistrySnapshot()

			type srcInfo struct {
				ID   string
				Addr string
			}
			var sources []srcInfo
			workerCount := 0

			for key, addr := range regSnap {
				if addr == "" {
					continue
				}
				parts := strings.SplitN(key, ":", 2)
				if len(parts) != 2 {
					continue
				}
				kindStr, id := parts[0], parts[1]

				if kindStr == controlpb.StageKind_STAGE_KIND_SOURCE.String() {
					sources = append(sources, srcInfo{ID: id, Addr: addr})
				} else {
					workerCount++
				}
			}

			log.Printf("[CTRL  ] issue barrier epoch=%d to %d source(s), workers=%d",
				k, len(sources), workerCount)

			for _, src := range sources {
				go func(src srcInfo) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					conn, err := grpc.DialContext(ctx, src.Addr, grpc.WithInsecure(), grpc.WithBlock())
					if err != nil {
						log.Printf("[CTRL  ] epoch=%d source=%s dial err: %v", k, src.ID, err)
						return
					}
					defer conn.Close()

					wc := controlpb.NewWorkerClient(conn)
					if _, err := wc.ReceiveBarrier(context.Background(), &controlpb.Barrier{Epoch: k}); err != nil {
						log.Printf("[CTRL  ] epoch=%d source=%s rpc err: %v", k, src.ID, err)
						return
					}
					log.Printf("[CTRL  ] epoch=%d delivered to source=%s", k, src.ID)
				}(src)
			}
		}
	}()
}

/*
   --- Raft Join plumbing (HTTP) ---
*/

// payload used by /join
type joinRequest struct {
	ID   string `json:"id"`   // node-id, e.g. "ctrl-2"
	Addr string `json:"addr"` // raft-bind, e.g. "127.0.0.1:9002"
}

// startHTTPJoinServer starts a tiny HTTP server that exposes /join so other
// controller nodes can ask this node (usually the leader) to AddVoter.
func startHTTPJoinServer(node *raftctrl.Node, httpAddr string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if node.Raft.State() != raft.Leader {
			http.Error(w, "not leader", http.StatusPreconditionFailed)
			return
		}

		var req joinRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.ID == "" || req.Addr == "" {
			http.Error(w, "id and addr required", http.StatusBadRequest)
			return
		}

		f := node.Raft.AddVoter(
			raft.ServerID(req.ID),
			raft.ServerAddress(req.Addr),
			0, 0,
		)
		if err := f.Error(); err != nil {
			http.Error(w, fmt.Sprintf("AddVoter error: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("[JOIN  ] added voter id=%s addr=%s", req.ID, req.Addr)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	go func() {
		log.Printf("[HTTP ] join server listening on %s", httpAddr)
		if err := http.ListenAndServe(httpAddr, mux); err != nil {
			log.Fatalf("http join server: %v", err)
		}
	}()
}

// joinExistingCluster sends an HTTP POST /join to an existing controller
// (which should be the current leader or at least a member) so it can
// call AddVoter on our behalf.
func joinExistingCluster(joinURL, nodeID, raftAddr string) error {
	body, _ := json.Marshal(joinRequest{ID: nodeID, Addr: raftAddr})
	req, err := http.NewRequest(http.MethodPost, joinURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join failed: %s", resp.Status)
	}
	return nil
}

func main() {
	addr := flag.String("addr", ":7001", "controller gRPC listen address (for workers)")
	filterAddr := flag.String("filter-addr", "127.0.0.1:7102", "filter service address")
	tokenizeAddr := flag.String("tokenize-addr", "127.0.0.1:7103", "tokenize service address")
	wcountAddr := flag.String("wcount-addr", "127.0.0.1:7104", "wcount service address")
	sinkAddr := flag.String("sink-addr", "127.0.0.1:7105", "sink service address")

	nodeID := flag.String("node-id", "ctrl-1", "raft node ID")
	raftBind := flag.String("raft-bind", "127.0.0.1:9001", "raft bind address (host:port)")
	raftDir := flag.String("raft-dir", "./data/ctrl-1", "raft data directory")
	raftBootstrap := flag.Bool("raft-bootstrap", false, "bootstrap Raft cluster (first node only)")

	httpAddr := flag.String("http-addr", ":7100", "HTTP join server address (host:port)")
	joinURL := flag.String("join", "", "join URL for existing controller, e.g. http://127.0.0.1:7100/join")

	flag.Parse()

	// create Raft node
	node, err := raftctrl.NewNode(raftctrl.Opts{
		NodeID:    *nodeID,
		RaftBind:  *raftBind,
		DataDir:   *raftDir,
		Bootstrap: *raftBootstrap,
	})
	if err != nil {
		log.Fatalf("new raft node: %v", err)
	}

	// Start HTTP join server for *this* controller
	startHTTPJoinServer(node, *httpAddr)

	// If joinURL is provided and we are not bootstrapping, join existing cluster
	if *joinURL != "" && !*raftBootstrap {
		// ensure /join path is present
		j := *joinURL
		if !strings.HasSuffix(j, "/join") {
			if strings.HasSuffix(j, "/") {
				j = j + "join"
			} else {
				j = j + "/join"
			}
		}
		if err := joinExistingCluster(j, *nodeID, *raftBind); err != nil {
			log.Fatalf("join cluster: %v", err)
		}
		log.Printf("[CTRL  ] joined cluster via %s", j)
	}

	s := newServer(node, *filterAddr, *tokenizeAddr, *wcountAddr, *sinkAddr)

	gs := grpc.NewServer()
	controlpb.RegisterRegistryServer(gs, s)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("controller listen %s: %v", *addr, err)
	}
	log.Printf("[CTRL  ] listening=%s", ln.Addr().String())
	log.Printf("[CTRL  ] pipeline: source→%s → %s → %s → %s",
		*filterAddr, *tokenizeAddr, *wcountAddr, *sinkAddr)

	// start leader-only barrier ticker
	s.startBarrierTicker()

	if err := gs.Serve(ln); err != nil {
		log.Fatalf("grpc serve: %v", err)
	}
}
