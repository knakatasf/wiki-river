package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	controlpb "github.com/knakatasf/wiki-river/internal/proto/control"
)

var epoch int64

type registry struct {
	mu   sync.RWMutex
	data map[string]regEntry
}
type regEntry struct {
	Kind controlpb.StageKind
	ID   string
	Addr string
	Time time.Time
}

type server struct {
	controlpb.UnimplementedRegistryServer

	reg *registry

	sourceNext   string
	filterNext   string
	tokenizeNext string
	wcountNext   string

	muAcks sync.Mutex
	// { epoch: { KIND:ID: { *placeholder* } } }
	expected map[int64]map[string]struct{}
	received map[int64]map[string]struct{}
}

func newServer(sourceNext, filterNext, tokenizeNext, wcountNext string) *server {
	return &server{
		reg:          &registry{data: make(map[string]regEntry)},
		sourceNext:   sourceNext,
		filterNext:   filterNext,
		tokenizeNext: tokenizeNext,
		wcountNext:   wcountNext,
		expected:     make(map[int64]map[string]struct{}),
		received:     make(map[int64]map[string]struct{}),
	}
}

// each node as client calls this method indirecly
func (s *server) Register(ctx context.Context, hello *controlpb.Hello) (*controlpb.Config, error) {
	key := fmt.Sprintf("%s:%s", hello.GetKind().String(), hello.GetId())

	s.reg.mu.Lock()
	s.reg.data[key] = regEntry{
		Kind: hello.GetKind(),
		ID:   hello.GetId(),
		Addr: hello.GetAddr(),
		Time: time.Now(),
	}
	s.reg.mu.Unlock()

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
		downstream = ""
	default:
		downstream = ""
	}

	log.Printf("[REG   ] %s id=%s addr=%s -> downstream=%s",
		hello.GetKind().String(), hello.GetId(), hello.GetAddr(), downstream)

	return &controlpb.Config{DownstreamAddr: downstream}, nil
}

func (s *server) startBarrierTicker() {
	t := time.NewTicker(30 * time.Second)
	go func() {
		defer t.Stop()
		for range t.C { // Every 30 seconds
			k := atomic.AddInt64(&epoch, 1)

			snap := make(map[string]struct{})
			s.reg.mu.RLock()
			for _, e := range s.reg.data {
				// Populate expected dict with all the nodes except source
				if e.Kind != controlpb.StageKind_STAGE_KIND_SOURCE && e.Addr != "" {
					key := fmt.Sprintf("%s:%s", e.Kind.String(), e.ID)
					snap[key] = struct{}{} // this is just a placeholder, empty struct
				}
			}
			s.reg.mu.RUnlock()

			s.muAcks.Lock()
			s.expected[k] = snap
			s.received[k] = make(map[string]struct{})
			s.muAcks.Unlock()

			s.reg.mu.RLock()
			var targets []regEntry // This is actually just one node to be registered
			for _, e := range s.reg.data {
				if e.Kind == controlpb.StageKind_STAGE_KIND_SOURCE && e.Addr != "" {
					targets = append(targets, e)
				}
			}
			s.reg.mu.RUnlock()

			log.Printf("[CTRL  ] issue barrier epoch=%d to %d source(s)", k, len(targets))
			for _, e := range targets {
				go func(e regEntry) { // don't want to block so use go routine
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					conn, err := grpc.DialContext(ctx, e.Addr, grpc.WithInsecure(), grpc.WithBlock())
					if err != nil {
						log.Printf("[CTRL  ] epoch=%d source=%s dial err: %v", k, e.ID, err)
						return
					}
					defer conn.Close()

					wc := controlpb.NewWorkerClient(conn)
					// ReceiveBarrier() actually sends a barrier to the source node
					if _, err := wc.ReceiveBarrier(context.Background(), &controlpb.Barrier{Epoch: k}); err != nil {
						log.Printf("[CTRL  ] epoch=%d source=%s rpc err: %v", k, e.ID, err)
						return
					}
					log.Printf("[CTRL  ] epoch=%d delivered to source=%s", k, e.ID)
				}(e)
			}
		}
	}()
}

// each node except source will call this service method
func (s *server) AckBarrier(ctx context.Context, in *controlpb.AckBarrierReq) (*controlpb.AckBarrierResp, error) {
	key := fmt.Sprintf("%s:%s", in.GetKind().String(), in.GetId()) // KIND:ID like SINK:S1

	s.muAcks.Lock()
	defer s.muAcks.Unlock()

	exp, ok := s.expected[in.GetEpoch()] // should return the placeholder struct {}
	if !ok {
		log.Printf("[CTRL  ] ack(epoch=%d) from %s (no expected set)", in.GetEpoch(), key)
		return &controlpb.AckBarrierResp{Ok: true}, nil
	}

	// ack is already received by controller (not likely happens...)
	if _, already := s.received[in.GetEpoch()][key]; already {
		return &controlpb.AckBarrierResp{Ok: true}, nil
	}

	s.received[in.GetEpoch()][key] = struct{}{}

	got := len(s.received[in.GetEpoch()]) // how many acks received so far
	need := len(exp)                      // expected number of acks
	log.Printf("[CTRL  ] ack epoch=%d %s (%d/%d)", in.GetEpoch(), key, got, need)

	if got == need {
		log.Printf("[CTRL  ] epoch=%d COMPLETE (%d/%d)", in.GetEpoch(), got, need)
	}
	return &controlpb.AckBarrierResp{Ok: true}, nil
}

func main() {
	addr := flag.String("addr", ":7001", "controller listen address")
	filterAddr := flag.String("filter-addr", "127.0.0.1:7102", "filter service address")
	tokenizeAddr := flag.String("tokenize-addr", "127.0.0.1:7103", "tokenize service address")
	wcountAddr := flag.String("wcount-addr", "127.0.0.1:7104", "wcount service address")
	sinkAddr := flag.String("sink-addr", "127.0.0.1:7105", "sink service address")
	flag.Parse()

	s := newServer(*filterAddr, *tokenizeAddr, *wcountAddr, *sinkAddr)

	gs := grpc.NewServer()
	controlpb.RegisterRegistryServer(gs, s)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("controller listen %s: %v", *addr, err)
	}
	log.Printf("[CTRL  ] listening=%s", ln.Addr().String())
	log.Printf("[CTRL  ] pipeline: source→%s → %s → %s → %s",
		*filterAddr, *tokenizeAddr, *wcountAddr, *sinkAddr)

	s.startBarrierTicker()

	if err := gs.Serve(ln); err != nil {

		log.Fatalf("grpc serve: %v", err)
	}
}
