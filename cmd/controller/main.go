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
}

func newServer(sourceNext, filterNext, tokenizeNext, wcountNext string) *server {
	return &server{
		reg:          &registry{data: make(map[string]regEntry)},
		sourceNext:   sourceNext,
		filterNext:   filterNext,
		tokenizeNext: tokenizeNext,
		wcountNext:   wcountNext,
	}
}

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
		for range t.C {
			k := atomic.AddInt64(&epoch, 1)

			s.reg.mu.RLock()
			var targets []regEntry
			for _, e := range s.reg.data {
				if e.Kind == controlpb.StageKind_STAGE_KIND_SOURCE && e.Addr != "" {
					targets = append(targets, e)
				}
			}
			s.reg.mu.RUnlock()

			log.Printf("[CTRL  ] issue barrier epoch=%d to %d source(s)", k, len(targets))
			for _, e := range targets {
				go func(e regEntry) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					conn, err := grpc.DialContext(ctx, e.Addr, grpc.WithInsecure(), grpc.WithBlock())
					if err != nil {
						log.Printf("[CTRL  ] epoch=%d source=%s dial err: %v", k, e.ID, err)
						return
					}
					defer conn.Close()

					wc := controlpb.NewWorkerClient(conn)
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
