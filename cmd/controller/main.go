package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	controlpb "github.com/knakatasf/wiki-river/internal/proto/control"
)

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

	// hard coding for now
	sourceNext   string // -> filter
	filterNext   string // -> tokenize
	tokenizeNext string // -> wcount
	wcountNext   string // -> sink
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
		downstream = "" // terminal
	default:
		downstream = ""
	}

	log.Printf("[REG] %s id=%s addr=%s -> downstream=%s",
		hello.GetKind().String(), hello.GetId(), hello.GetAddr(), downstream)

	return &controlpb.Config{DownstreamAddr: downstream}, nil
}

func main() {
	addr := flag.String("addr", ":7001", "controller listen address")
	filterAddr := flag.String("filter-addr", "127.0.0.1:7102", "filter service address")
	tokenizeAddr := flag.String("tokenize-addr", "127.0.0.1:7103", "tokenize service address")
	wcountAddr := flag.String("wcount-addr", "127.0.0.1:7104", "wcount service address")
	sinkAddr := flag.String("sink-addr", "127.0.0.1:7105", "sink service address")

	s := newServer(*filterAddr, *tokenizeAddr, *wcountAddr, *sinkAddr)

	gs := grpc.NewServer()
	controlpb.RegisterRegistryServer(gs, s) // stub the instantiated controller server into the new grpc.NewServer()
	// so, we want to use gs for later

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("controller listen %s: %v", *addr, err)
	}
	log.Printf("[CTRL] listening=%s", ln.Addr().String())
	log.Printf("[CTRL] pipeline: source→%s → %s → %s → %s",
		*filterAddr, *tokenizeAddr, *wcountAddr, *sinkAddr)

	if err :=
		gs.Serve(ln); err != nil { // when receiving a gRPC request from worker node (client), and it looks at service = Registry, method = Register
		// and then, it invokes registered handler (s *server)'s Register method defined in this file
		log.Fatalf("grpc serve: %v", err)
	}
}
