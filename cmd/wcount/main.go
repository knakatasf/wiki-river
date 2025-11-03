package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	controlpb "github.com/knakatasf/wiki-river/internal/proto/control"
	streampb "github.com/knakatasf/wiki-river/internal/proto/stream"
	"google.golang.org/grpc"
)

const queueCap = 1000 // backpressure: bounded queue

type stageServer struct {
	streampb.UnimplementedStageServer

	inCh chan *streampb.Record // receives from upstream via gRPC stream
}

type wcKey struct {
	Win  int64
	Wiki string
	Word string
}

var (
	wcMu  sync.Mutex
	wcMap = make(map[wcKey]int64)
)

func newStageServer() *stageServer {
	return &stageServer{inCh: make(chan *streampb.Record, queueCap)}
}

// Push upstream opens a client-stream and sends many Records
// We recv in a loop and put them in inCh (this is bounded)
func (s *stageServer) Push(stream streampb.Stage_PushServer) error {
	for {
		rec, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&streampb.Ack{Ok: true})
		}
		if err != nil {
			return err
		}
		s.inCh <- rec
	}
}

func operatorFn(rec *streampb.Record) []*streampb.Record {
	if rec.Word == "" {
		return nil
	}

	win := rec.GetTs() / 600000 // windowed count, refresh the count every 10 mins
	k := wcKey{Win: win, Wiki: rec.GetWiki(), Word: rec.GetWord()}

	wcMu.Lock()
	wcMap[k]++
	c := wcMap[k]
	wcMu.Unlock()

	out := *rec
	out.WindowId = win
	out.Count = c
	return []*streampb.Record{&out}
}

func main() {
	role := strings.ToUpper(os.Getenv("ROLE"))
	if role == "" {
		role = "WCOUNT"
	}

	id := flag.String("id", "C1", "replica ID")
	addr := flag.String("addr", ":7104", "listen address (host:port)")
	downFlag := flag.String("downstream", "", "downstream address (host:port)")
	controller := flag.String("controller", "127.0.0.1:7001", "controller address (optional; used to fetch downstream if --downstream is empty)")
	flag.Parse()

	// 1: Start gRPC server to accept upstream Push streams
	srv := newStageServer()
	gs := grpc.NewServer()
	streampb.RegisterStageServer(gs, srv) // now gs is the receiver of messages from upstream

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen %s: %v", *addr, err)
	}
	listenAddr := ln.Addr().String()

	// Resolve downstream: prefer --downstream, otherwise ask controller
	downstream := *downFlag

	// Reuse a single controller connection (for Register and AckBarrier)
	var ctrl controlpb.RegistryClient
	var ctrlConn *grpc.ClientConn
	if *controller != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cconn, err := grpc.DialContext(ctx, *controller, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("failed to dial controller %s: %v", *controller, err)
		}
		ctrlConn = cconn
		ctrl = controlpb.NewRegistryClient(ctrlConn)
	}

	if downstream == "" && ctrl != nil {
		cfg, err := ctrl.Register(context.Background(), &controlpb.Hello{
			Kind: controlpb.StageKind_STAGE_KIND_WCOUNT,
			Id:   *id,
			Addr: listenAddr,
		}) // this invokes gRPC's protobuf's Registry method -> indirectly calls server's Register function through its gs.Serve()
		if err != nil {
			log.Fatalf("failed to register with controller: %v", err)
		}
		downstream = cfg.GetDownstreamAddr()
	}

	if downstream == "" {
		log.Fatalf("no downstream resolved: set --downstream or provide --controller")
	}

	log.Printf("[%-8s] id=%s listening=%s downstream=%s controller=%s", role, *id, listenAddr, downstream, *controller)

	var client streampb.StageClient
	var conn *grpc.ClientConn
	var push streampb.Stage_PushClient
	if downstream != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err = grpc.DialContext(ctx, downstream, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("failed to dial downstream %s: %v", downstream, err)
		}
		client = streampb.NewStageClient(conn)

		ctxPush, cancelPush := context.WithCancel(context.Background())
		defer cancelPush()

		push, err = client.Push(ctxPush) // client side stream
		if err != nil {
			log.Fatalf("failed to push downstream %s: %v", downstream, err)
		}
		defer func() {
			ack, cerr := push.CloseAndRecv()
			if cerr != nil {
				log.Fatalf("failed to receive ack from push: %v", cerr)
			} else {
				log.Printf("downstream Ack: ok=%v reason=%s", ack.GetOk(), ack.GetReason())
			}
			if conn != nil {
				_ = conn.Close()
			}
			if ctrlConn != nil {
				_ = ctrlConn.Close()
			}
		}()
	}

	// 3: Processing pipelines:
	//	- goroutine A: serve gRPC (receives -> srv.inCh)
	//	- goroutine B: read from srv.inCh, apply operatorFn, send results to downstream
	// Backpressure: if downstream is slow, push.Send will block; plus inCh is bounded

	// goroutine A: serve gRPC; receiving messages from upstream
	go func() {
		if err := gs.Serve(ln); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// goroutine B: process data by operatorFn() and forward
	for rec := range srv.inCh {
		if rec.GetIsBarrier() {
			log.Printf("[%-8s] id=%s barrier seen epoch=%d", role, *id, rec.GetEpoch())
			if push != nil {
				if err := push.Send(rec); err != nil {
					log.Fatalf("failed to forward barrier: %v", err)
				}
			}
			// best-effort ack to controller to match FILTER/TOKENIZE behavior
			if ctrl != nil {
				_, _ = ctrl.AckBarrier(context.Background(), &controlpb.AckBarrierReq{
					Kind:  controlpb.StageKind_STAGE_KIND_WCOUNT,
					Id:    *id,
					Epoch: rec.GetEpoch(),
				})
			}
			continue
		}
		out := operatorFn(rec)
		if push == nil {
			continue
		}
		for _, o := range out {
			if err := push.Send(o); err != nil {
				log.Fatalf("failed to push record: %v", err)
			}
		}
	}
}
