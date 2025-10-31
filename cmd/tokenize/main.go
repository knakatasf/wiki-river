package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strings"
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
	txt := rec.GetTitle()
	if txt == "" {
		txt = rec.GetComment()
	}
	words := strings.Fields(strings.ToLower(txt))
	out := make([]*streampb.Record, 0, len(words))
	for _, w := range words {
		if w == "" {
			continue
		}
		c := *rec
		c.Word = w
		out = append(out, &c)
	}
	return out
}

func main() {
	role := strings.ToUpper(os.Getenv("ROLE"))
	if role == "" {
		role = "TOKENIZE"
	}

	id := flag.String("id", "T1", "replica ID")
	addr := flag.String("addr", ":7103", "listen address (host:port)")
	downFlag := flag.String("downstream", "", "downstream address (host:port)")
	controller := flag.String("controller", "127.0.0.1:7001", "controller address (optional; used to fetch downstream if --downstream is empty)")
	flag.Parse()

	// 1: Start gRPC server to accept upstream Push streams
	srv := newStageServer()
	gs := grpc.NewServer()
	streampb.RegisterStageServer(gs, srv)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen %s: %v", *addr, err)
	}
	listenAddr := ln.Addr().String()

	// Resolve downstream: prefer --downstream, otherwise ask controller
	downstream := *downFlag
	if downstream == "" && *controller != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctrlConn, err := grpc.DialContext(ctx, *controller, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("failed to dial controller %s: %v", *controller, err)
		}
		defer ctrlConn.Close()

		reg := controlpb.NewRegistryClient(ctrlConn)
		cfg, err := reg.Register(context.Background(), &controlpb.Hello{
			Kind: controlpb.StageKind_STAGE_KIND_TOKENIZE,
			Id:   *id,
			Addr: listenAddr,
		})
		if err != nil {
			log.Fatalf("failed to register with controller: %v", err)
		}
		downstream = cfg.GetDownstreamAddr()
	}

	log.Printf("[%-8s] id=%s listening=%s downstream=%s", role, *id, listenAddr, downstream)

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
		}()
	}

	// 3: Processing pipelines:
	//	- goroutine A: serve gRPC (receives -> srv.inCh)
	//	- goroutine B: read from srv.inCh, apply operatorFn, send results to downstream
	// Backpressure: if downstream is slow, push.Send will block; plus inCh is bounded

	// goroutine A: serve gRPC
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
