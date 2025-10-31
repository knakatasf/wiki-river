package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	controlpb "github.com/knakatasf/wiki-river/internal/proto/control"
	streampb "github.com/knakatasf/wiki-river/internal/proto/stream"
	"google.golang.org/grpc"
)

type wikiEvent struct {
	Title   string `json:"title"`
	Comment string `json:"comment"`
	User    string `json:"user"`
	Bot     bool   `json:"bot"`
	Wiki    string `json:"wiki"`
	Server  string `json:"server_name"`
	TS      int64  `json:"timestamp"` // seconds since epoch
}

func readRecentChangeSSE(ctx context.Context, url string, out chan<- *streampb.Record) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil) // http GET request
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", "wiki-river-client/1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("sse status %s", resp.Status)
	}

	defer resp.Body.Close()
	scan := bufio.NewScanner(resp.Body)
	scan.Buffer(make([]byte, 0, 1<<20), 1<<20) // up to 1MB lines

	var buf strings.Builder
	for scan.Scan() {
		line := scan.Text()
		if strings.HasPrefix(line, "data:") {
			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}
			buf.WriteString(strings.TrimSpace(line[5:]))
			continue
		}
		if line == "" {
			if buf.Len() > 0 {
				raw := buf.String()
				buf.Reset()

				var wikiEvent wikiEvent
				if err := json.Unmarshal([]byte(raw), &wikiEvent); err == nil {
					rec := &streampb.Record{
						Ts:      wikiEvent.TS * 1000, // convert seconds -> ms
						Wiki:    wikiEvent.Wiki,
						Title:   wikiEvent.Title,
						Comment: wikiEvent.Comment,
						User:    wikiEvent.User,
						Bot:     wikiEvent.Bot,
					}
					// Go select statement; tries to put rec to the channel,
					// but at the same time (concurrently) watch for cancellation
					select {
					case out <- rec:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	}
	return scan.Err()
}

// workerSrv implements the control-plane RPCs the controller calls on the Source.
type workerSrv struct {
	controlpb.UnimplementedWorkerServer
	downstream string // where to push the barrier record (Filter addr)
}

func (w *workerSrv) ReceiveBarrier(ctx context.Context, b *controlpb.Barrier) (*controlpb.Ack, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(cctx, w.downstream, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return &controlpb.Ack{Ok: false, Reason: fmt.Sprintf("dial downstream: %v", err)}, nil
	}
	defer conn.Close()

	client := streampb.NewStageClient(conn)
	stream, err := client.Push(ctx)
	if err != nil {
		return &controlpb.Ack{Ok: false, Reason: fmt.Sprintf("open push: %v", err)}, nil
	}

	rec := &streampb.Record{IsBarrier: true, Epoch: b.GetEpoch()}
	if err := stream.Send(rec); err != nil {
		_, _ = stream.CloseAndRecv()
		return &controlpb.Ack{Ok: false, Reason: fmt.Sprintf("send: %v", err)}, nil
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return &controlpb.Ack{Ok: false, Reason: fmt.Sprintf("close: %v", err)}, nil
	}
	log.Printf("[SOURCE ] injected barrier epoch=%d", b.GetEpoch())
	return &controlpb.Ack{Ok: true}, nil
}

func main() {
	role := strings.ToUpper(os.Getenv("ROLE"))
	if role == "" {
		role = "SOURCE"
	}

	id := flag.String("id", "SRC", "replica ID")
	addr := flag.String("addr", ":7101", "listen address (host:port)")
	controller := flag.String("controller", "127.0.0.1:7001", "controller address (Registry). Empty to skip register")
	down := flag.String("downstream", "", "downstream address (host:port)")
	sseURL := flag.String("sse-url", "https://stream.wikimedia.org/v2/stream/recentchange", "Wikimedia EventStreams URL")
	flag.Parse()

	// Resolve downstream: prefer --downstream; otherwise ask controller
	downstream := *down
	if downstream == "" && *controller != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctrlConn, err := grpc.DialContext(ctx, *controller, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("dial controller %s: %v", *controller, err)
		}
		defer ctrlConn.Close()

		reg := controlpb.NewRegistryClient(ctrlConn)
		cfg, err := reg.Register(context.Background(), &controlpb.Hello{
			Kind: controlpb.StageKind_STAGE_KIND_SOURCE,
			Id:   *id,
			Addr: *addr,
		})
		if err != nil {
			log.Fatalf("register with controller: %v", err)
		}
		downstream = cfg.GetDownstreamAddr()
	}

	if downstream == "" {
		log.Fatalf("no downstream resolved: set --downstream or provide --controller")
	}

	go func() {
		gs := grpc.NewServer()
		controlpb.RegisterWorkerServer(gs, &workerSrv{downstream: downstream})
		ln, err := net.Listen("tcp", *addr)
		if err != nil {
			log.Fatalf("source listen %s: %v", *addr, err)
		}
		log.Printf("[SOURCE ] listening(control)=%s", ln.Addr().String())
		if err := gs.Serve(ln); err != nil { // This calls ReceiveBarrier defined above when the controller sends a barrier
			log.Fatalf("source serve: %v", err)
		}
	}()

	ctxDial, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()
	conn, err := grpc.DialContext(ctxDial, downstream, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("dial downstream %s: %v", downstream, err)
	}
	defer conn.Close()

	client := streampb.NewStageClient(conn)
	log.Printf("[%-6s] id=%s downstream=%s sse_url=%s", role, *id, downstream, *sseURL)

	sendSSE := func() error {
		ctx := context.Background()
		stream, err := client.Push(ctx)
		if err != nil {
			return fmt.Errorf("open Push: %v", err)
		}

		recCh := make(chan *streampb.Record, 1024)
		// delegate one goroutine to read Wikipedia event
		go func() {
			if err := readRecentChangeSSE(ctx, *sseURL, recCh); err != nil {
				log.Printf("SSE reader ended: %v", err)
			}
			close(recCh)
		}()

		sent := 0
		for rec := range recCh {
			if err := stream.Send(rec); err != nil {
				_, _ = stream.CloseAndRecv()
				return fmt.Errorf("send: %v", err)
			}
			sent++

			if sent%100 == 0 {
				log.Printf("[%-6s] streamed %d records (SSE)", role, sent)
			}
		}

		ack, err := stream.CloseAndRecv()
		if err != nil {
			return fmt.Errorf("close/recv ack: %v", err)
		}
		log.Printf("[%-6s] SSE done sent=%d ack.ok=%v reason=%q", role, sent, ack.GetOk(), ack.GetReason())
		return nil
	}

	if err := sendSSE(); err != nil {
		log.Fatalf("SSE mode error: %v", err)
	}
}
