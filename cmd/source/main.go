package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	controlpb "github.com/knakatasf/wiki-river/internal/proto/control"
	streampb "github.com/knakatasf/wiki-river/internal/proto/stream"
	"google.golang.org/grpc"
)

type jsonlEvent struct {
	TS      int64  `json:"ts"` // ms since epoch (preferred)
	Wiki    string `json:"wiki"`
	Title   string `json:"title"`
	Comment string `json:"comment"`
	User    string `json:"user"`
	Bot     bool   `json:"bot"`
}

func main() {
	role := strings.ToUpper(os.Getenv("ROLE"))
	if role == "" {
		role = "SOURCE"
	}

	id := flag.String("id", "SRC", "replica ID")
	controller := flag.String("controller", "127.0.0.1:7001", "controller address (Registry). Empty to skip register")
	down := flag.String("downstream", "", "downstream address (host:port)")
	file := flag.String("file", "./data/wiki_sample.jsonl", "path to JSONL file to replay")
	rate := flag.Int("rate", 0, "max records/sec (0 = unlimited)")
	sleep := flag.Duration("sleep", 0, "fixed sleep between records (e.g., 10ms); ignored if --rate>0")
	loop := flag.Bool("loop", false, "loop file when EOF")
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
		// Source has no listen addr; send a placeholder (e.g., "client")
		cfg, err := reg.Register(context.Background(), &controlpb.Hello{
			Kind: controlpb.StageKind_STAGE_KIND_SOURCE,
			Id:   *id,
			Addr: "client", // not serving; just identify ourselves
		})
		if err != nil {
			log.Fatalf("register with controller: %v", err)
		}
		downstream = cfg.GetDownstreamAddr()
	}

	if downstream == "" {
		log.Fatalf("no downstream resolved: set --downstream or provide --controller")
	}

	ctxDial, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()
	conn, err := grpc.DialContext(ctxDial, downstream, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("dial downstream %s: %v", downstream, err)
	}
	defer conn.Close()

	client := streampb.NewStageClient(conn)

	log.Printf("[%-6s] id=%s downstream=%s file=%s rate=%d/s sleep=%s loop=%v",
		role, *id, downstream, *file, *rate, sleep.String(), *loop)

	sendOnce := func() (int, error) {
		ctx := context.Background()
		stream, err := client.Push(ctx)
		if err != nil {
			return 0, fmt.Errorf("open Push: %v", err)
		}

		// open file
		f, err := os.Open(*file)
		if err != nil {
			return 0, fmt.Errorf("open file: %v", err)
		}
		defer f.Close()

		sc := bufio.NewScanner(f)
		buf := make([]byte, 0, 1024*1024)
		sc.Buffer(buf, 8*1024*1024)

		var throttle <-chan time.Time
		if *rate > 0 {
			interval := time.Second / time.Duration(*rate)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			throttle = ticker.C
		}

		sent := 0
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var ev jsonlEvent
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				log.Printf("skip bad json: %v", err)
				continue
			}

			// fill record
			rec := &streampb.Record{
				Ts:      ev.TS,
				Wiki:    ev.Wiki,
				Title:   ev.Title,
				Comment: ev.Comment,
				User:    ev.User,
				Bot:     ev.Bot,
			}

			if rec.Ts == 0 {
				rec.Ts = time.Now().UnixMilli()
			}

			if *rate > 0 {
				<-throttle
			} else if *sleep > 0 {
				time.Sleep(*sleep)
			}

			if err := stream.Send(rec); err != nil {
				_, _ = stream.CloseAndRecv()
				return sent, fmt.Errorf("send: %v", err)
			}
			sent++
		}
		if err := sc.Err(); err != nil {
			log.Printf("scan failed: %v", err)
		}

		ack, err := stream.CloseAndRecv()
		if err != nil {
			return sent, fmt.Errorf("close/recv ack: %v", err)
		}
		log.Printf("[%-6s] sent=%d ack.ok=%v reason=%q", role, sent, ack.GetOk(), ack.GetReason())
		return sent, nil
	}

	total := 0
	for {
		n, err := sendOnce()
		total += n
		if err != nil {
			log.Printf("stream error, will retry in 1s: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if *loop {
			continue
		}
		break
	}

	log.Printf("[%-6s] DONE total_sent=%d", role, total)
}
