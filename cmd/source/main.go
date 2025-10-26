package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
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

func readRecentChangeSSE(ctx context.Context, url, wantWiki string, out chan<- *streampb.Record) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")

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

				var ev struct {
					Title   string `json:"title"`
					Comment string `json:"comment"`
					User    string `json:"user"`
					Bot     bool   `json:"bot"`
					Wiki    string `json:"wiki"`
					Server  string `json:"server_name"`
					TS      int64  `json:"timestamp"` // seconds since epoch
				}
				if err := json.Unmarshal([]byte(raw), &ev); err == nil {
					wiki := ev.Wiki
					if wiki == "" {
						wiki = ev.Server // some streams place the wiki in server_name
					}
					if wantWiki != "" && wiki != wantWiki {
						continue
					}
					rec := &streampb.Record{
						Ts:      ev.TS * 1000, // convert seconds -> ms
						Wiki:    wiki,
						Title:   ev.Title,
						Comment: ev.Comment,
						User:    ev.User,
						Bot:     ev.Bot,
					}
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
	mode := flag.String("mode", "file", "source mode: file|sse")
	sseURL := flag.String("sse-url", "https://stream.wikimedia.org/v2/stream/recentchange", "Wikimedia EventStreams URL")
	wikiFilter := flag.String("wiki", "enwiki", "if non-empty, only forward events from this wiki (server_name/wiki field)")
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

	log.Printf("[%-6s] id=%s mode=%s downstream=%s file=%s sse_url=%s wiki=%s rate=%d/s sleep=%s loop=%v",
		role, *id, *mode, downstream, *file, *sseURL, *wikiFilter, *rate, sleep.String(), *loop)

	// ---- FILE MODE (unchanged behavior) ----
	sendOnceFile := func() (int, error) {
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

	// ---- SSE MODE (new) ----
	sendSSE := func() error {
		ctx := context.Background()
		stream, err := client.Push(ctx)
		if err != nil {
			return fmt.Errorf("open Push: %v", err)
		}

		recCh := make(chan *streampb.Record, 1024)
		// read from Wikimedia EventStreams in a goroutine
		go func() {
			if err := readRecentChangeSSE(ctx, *sseURL, *wikiFilter, recCh); err != nil {
				log.Printf("SSE reader ended: %v", err)
			}
			close(recCh)
		}()

		// optional pacing even in SSE mode
		var throttle <-chan time.Time
		var ticker *time.Ticker
		if *rate > 0 {
			interval := time.Second / time.Duration(*rate)
			ticker = time.NewTicker(interval)
			throttle = ticker.C
			defer ticker.Stop()
		}

		sent := 0
		for rec := range recCh {
			if *rate > 0 {
				<-throttle
			} else if *sleep > 0 {
				time.Sleep(*sleep)
			}

			if err := stream.Send(rec); err != nil {
				_, _ = stream.CloseAndRecv()
				return fmt.Errorf("send: %v", err)
			}
			sent++

			// Optional: log every so often
			if sent%200 == 0 {
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

	// dispatch by mode
	switch strings.ToLower(*mode) {
	case "file":
		total := 0
		for {
			n, err := sendOnceFile()
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

	case "sse":
		// Run until the SSE reader closes or you ^C the process.
		if err := sendSSE(); err != nil {
			log.Fatalf("SSE mode error: %v", err)
		}

	default:
		log.Fatalf("unknown --mode=%q (expected file|sse)", *mode)
	}
}
