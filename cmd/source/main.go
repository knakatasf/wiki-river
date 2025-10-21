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
	down := flag.String("downstream", "127.0.0.1:7102", "downstream address (host:port)")
	file := flag.String("file", "./data/wiki_sample.jsonl", "path to JSONL file to replay")
	rate := flag.Int("rate", 0, "max records/sec (0 = unlimited)")
	sleep := flag.Duration("sleep", 0, "fixed sleep between records (e.g., 10ms); ignored if --rate>0")
	loop := flag.Bool("loop", false, "loop file when EOF")
	flag.Parse()

	if *down == "" {
		log.Fatalf("--downstream is required")
	}

	ctxDial, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()
	conn, err := grpc.DialContext(ctxDial, *down, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("dial downstream %s: %v", *down, err)
	}
	defer conn.Close()

	client := streampb.NewStageClient(conn)

	log.Printf("[%-6s] id=%s downstream=%s file=%s rate=%d/s sleep=%s loop=%v",
		role, *id, *down, *file, *rate, sleep.String(), *loop)

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
