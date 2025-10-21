package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	streampb "github.com/knakatasf/wiki-river/internal/proto/stream"
	"google.golang.org/grpc"
	_ "modernc.org/sqlite" // pure-Go SQLite driver (no CGO)
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

func main() {
	role := strings.ToUpper(os.Getenv("ROLE"))
	if role == "" {
		role = "SINK"
	}

	id := flag.String("id", "S1", "replica ID")
	addr := flag.String("addr", "7105", "listen address (host:port)")
	dbPath := flag.String("db", "results.db", "sqlite file path")
	flag.Parse()

	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", *dbPath))
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
	  CREATE TABLE IF NOT EXISTS word_counts (
	    window_id INTEGER NOT NULL,
	    wiki      TEXT    NOT NULL,
	    word      TEXT    NOT NULL,
	    count     INTEGER NOT NULL,
	    PRIMARY KEY (window_id, wiki, word)
	  );
	`); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	upsert, err := db.Prepare(`
	  INSERT INTO word_counts(window_id, wiki, word, count)
	  VALUES (?, ?, ?, ?)
	  ON CONFLICT(window_id, wiki, word) DO UPDATE SET count=excluded.count;
	`)
	if err != nil {
		log.Fatalf("prepare upsert: %v", err)
	}
	defer upsert.Close()

	srv := newStageServer()
	gs := grpc.NewServer()
	streampb.RegisterStageServer(gs, srv)

	// Listening connection request from the upstream (ln: listen)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("[%-6s] id=%s listening=%s db=%s", role, *id, ln.Addr().String(), *dbPath)

	// goroutine for
	go func() {
		if err := gs.Serve(ln); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	var total uint64
	ctx := context.Background()

	for {
		select {
		case rec := <-srv.inCh:
			win := rec.GetWindowId()
			if win == 0 && rec.GetTs() > 0 {
				win = rec.GetTs() / 60000 // 60s tumbling by event-time
			}
			cnt := rec.GetCount()
			if cnt == 0 {
				cnt = 1
			}
			if rec.GetWord() == "" {
				// If a stage upstream hasn't tokenized yet, skip or map a default key.
				continue
			}
			if _, err := upsert.ExecContext(ctx, win, rec.GetWiki(), rec.GetWord(), cnt); err != nil {
				log.Printf("upsert error: %v", err)
			} else {
				total++
			}

		case <-tick.C:
			log.Printf("[%-6s] id=%s total_upserts=%d queue=%d/%d", role, *id, total, len(srv.inCh), queueCap)
		}
	}
}
