package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	role := os.Getenv("ROLE") // optional, just for logging
	addr := flag.String("addr", ":0", "listen address (host:port)")
	down := flag.String("downstream", "", "downstream address (host:port)")
	id := flag.String("id", "", "replica ID")
	flag.Parse()

	if *id == "" {
		*id = role
	}
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	fmt.Printf("[%-9s] id=%s listening=%s downstream=%s\n", role, *id, l.Addr().String(), *down)

	// For week-1 step 1 this is enough: process stays up so you can “go run”.
	select {}
}
