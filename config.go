package main

import (
	"flag"
	"strings"
)

type config struct {
	id    uint64
	peers []string
	join  bool
	port  int
}

func parseFlag(cfg *config) {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	port := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	cfg.peers = strings.Split(*cluster, ",")
	cfg.id = uint64(*id)
	cfg.port = *port
	cfg.join = *join
}
