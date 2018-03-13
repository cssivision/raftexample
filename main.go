package main

import (
	"github.com/coreos/etcd/raft/raftpb"
)

func main() {
	cfg := new(config)
	parseFlag(cfg)
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(cfg, getSnapshot, proposeC, confChangeC)

	newKVStore(<-snapshotterReady, confChangeC, proposeC, commitC, errorC)
	serveHTTP(kvs, cfg.port, errorC)
}
