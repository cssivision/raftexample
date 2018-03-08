package main

import (
	"github.com/coreos/etcd/raft/raftpb"
)

func main() {
	cfg := new(config)
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	newRaftNode(cfg, getSnapshot, proposeC, confChangeC)
}
