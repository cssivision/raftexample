package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"github.com/coreos/etcd/raftsnap"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]string // current committed key-value pairs
	snapshotter *raftsnap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *raftsnap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			snapshot, err := s.snapshotter.Load()
			if err != nil && err == raftsnap.ErrNoSnapshot {
				continue
			}
			if err != nil && err != raftsnap.ErrNoSnapshot {
				log.Fatalf("raftexample: kvstore load snapshot err (%v)", err)
			}
			log.Printf("raftexample: kvstore loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)

			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Fatalf("raftexample: kvstore recover from snapshot err (%v)", err)
			}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: kvstore could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.kvStore[dataKv.Key] = dataKv.Val
		s.mu.Unlock()
	}
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	s.kvStore = store
	s.mu.Unlock()
	return nil
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}
