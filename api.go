package main

import (
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {

}

func serveHTTP(kv *kvstore, port int, errorC <-chan error) {
	server := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{},
	}

	go func() {
		log.Printf("http api server start at %v", server.Addr)
		log.Fatalln(server.ListenAndServe())
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
