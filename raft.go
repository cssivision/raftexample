package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/raftsnap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

var defaultSnapCount uint64 = 10000

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *string           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id          uint64   // client ID for raft session
	peers       []string // raft peers
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	getSnapshot func() ([]byte, error)
	lastIndex   uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *raftsnap.Snapshotter
	snapshotterReady chan *raftsnap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

func (rc *raftNode) openSnapshot() *raftpb.Snapshot {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = raftsnap.New(rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != raftsnap.ErrEmptySnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}

	return snapshot
}

func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !fileutil.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}
	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.openSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		if err := rc.raftStorage.ApplySnapshot(*snapshot); err != nil {
			log.Fatalf("raftexample: failed to apply snapshot (%v)", err)
		}
	}
	if err := rc.raftStorage.SetHardState(st); err != nil {
		log.Fatalf("raftexample: failed to set hardstate (%v)", err)
	}

	if err := rc.raftStorage.Append(ents); err != nil {
		log.Fatalf("raftexample: failed to append entries (%v)", err)
	}

	return w
}

func (rc *raftNode) startRaft() {
	// replays WAL entries into the raft instance.
	rc.wal = rc.replayWAL()

	c := &raft.Config{
		ID:              rc.id,
		ElectionTick:    200,
		HeartbeatTick:   20,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	// waldir store all the entry, so only determine waldir
	if fileutil.Exist(rc.waldir) {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.id),
		Raft:        rc,
		ClusterID:   0x1000,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(rc.id))),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != int(rc.id) {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed start raft httpserver: (%v)", err)
	}
}

// stoppableListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopc message
type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

func newStoppableListener(host string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", host)
	if err != nil {
		return nil, err
	}

	return &stoppableListener{
		TCPListener: ln.(*net.TCPListener),
		stopc:       stopc,
	}, nil
}

func (ln *stoppableListener) Accept() (net.Conn, error) {
	errc := make(chan error, 1)
	connc := make(chan *net.TCPConn, 1)
	go func() {
		c, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- c
	}()

	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case c := <-connc:
		c.SetKeepAlive(true)
		c.SetKeepAlivePeriod(3 * time.Minute)
		return c, nil
	}
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(cfg *config, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *raftsnap.Snapshotter) {

	commitC := make(chan *string)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
		id:          cfg.id,
		peers:       cfg.peers,
		join:        cfg.join,
		waldir:      fmt.Sprintf("raftexample-%d", cfg.id),
		snapdir:     fmt.Sprintf("raftexample-%d-snap", cfg.id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapCount,

		snapshotterReady: make(chan *raftsnap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

// implementation rafthttp.Raft interface
func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
