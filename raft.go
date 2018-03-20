package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
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

var defaultSnapCount uint64 = 5

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
	if err != nil && err != raftsnap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	if snapshot != nil {
		rc.commitC <- nil
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
	oldwal := fileutil.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	c := &raft.Config{
		ID:              rc.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	// waldir store all the entry, so only determine waldir
	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
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
	go rc.serveChannels()
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		log.Fatalf("raftexample: take snapshot err (%v)", err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index
	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// handle request from kv store.
	go func() {
		var confChangeCount uint64
		for rc.confChangeC != nil && rc.proposeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					rc.node.Propose(context.TODO(), []byte(prop))
				}
			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			if err := rc.wal.Save(rd.HardState, rd.Entries); err != nil {
				log.Fatalf("raftexample: save hardstate and entries err (%v)", err)
			}
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rc.saveSnap(rd.Snapshot); err != nil {
					log.Fatalf("raftexample: save snapshot err (%v)", err)
				}
				if err := rc.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
					log.Fatalf("raftexample: apply snapshot err (%v)", err)
				}
				if err := rc.publishSnapshot(rd.Snapshot); err != nil {
					log.Fatalf("raftexample: publish snapshot err (%v)", err)
				}
			}

			if err := rc.raftStorage.SetHardState(rd.HardState); err != nil {
				log.Fatalf("raftexample: set hardstate err (%v)", err)
			}
			if err := rc.raftStorage.Append(rd.Entries); err != nil {
				log.Fatalf("raftexample: append entries err (%v)", err)
			}
			rc.transport.Send(rd.Messages)

			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot()
			rc.node.Advance()
		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return
		case <-rc.stopc:
			rc.stop()
			return
		}
	}

}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

var snapshotCatchUpEntriesN uint64 = 5

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex < rc.snapCount {
		return
	}

	log.Printf("raftexample: start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Fatalf("raftexample: get snapshot err (%v)", err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		log.Fatalf("raftexample: create snapshot err (%v)", err)
	}
	if err := rc.saveSnap(snap); err != nil {
		log.Fatalf("raftexample: save snapshot err (%v)", err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN + 1
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		log.Fatalf("raftexample: compacted log at index err (%v)", err)
	}

	log.Printf("raftexample: compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty entry
				break
			}
			s := string(ents[i].Data)
			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				log.Fatalf("unmarshal entryconf err (%v)", err)
			}
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index
	}
	return true
}

// discard entries that have been applied.
func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rc *raftNode) publishSnapshot(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)
	if snapshot.Metadata.Index <= rc.appliedIndex {
		return fmt.Errorf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshot.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil
	rc.confState = snapshot.Metadata.ConfState
	rc.snapshotIndex = snapshot.Metadata.Index
	rc.appliedIndex = snapshot.Metadata.Index
	return nil
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed start raft httpserver: (%v)", err)
	}

	log.Printf("raftexample: raft server start at %v\n", url)
	server := &http.Server{Handler: rc.transport.Handler()}
	err = server.Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Server err (%v)", err)
	}
	close(rc.httpdonec)
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
