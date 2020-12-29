package raft

import (
	"context"
	"time"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

func (n *node) runbatcher() {
	t := time.NewTicker(100 * time.Microsecond)
	go func() {
		for range t.C {
			n.bconds.Signal()
		}
	}()

	for {
		now := time.Now()

		n.bmu.Lock()

		for /*len(n.batches) < 500 &&*/ time.Since(now) < 1*time.Millisecond {
			n.bconds.Wait()
		}

		batches := n.batches
		n.batches = nil

		n.bmu.Unlock()

		if len(batches) == 0 {
			continue
		}

		entries := make([]pb.Entry, 0, len(batches))
		for _, b := range batches {
			entries = append(entries, pb.Entry{Data: b.data})
		}

		err := n.stepWait(context.Background(), pb.Message{Type: pb.MsgProp, Entries: entries})

		for i := range batches {
			batches[i].err = &err
		}

		n.bcondc.Broadcast()
	}
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	b := &batch{data: data}

	n.bmu.Lock()
	defer n.bmu.Unlock()

	n.batches = append(n.batches, b)
	n.bconds.Signal()

	for b.err == nil {
		n.bcondc.Wait()
	}

	return *b.err
}
