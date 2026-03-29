package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "afsfs/generated/afs"
)

// PeerInfo holds the ID and address of a peer server
type PeerInfo struct {
	ID   string
	Addr string
}

// peerClient holds a gRPC connection to one peer server
type peerClient struct {
	id        string // server ID e.g. "s2"
	addr      string // server address e.g. "localhost:50052"
	stub      pb.AFSServiceClient
	failCount atomic.Int64 // consecutive heartbeat failures — used for log throttling
}

// newPeerClient creates a gRPC connection to a peer server
func newPeerClient(id, addr string) (*peerClient, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to peer %s: %w", addr, err)
	}
	return &peerClient{
		id:   id,
		addr: addr,
		stub: pb.NewAFSServiceClient(conn),
	}, nil
}

// replicateToAll forwards a write to all backups in parallel
// waits for all ACKs before returning — synchronous replication
func (h *Handler) replicateToAll(
	operation string,
	path string,
	data []byte,
	version int64,
) error {
	h.peersMu.RLock()
	peers := make([]*peerClient, len(h.peers))
	copy(peers, h.peers)
	h.peersMu.RUnlock()

	if len(peers) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(peers))

	for _, peer := range peers {
		wg.Add(1)
		go func(p *peerClient) {
			defer wg.Done()
			if err := h.replicateToPeer(p, operation, path, data, version); err != nil {
				log.Printf("replication to %s failed: %v", p.addr, err)
				errs <- err
			}
		}(peer)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// replicateToPeer sends one Replicate RPC to one backup
func (h *Handler) replicateToPeer(
	peer *peerClient,
	operation string,
	path string,
	data []byte,
	version int64,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := peer.stub.Replicate(ctx, &pb.ReplicateRequest{
		Operation: operation,
		Path:      path,
		Data:      data,
		Version:   version,
	})
	if err != nil {
		return fmt.Errorf("Replicate RPC: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("Replicate error: %s", resp.Error)
	}
	return nil
}
