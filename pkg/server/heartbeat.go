package server

import (
	"context"
	"log"
	"time"

	pb "afsfs/generated/afs"
)

const (
	heartbeatInterval = 1 * time.Second
	heartbeatTimeout  = 3 * time.Second
	monitorPoll       = 500 * time.Millisecond
	startupGrace      = 6 * time.Second
)

func (h *Handler) startHeartbeatSender() {
	go func() {
		for {
			time.Sleep(heartbeatInterval)
			if !h.isPrimary() {
				return
			}
			for _, peer := range h.copyPeers() {
				go h.sendHeartbeat(peer)
			}
		}
	}()
}

func (h *Handler) sendHeartbeat(peer *peerClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := peer.stub.Heartbeat(ctx, &pb.HeartbeatRequest{ServerId: h.serverID})
	if err != nil {
		// Only log every ~10 failures per peer to avoid flooding the terminal
		// when a backup is down. The primary correctly keeps retrying.
		count := peer.failCount.Add(1)
		if count == 1 || count%10 == 0 {
			log.Printf("heartbeat to %s failed (x%d): peer is down", peer.addr, count)
		}
	} else {
		peer.failCount.Store(0) // reset on success
	}
}

func (h *Handler) startHeartbeatMonitor() {
	go func() {
		time.Sleep(startupGrace)
		h.resetHeartbeat()

		for {
			time.Sleep(monitorPoll)

			if h.isPrimary() {
				return
			}

			h.lastHeartbeatMu.Lock()
			last := h.lastHeartbeat
			h.lastHeartbeatMu.Unlock()

			if time.Since(last) > heartbeatTimeout {
				log.Printf("server %s: no heartbeat for %v — starting election",
					h.serverID, heartbeatTimeout)
				h.resetHeartbeat()
				h.startElection()
				if h.isPrimary() {
					return
				}
				h.resetHeartbeat()
			}
		}
	}()
}

func (h *Handler) resetHeartbeat() {
	h.lastHeartbeatMu.Lock()
	h.lastHeartbeat = time.Now()
	h.lastHeartbeatMu.Unlock()
}

func (h *Handler) Heartbeat(
	ctx context.Context,
	req *pb.HeartbeatRequest,
) (*pb.HeartbeatResponse, error) {
	h.lastHeartbeatMu.Lock()
	h.lastHeartbeat = time.Now()
	h.lastHeartbeatMu.Unlock()

	if !h.isPrimary() {
		h.peersMu.RLock()
		for _, peer := range h.peers {
			if peer.id == req.ServerId {
				h.primaryMu.Lock()
				h.knownPrimaryAddr = peer.addr
				h.primaryMu.Unlock()
				break
			}
		}
		h.peersMu.RUnlock()
	}

	return &pb.HeartbeatResponse{IsPrimary: h.isPrimary()}, nil
}
