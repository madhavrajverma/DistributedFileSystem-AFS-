package server

import (
	"context"
	"log"
	"time"

	pb "afsfs/generated/afs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// grpcDial opens a raw gRPC connection to addr (used for one-off pings).
func grpcDial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// startElection uses a simple bully-style approach:
// every live server picks the lowest-ID winner among all reachable servers + itself.
// The winner then broadcasts AnnouncePrimary to all live peers.
func (h *Handler) startElection() {
	log.Printf("server %s starting election", h.serverID)

	winnerID := h.serverID
	winnerAddr := h.serverAddr

	h.peersMu.RLock()
	peers := make([]*peerClient, len(h.peers))
	copy(peers, h.peers)
	h.peersMu.RUnlock()

	var livePeers []*peerClient
	for _, peer := range peers {
		if h.isAlive(peer) {
			livePeers = append(livePeers, peer)
			log.Printf("server %s: peer %s is alive", h.serverID, peer.id)
			if peer.id < winnerID {
				winnerID = peer.id
				winnerAddr = peer.addr
			}
		} else {
			log.Printf("server %s: peer %s is NOT alive", h.serverID, peer.id)
		}
	}

	log.Printf("election result: %s (%s) becomes primary", winnerID, winnerAddr)

	if winnerID == h.serverID {
		// I won — promote myself and tell everyone
		h.promoteToPrimary()
		h.broadcastAnnouncePrimary(livePeers)
	} else {
		log.Printf("server %s: %s is new primary, updating knownPrimary", h.serverID, winnerID)
		h.setKnownPrimary(winnerAddr)
		// Trigger a sync from the newly elected primary so we catch up on
		// any writes we missed while the old primary was failing over.
		go h.syncFromPrimaryWithRetry()
	}
}

// broadcastAnnouncePrimary tells all live backups who the new primary is.
func (h *Handler) broadcastAnnouncePrimary(livePeers []*peerClient) {
	req := &pb.AnnouncePrimaryRequest{
		PrimaryAddr: h.serverAddr,
		PrimaryId:   h.serverID,
	}
	for _, peer := range livePeers {
		go func(p *peerClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := p.stub.AnnouncePrimary(ctx, req)
			if err != nil {
				log.Printf("AnnouncePrimary to %s failed: %v", p.addr, err)
			}
		}(peer)
	}
}

// AnnouncePrimary is called on backups by the winner of an election.
func (h *Handler) AnnouncePrimary(
	ctx context.Context,
	req *pb.AnnouncePrimaryRequest,
) (*pb.AnnouncePrimaryResponse, error) {
	log.Printf("server %s: AnnouncePrimary received — new primary is %s (%s)",
		h.serverID, req.PrimaryId, req.PrimaryAddr)
	h.setKnownPrimary(req.PrimaryAddr)

	// Sync from the new primary so we have all files it wrote
	// while we didn't know it was primary.
	go h.syncFromPrimaryWithRetry()

	return &pb.AnnouncePrimaryResponse{}, nil
}

// setKnownPrimary stores the address of the current primary for GetPrimary answers.
func (h *Handler) setKnownPrimary(addr string) {
	h.primaryMu.Lock()
	h.knownPrimaryAddr = addr
	h.primary = (addr == h.serverAddr)
	h.primaryMu.Unlock()
}

func (h *Handler) promoteToPrimary() {
	h.primaryMu.Lock()
	h.primary = true
	h.knownPrimaryAddr = h.serverAddr
	h.primaryMu.Unlock()

	h.resetHeartbeat()

	log.Printf("server %s is now PRIMARY", h.serverID)

	// Guard: only start one sender goroutine even if promoteToPrimary is
	// called concurrently (two backups both elect us simultaneously).
	if h.senderRunning.CompareAndSwap(false, true) {
		h.startHeartbeatSender()
	}
}

func (h *Handler) isPrimary() bool {
	h.primaryMu.RLock()
	defer h.primaryMu.RUnlock()
	return h.primary
}

func (h *Handler) isAlive(peer *peerClient) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := peer.stub.GetPrimary(ctx, &pb.GetPrimaryRequest{})
	return err == nil
}

// GetPrimary returns the primary address.
// If this server is primary → return self.
// If it knows the primary address → return that.
// Otherwise ask peers.
func (h *Handler) GetPrimary(
	ctx context.Context,
	req *pb.GetPrimaryRequest,
) (*pb.GetPrimaryResponse, error) {
	log.Printf("GetPrimary called on %s isPrimary=%v addr=%s",
		h.serverID, h.isPrimary(), h.serverAddr)

	h.primaryMu.RLock()
	known := h.knownPrimaryAddr
	isPrim := h.primary
	h.primaryMu.RUnlock()

	if isPrim {
		return &pb.GetPrimaryResponse{PrimaryAddr: h.serverAddr}, nil
	}

	if known != "" {
		// Verify the known primary is still alive by pinging it directly.
		verified := false
		for _, peer := range h.copyPeers() {
			if peer.addr == known {
				if h.isAlive(peer) {
					verified = true
				}
				break
			}
		}
		if !verified {
			// Try a raw gRPC ping in case the peer isn't in our peer list
			rawConn, rawErr := grpcDial(known)
			if rawErr == nil {
				rawStub := pb.NewAFSServiceClient(rawConn)
				pingCtx, pingCancel := context.WithTimeout(context.Background(), 1*time.Second)
				_, pingErr := rawStub.GetPrimary(pingCtx, &pb.GetPrimaryRequest{})
				pingCancel()
				rawConn.Close()
				if pingErr == nil {
					verified = true
				}
			}
		}
		if verified {
			return &pb.GetPrimaryResponse{PrimaryAddr: known}, nil
		}
		// known primary is dead — clear it
		h.primaryMu.Lock()
		h.knownPrimaryAddr = ""
		h.primaryMu.Unlock()
	}

	// ask peers
	for _, peer := range h.copyPeers() {
		pCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := peer.stub.GetPrimary(pCtx, req)
		cancel()
		if err == nil && resp.PrimaryAddr != "" {
			return resp, nil
		}
	}

	return &pb.GetPrimaryResponse{PrimaryAddr: ""}, nil
}

// SyncState sends all output files to a recovering peer.
// Only outputDir is synced — input files are static and assumed present on all servers.
func (h *Handler) SyncState(
	ctx context.Context,
	req *pb.SyncStateRequest,
) (*pb.SyncStateResponse, error) {
	log.Printf("SyncState requested by %s", req.ServerId)

	files, err := h.collectFilesForSync(h.outputDir)
	if err != nil {
		log.Printf("SyncState: error collecting files: %v", err)
		return &pb.SyncStateResponse{}, nil
	}
	return &pb.SyncStateResponse{Files: files}, nil
}

func (h *Handler) copyPeers() []*peerClient {
	h.peersMu.RLock()
	peers := make([]*peerClient, len(h.peers))
	copy(peers, h.peers)
	h.peersMu.RUnlock()
	return peers
}
