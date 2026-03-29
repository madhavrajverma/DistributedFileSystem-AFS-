package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "afsfs/generated/afs"
	"afsfs/pkg/server"
)

func main() {
	id := flag.String("id", "s1", "server id")
	host := flag.String("host", "localhost", "hostname to advertise to clients")
	port := flag.String("port", "50051", "port to listen on")
	inputDir := flag.String("inputDir", "/tmp/afs-input", "input files directory")
	outputDir := flag.String("outputDir", "/tmp/afs-output", "output files directory")
	peers := flag.String("peers", "", "peer servers as id=addr,id=addr")
	primary := flag.Bool("primary", false, "start as primary")

	flag.Parse()

	// parse peers
	var peerInfos []server.PeerInfo
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				peerInfos = append(peerInfos, server.PeerInfo{
					ID:   parts[0],
					Addr: parts[1],
				})
			}
		}
	}

	// advertised address uses host flag — not hardcoded localhost
	addr := fmt.Sprintf("%s:%s", *host, *port)
	log.Printf("Starting server id = %s addr = %s", *id, addr)

	// ── Primary-deferral logic ───────────────────────────────────────────
	// If we were asked to start as primary (-primary=true) but another
	// server is already acting as primary (e.g. we are recovering after a
	// crash and s2 won the election while we were down), we must NOT claim
	// primary — that would cause split-brain.
	//
	// We ask every peer "who is the primary?".  If any peer names someone
	// OTHER than us as the current primary, we start as a backup instead.
	startAsPrimary := *primary
	if startAsPrimary {
		existingPrimary := findExistingPrimary(peerInfos, addr)
		if existingPrimary != "" && existingPrimary != addr {
			log.Printf("server %s: another primary already exists at %s — starting as BACKUP instead",
				*id, existingPrimary)
			startAsPrimary = false
		}
	}

	handler, err := server.NewHandler(
		*inputDir,
		*outputDir,
		*id,
		addr,
		peerInfos,
		startAsPrimary,
	)
	if err != nil {
		log.Fatalf("failed to init handler: %v", err)
	}

	log.Printf("handler ready inputDir=%s outputDir=%s primary=%v", *inputDir, *outputDir, startAsPrimary)

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", *port, err)
	}
	log.Printf("listening on port %s", *port)

	grpcServer := grpc.NewServer()
	pb.RegisterAFSServiceServer(grpcServer, handler)

	log.Printf("server %s is running ...", *id)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// findExistingPrimary asks all peers if a primary already exists.
// Returns the primary address if one is found, or "" if none is reachable.
func findExistingPrimary(peers []server.PeerInfo, myAddr string) string {
	for _, p := range peers {
		conn, err := grpc.NewClient(p.Addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		stub := pb.NewAFSServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := stub.GetPrimary(ctx, &pb.GetPrimaryRequest{})
		cancel()
		conn.Close()
		if err == nil && resp.PrimaryAddr != "" {
			return resp.PrimaryAddr
		}
	}
	return ""
}
