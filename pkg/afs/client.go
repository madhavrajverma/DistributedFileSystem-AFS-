package afs

import (
	pb "afsfs/generated/afs"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the AFS client library.
// Applications use this to open, read, write, and close files.
type Client struct {
	grpcConn *grpc.ClientConn
	stub     pb.AFSServiceClient
	cache    *cacheManager
	clientId string       // unique id for each client
	reqSeq   atomic.Int64 // incremented per request

	serverAddrs []string   // all known server addresses
	primaryAddr string     // current primary address
	primaryMu   sync.Mutex // protects grpcConn, stub, primaryAddr
}

func NewClient(serverAddrs []string, cacheDir string) (*Client, error) {
	if len(serverAddrs) == 0 {
		return nil, fmt.Errorf("need at least one server address")
	}

	cache, err := newCacheManager(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("creating cache manager: %w", err)
	}

	clientID := fmt.Sprintf("client-%d-%d", os.Getpid(), time.Now().UnixNano())

	c := &Client{
		serverAddrs: serverAddrs,
		cache:       cache,
		clientId:    clientID,
	}

	if err := c.connectToPrimary(); err != nil {
		return nil, fmt.Errorf("finding primary: %w", err)
	}

	return c, nil
}

// connectToPrimary tries each server until it finds an alive primary.
// It verifies the reported primary is actually reachable before returning.
func (c *Client) connectToPrimary() error {
	for _, addr := range c.serverAddrs {
		// Ask this server who the primary is
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		stub := pb.NewAFSServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := stub.GetPrimary(ctx, &pb.GetPrimaryRequest{})
		cancel()
		conn.Close()

		if err != nil || resp.PrimaryAddr == "" {
			continue
		}

		primaryAddr := resp.PrimaryAddr

		// Verify the reported primary is actually alive with a ping
		primaryConn, err := grpc.NewClient(primaryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		primaryStub := pb.NewAFSServiceClient(primaryConn)
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, pingErr := primaryStub.Heartbeat(pingCtx, &pb.HeartbeatRequest{ServerId: "client"})
		pingCancel()
		if pingErr != nil {
			// Reported primary is unreachable — skip and try next server
			primaryConn.Close()
			log.Printf("client: primary %s reported by %s is unreachable: %v", primaryAddr, addr, pingErr)
			continue
		}

		c.grpcConn = primaryConn
		c.stub = primaryStub
		c.primaryAddr = primaryAddr
		log.Printf("connected to primary at %s", primaryAddr)
		return nil
	}
	return fmt.Errorf("could not find primary among %v", c.serverAddrs)
}

// reconnectToPrimary retries finding the primary in a loop with backoff.
// This is necessary after a primary failure to wait for the election to complete.
func (c *Client) reconnectToPrimary() error {
	c.primaryMu.Lock()
	defer c.primaryMu.Unlock()
	log.Printf("client: primary failed or not primary — reconnecting...")
	if c.grpcConn != nil {
		c.grpcConn.Close()
		c.grpcConn = nil
	}
	// Retry with backoff to wait for election (election takes ~3s)
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			wait := time.Duration(500+attempt*500) * time.Millisecond
			log.Printf("client: waiting %v before retry %d to find new primary", wait, attempt+1)
			time.Sleep(wait)
		}
		if err := c.connectToPrimary(); err == nil {
			return nil
		}
	}
	return fmt.Errorf("could not find primary after election among %v", c.serverAddrs)
}

// CloseConn closes the gRPC connection.
func (c *Client) CloseConn() {
	c.primaryMu.Lock()
	defer c.primaryMu.Unlock()
	if c.grpcConn != nil {
		c.grpcConn.Close()
	}
}

// Open opens a remote file and caches it locally.
// Returns a client-side handle ID.
func (c *Client) Open(path string, write bool) (int64, error) {
	seq := c.reqSeq.Add(1)

	openResp, err := retryWithFailover(c, func(ctx context.Context) (*pb.OpenResponse, error) {
		return c.stub.Open(ctx, &pb.OpenRequest{
			Path:     path,
			Write:    write,
			ClientId: c.clientId,
			ReqSeq:   seq,
		})
	}, func(r *pb.OpenResponse) string { return r.Error })

	if err != nil {
		return 0, fmt.Errorf("Open RPC failed: %w", err)
	}
	if openResp.Error != "" {
		return 0, fmt.Errorf("Open RPC error: %s", openResp.Error)
	}

	serverHandleID := openResp.FileHandleId

	// Check the local cache
	cachedVersion, inCache := c.cache.isCached(path)

	if inCache {
		authResp, err := retryRPC(func(ctx context.Context) (*pb.TestAuthResponse, error) {
			return c.stub.TestAuth(ctx, &pb.TestAuthRequest{
				Path:          path,
				CachedVersion: cachedVersion,
			})
		})

		if err == nil && authResp.IsValid {
			localPath := c.cache.localPath(path)
			fd, err := openLocalFile(localPath, write)
			if err != nil {
				return 0, fmt.Errorf("opening local cache: %w", err)
			}
			clientHandle := c.cache.openFile(path, fd, serverHandleID)
			return clientHandle, nil
		}
	}

	fetchResp, err := retryRPC(func(ctx context.Context) (*pb.FetchFileResponse, error) {
		return c.stub.FetchFile(ctx, &pb.FetchFileRequest{
			Path: path,
		})
	})

	if err != nil {
		return 0, fmt.Errorf("FetchFile RPC failed: %w", err)
	}
	if fetchResp.Error != "" {
		return 0, fmt.Errorf("FetchFile RPC error: %s", fetchResp.Error)
	}

	localPath := c.cache.localPath(path)
	if err := os.WriteFile(localPath, fetchResp.Data, 0644); err != nil {
		return 0, fmt.Errorf("writing to local cache: %w", err)
	}

	c.cache.storeCacheEntry(path, fetchResp.Version)

	fd, err := openLocalFile(localPath, write)
	if err != nil {
		return 0, fmt.Errorf("opening local cache file: %w", err)
	}

	clientHandle := c.cache.openFile(path, fd, serverHandleID)
	return clientHandle, nil
}

func (c *Client) Read(handleID int64, buf []byte) (int, error) {
	entry, exists := c.cache.getOpenEntry(handleID)
	if !exists {
		return 0, fmt.Errorf("unknown handle: %d", handleID)
	}

	n, err := entry.localFd.Read(buf)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("reading local file: %w", err)
	}
	return n, err
}

func (c *Client) Write(handleID int64, data []byte) (int, error) {
	entry, exists := c.cache.getOpenEntry(handleID)
	if !exists {
		return 0, fmt.Errorf("unknown handle: %d", handleID)
	}

	n, err := entry.localFd.Write(data)
	if err != nil {
		return 0, fmt.Errorf("writing local file: %w", err)
	}

	c.cache.markDirty(handleID)
	return n, nil
}

func (c *Client) Close(handleID int64) error {
	entry, exists := c.cache.closeFile(handleID)
	if !exists {
		return fmt.Errorf("unknown handle: %d", handleID)
	}

	entry.localFd.Close()

	if entry.dirty {
		localPath := c.cache.localPath(entry.path)

		data, err := os.ReadFile(localPath)
		if err != nil {
			return fmt.Errorf("reading modified file: %w", err)
		}

		seq := c.reqSeq.Add(1)
		storeResp, err := retryWithFailover(c, func(ctx context.Context) (*pb.StoreFileResponse, error) {
			return c.stub.StoreFile(ctx, &pb.StoreFileRequest{
				Path:     entry.path,
				Data:     data,
				ClientId: c.clientId,
				ReqSeq:   seq,
			})
		}, func(r *pb.StoreFileResponse) string { return r.Error })

		if err != nil {
			return fmt.Errorf("StoreFile RPC failed: %w", err)
		}
		if storeResp.Error != "" {
			return fmt.Errorf("StoreFile RPC error: %s", storeResp.Error)
		}

		c.cache.updateVersion(entry.path, storeResp.Version)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeResp, err := c.stub.CloseFile(ctx, &pb.CloseFileRequest{FileHandleId: entry.serverHandleID})
	if err != nil {
		// Close errors are non-fatal; the file content was already flushed
		log.Printf("CloseFile RPC warning: %v", err)
		return nil
	}
	if closeResp.Error != "" {
		log.Printf("CloseFile RPC warning: %s", closeResp.Error)
	}
	return nil
}

// CreateFile creates a new output file on the primary.
func (c *Client) CreateFile(path string) (int64, error) {
	seq := c.reqSeq.Add(1)

	resp, err := retryWithFailover(c, func(ctx context.Context) (*pb.CreateFileResponse, error) {
		return c.stub.CreateFile(ctx, &pb.CreateFileRequest{
			Path:     path,
			ClientId: c.clientId,
			ReqSeq:   seq,
		})
	}, func(r *pb.CreateFileResponse) string { return r.Error })

	if err != nil {
		return 0, fmt.Errorf("CreateFile RPC failed: %w", err)
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("CreateFile RPC error: %s", resp.Error)
	}

	localPath := c.cache.localPath(path)
	if err := os.WriteFile(localPath, []byte{}, 0644); err != nil {
		return 0, fmt.Errorf("creating local cache file: %w", err)
	}

	c.cache.storeCacheEntry(path, 1)

	fd, err := openLocalFile(localPath, true)
	if err != nil {
		return 0, fmt.Errorf("opening local cache file: %w", err)
	}

	clientHandle := c.cache.openFile(path, fd, resp.FileHandleId)
	return clientHandle, nil
}

func (c *Client) DeleteFile(path string) error {
	seq := c.reqSeq.Add(1)

	resp, err := retryWithFailover(c, func(ctx context.Context) (*pb.DeleteFileResponse, error) {
		return c.stub.DeleteFile(ctx, &pb.DeleteFileRequest{
			Path:     path,
			ClientId: c.clientId,
			ReqSeq:   seq,
		})
	}, func(r *pb.DeleteFileResponse) string { return r.Error })

	if err != nil {
		return fmt.Errorf("DeleteFile RPC failed: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("DeleteFile RPC error: %s", resp.Error)
	}

	localPath := c.cache.localPath(path)
	os.Remove(localPath)

	c.cache.mu.Lock()
	delete(c.cache.cached, path)
	c.cache.mu.Unlock()

	return nil
}

func openLocalFile(path string, write bool) (*os.File, error) {
	if write {
		return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	}
	return os.Open(path)
}

// retryWithFailover retries an RPC up to maxRetries times.
// On a transport error OR a "not primary" application error it reconnects to
// the new primary before retrying. c is the client for reconnect access.
func retryWithFailover[T any](
	c *Client,
	fn func(ctx context.Context) (T, error),
	getAppError func(T) string,
) (T, error) {
	const maxRetries = 8
	const timeout = 5 * time.Second

	var zero T
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond * time.Duration(attempt))
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		result, err := fn(ctx)
		cancel()

		if err != nil {
			lastErr = err
			log.Printf("client: RPC error (attempt %d): %v — trying reconnect", attempt+1, err)
			if reconnErr := c.reconnectToPrimary(); reconnErr != nil {
				log.Printf("client: reconnect failed: %v", reconnErr)
			}
			continue
		}

		// Check for application-level "not primary" error
		if appErr := getAppError(result); strings.Contains(appErr, "not primary") {
			lastErr = fmt.Errorf("not primary: %s", appErr)
			log.Printf("client: received 'not primary' (attempt %d) — reconnecting", attempt+1)
			if reconnErr := c.reconnectToPrimary(); reconnErr != nil {
				log.Printf("client: reconnect failed: %v", reconnErr)
			}
			continue
		}

		return result, nil
	}

	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

// retryRPC retries a read-only RPC (no failover needed — reads work on any replica).
func retryRPC[T any](fn func(ctx context.Context) (T, error)) (T, error) {
	const maxRetries = 3
	const timeout = 5 * time.Second
	const baseDelay = 100 * time.Millisecond

	var zero T
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(baseDelay * time.Duration(1<<(attempt-1)))
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		result, err := fn(ctx)
		cancel()

		if err == nil {
			return result, nil
		}

		lastErr = err
	}
	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}
