# AFS Distributed Filesystem — Part 1

A user-space distributed filesystem inspired by the Andrew File System (AFS), built from scratch in Go using gRPC. Three replica servers provide fault-tolerant, highly-available storage with automatic leader election and state recovery.


## Filesystem API — How to Integrate with Any Client

The client library is in `pkg/afs/client.go`. Any application (e.g. a prime-finder worker) can use it with six calls:

```go
import "afsfs/pkg/afs"

// 1. Connect  auto-discovers the current primary
client, err := afs.NewClient(
    []string{"localhost:50051", "localhost:50052", "localhost:50053"},
    "/tmp/my-cache",   // local directory for cached files
)

// 2. Read an input file (whole file cached locally on first open)
handle, err := client.Open("input_dataset_001.txt", false)
buf := make([]byte, 4096)
n, err := client.Read(handle, buf)
client.Close(handle)

// 3.Create and write an output file
handle, err = client.CreateFile("primes.txt")
client.Write(handle, []byte("2\n3\n5\n7\n"))
client.Close(handle)   //  flushes to all 3 servers here

// 4. Open an existing output file for append / read-back
handle, err = client.Open("primes.txt", true)

// 5. Delete a file
client.DeleteFile("primes.txt")

// 6. Close the connection when done
client.CloseConn()
```

**The client handles everything automatically:**
- Discovers the current primary (works even after failover)
- Caches files locally  reads never hit the network after the first open
- Retries failed RPCs and reconnects to the new primary after a server crash
- All writes are replicated to all 3 servers before `Close` returns


## What's Implemented

| Task | Feature | Points |
|------|---------|--------|
| **1A** | Open, Read, Write, Close, Create, Delete over gRPC | 15% |
| **1B** | Whole-file client-side caching with `TestAuth` version validation | 15% |
| **2** | At-least-once RPCs, idempotency (clientID + reqSeq dedup), client crash safety | 10% |
| **3** | Primary-backup replication, heartbeat failure detection, lowest-ID leader election, client failover, server recovery sync | 15% |



## Overview

This system stores large input datasets and output result files across a cluster of servers, giving clients a simple file API (`Open`, `Read`, `Write`, `Close`) that looks like local I/O but is backed by a fault-tolerant distributed storage layer.



### Key Properties

| Property | Mechanism |
|----------|-----------|
| **Remote storage** | gRPC-backed file server with persistent disk |
| **Whole-file caching** | Client fetches entire file on first open; all subsequent reads are local |
| **Cache consistency** | `TestAuth` RPC checks server version before trusting cached copy |
| **High availability** | Three replica servers; system survives one server failure |
| **Automatic failover** | Heartbeat-based failure detection + lowest-ID leader election |
| **Self-healing** | Recovered server pulls missing files from current primary |
| **Idempotency** | Every mutating RPC carries a `(clientID, reqSeq)` dedup key |


## AFS Protocol (Tasks 1A & 1B)

### Task 1A - Basic Client-Server with RPC

All file operations happen via gRPC RPCs defined in `proto/afs.proto`.

#### RPC Catalog

| RPC | Direction | Description |
|-----|-----------|-------------|
| `Open` | client → server | Register intent to open; returns handle ID |
| `FetchFile` | client → server | Download entire file bytes + version number |
| `TestAuth` | client → server | Check if client's cached version is still fresh |
| `StoreFile` | client → primary | Upload modified file bytes; primary replicates |
| `CreateFile` | client → primary | Create new empty file; primary replicates |
| `DeleteFile` | client → primary | Remove file; primary replicates |
| `CloseFile` | client → server | Release server-side handle |

#### Open Flow

```
client.Open("input_dataset_001.txt", false)

  1. RPC: Open(path, write=false)
       server: create handle entry → return handleID
       
  2a. Cache miss → RPC: FetchFile(path)
        server: os.ReadFile(path) → send bytes + version
        client: write bytes to /tmp/afs-cache/input_dataset_001.txt
                record version in cache table
                open local file → return clientHandle
                
  2b. Cache hit → RPC: TestAuth(path, cachedVersion)
        server: compare cachedVersion == versions[path]
        if match: use local cache (NO download)
        if mismatch: fall through to FetchFile
```

#### Write & Close Flow

```
client.Write(handle, data)
  → writes to local /tmp/afs-cache/<file>   (NO network yet)
  → marks handle as dirty

client.Close(handle)
  → if dirty:
      data = os.ReadFile(localCachePath)
      RPC: StoreFile(path, data)
          primary: write to disk → bump version → replicate to all backups
          return new version
      update local version in cache table
  → RPC: CloseFile(handleID)
```

### Task 1B  Whole-File Caching

The cache is a simple **version-tagged file store** on the client's local disk.

```
Cache table (in-memory):
  "input_dataset_001.txt" → { localPath: "/tmp/afs-cache/...", version: 3 }

On Open:
  if cached:
    ask server: TestAuth(path, localVersion)
    if server.version == localVersion → use cache (no download)
    if server.version >  localVersion → re-fetch (cache stale)
  if not cached:
    fetch full file → store to /tmp/afs-cache/ → record version
```


## Fault Tolerance (Task 2)

### At-Least-Once Semantics + Idempotency

Every mutating RPC carries two fields:
- `clientID` — unique per-process string (e.g. `"client-12345-1711900000"`)
- `reqSeq` — monotonically increasing integer per client

Combined key: `"client-12345-1711900000:42"`

The server keeps a **dedup table** (`map[string]dedupEntry`). If the same `(clientID, reqSeq)` arrives twice (client retry after timeout), the server returns the cached response without re-executing the write. This makes all mutating RPCs **exactly-once** from the application's perspective.

### Client Retry Logic

`retryWithFailover` in `pkg/afs/client.go` retries failed RPCs up to **8 times**:
- On transport error -> calls `reconnectToPrimary()` then retries
- On `"not primary"` application error -> calls `reconnectToPrimary()` then retries
- `reconnectToPrimary()` retries finding the primary for up to ~5 seconds with backoff (waits for election to complete)


## Replication & Leader Election (Task 3)

> All replication and election code is written from scratch. No Raft, Paxos, or consensus libraries are used.

### Design: Synchronous Primary-Backup Replication

- One primary handles **all writes** (`StoreFile`, `CreateFile`, `DeleteFile`)
- Primary replicates every write to **all backups in parallel** via `Replicate` RPC and **waits for all ACKs** (`sync.WaitGroup`) before returning success to the client
- Backups serve reads and respond to `GetPrimary` queries
- Guarantees: after a write is acknowledged, all live servers have the data

### Component 1: Heartbeat — Failure Detection

**Code: `pkg/server/heartbeat.go`**
```
Primary → each backup, every 1 second:
  Heartbeat { serverID: "s1" }

Backup receives:
  -> updates lastHeartbeat = time.Now()
  -> if sender's ID matches a known peer, stores knownPrimaryAddr = peer.addr
     (backups learn who the primary is passively from heartbeats)

Backup monitor goroutine (polls every 500ms):
  startupGrace = 6 seconds — no election during startup (prevents false elections while other servers are still booting)

  after grace period:
    if time.Since(lastHeartbeat) > 3s:
      → resetHeartbeat() to prevent re-triggering
      → startElection()
      → if I became primary → exit loop, heartbeat sender is running
      → if still backup → resetHeartbeat() again, keep looping
      (the loop never exits for a backup — it detects future primary failures too)
```

**On restart (`cmd/server/main.go`):**
When a server restarts with `-primary=true`, it first calls `findExistingPrimary()` — it asks all peers via `GetPrimary`. If any peer reports a different primary already exists, this server starts as a backup instead. This prevents split-brain when the original primary restarts after a failover.

### Component 2: Leader Election — Lowest-ID  Algorithm

**Code: `pkg/server/election.go` → `startElection()`**

```
Triggered when a backup detects no heartbeat for 3 seconds:

1. Ping all peers with GetPrimary RPC (1s timeout)
   -> build livePeers = [peers that responded]

2. Determine winner:
   winner = self
   for each livePeer:
     if livePeer.id < winner.id:   <- lexicographic compare: "s2" < "s3"
       winner = livePeer

3. If I am the winner:
   a. promoteToPrimary()
      → primary = true
      → knownPrimaryAddr = my address
      → senderRunning.CompareAndSwap(false, true)
         → start heartbeat sender (guard prevents duplicate goroutines if
            two backups both elect us simultaneously)
   b. broadcastAnnouncePrimary(livePeers)
      -> AnnouncePrimary { primaryAddr, primaryId } → each live peer (async)

4. If I am NOT the winner:
   a. setKnownPrimary(winner.addr)
   b. go syncFromPrimaryWithRetry()
      -> pulls any files written during the transition

AnnouncePrimary RPC handler (on backup):
  → setKnownPrimary(winner.addr)
  → go syncFromPrimaryWithRetry()
```

**Why lowest ID?** IDs are strings (`"s1"`, `"s2"`, `"s3"`). Lexicographic min is deterministic  all live servers independently compute the same winner without any coordination messages beyond the ping. `"s2"` always beats `"s3"`; if s1 is dead and s2/s3 both run elections, both elect s2.

### Component 3: Synchronous Write Replication

**Code: `pkg/server/replication.go` + `pkg/server/handler.go`**

```
On StoreFile / CreateFile / DeleteFile (primary only):

1. Guard:  if !isPrimary() → return "not primary"
2. Dedup:  if dedupTable[clientID:reqSeq] exists → return cached response
3. Write:  os.WriteFile(path+".tmp") → os.Rename(path)   (atomic)
4. Version: versions[path]++
5. Replicate (parallel, sync.WaitGroup):
     for each peer:
       go Replicate { operation, path, data, version }  (2s timeout)
       on failure: log warning, continue (best-effort — client still gets ACK)
6. Return StoreFileResponse{Version: newVersion}

Backup Replicate RPC handler:
  "store"  → atomic write-rename → versions[path] = version
  "create" → os.WriteFile(empty) → versions[path] = 1
  "delete" → os.Remove(path)     → delete versions[path]
```

### Component 4: Client Primary Discovery

**Code: `pkg/afs/client.go` --> `connectToPrimary()` and `reconnectToPrimary()`**

```
NewClient(serverAddrs):
  for each server address (tries all until one answers):
    GetPrimary RPC (2s timeout) → get primaryAddr
    if primaryAddr != "":
      Heartbeat ping to primaryAddr (verifies it's alive)
      if alive → connect, store as c.stub

GetPrimary logic on each server (election.go):
  if isPrimary()     → return my own address
  if knownPrimary != "":
    verify alive (peer lookup or raw gRPC ping)
    if alive  → return knownPrimary
    if dead   → clear knownPrimary, fall through
  ask each peer GetPrimary → forward first non-empty answer

reconnectToPrimary (called on RPC failure or "not primary" error):
  retries connectToPrimary up to 10 times with 500ms-5s backoff
  (waits for election to complete, which takes ~3-4 seconds)
```

### Component 5: State Sync on Recovery

**Code: `pkg/server/handler.go` -> `syncFromPrimaryWithRetry()`**

```
Called at startup (backup) and on AnnouncePrimary / election result:

retry up to 30 times, 2s between attempts (first attempt is immediate):

  1. Check knownPrimaryAddr; if empty, ask peers via GetPrimary
  2. Don't sync from self (guard for edge case)
  3. Get AFSServiceClient for primary:
       prefer existing peer connection; fall back to raw grpc.NewClient
  4. SyncState(myServerID) → primary returns all output files + versions
     (only outputDir files are synced — input files are static on all servers)
  5. For each entry received:
       fullPath = outputDir / basename(entry.Path)
       if !exists locally OR entry.Version > localVersion:
         os.WriteFile(fullPath, entry.Data)
         versions[fullPath] = entry.Version
         log "synced <file> (version N)"
  6. Log total files synced and return
```

### Full Failover Sequence (End-to-End)
```
t=0s   s1 (primary) crashes

t=3s   s2, s3 monitors: time.Since(lastHeartbeat) > 3s → election triggered

t=3-4s s2 runs startElection():
         GetPrimary(s1) → timeout (dead)
         GetPrimary(s3) → OK (alive)
         livePeers = [s3], winnerID = min("s2","s3") = "s2"
         promoteToPrimary() → s2 is now primary
         broadcastAnnouncePrimary([s3])
         s3: setKnownPrimary("s2:50052"), syncFromPrimaryWithRetry()

       s3 also runs startElection():
         GetPrimary(s2) → OK (alive)
         winnerID = min("s3","s2") = "s2"
         setKnownPrimary("s2:50052")
         [both elections independently elect s2 — no conflict]

t=4s   Client's next RPC to s1 fails (transport error):
         retryWithFailover -> reconnectToPrimary()
           try s1:50051 -> GetPrimary times out
           try s2:50052 -> GetPrimary returns "s2:50052" -> verified alive
           connect to s2 -> success
         retry StoreFile on s2 -> s2 replicates to s3 -> ACK

t=?    s1 restarts with -primary=false:
         findExistingPrimary() → s2 reports itself as primary → s1 starts as backup
         syncFromPrimaryWithRetry():
           GetPrimary on s2 → "s2:50052"
           SyncState from s2 → receive files written while s1 was dead
           write all missing/stale files, update versions
           log "server s1 synced N files from primary s2:50052"
         s1 now in sync; s2 heartbeats resume to s1
```


## Prerequisites

- **Docker + Docker Compose** OR
- **Go 1.21+** (for running locally)
## Option A Run with Docker

### 1. Setup

```bash
# Clone / unzip the project
cd afsfs

# Create required directories and seed input data
mkdir -p testdata/input testdata/output-s1 testdata/output-s2 testdata/output-s3
printf "7\n11\n13\n4\n6\n2\n3\n15\n17\n9\n" > testdata/input/input_dataset_001.txt
```

### 2. Start all 3 servers

```bash
docker compose up --build -d
docker compose ps
```

Expected:
```
NAME   STATUS    PORTS
s1     Running   0.0.0.0:50051->50051/tcp   ← primary
s2     Running   0.0.0.0:50052->50052/tcp   ← backup
s3     Running   0.0.0.0:50053->50053/tcp   ← backup
```

### 3. Run automated tests

```bash
S="s1:50051,s2:50052,s3:50053"

# Task 1A — Basic RPC (Open/Read/Write/Close/Create/Delete)
docker compose run --rm client go run ./tests/test1a -servers $S

# Task 1B — Client-side caching (cache miss → FetchFile, cache hit → TestAuth)
docker compose run --rm client go run ./tests/test1b -servers $S

# Task 3A — Replication (write appears on ALL 3 server disks)
docker compose run --rm client go run ./tests/test3a -servers $S

# Task 2B — Client crash safety (partial write never reaches server)
docker compose run --rm client go run ./tests/test2b -servers $S
```

### 4. Verify replication on disk

After test3a, check that all 3 servers have identical content:

```bash
docker exec s1 cat /data/output/replicated.txt
docker exec s2 cat /data/output/replicated.txt
docker exec s3 cat /data/output/replicated.txt
```

All three must print: `replication test - this must appear on all 3 servers`

### 5. Demonstrate failover (Task 3B)

```bash
# Kill the primary
docker compose stop s1

# Wait 4 seconds for automatic election
sleep 4

# Check s2 elected itself as primary
docker compose logs s2 | grep "PRIMARY"

# Client automatically reconnects to new primary — write still works
docker compose run --rm client go run ./tests/test3a -servers $S

# Verify file is on s2 and s3 (not s1 — it's dead)
docker exec s2 cat /data/output/replicated.txt
docker exec s3 cat /data/output/replicated.txt
```

### 6. Demonstrate recovery (Task 3C)

```bash
# Bring s1 back as a backup
docker compose start s1

# Wait for sync
sleep 5

# s1 should have synced files it missed while dead
docker compose logs s1 | grep "synced"
docker exec s1 cat /data/output/replicated.txt   # ← file it missed, now synced
```


### 7. Interactive tests (follow on-screen prompts) 

### These are extra testcases
```bash
# Task 2A — read from local cache after server crash
docker compose run --rm client go run ./tests/test2a -servers $S

# Task 3B — manual failover
docker compose run --rm client go run ./tests/test3b -servers $S

# Task 3C — manual recovery (run after 3B)
docker compose run --rm client go run ./tests/test3c -servers $S
```

## Option B  Run Locally (No Docker)
### 1. Build

```bash
cd afsfs
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

### 2. Setup test data

```bash
mkdir -p testdata/input testdata/output-s1 testdata/output-s2 testdata/output-s3
printf "7\n11\n13\n4\n6\n2\n3\n15\n17\n9\n" > testdata/input/input_dataset_001.txt
```

### 3. Start 3 servers (3 separate terminals)
```bash
# Terminal 1 — PRIMARY
./bin/server -id s1 -host localhost -port 50051 -primary=true \
  -peers s2=localhost:50052,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s1

# Terminal 2 — backup
./bin/server -id s2 -host localhost -port 50052 -primary=false \
  -peers s1=localhost:50051,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s2

# Terminal 3 — backup
./bin/server -id s3 -host localhost -port 50053 -primary=false \
  -peers s1=localhost:50051,s2=localhost:50052 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s3
```


### 4. Run all automated tests 
```bash
S="localhost:50051,localhost:50052,localhost:50053"
go run ./tests/test1a -servers $S
go run ./tests/test1b -servers $S
go run ./tests/test2b -servers $S
go run ./tests/test3a -servers $S
```

### 5. Failover demo
```bash
S="localhost:50051,localhost:50052,localhost:50053"

# Kill primary (only the LISTENING process on 50051, not peers connected to it)
kill $(lsof -ti:50051 -sTCP:LISTEN)

# Wait for election (4 seconds)
sleep 4

# s2 or s3 elected — client reconnects automatically
go run ./tests/test3a -servers $S   # still works! run first this S="localhost:50051,localhost:50052,localhost:50053"

# Restart s1 as backup
./bin/server -id s1 -host localhost -port 50051 -primary=false \
  -peers s2=localhost:50052,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s1 &

# Wait for sync
sleep 5
cat testdata/output-s1/replicated.txt   # file it missed  now synced
```


## Project Structure
```
afsfs/
├── cmd/
│   ├── server/main.go          # server binary
│   └── client/main.go          # demo client
├── pkg/
│   ├── server/
│   │   ├── handler.go          # RPC handlers (Open/Store/Create/Delete/Replicate/SyncState)
│   │   ├── election.go         # Leader election + AnnouncePrimary + GetPrimary
│   │   ├── heartbeat.go        # Heartbeat sender (primary) + monitor (backup)
│   │   └── replication.go      # replicateToAll + peer connections
│   └── afs/
│       ├── client.go           # Client library API (Open/Read/Write/Close/Create/Delete)
│       └── cache.go            # Local cache manager (version tracking)
├── proto/afs.proto             # gRPC service definition
├── generated/afs/              # Generated gRPC Go stubs
├── tests/
│   ├── test1a/  test1b/        # Task 1 tests
│   ├── test2a/  test2b/        # Task 2 tests
│   └── test3a/  test3b/  test3c/  # Task 3 tests
├── testdata/
│   ├── input/                  # Input datasets (read-only)
│   ├── output-s1/              # Server s1 persistent storage
│   ├── output-s2/              # Server s2 persistent storage
│   └── output-s3/              # Server s3 persistent storage
├── Dockerfile
├── docker-compose.yml               
```








## How the Algorithm Works
### Client API
All operations use gRPC RPCs. The client maintains a local file cache — first `Open` downloads the whole file, subsequent opens check `TestAuth` (version match) and use the local copy if fresh. Modified files are uploaded only on `Close`.

### Replication (Task 3)
- **Primary** handles all writes (`StoreFile`, `CreateFile`, `DeleteFile`)
- On each write: primary replicates to all backups via `Replicate` RPC **before** returning success to client
- Primary sends `Heartbeat` to all backups every 1 second

### Leader Election
- Backups monitor heartbeats — if no heartbeat for **3 seconds**, trigger election
- Each server pings all peers, picks the **lowest server ID** among live servers as winner
- Winner promotes itself and broadcasts `AnnouncePrimary` to all live peers
- Monitor loop **re-arms** after each election (detects future failures too)

### Client Failover
- On RPC failure or `"not primary"` error: client calls `reconnectToPrimary()`
- Iterates through all known server addresses, calls `GetPrimary`, connects to winner
- Retries the failed operation up to 5 times

### Server Recovery
- Restarted server calls `GetPrimary` → finds current primary → calls `SyncState`
- Downloads all files with newer versions than its local copies
- Resumes normal backup operation


## Server Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-id` | Server identifier | `s1` |
| `-host` | Advertised hostname | `localhost` |
| `-port` | Listen port | `50051` |
| `-primary` | Start as primary | `false` |
| `-peers` | Peers as `id=host:port,...` | — |
| `-inputDir` | Input files directory | `/tmp/afs-input` |
| `-outputDir` | Output files directory | `/tmp/afs-output` |

---

## Troubleshooting
**Ports already in use:**
```bash
kill $(lsof -ti:50051,50052,50053)
```

**Election not triggering after kill:**
```bash
# Election fires 3s after last heartbeat — wait at least 4s
sleep 4 && grep "PRIMARY" /tmp/s2.log
```

**File missing from a server:**
```bash
# Check replication logs
grep -i "replicate" /tmp/s1.log
```
