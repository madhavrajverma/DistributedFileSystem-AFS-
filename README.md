# AFS Distributed Filesystem — Part 1

A user-space distributed filesystem inspired by the Andrew File System (AFS), built from scratch in Go using gRPC.

## What's Implemented

| Task | Feature | Points |
|------|---------|--------|
| **1A** | Open, Read, Write, Close, Create, Delete over gRPC | 15% |
| **1B** | Whole-file client-side caching with `TestAuth` version validation | 15% |
| **2** | At-least-once RPCs, idempotency (clientID + reqSeq dedup), client crash safety | 10% |
| **3** | Primary-backup replication, heartbeat failure detection, lowest-ID leader election, client failover, server recovery sync | 15% |

---

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

### Task 1A — Basic Client-Server with RPC

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

### Task 1B — Whole-File Caching

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

**Why whole-file?** The workload is read-heavy (input files are static). Fetching once and serving all reads locally eliminates repeated network round-trips.







## Prerequisites

- **Docker + Docker Compose** (recommended), OR
- **Go 1.21+** (for running locally)

---

## Option A — Run with Docker (Recommended)

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



## Option B — Run Locally (No Docker)
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
├── run_tests.sh                # Automated test runner
└── tests.md                    # Detailed test descriptions
```

---

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
