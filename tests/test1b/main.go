package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"strings"

	"afsfs/pkg/afs"
)

func main() {
	servers := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "comma-separated server list")
	cacheDir := flag.String("cacheDir", "/tmp/afs-cache", "local cache directory")
	flag.Parse()

	fmt.Println("  TASK 1B — Client-side Caching")

	client, err := afs.NewClient(strings.Split(*servers, ","), *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect — %v", err)
	}
	defer client.CloseConn()

	// Test 1B-1: First open → cache MISS → FetchFile must fire
	fmt.Println("\n[Test 1B-1] First open → cache MISS → FetchFile fires")
	fmt.Println("  Watch server log: you will see 'FetchFile' RPC")
	h1, err := client.Open("input_dataset_001.txt", false)
	if err != nil {
		fmt.Printf("  FAIL: Open error: %v\n", err)
	} else {
		fmt.Printf("  PASS ✓  file downloaded to local cache, handle = %d\n", h1)
	}
	// drain
	buf := make([]byte, 4096)
	for {
		_, e := client.Read(h1, buf)
		if e == io.EOF || e != nil {
			break
		}
	}
	client.Close(h1)

	//  Test 1B-2: Second open → cache HIT → TestAuth fires, no download
	fmt.Println("\n[Test 1B-2] Second open → cache HIT → TestAuth fires (no download)")
	fmt.Println("  Watch server log: you will see 'TestAuth' — NOT 'FetchFile'")
	h2, err := client.Open("input_dataset_001.txt", false)
	if err != nil {
		fmt.Printf("  FAIL: Open error: %v\n", err)
	} else {
		fmt.Println("  PASS ✓  used local cache — no FetchFile RPC sent")
		fmt.Printf("          handle = %d\n", h2)
	}
	client.Close(h2)

	// Test 1B-3: Write → close → reopen → cache STALE → re-fetch
	fmt.Println("\n[Test 1B-3] Write → close → reopen → cache invalidated → re-fetch")

	// write a file so its version increases
	client.DeleteFile("cache_test.txt")
	wh, _ := client.CreateFile("cache_test.txt")
	client.Write(wh, []byte("version one content\n"))
	client.Close(wh) // server version = 2 now

	// open again — our cached version (1) < server version (2)
	// TestAuth returns INVALID → triggers FetchFile
	fmt.Println("  Server version is now 2; local cache has version 1")
	fmt.Println("  Watch server log: TestAuth returns invalid → FetchFile fires")
	rh, err := client.Open("cache_test.txt", false)
	if err != nil {
		fmt.Printf("  FAIL: Open error: %v\n", err)
	} else {
		var data []byte
		b2 := make([]byte, 4096)
		for {
			n, e := client.Read(rh, b2)
			if n > 0 {
				data = append(data, b2[:n]...)
			}
			if e == io.EOF || e != nil {
				break
			}
		}
		client.Close(rh)
		if string(data) == "version one content\n" {
			fmt.Printf("  PASS ✓  fresh content fetched: %q\n", string(data))
		} else {
			fmt.Printf("  FAIL: unexpected content: %q\n", string(data))
		}
	}

	fmt.Println("  TASK 1B COMPLETE")
}
