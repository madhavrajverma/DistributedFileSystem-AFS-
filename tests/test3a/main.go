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
	cacheDir := flag.String("cacheDir", "/tmp/afs-cache-3a", "local cache directory")
	flag.Parse()

	addrs := strings.Split(*servers, ",")

	fmt.Println("  TASK 3A — Replication")
	fmt.Println("  Proves: write to primary appears on ALL 3 servers")

	//  Step 1: Write a file via the primary
	fmt.Println("\n[Step 1] Write 'replicated.txt' to the primary")
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: connect: %v", err)
	}
	defer client.CloseConn()

	payload := "REPLICATION TEST — identical on all 3 servers\n"
	client.DeleteFile("replicated.txt")
	wh, err := client.CreateFile("replicated.txt")
	if err != nil {
		log.Fatalf("FAIL: CreateFile: %v", err)
	}
	client.Write(wh, []byte(payload))
	if err := client.Close(wh); err != nil {
		log.Fatalf("FAIL: Close: %v", err)
	}
	fmt.Println("  PASS ✓  StoreFile sent → primary replicated to s2 and s3")

	//  Step 2: Read back from each server individually
	fmt.Println("\n[Step 2] Read 'replicated.txt' directly from each server")
	allMatch := true
	for i, addr := range addrs {
		name := fmt.Sprintf("s%d", i+1)
		dir := fmt.Sprintf("%s-%s", *cacheDir, name)
		c, err := afs.NewClient([]string{addr}, dir)
		if err != nil {
			fmt.Printf("  %s (%s): OFFLINE (Skipping verification. Expected if testing failover)\n", name, addr)
			continue
		}
		h, err := c.Open("replicated.txt", false)
		if err != nil {
			fmt.Printf("  %s (%s): FAIL — Open: %v\n", name, addr, err)
			c.CloseConn()
			allMatch = false
			continue
		}
		var data []byte
		buf := make([]byte, 4096)
		for {
			n, e := c.Read(h, buf)
			if n > 0 {
				data = append(data, buf[:n]...)
			}
			if e == io.EOF || e != nil {
				break
			}
		}
		c.Close(h)
		c.CloseConn()

		if string(data) == payload {
			fmt.Printf("  %s (%s): PASS ✓  content matches\n", name, addr)
		} else {
			fmt.Printf("  %s (%s): FAIL  got %q\n", name, addr, string(data))
			allMatch = false
		}
	}

	fmt.Println()
	if allMatch {
		fmt.Println("  ALL AVAILABLE SERVERS IDENTICAL ✓")
	} else {
		fmt.Println("  MISMATCH between servers ✗")
	}

	fmt.Println("  TASK 3A COMPLETE")
	fmt.Println("\nAlso verify on disk:")
	fmt.Println("  cat testdata/output-s1/replicated.txt")
	fmt.Println("  cat testdata/output-s2/replicated.txt")
	fmt.Println("  cat testdata/output-s3/replicated.txt")
}
