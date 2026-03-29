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

	fmt.Println("  TASK 1A — Basic RPC")

	client, err := afs.NewClient(strings.Split(*servers, ","), *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect — %v", err)
	}
	defer client.CloseConn()

	//  Test 1A-1: Open a file → get a handle
	fmt.Println("\n[Test 1A-1] Open a file → get a handle")
	h, err := client.Open("input_dataset_001.txt", false)
	if err != nil {
		fmt.Printf("  FAIL: Open returned error: %v\n", err)
	} else {
		fmt.Printf("  PASS ✓  handle = %d\n", h)
	}

	//  Test 1A-2: Read bytes from the open file
	fmt.Println("\n[Test 1A-2] Read bytes from open file")
	buf := make([]byte, 4096)
	var content []byte
	for {
		n, err := client.Read(h, buf)
		if n > 0 {
			content = append(content, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("  FAIL: Read error: %v\n", err)
			break
		}
	}
	if len(content) > 0 {
		fmt.Printf("  PASS ✓  read %d bytes\n", len(content))
		fmt.Printf("  Contents:\n%s\n", string(content))
	} else {
		fmt.Println("  FAIL: no bytes read")
	}
	client.Close(h)

	//  Test 1A-3: Create a new file
	fmt.Println("\n[Test 1A-3] Create a new file")
	client.DeleteFile("task1a_output.txt") // clean up if exists
	wh, err := client.CreateFile("task1a_output.txt")
	if err != nil {
		fmt.Printf("  FAIL: CreateFile error: %v\n", err)
	} else {
		fmt.Printf("  PASS ✓  file created, handle = %d\n", wh)
	}

	//  Test 1A-4: Write bytes to the new file
	fmt.Println("\n[Test 1A-4] Write bytes to new file")
	payload := "Hello from AFS client!\nPart 1A write test.\n"
	n, err := client.Write(wh, []byte(payload))
	if err != nil {
		fmt.Printf("  FAIL: Write error: %v\n", err)
	} else {
		fmt.Printf("  PASS ✓  wrote %d bytes to local cache\n", n)
		fmt.Println("  (data held locally until Close)")
	}

	//  Test 1A-5: Close → flush to server
	fmt.Println("\n[Test 1A-5] Close file → verify flush to server")
	if err := client.Close(wh); err != nil {
		fmt.Printf("  FAIL: Close error: %v\n", err)
	} else {
		fmt.Println("  StoreFile RPC fired → data sent to primary → replicated to all backups")

		// Read it back with a fresh open to confirm
		rh, err := client.Open("task1a_output.txt", false)
		if err != nil {
			fmt.Printf("  FAIL: re-open error: %v\n", err)
		} else {
			var back []byte
			b2 := make([]byte, 4096)
			for {
				n, e := client.Read(rh, b2)
				if n > 0 {
					back = append(back, b2[:n]...)
				}
				if e == io.EOF || e != nil {
					break
				}
			}
			client.Close(rh)
			if string(back) == payload {
				fmt.Println("  PASS ✓  readback matches written content")
			} else {
				fmt.Printf("  FAIL: mismatch — got %q\n", string(back))
			}
		}
	}

	fmt.Println("  TASK 1A COMPLETE")
	fmt.Println("\nVerify on each server disk:")
	fmt.Println("  cat testdata/output-s1/task1a_output.txt")
	fmt.Println("  cat testdata/output-s2/task1a_output.txt")
	fmt.Println("  cat testdata/output-s3/task1a_output.txt")
}
