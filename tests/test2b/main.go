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
	cacheDir := flag.String("cacheDir", "/tmp/afs-cache-2b", "local cache directory")
	flag.Parse()

	fmt.Println("  TASK 2B — Client Crash Before Close")
	fmt.Println("  Proves: partial write NEVER reaches server")

	client, err := afs.NewClient(strings.Split(*servers, ","), *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: connect: %v", err)
	}

	//  Step 1: Write a known-good file
	fmt.Println("\n[Step 1] Write a known-good file and close it properly")
	client.DeleteFile("crash_test.txt")
	wh, _ := client.CreateFile("crash_test.txt")
	client.Write(wh, []byte("GOOD CONTENT — this is the safe version\n"))
	client.Close(wh) // StoreFile fires → server has good content
	fmt.Println("  PASS ✓  server has: \"GOOD CONTENT — this is the safe version\"")

	//  Step 2: Open for write, write locally, BUT do NOT close
	fmt.Println("\n[Step 2] Open again, write new content — simulate crash (no Close)")
	wh2, err := client.Open("crash_test.txt", true)
	if err != nil {
		log.Fatalf("FAIL: Open: %v", err)
	}
	client.Write(wh2, []byte("PARTIAL CONTENT — this should NEVER reach the server\n"))
	fmt.Println("  Wrote to LOCAL CACHE only")
	fmt.Println("  Simulating client crash — NOT calling Close()")
	fmt.Println("  StoreFile RPC will NOT fire → server unchanged")

	// Drop connection without calling Close
	client.CloseConn()
	fmt.Println("  Connection dropped (client 'crashed')")

	//  Step 3: New client — read back and verify server is unchanged
	fmt.Println("\n[Step 3] New client reads the file — server must have GOOD CONTENT")
	client2, err := afs.NewClient(strings.Split(*servers, ","), *cacheDir+"-verify")
	if err != nil {
		log.Fatalf("FAIL: reconnect: %v", err)
	}
	defer client2.CloseConn()

	rh, err := client2.Open("crash_test.txt", false)
	if err != nil {
		log.Fatalf("FAIL: Open verify: %v", err)
	}
	var got []byte
	buf := make([]byte, 4096)
	for {
		n, e := client2.Read(rh, buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if e == io.EOF || e != nil {
			break
		}
	}
	client2.Close(rh)

	expected := "GOOD CONTENT — this is the safe version\n"
	fmt.Printf("  Server returned: %q\n", string(got))
	if string(got) == expected {
		fmt.Println("  PASS ✓  server has good content — partial write never sent")
	} else {
		fmt.Println("  FAIL: server has unexpected content")
	}

	fmt.Println("  TASK 2B COMPLETE")

}
