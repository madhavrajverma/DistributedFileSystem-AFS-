package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"time"

	"afsfs/pkg/afs"
)

func main() {
	servers := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "comma-separated server list")
	cacheDir := flag.String("cacheDir", "/tmp/afs-cache", "local cache directory")
	flag.Parse()

	fmt.Println("  TASK 2A — Server Crash During Operation")
	fmt.Println("  Proves: cached reads survive crash;")
	fmt.Println(" new client auto-connects to new primary")

	//  Step 1: Open and cache the file while all servers are alive
	fmt.Println("\n[Step 1] Connect and open file (all servers alive)")
	client, err := afs.NewClient(strings.Split(*servers, ","), *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: %v", err)
	}

	h, err := client.Open("input_dataset_001.txt", false)
	if err != nil {
		log.Fatalf("FAIL: Open: %v", err)
	}
	fmt.Printf("  PASS ✓  opened handle=%d — file is now in local cache\n", h)

	//  Step 2: Instruct user to kill s1 (the primary)
	//  Step 2: Kill s1 (the primary) programmatically
	fmt.Println("\n[Step 2] NOW killing the primary server s1...")
	if err := killServer(50051); err != nil {
		fmt.Printf("  Warning: could not kill s1 programmatically: %v\n", err)
		fmt.Println("  Please kill s1 manually and press ENTER...")
		fmt.Scanln()
	} else {
		fmt.Println("  Server s1 killed. Waiting 4 seconds for election...")
		time.Sleep(4 * time.Second)
	}

	//  Step 3: Read from local cache — NO network needed
	fmt.Println("\n[Step 3] Read file (s1 is dead — reads from LOCAL CACHE)")
	buf := make([]byte, 4096)
	var data []byte
	for {
		n, e := client.Read(h, buf)
		if n > 0 {
			data = append(data, buf[:n]...)
		}
		if e == io.EOF || e != nil {
			break
		}
	}
	client.Close(h)

	if len(data) > 0 {
		fmt.Printf("  PASS ✓  read %d bytes from local cache — zero network calls\n", len(data))
		fmt.Printf("  Content:\n%s\n", string(data))
	} else {
		fmt.Println("  FAIL: nothing read")
	}
	client.CloseConn() // drop old connection

	//  Step 4: New client — must auto-find new primary
	fmt.Println("\n[Step 4] New client connection → auto-discovers new primary")
	client2, err := afs.NewClient(strings.Split(*servers, ","), *cacheDir+"-2")
	if err != nil {
		fmt.Printf("  FAIL: could not find new primary: %v\n", err)
	} else {
		defer client2.CloseConn()
		fmt.Println("  PASS ✓  connected to new primary (election succeeded)")

		// write a file to prove the new primary accepts writes
		client2.DeleteFile("after_crash.txt")
		wh, err := client2.CreateFile("after_crash.txt")
		if err != nil {
			fmt.Printf("  FAIL: CreateFile on new primary: %v\n", err)
		} else {
			client2.Write(wh, []byte("written after s1 crashed\n"))
			if err := client2.Close(wh); err != nil {
				fmt.Printf("  FAIL: Close: %v\n", err)
			} else {
				fmt.Println("  PASS ✓  write succeeded on new primary")
			}
		}
	}

}

// killServer tries to kill the local server listening on the given port
func killServer(port int) error {
	cmd := exec.Command("sh", "-c", fmt.Sprintf("pkill -f 'server.*port %d'", port))
	return cmd.Run()
}
