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
	cacheDir := flag.String("cacheDir", "/tmp/afs-cache-3b", "local cache directory")
	flag.Parse()

	addrs := strings.Split(*servers, ",")

	fmt.Println("  TASK 3B — Primary Failover")
	fmt.Println("  Proves: write completes on new primary after s1 dies")

	//  Step 1: Kill the primary
	fmt.Println("\n[Step 1] Killing the primary server (s1) now...")
	if err := killServer(50051); err != nil {
		fmt.Printf("  Warning: could not kill s1 programmatically: %v\n", err)
		fmt.Println("  Please kill s1 manually and press ENTER...")
		fmt.Scanln()
	} else {
		fmt.Println("  Server s1 killed. Waiting 4 seconds for election...")
		time.Sleep(4 * time.Second)
	}

	//  Step 2: Connect — client must find new primary automatically
	fmt.Println("\n[Step 2] Connect — client discovers new primary via GetPrimary RPC")
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: could not find new primary: %v\n(Is at least one of s2/s3 alive?)", err)
	}
	defer client.CloseConn()
	fmt.Println("  PASS ✓  connected to new primary (election succeeded)")

	//  Step 3: Write to new primary
	fmt.Println("\n[Step 3] Write 'failover.txt' to the new primary")
	payload := "Written AFTER s1 died — failover works!\n"
	client.DeleteFile("failover.txt")
	wh, err := client.CreateFile("failover.txt")
	if err != nil {
		log.Fatalf("FAIL: CreateFile on new primary: %v", err)
	}
	client.Write(wh, []byte(payload))
	if err := client.Close(wh); err != nil {
		log.Fatalf("FAIL: Close: %v", err)
	}
	fmt.Println("  PASS ✓  write succeeded on new primary")

	//  Step 4: Read back to verify
	fmt.Println("\n[Step 4] Read back 'failover.txt' to verify")
	rh, err := client.Open("failover.txt", false)
	if err != nil {
		log.Fatalf("FAIL: Open: %v", err)
	}
	var data []byte
	buf := make([]byte, 4096)
	for {
		n, e := client.Read(rh, buf)
		if n > 0 {
			data = append(data, buf[:n]...)
		}
		if e == io.EOF || e != nil {
			break
		}
	}
	client.Close(rh)

	if string(data) == payload {
		fmt.Printf("  PASS ✓  content correct: %q\n", string(data))
	} else {
		fmt.Printf("  FAIL: got %q\n", string(data))
	}

	fmt.Println("  TASK 3B COMPLETE")
	fmt.Println("\nVerify on disk (s1 will NOT have it — it was dead):")
	fmt.Println("  cat testdata/output-s2/failover.txt")
	fmt.Println("  cat testdata/output-s3/failover.txt")
}

// killServer tries to kill the local server listening on the given port
func killServer(port int) error {
	cmd := exec.Command("sh", "-c", fmt.Sprintf("pkill -f 'server.*port %d'", port))
	return cmd.Run()
}
