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

	addrs := strings.Split(*servers, ",")

	fmt.Println("  TASK 3C — Recovery and State Sync")
	fmt.Println("  Proves: dead server syncs all missed files on restart")
	fmt.Println()
	fmt.Println("  Run this IMMEDIATELY AFTER Test 3B.")
	fmt.Println("  s1 should still be dead. s2 or s3 is the current primary.")

	//  Step 1: Bring s1 back
	fmt.Println("\n[Step 1] Restarting s1 as a backup (not primary)...")
	if err := restartS1(); err != nil {
		fmt.Printf("  Warning: could not restart s1 programmatically: %v\n", err)
		fmt.Println("  Please restart s1 manually and press ENTER...")
		fmt.Scanln()
	} else {
		fmt.Println("  Server s1 restarted. Waiting 5 seconds for it to sync missing files...")
		time.Sleep(5 * time.Second)
	}

	//  Step 2: Write a new file — now all 3 should be alive
	fmt.Println("\n[Step 2] Write 'recovery.txt' — all 3 servers should receive it")
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: connect: %v", err)
	}
	defer client.CloseConn()

	payload := "Written after s1 recovered — all 3 servers must have this\n"
	client.DeleteFile("recovery.txt")
	wh, err := client.CreateFile("recovery.txt")
	if err != nil {
		log.Fatalf("FAIL: CreateFile: %v", err)
	}
	client.Write(wh, []byte(payload))
	if err := client.Close(wh); err != nil {
		log.Fatalf("FAIL: Close: %v", err)
	}
	fmt.Println("  PASS ✓  recovery.txt written and replicated")

	//  Step 3: Read from each server — verify all 3 have BOTH files
	fmt.Println("\n[Step 3] Read from each server — must have BOTH recovery.txt AND failover.txt")
	for i, addr := range addrs {
		name := fmt.Sprintf("s%d", i+1)
		dir := fmt.Sprintf("%s-%s", *cacheDir, name)

		c, err := afs.NewClient([]string{addr}, dir)
		if err != nil {
			fmt.Printf("  %s (%s): FAIL — cannot connect: %v\n", name, addr, err)
			continue
		}

		// check recovery.txt
		h, err := c.Open("recovery.txt", false)
		if err != nil {
			fmt.Printf("  %s (%s): recovery.txt — FAIL: %v\n", name, addr, err)
		} else {
			var d []byte
			b := make([]byte, 4096)
			for {
				n, e := c.Read(h, b)
				if n > 0 {
					d = append(d, b[:n]...)
				}
				if e == io.EOF || e != nil {
					break
				}
			}
			c.Close(h)
			if string(d) == payload {
				fmt.Printf("  %s (%s): recovery.txt — PASS ✓\n", name, addr)
			} else {
				fmt.Printf("  %s (%s): recovery.txt — FAIL (got %q)\n", name, addr, string(d))
			}
		}

		// check failover.txt (written while s1 was dead — s1 must have synced it)
		h2, err := c.Open("failover.txt", false)
		if err != nil {
			fmt.Printf("  %s (%s): failover.txt  — FAIL: %v\n", name, addr, err)
		} else {
			var d2 []byte
			b2 := make([]byte, 4096)
			for {
				n, e := c.Read(h2, b2)
				if n > 0 {
					d2 = append(d2, b2[:n]...)
				}
				if e == io.EOF || e != nil {
					break
				}
			}
			c.Close(h2)
			if len(d2) > 0 {
				fmt.Printf("  %s (%s): failover.txt  — PASS ✓  (synced while s1 was dead)\n", name, addr)
			} else {
				fmt.Printf("  %s (%s): failover.txt  — FAIL (empty — sync did not work)\n", name, addr)
			}
		}

		c.CloseConn()
		fmt.Println()
	}

	fmt.Println("  TASK 3C COMPLETE")
	fmt.Println("\nVerify manually:")
	fmt.Println("  cat testdata/output-s1/failover.txt   ← s1 synced this while it was dead")
	fmt.Println("  cat testdata/output-s1/recovery.txt")
}

// restartS1 restarts s1 as a backup
func restartS1() error {
	cmd := exec.Command("sh", "-c", "./bin/server -id s1 -host localhost -port 50051 -primary=false -peers s2=localhost:50052,s3=localhost:50053 -inputDir ./testdata/input -outputDir ./testdata/output-s1 > /tmp/s1_recovered.log 2>&1 &")
	return cmd.Run()
}
