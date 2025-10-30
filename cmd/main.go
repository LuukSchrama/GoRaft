package main

import (
	"Rafting/node"
	"log"
	"os"
	"time"
)

func main() {
	nodeID := os.Args[1]
	configPath := os.Args[2]
	httpPort := 8080

	n, err := node.LoadClusterConfig(configPath, nodeID)
	if err != nil {
		log.Fatalf("Error loading cluster config: %v", err)
	}

	go func() {
		if err := n.StartGRPCServer(); err != nil {
			log.Fatalf("Error starting GRPC server: %v", err)
		}
	}()
	log.Printf("Starting node %s", nodeID)

	go n.StartKVHTTPServer(httpPort)

	time.Sleep(5 * time.Second)

	err = n.ConnectToPeers()
	if err != nil {
		log.Fatalf("Error connecting to peers: %v", err)
	}
	log.Printf("Connected to peers")

	go n.RunElectionTimer()

	select {}
}
