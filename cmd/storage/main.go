package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/shenghungKuan/distributed-file-system/internal/storage"
)

func main() {
	// Parse command line flags
	controllerAddr := flag.String("controller", "localhost:8080", "Controller address (host:port)")
	port := flag.Int("port", 8081, "Port to listen on")
	storageDir := flag.String("dir", "", "Directory to store chunks")
	flag.Parse()

	if *storageDir == "" {
		log.Fatal("Storage directory (-dir) must be specified")
	}

	// Create storage directory if it doesn't exist
	storageDir, err := filepath.Abs(*storageDir)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}
	
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	// Create and start storage node
	node, err := storage.NewStorageNode(*controllerAddr, *port, storageDir)
	if err != nil {
		log.Fatalf("Failed to create storage node: %v", err)
	}

	// Start the storage node
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start storage node: %v", err)
	}

	log.Printf("Storage node started. Listening on port %d, storing chunks in %s", *port, storageDir)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down storage node...")
	node.Shutdown()
}