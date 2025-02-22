package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/marcus/distributed-file-system/internal/controller"
)

func main() {
	// Parse command line flags
	port := flag.Int("port", 8080, "Port to listen on")
	flag.Parse()

	// Create and start controller
	ctrl, err := controller.NewController(*port)
	if err != nil {
		log.Fatalf("Failed to create controller: %v", err)
	}

	// Start listening for connections
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	log.Printf("Controller listening on port %d", *port)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start controller in a goroutine
	go ctrl.Start(listener)

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down controller...")
	ctrl.Shutdown()
}