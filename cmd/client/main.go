package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/marcus/distributed-file-system/internal/client"
)

func printUsage() {
	fmt.Println("Available commands:")
	fmt.Println("  store <local_file> [chunk_size_mb]  - Store a file (optional chunk size in MB)")
	fmt.Println("  retrieve <filename> <local_path>    - Retrieve a file")
	fmt.Println("  delete <filename>                   - Delete a file")
	fmt.Println("  list                               - List all files")
	fmt.Println("  nodes                              - List active storage nodes")
	fmt.Println("  help                               - Show this help")
	fmt.Println("  exit                               - Exit the client")
}

func main() {
	// Parse command line flags
	controllerAddr := flag.String("controller", "localhost:8080", "Controller address (host:port)")
	flag.Parse()

	// Create client
	c, err := client.NewClient(*controllerAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	fmt.Println("DFS Client")
	fmt.Println("Type 'help' for available commands")

	// Start interactive command loop
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		cmd := scanner.Text()
		args := strings.Fields(cmd)
		if len(args) == 0 {
			continue
		}

		switch args[0] {
		case "store":
			if len(args) < 2 {
				fmt.Println("Usage: store <local_file> [chunk_size_mb]")
				continue
			}
			chunkSize := uint32(64) // Default 64MB chunks
			if len(args) > 2 {
				size, err := strconv.ParseUint(args[2], 10, 32)
				if err != nil {
					fmt.Printf("Invalid chunk size: %v\n", err)
					continue
				}
				chunkSize = uint32(size)
			}
			if err := c.StoreFile(args[1], chunkSize); err != nil {
				fmt.Printf("Error storing file: %v\n", err)
			} else {
				fmt.Println("File stored successfully")
			}

		case "retrieve":
			if len(args) != 3 {
				fmt.Println("Usage: retrieve <filename> <local_path>")
				continue
			}
			if err := c.RetrieveFile(args[1], args[2]); err != nil {
				fmt.Printf("Error retrieving file: %v\n", err)
			} else {
				fmt.Println("File retrieved successfully")
			}

		case "delete":
			if len(args) != 2 {
				fmt.Println("Usage: delete <filename>")
				continue
			}
			if err := c.DeleteFile(args[1]); err != nil {
				fmt.Printf("Error deleting file: %v\n", err)
			} else {
				fmt.Println("File deleted successfully")
			}

		case "list":
			files, err := c.ListFiles()
			if err != nil {
				fmt.Printf("Error listing files: %v\n", err)
				continue
			}
			if len(files) == 0 {
				fmt.Println("No files stored")
				continue
			}
			fmt.Println("Stored files:")
			for _, file := range files {
				fmt.Printf("  %s (%d bytes, %d chunks)\n", 
					file.Name, file.Size, file.NumChunks)
			}

		case "nodes":
			nodes, err := c.ListNodes()
			if err != nil {
				fmt.Printf("Error listing nodes: %v\n", err)
				continue
			}
			if len(nodes) == 0 {
				fmt.Println("No active storage nodes")
				continue
			}
			fmt.Println("Active storage nodes:")
			for _, node := range nodes {
				fmt.Printf("  %s at %s\n    Free space: %d GB\n    Total requests: %d\n",
					node.ID, node.Address, node.FreeSpace/(1024*1024*1024), node.TotalRequests)
			}

		case "help":
			printUsage()

		case "exit":
			return

		default:
			fmt.Printf("Unknown command: %s\n", args[0])
			printUsage()
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading input: %v", err)
	}
}