# Distributed File System

A distributed file system (DFS) implementation in Go that supports parallel storage/retrieval, fault tolerance, and file integrity verification.

## Features

- Parallel storage and retrieval of large files
- File chunking with configurable chunk sizes
- 3x replication for fault tolerance
- Corruption detection and repair using checksums
- Protocol Buffers for message serialization
- Interactive client interface
- Storage node failure detection and recovery
- Load balancing across storage nodes

## Components

### Controller
- Manages system resources and metadata
- Tracks active storage nodes
- Handles chunk placement and replication
- Monitors node health through heartbeats
- Coordinates failure recovery

### Storage Node
- Stores and retrieves file chunks
- Verifies chunk integrity using checksums
- Participates in chunk replication
- Reports health status to controller
- Manages local storage

### Client
- Splits files into chunks for storage
- Parallel upload/download of chunks
- Communicates with controller for metadata
- Provides command-line interface
- Supports basic file operations (store, retrieve, delete, list)

## Building

1. Install Go 1.21 or later
2. Install Protocol Buffers compiler:
   ```bash
   # macOS
   brew install protobuf
   
   # Ubuntu/Debian
   sudo apt-get install protobuf-compiler
   ```

3. Install Go Protocol Buffers plugin:
   ```bash
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   ```

4. Set the path:
   ```bash
   PATH="$PATH:${GOPATH}/bin:${HOME}/go/bin"
   ```

5. Generate Protocol Buffer code:
   ```bash
   protoc --go_out=. proto/dfs.proto
   ```

6. Build the binary for each:
   ```bash
   sh build.sh
   ```

## Running

1. Start the controller:
   ```bash
   ./bin/controller [-port 8080]
   ```

2. Start multiple storage nodes:
   ```bash
   # Storage node 1
   ./bin/storage [-controller localhost:8080] -port 8081 -dir data/node1  
   
   # Storage node 2
   ./bin/storage [-controller localhost:8080] -port 8082 -dir data/node2  
   
   # Storage node 3
   ./bin/storage [-controller localhost:8080] -port 8083 -dir data/node3  
   ```

3. Run the client:
   ```bash
   ./bin/client [-controller localhost:8080]
   ```

## Client Commands

- `store <local_file> [chunk_size_mb]` - Store a file with optional chunk size in MB (default: 64MB)
- `retrieve <filename> <local_path>` - Retrieve a file <filename> from nodes and store it into <local_path>
- `delete <filename>` - Delete a file
- `list` - List all stored files
- `nodes` - List active storage nodes
- `help` - Show available commands
- `exit` - Exit the client

## Example Usage

```bash
# Store a file with 32MB chunks
> store large_file.dat 32
File stored successfully

# List stored files
> list
Stored files:
  large_file.dat (1073741824 bytes, 32 chunks)

# Check active nodes
> nodes
Active storage nodes:
  node1 at localhost:8081
    Free space: 95 GB
    Total requests: 42
  node2 at localhost:8082
    Free space: 98 GB
    Total requests: 38
  node3 at localhost:8083
    Free space: 97 GB
    Total requests: 40

# Retrieve the file
> retrieve large_file.dat retrieved_file.dat
File retrieved successfully
```

## Fault Tolerance

The system maintains three copies of each chunk across different storage nodes. If a storage node fails:

1. The controller detects the failure through missed heartbeats
2. Remaining replicas are used for file operations
3. New replicas are created on other nodes to maintain 3x replication
4. The system can handle up to two simultaneous node failures

If a chunk is corrupted:

1. The corruption is detected using checksums
2. A valid replica is used to serve the request
3. The corrupted chunk is repaired using a valid replica

## Implementation Details

- Uses TCP sockets for all communication
- Protocol Buffers ensure backward compatibility
- Heartbeats every 5 seconds from storage nodes
- 15-second timeout for node failure detection
- SHA-256 checksums for chunk integrity
- Parallel chunk transfer for improved performance
- Pipeline replication for efficient chunk distribution

## Limitations

- No authentication/authorization
- No encryption of data in transit or at rest
- Basic load balancing (round-robin)
- Simple replication strategy
- No support for file modifications (append/update)
- No automatic scaling