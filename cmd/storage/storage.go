package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	pb "dfs/proto"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type StorageNode struct {
	controllerAddr string
	port           int
	storageDir     string
	nodeID         string

	listener       net.Listener
	controllerConn net.Conn

	totalRequests uint64
	mu            sync.RWMutex

	shutdown chan struct{}
}

func NewStorageNode(controllerAddr string, port int, storageDir string) (*StorageNode, error) {
	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %v", err)
	}

	return &StorageNode{
		controllerAddr: controllerAddr,
		port:           port,
		storageDir:     storageDir,
		nodeID:         uuid.New().String(),
		shutdown:       make(chan struct{}),
	}, nil
}

func (s *StorageNode) Start() error {
	// Start listening for chunk requests
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}
	s.listener = listener

	// Connect to controller
	if err := s.connectToController(); err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}

	// Start heartbeat goroutine
	go s.sendHeartbeats()

	// Start handling connections
	go s.handleConnections()

	return nil
}

func (s *StorageNode) Shutdown() {
	close(s.shutdown)
	if s.listener != nil {
		s.listener.Close()
	}
	if s.controllerConn != nil {
		s.controllerConn.Close()
	}
}

func (s *StorageNode) connectToController() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Attempting to connect to controller at %s", s.controllerAddr)

	// Close existing connection if any
	if s.controllerConn != nil {
		log.Printf("Closing existing controller connection")
		s.controllerConn.Close()
		s.controllerConn = nil
	}

	// Create connection with keepalive
	dialer := net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	conn, err := dialer.Dial("tcp", s.controllerAddr)
	if err != nil {
		return fmt.Errorf("dial failed: %v", err)
	}

	// Enable TCP keepalive
	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		conn.Close()
		return fmt.Errorf("failed to enable keepalive: %v", err)
	}
	if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
		conn.Close()
		return fmt.Errorf("failed to set keepalive period: %v", err)
	}

	log.Printf("Successfully connected to controller")
	s.controllerConn = conn
	return nil
}

func (s *StorageNode) sendHeartbeats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdown:
			return
		case <-ticker.C:
			if err := s.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
				// Try to reconnect
				if err := s.connectToController(); err != nil {
					log.Printf("Failed to reconnect to controller: %v", err)
				}
			}
		}
	}
}

func (s *StorageNode) sendHeartbeat() error {
	s.mu.Lock() // Use full lock to prevent connection changes during send
	defer s.mu.Unlock()

	if s.controllerConn == nil {
		return fmt.Errorf("no connection to controller")
	}

	heartbeat := &pb.Heartbeat{
		NodeId:        s.nodeID,
		Address:       fmt.Sprintf(":%d", s.port),
		FreeSpace:     s.getFreeSpace(),
		TotalRequests: s.totalRequests,
	}

	msgBytes, err := proto.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat: %v", err)
	}

	// Set longer write deadline for the entire operation
	if err := s.controllerConn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		log.Printf("Failed to set write deadline: %v", err)
		s.controllerConn.Close()
		s.controllerConn = nil
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	// Send message size first
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(msgBytes)))

	log.Printf("Sending heartbeat size: %d bytes", len(msgBytes))
	if _, err := s.controllerConn.Write(sizeBuf); err != nil {
		log.Printf("Failed to send message size: %v", err)
		s.controllerConn.Close()
		s.controllerConn = nil
		return fmt.Errorf("failed to send message size: %v", err)
	}

	// Send message with a small delay to prevent overwhelming the receiver
	time.Sleep(10 * time.Millisecond)

	log.Printf("Sending heartbeat message")
	if _, err := s.controllerConn.Write(msgBytes); err != nil {
		log.Printf("Failed to send message: %v", err)
		s.controllerConn.Close()
		s.controllerConn = nil
		return fmt.Errorf("failed to send message: %v", err)
	}

	// Reset write deadline
	if err := s.controllerConn.SetWriteDeadline(time.Time{}); err != nil {
		log.Printf("Failed to reset write deadline: %v", err)
		return fmt.Errorf("failed to reset write deadline: %v", err)
	}

	log.Printf("Successfully sent heartbeat")
	return nil
}

func (s *StorageNode) handleConnections() {
	for {
		select {
		case <-s.shutdown:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *StorageNode) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read message size
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		log.Printf("Error reading message size: %v", err)
		return
	}
	size := binary.BigEndian.Uint32(sizeBuf)

	// Read message
	msgBuf := make([]byte, size)
	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		log.Printf("Error reading message: %v", err)
		return
	}

	s.mu.Lock()
	s.totalRequests++
	s.mu.Unlock()

	// Try to unmarshal as different message types
	storeReq := &pb.ChunkStoreRequest{}
	if proto.Unmarshal(msgBuf, storeReq) == nil {
		resp := s.handleChunkStore(storeReq)
		s.sendResponse(conn, resp)
		return
	}

	retrieveReq := &pb.ChunkRetrieveRequest{}
	if proto.Unmarshal(msgBuf, retrieveReq) == nil {
		resp := s.handleChunkRetrieve(retrieveReq)
		s.sendResponse(conn, resp)
		return
	}

	deleteReq := &pb.DeleteRequest{}
	if proto.Unmarshal(msgBuf, deleteReq) == nil {
		resp := s.handleChunkDelete(deleteReq)
		s.sendResponse(conn, resp)
		return
	}

	log.Printf("Unknown message type received")
}

func (s *StorageNode) sendResponse(conn net.Conn, msg proto.Message) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		return
	}

	// Send message size
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(msgBytes)))
	if _, err := conn.Write(sizeBuf); err != nil {
		log.Printf("Error sending response size: %v", err)
		return
	}

	// Send message
	if _, err := conn.Write(msgBytes); err != nil {
		log.Printf("Error sending response: %v", err)
		return
	}
}

func (s *StorageNode) handleChunkStore(req *pb.ChunkStoreRequest) *pb.ChunkStoreResponse {
	// Store the chunk locally
	if err := s.storeChunk(req.ChunkId, req.Data); err != nil {
		return &pb.ChunkStoreResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	// Forward to replica nodes if any
	for _, nodeAddr := range req.ReplicaNodes {
		if err := s.forwardChunk(nodeAddr, req); err != nil {
			log.Printf("Failed to forward chunk to %s: %v", nodeAddr, err)
		}
	}

	return &pb.ChunkStoreResponse{
		Success: true,
	}
}

func (s *StorageNode) handleChunkRetrieve(req *pb.ChunkRetrieveRequest) *pb.ChunkRetrieveResponse {
	data, checksum, err := s.retrieveChunk(req.ChunkId)
	if err != nil {
		// Since ChunkRetrieveResponse doesn't have an error field,
		// we just return corrupted data
		return &pb.ChunkRetrieveResponse{
			Corrupted: true,
		}
	}

	return &pb.ChunkRetrieveResponse{
		Data:      data,
		Checksum:  checksum,
		Corrupted: false,
	}
}

func (s *StorageNode) handleChunkDelete(req *pb.DeleteRequest) *pb.DeleteResponse {
	// For delete requests, the filename is the chunk ID
	path := filepath.Join(s.storageDir, req.Filename)
	if err := os.Remove(path); err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &pb.DeleteResponse{
		Success: true,
	}
}

func (s *StorageNode) forwardChunk(nodeAddr string, req *pb.ChunkStoreRequest) error {
	// Remove the current node from replica list to prevent infinite forwarding
	replicaNodes := make([]string, 0)
	for _, addr := range req.ReplicaNodes {
		if addr != nodeAddr {
			replicaNodes = append(replicaNodes, addr)
		}
	}

	forwardReq := &pb.ChunkStoreRequest{
		ChunkId:      req.ChunkId,
		ChunkIndex:   req.ChunkIndex,
		Data:         req.Data,
		ReplicaNodes: replicaNodes,
	}

	// Connect to replica node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to replica node: %v", err)
	}
	defer conn.Close()

	// Send request
	msgBytes, err := proto.Marshal(forwardReq)
	if err != nil {
		return fmt.Errorf("failed to marshal forward request: %v", err)
	}

	// Send message size
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(msgBytes)))
	if _, err := conn.Write(sizeBuf); err != nil {
		return fmt.Errorf("failed to send forward request size: %v", err)
	}

	// Send message
	if _, err := conn.Write(msgBytes); err != nil {
		return fmt.Errorf("failed to send forward request: %v", err)
	}

	// Read response
	resp := &pb.ChunkStoreResponse{}
	if err := s.receiveMessage(conn, resp); err != nil {
		return fmt.Errorf("failed to receive forward response: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("forward failed: %s", resp.Error)
	}

	return nil
}

func (s *StorageNode) receiveMessage(conn net.Conn, msg proto.Message) error {
	// Read message size
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return fmt.Errorf("failed to read message size: %v", err)
	}
	size := binary.BigEndian.Uint32(sizeBuf)

	// Read message
	msgBuf := make([]byte, size)
	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		return fmt.Errorf("failed to read message: %v", err)
	}

	if err := proto.Unmarshal(msgBuf, msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %v", err)
	}

	return nil
}

func (s *StorageNode) storeChunk(chunkID string, data []byte) error {
	path := filepath.Join(s.storageDir, chunkID)

	// Calculate checksum
	checksum := sha256.Sum256(data)

	// Create chunk file
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create chunk file: %v", err)
	}
	defer file.Close()

	// Write checksum first (32 bytes)
	if _, err := file.Write(checksum[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %v", err)
	}

	// Write data
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}

func (s *StorageNode) retrieveChunk(chunkID string) ([]byte, []byte, error) {
	path := filepath.Join(s.storageDir, chunkID)

	// Open chunk file
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open chunk file: %v", err)
	}
	defer file.Close()

	// Read checksum (32 bytes)
	storedChecksum := make([]byte, 32)
	if _, err := io.ReadFull(file, storedChecksum); err != nil {
		return nil, nil, fmt.Errorf("failed to read checksum: %v", err)
	}

	// Read data
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read data: %v", err)
	}

	// Verify checksum
	checksum := sha256.Sum256(data)
	if string(checksum[:]) != string(storedChecksum) {
		return data, storedChecksum, fmt.Errorf("chunk corrupted")
	}

	return data, storedChecksum, nil
}

func (s *StorageNode) getFreeSpace() uint64 {
	var stat unix.Statfs_t
	if err := unix.Statfs(s.storageDir, &stat); err != nil {
		log.Printf("Error getting disk space: %v", err)
		return 0
	}
	return stat.Bavail * uint64(stat.Bsize)
}
