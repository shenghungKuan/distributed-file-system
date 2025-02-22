package storage

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

	pb "github.com/marcus/distributed-file-system/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type StorageNode struct {
	controllerAddr string
	port          int
	storageDir    string
	nodeID        string

	listener     net.Listener
	controllerConn net.Conn

	totalRequests uint64
	mu           sync.RWMutex

	shutdown     chan struct{}
}

func NewStorageNode(controllerAddr string, port int, storageDir string) (*StorageNode, error) {
	return &StorageNode{
		controllerAddr: controllerAddr,
		port:          port,
		storageDir:    storageDir,
		nodeID:        uuid.New().String(),
		shutdown:      make(chan struct{}),
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
	conn, err := net.Dial("tcp", s.controllerAddr)
	if err != nil {
		return err
	}
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
	s.mu.RLock()
	heartbeat := &pb.Heartbeat{
		NodeId:        s.nodeID,
		Address:       fmt.Sprintf(":%d", s.port),
		FreeSpace:     s.getFreeSpace(),
		TotalRequests: s.totalRequests,
	}
	s.mu.RUnlock()

	msgBytes, err := proto.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat: %v", err)
	}

	// Send message size first
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(msgBytes)))
	if _, err := s.controllerConn.Write(sizeBuf); err != nil {
		return fmt.Errorf("failed to send message size: %v", err)
	}

	// Send message
	if _, err := s.controllerConn.Write(msgBytes); err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

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
	if _, err := conn.Read(sizeBuf); err != nil {
		log.Printf("Error reading message size: %v", err)
		return
	}
	size := binary.BigEndian.Uint32(sizeBuf)

	// Read message
	msgBuf := make([]byte, size)
	if _, err := conn.Read(msgBuf); err != nil {
		log.Printf("Error reading message: %v", err)
		return
	}

	s.mu.Lock()
	s.totalRequests++
	s.mu.Unlock()

	// TODO: Implement message type detection and handling
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
	// TODO: Implement actual disk space checking
	return 1024 * 1024 * 1024 * 100 // Return 100GB for now
}