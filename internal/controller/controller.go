package controller

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/marcus/distributed-file-system/proto"
	"google.golang.org/protobuf/proto"
)

const (
	heartbeatTimeout = 15 * time.Second
	minReplicas     = 3
)

type nodeInfo struct {
	id            string
	address       string
	freeSpace     uint64
	totalRequests uint64
	lastHeartbeat time.Time
	storedFiles   map[string]bool
}

type chunkInfo struct {
	id       string
	index    uint32
	size     uint64
	nodes    []string // Primary node first, then replicas
	checksum []byte
}

type fileInfo struct {
	name      string
	size      uint64
	numChunks uint32
	chunks    []*chunkInfo
}

type Controller struct {
	port int
	mu   sync.RWMutex

	nodes map[string]*nodeInfo  // node ID -> node info
	files map[string]*fileInfo  // filename -> file info

	shutdown chan struct{}
}

func NewController(port int) (*Controller, error) {
	return &Controller{
		port:     port,
		nodes:    make(map[string]*nodeInfo),
		files:    make(map[string]*fileInfo),
		shutdown: make(chan struct{}),
	}, nil
}

func (c *Controller) Start(listener net.Listener) {
	go c.checkHeartbeats()

	for {
		select {
		case <-c.shutdown:
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go c.handleConnection(conn)
		}
	}
}

func (c *Controller) Shutdown() {
	close(c.shutdown)
}

func (c *Controller) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read message size (4 bytes)
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

	// Handle different message types
	// TODO: Implement message type detection and handling
	c.handleHeartbeat(msgBuf)
}

func (c *Controller) handleHeartbeat(msgBytes []byte) error {
	heartbeat := &pb.Heartbeat{}
	if err := proto.Unmarshal(msgBytes, heartbeat); err != nil {
		return fmt.Errorf("failed to unmarshal heartbeat: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.nodes[heartbeat.NodeId]
	if !exists {
		// New node joining
		node = &nodeInfo{
			id:            heartbeat.NodeId,
			address:       heartbeat.Address,
			storedFiles:   make(map[string]bool),
		}
		c.nodes[heartbeat.NodeId] = node
		log.Printf("New storage node joined: %s at %s", heartbeat.NodeId, heartbeat.Address)
	}

	// Update node info
	node.freeSpace = heartbeat.FreeSpace
	node.totalRequests = heartbeat.TotalRequests
	node.lastHeartbeat = time.Now()

	return nil
}

func (c *Controller) checkHeartbeats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.shutdown:
			return
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			
			// Check for dead nodes
			for id, node := range c.nodes {
				if now.Sub(node.lastHeartbeat) > heartbeatTimeout {
					log.Printf("Node %s timed out, initiating recovery", id)
					c.handleNodeFailure(id)
					delete(c.nodes, id)
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *Controller) handleNodeFailure(nodeID string) {
	// TODO: Implement node failure recovery
	// 1. Find all chunks stored on the failed node
	// 2. For each chunk:
	//    - Check if remaining replicas meet minimum requirement
	//    - If not, create new replicas on available nodes
	//    - Update chunk location information
}

func (c *Controller) selectStorageNodes(numChunks uint32, chunkSize uint64) ([]*pb.ChunkPlacement, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.nodes) < minReplicas {
		return nil, fmt.Errorf("not enough storage nodes available (have %d, need %d)", len(c.nodes), minReplicas)
	}

	placements := make([]*pb.ChunkPlacement, numChunks)
	for i := uint32(0); i < numChunks; i++ {
		chunkID := uuid.New().String()
		
		// TODO: Implement more sophisticated node selection based on:
		// - Available space
		// - Load balancing
		// - Network topology
		// - Current chunk distribution
		
		placement := &pb.ChunkPlacement{
			ChunkId:      chunkID,
			ChunkIndex:   i,
			StorageNodes: make([]string, minReplicas),
		}
		placements[i] = placement
	}

	return placements, nil
}