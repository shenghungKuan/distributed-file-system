package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "dfs/proto"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const (
	heartbeatTimeout = 15 * time.Second
	minReplicas      = 3
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
	id    string
	index uint32
	size  uint64
	nodes []string // Primary node first, then replicas
	// checksum []byte
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

	nodes map[string]*nodeInfo // node ID -> node info
	files map[string]*fileInfo // filename -> file info

	storageConns map[string]net.Conn // node ID -> persistent connection
	connMu       sync.RWMutex        // separate mutex for connection management

	shutdown chan struct{}
}

func NewController(port int) (*Controller, error) {
	return &Controller{
		port:         port,
		nodes:        make(map[string]*nodeInfo),
		files:        make(map[string]*fileInfo),
		storageConns: make(map[string]net.Conn),
		shutdown:     make(chan struct{}),
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

			// Read first message to determine connection type
			msgType, msgBuf, err := c.readMessage(conn)
			if err != nil {
				log.Printf("Error reading initial message: %v", err)
				conn.Close()
				continue
			}

			if msgType == "heartbeat" {
				// Storage node connection
				log.Printf("Storage node connected from %s", conn.RemoteAddr())
				go c.handleStorageNodeConnection(conn, msgBuf)
			} else {
				// Client connection
				log.Printf("Client connected from %s", conn.RemoteAddr())
				go c.handleClientConnection(conn, msgType, msgBuf)
			}
		}
	}
}

func (c *Controller) Shutdown() {
	close(c.shutdown)

	// Close all storage node connections
	c.connMu.Lock()
	for _, conn := range c.storageConns {
		conn.Close()
	}
	c.storageConns = make(map[string]net.Conn)
	c.connMu.Unlock()
}

func (c *Controller) readMessage(conn net.Conn) (string, []byte, error) {
	// Read message size
	sizeBuf := make([]byte, 4)
	if _, err := conn.Read(sizeBuf); err != nil {
		return "", nil, fmt.Errorf("error reading message size: %v", err)
	}
	size := binary.BigEndian.Uint32(sizeBuf)

	// Read message
	msgBuf := make([]byte, size)
	if _, err := conn.Read(msgBuf); err != nil {
		return "", nil, fmt.Errorf("error reading message: %v", err)
	}

	// Try to identify message type by attempting to unmarshal into each type
	// and verifying the content is valid

	// Try heartbeat first since it's most common
	heartbeat := &pb.Heartbeat{}
	if err := proto.Unmarshal(msgBuf, heartbeat); err == nil {
		if heartbeat.NodeId != "" { // Verify it's a valid heartbeat
			log.Printf("Controller: heartbeat from node %s", heartbeat.NodeId)
			return "heartbeat", msgBuf, nil
		}
	}

	// Try store request
	storeReq := &pb.StoreRequest{}
	if err := proto.Unmarshal(msgBuf, storeReq); err == nil {
		if storeReq.Filename != "" && storeReq.NumChunks > 0 { // Verify it's a valid store request
			log.Printf("Controller: store request for file %s", storeReq.Filename)
			return "store", msgBuf, nil
		}
	}

	// Try retrieve request
	retrieveReq := &pb.RetrieveRequest{}
	if err := proto.Unmarshal(msgBuf, retrieveReq); err == nil {
		if retrieveReq.Filename != "" { // Verify it's a valid retrieve request
			log.Printf("Controller: retrieve request for file %s", retrieveReq.Filename)
			return "retrieve", msgBuf, nil
		}
	}

	// Try delete request
	deleteReq := &pb.DeleteRequest{}
	if err := proto.Unmarshal(msgBuf, deleteReq); err == nil {
		if deleteReq.Filename != "" { // Verify it's a valid delete request
			log.Printf("Controller: delete request for file %s", deleteReq.Filename)
			return "delete", msgBuf, nil
		}
	}

	// Try list request
	listReq := &pb.ListRequest{}
	if err := proto.Unmarshal(msgBuf, listReq); err == nil {
		if listReq.ListRequest {
			log.Printf("Controller: list request")
			return "list", msgBuf, nil
		}
	}

	// Try node status request
	statusReq := &pb.NodeStatusRequest{}
	if err := proto.Unmarshal(msgBuf, statusReq); err == nil {
		if statusReq.NodeStatus {
			log.Printf("Controller: status request")
			return "status", msgBuf, nil
		}
	}

	log.Printf("Controller: unknown message type")
	return "unknown", msgBuf, nil
}

func (c *Controller) handleStorageNodeConnection(conn net.Conn, initialMsg []byte) {
	// Handle initial heartbeat
	heartbeat := &pb.Heartbeat{}
	if err := proto.Unmarshal(initialMsg, heartbeat); err != nil {
		log.Printf("Error unmarshaling initial heartbeat: %v", err)
		conn.Close()
		return
	}

	// Store the connection
	c.connMu.Lock()
	if oldConn, exists := c.storageConns[heartbeat.NodeId]; exists {
		log.Printf("Closing old connection for node %s", heartbeat.NodeId)
		oldConn.Close()
	}
	c.storageConns[heartbeat.NodeId] = conn
	c.connMu.Unlock()

	// Handle the heartbeat
	if err := c.handleHeartbeat(heartbeat); err != nil {
		log.Printf("Error handling initial heartbeat: %v", err)
		conn.Close()
		return
	}

	// Set TCP keepalive
	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		log.Printf("Error setting keepalive: %v", err)
	}
	if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
		log.Printf("Error setting keepalive period: %v", err)
	}

	defer func() {
		c.connMu.Lock()
		delete(c.storageConns, heartbeat.NodeId)
		c.connMu.Unlock()
		conn.Close()
	}()

	// Continue reading heartbeats
	for {
		msgType, msgBuf, err := c.readMessage(conn)
		if err != nil {
			log.Printf("Error reading from storage node %s: %v", heartbeat.NodeId, err)
			return
		}

		if msgType != "heartbeat" {
			log.Printf("Unexpected message type from storage node: %s", msgType)
			continue
		}

		heartbeat := &pb.Heartbeat{}
		if err := proto.Unmarshal(msgBuf, heartbeat); err != nil {
			log.Printf("Error unmarshaling heartbeat: %v", err)
			continue
		}

		if err := c.handleHeartbeat(heartbeat); err != nil {
			log.Printf("Error handling heartbeat: %v", err)
			return
		}
	}
}

func (c *Controller) handleClientConnection(conn net.Conn, msgType string, msgBuf []byte) {
	defer conn.Close()

	var resp proto.Message
	switch msgType {
	case "store":
		req := &pb.StoreRequest{}
		proto.Unmarshal(msgBuf, req)
		resp = c.handleStoreRequest(req)
	case "retrieve":
		req := &pb.RetrieveRequest{}
		proto.Unmarshal(msgBuf, req)
		resp = c.handleRetrieveRequest(req)
	case "delete":
		req := &pb.DeleteRequest{}
		proto.Unmarshal(msgBuf, req)
		resp = c.handleDeleteRequest(req)
	case "list":
		req := &pb.ListRequest{}
		proto.Unmarshal(msgBuf, req)
		resp = c.handleListRequest(req)
	case "status":
		req := &pb.NodeStatusRequest{}
		proto.Unmarshal(msgBuf, req)
		resp = c.handleNodeStatusRequest(req)
	default:
		log.Printf("Unknown message type from client: %s", msgType)
		return
	}

	if resp != nil {
		c.sendResponse(conn, resp)
	}
}

func (c *Controller) sendResponse(conn net.Conn, msg proto.Message) {
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

func (c *Controller) handleHeartbeat(heartbeat *pb.Heartbeat) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.nodes[heartbeat.NodeId]
	if !exists {
		// New node joining
		node = &nodeInfo{
			id:          heartbeat.NodeId,
			address:     heartbeat.Address,
			storedFiles: make(map[string]bool),
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

func (c *Controller) handleStoreRequest(req *pb.StoreRequest) *pb.StoreResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if file already exists
	if _, exists := c.files[req.Filename]; exists {
		// Since StoreResponse doesn't have an error field, we just return empty placements
		return &pb.StoreResponse{
			ChunkPlacements: nil,
		}
	}

	// Select storage nodes for chunks
	placements, err := c.selectStorageNodes(req.NumChunks, uint64(req.ChunkSize))
	if err != nil {
		// Since StoreResponse doesn't have an error field, we just return empty placements
		return &pb.StoreResponse{
			ChunkPlacements: nil,
		}
	}

	// Create file info
	file := &fileInfo{
		name:      req.Filename,
		size:      req.TotalSize,
		numChunks: req.NumChunks,
		chunks:    make([]*chunkInfo, req.NumChunks),
	}

	// Initialize chunk info
	for i, placement := range placements {
		file.chunks[i] = &chunkInfo{
			id:    placement.ChunkId,
			index: uint32(i),
			size:  uint64(req.ChunkSize),
			nodes: placement.StorageNodes,
		}
	}

	c.files[req.Filename] = file

	return &pb.StoreResponse{
		ChunkPlacements: placements,
	}
}

func (c *Controller) handleRetrieveRequest(req *pb.RetrieveRequest) *pb.RetrieveResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	file, exists := c.files[req.Filename]
	if !exists {
		// Since RetrieveResponse doesn't have an error field, we just return empty response
		return &pb.RetrieveResponse{
			ChunkPlacements: nil,
			TotalSize:       0,
			ChunkSize:       0,
		}
	}

	// Create chunk placements
	placements := make([]*pb.ChunkPlacement, len(file.chunks))
	for i, chunk := range file.chunks {
		placements[i] = &pb.ChunkPlacement{
			ChunkId:      chunk.id,
			ChunkIndex:   chunk.index,
			StorageNodes: chunk.nodes,
		}
	}

	return &pb.RetrieveResponse{
		ChunkPlacements: placements,
	}
}

func (c *Controller) handleDeleteRequest(req *pb.DeleteRequest) *pb.DeleteResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	file, exists := c.files[req.Filename]
	if !exists {
		return &pb.DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("file %s not found", req.Filename),
		}
	}

	// Remove file from nodes' stored files
	for _, chunk := range file.chunks {
		for _, nodeID := range chunk.nodes {
			if node, exists := c.nodes[nodeID]; exists {
				delete(node.storedFiles, chunk.id)
			}
		}
	}

	// Remove file info
	delete(c.files, req.Filename)

	return &pb.DeleteResponse{
		Success: true,
	}
}

func (c *Controller) handleListRequest(req *pb.ListRequest) *pb.ListResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	files := make([]*pb.FileInfo, 0, len(c.files))
	for _, file := range c.files {
		files = append(files, &pb.FileInfo{
			Filename:  file.name,
			Size:      file.size,
			NumChunks: file.numChunks,
		})
	}

	return &pb.ListResponse{
		Files: files,
	}
}

func (c *Controller) handleNodeStatusRequest(req *pb.NodeStatusRequest) *pb.NodeStatusResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*pb.NodeInfo, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, &pb.NodeInfo{
			NodeId:        node.id,
			Address:       node.address,
			FreeSpace:     node.freeSpace,
			TotalRequests: node.totalRequests,
		})
		log.Printf("Controller: Node Info: %v", node.id)
	}

	return &pb.NodeStatusResponse{
		Nodes: nodes,
	}
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
	// Find all chunks stored on the failed node
	for _, file := range c.files {
		for _, chunk := range file.chunks {
			// Check if the failed node was storing this chunk
			for i, node := range chunk.nodes {
				if node == nodeID {
					// Remove failed node from chunk's node list
					chunk.nodes = append(chunk.nodes[:i], chunk.nodes[i+1:]...)

					// If remaining replicas are below minimum, select new node
					if len(chunk.nodes) < minReplicas {
						// Find a new node to store the chunk
						var newNode string
						var maxFreeSpace uint64
						for id, node := range c.nodes {
							if !contains(chunk.nodes, id) && node.freeSpace > maxFreeSpace {
								newNode = id
								maxFreeSpace = node.freeSpace
							}
						}

						if newNode != "" {
							chunk.nodes = append(chunk.nodes, newNode)
							// Note: In a real implementation, we would also need to:
							// 1. Copy the chunk data to the new node
							// 2. Verify the copy was successful
							// 3. Update the node's stored files map
						}
					}
					break
				}
			}
		}
	}
}

func (c *Controller) selectStorageNodes(numChunks uint32, chunkSize uint64) ([]*pb.ChunkPlacement, error) {
	if len(c.nodes) < minReplicas {
		return nil, fmt.Errorf("not enough storage nodes available (have %d, need %d)", len(c.nodes), minReplicas)
	}

	placements := make([]*pb.ChunkPlacement, numChunks)
	for i := uint32(0); i < numChunks; i++ {
		chunkID := uuid.New().String()

		// Select nodes based on available space and load
		selectedNodes := make([]string, 0, minReplicas)
		nodeScores := make(map[string]float64)

		// Calculate scores for each node
		for id, node := range c.nodes {
			// Score based on free space (normalized to 0-1)
			spaceScore := float64(node.freeSpace) / float64(1024*1024*1024*1000) // Normalize to 1TB

			// Score based on load (inverse of total requests, normalized to 0-1)
			loadScore := 1.0 / (1.0 + float64(node.totalRequests)/1000.0)

			// Combined score (you can adjust weights as needed)
			nodeScores[id] = 0.7*spaceScore + 0.3*loadScore
		}

		// Select top nodes by score
		for len(selectedNodes) < minReplicas {
			var bestNode string
			var bestScore float64
			for id, score := range nodeScores {
				if score > bestScore && !contains(selectedNodes, id) {
					bestNode = id
					bestScore = score
				}
			}
			selectedNodes = append(selectedNodes, bestNode)
			delete(nodeScores, bestNode)
		}

		placements[i] = &pb.ChunkPlacement{
			ChunkId:      chunkID,
			ChunkIndex:   i,
			StorageNodes: selectedNodes,
		}
	}

	return placements, nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
