package client

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/shenghungKuan/distributed-file-system/proto"
	"google.golang.org/protobuf/proto"
)

type FileInfo struct {
	Name      string
	Size      uint64
	NumChunks uint32
}

type NodeInfo struct {
	ID            string
	Address       string
	FreeSpace     uint64
	TotalRequests uint64
}

type Client struct {
	controllerAddr string
}

func NewClient(controllerAddr string) (*Client, error) {
	return &Client{
		controllerAddr: controllerAddr,
	}, nil
}

func (c *Client) StoreFile(localPath string, chunkSizeMB uint32) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}

	chunkSize := chunkSizeMB * 1024 * 1024 // Convert MB to bytes
	numChunks := uint32((fileInfo.Size() + int64(chunkSize) - 1) / int64(chunkSize))

	// Request chunk placements from controller
	placements, err := c.requestChunkPlacements(filepath.Base(localPath), uint64(fileInfo.Size()), chunkSize, numChunks)
	if err != nil {
		return fmt.Errorf("failed to get chunk placements: %v", err)
	}

	// Store chunks in parallel
	var wg sync.WaitGroup
	errors := make(chan error, numChunks)

	for i := uint32(0); i < numChunks; i++ {
		wg.Add(1)
		go func(chunkIndex uint32) {
			defer wg.Done()

			// Calculate chunk size for this chunk
			start := int64(chunkIndex) * int64(chunkSize)
			size := int64(chunkSize)
			if start+size > fileInfo.Size() {
				size = fileInfo.Size() - start
			}

			// Read chunk data
			data := make([]byte, size)
			if _, err := file.ReadAt(data, start); err != nil {
				errors <- fmt.Errorf("failed to read chunk %d: %v", chunkIndex, err)
				return
			}

			// Store chunk
			placement := placements[chunkIndex]
			if err := c.storeChunk(placement, data); err != nil {
				errors <- fmt.Errorf("failed to store chunk %d: %v", chunkIndex, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) RetrieveFile(filename, localPath string) error {
	// Get chunk placements from controller
	resp, err := c.requestFileRetrieval(filename)
	if err != nil {
		return fmt.Errorf("failed to get chunk placements: %v", err)
	}

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %v", err)
	}
	defer file.Close()

	// Retrieve chunks in parallel
	var wg sync.WaitGroup
	errors := make(chan error, len(resp.ChunkPlacements))
	chunks := make([]*struct {
		index uint32
		data  []byte
	}, len(resp.ChunkPlacements))

	for i, placement := range resp.ChunkPlacements {
		wg.Add(1)
		go func(i int, placement *pb.ChunkPlacement) {
			defer wg.Done()

			// Try each node until successful
			var data []byte
			var retrieveErr error
			for _, nodeAddr := range placement.StorageNodes {
				data, _, retrieveErr = c.retrieveChunk(nodeAddr, placement.ChunkId)
				if retrieveErr == nil {
					break
				}
			}

			if retrieveErr != nil {
				errors <- fmt.Errorf("failed to retrieve chunk %d: %v", placement.ChunkIndex, retrieveErr)
				return
			}

			chunks[i] = &struct {
				index uint32
				data  []byte
			}{placement.ChunkIndex, data}
		}(i, placement)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		if err != nil {
			return err
		}
	}

	// Write chunks in order
	for _, chunk := range chunks {
		if _, err := file.Write(chunk.data); err != nil {
			return fmt.Errorf("failed to write chunk %d: %v", chunk.index, err)
		}
	}

	return nil
}

func (c *Client) DeleteFile(filename string) error {
	conn, err := c.connectToController()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send delete request
	req := &pb.DeleteRequest{
		Filename: filename,
	}

	if err := c.sendMessage(conn, req); err != nil {
		return fmt.Errorf("failed to send delete request: %v", err)
	}

	// Receive response
	resp := &pb.DeleteResponse{}
	if err := c.receiveMessage(conn, resp); err != nil {
		return fmt.Errorf("failed to receive delete response: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("delete failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) ListFiles() ([]FileInfo, error) {
	conn, err := c.connectToController()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Send list request
	req := &pb.ListRequest{}
	if err := c.sendMessage(conn, req); err != nil {
		return nil, fmt.Errorf("failed to send list request: %v", err)
	}

	// Receive response
	resp := &pb.ListResponse{}
	if err := c.receiveMessage(conn, resp); err != nil {
		return nil, fmt.Errorf("failed to receive list response: %v", err)
	}

	// Convert to FileInfo slice
	files := make([]FileInfo, len(resp.Files))
	for i, f := range resp.Files {
		files[i] = FileInfo{
			Name:      f.Filename,
			Size:      f.Size,
			NumChunks: f.NumChunks,
		}
	}

	return files, nil
}

func (c *Client) ListNodes() ([]NodeInfo, error) {
	conn, err := c.connectToController()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Send node status request
	req := &pb.NodeStatusRequest{}
	if err := c.sendMessage(conn, req); err != nil {
		return nil, fmt.Errorf("failed to send node status request: %v", err)
	}

	// Receive response
	resp := &pb.NodeStatusResponse{}
	if err := c.receiveMessage(conn, resp); err != nil {
		return nil, fmt.Errorf("failed to receive node status response: %v", err)
	}

	// Convert to NodeInfo slice
	nodes := make([]NodeInfo, len(resp.Nodes))
	for i, n := range resp.Nodes {
		nodes[i] = NodeInfo{
			ID:            n.NodeId,
			Address:       n.Address,
			FreeSpace:     n.FreeSpace,
			TotalRequests: n.TotalRequests,
		}
	}

	return nodes, nil
}

func (c *Client) connectToController() (net.Conn, error) {
	return net.Dial("tcp", c.controllerAddr)
}

func (c *Client) sendMessage(conn net.Conn, msg proto.Message) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// Send message size
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(msgBytes)))
	if _, err := conn.Write(sizeBuf); err != nil {
		return fmt.Errorf("failed to send message size: %v", err)
	}

	// Send message
	if _, err := conn.Write(msgBytes); err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	return nil
}

func (c *Client) receiveMessage(conn net.Conn, msg proto.Message) error {
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

func (c *Client) requestChunkPlacements(filename string, fileSize uint64, chunkSize uint32, numChunks uint32) ([]*pb.ChunkPlacement, error) {
	conn, err := c.connectToController()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Send store request
	req := &pb.StoreRequest{
		Filename:  filename,
		TotalSize: fileSize,
		ChunkSize: chunkSize,
		NumChunks: numChunks,
	}

	if err := c.sendMessage(conn, req); err != nil {
		return nil, fmt.Errorf("failed to send store request: %v", err)
	}

	// Receive response
	resp := &pb.StoreResponse{}
	if err := c.receiveMessage(conn, resp); err != nil {
		return nil, fmt.Errorf("failed to receive store response: %v", err)
	}

	return resp.ChunkPlacements, nil
}

func (c *Client) requestFileRetrieval(filename string) (*pb.RetrieveResponse, error) {
	conn, err := c.connectToController()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Send retrieve request
	req := &pb.RetrieveRequest{
		Filename: filename,
	}

	if err := c.sendMessage(conn, req); err != nil {
		return nil, fmt.Errorf("failed to send retrieve request: %v", err)
	}

	// Receive response
	resp := &pb.RetrieveResponse{}
	if err := c.receiveMessage(conn, resp); err != nil {
		return nil, fmt.Errorf("failed to receive retrieve response: %v", err)
	}

	return resp, nil
}

func (c *Client) storeChunk(placement *pb.ChunkPlacement, data []byte) error {
	// Calculate checksum
	checksum := sha256.Sum256(data)

	// Create store request
	req := &pb.ChunkStoreRequest{
		ChunkId:      placement.ChunkId,
		ChunkIndex:   placement.ChunkIndex,
		Data:         data,
		ReplicaNodes: placement.StorageNodes[1:], // Skip primary node
	}

	// Connect to primary storage node
	conn, err := net.Dial("tcp", placement.StorageNodes[0])
	if err != nil {
		return fmt.Errorf("failed to connect to storage node: %v", err)
	}
	defer conn.Close()

	// Send request
	if err := c.sendMessage(conn, req); err != nil {
		return fmt.Errorf("failed to send chunk store request: %v", err)
	}

	// Receive response
	resp := &pb.ChunkStoreResponse{}
	if err := c.receiveMessage(conn, resp); err != nil {
		return fmt.Errorf("failed to receive chunk store response: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("chunk store failed: %s", resp.Error)
	}

	return nil
}

func (c *Client) retrieveChunk(nodeAddr, chunkID string) ([]byte, []byte, error) {
	// Connect to storage node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to storage node: %v", err)
	}
	defer conn.Close()

	// Send retrieve request
	req := &pb.ChunkRetrieveRequest{
		ChunkId: chunkID,
	}

	if err := c.sendMessage(conn, req); err != nil {
		return nil, nil, fmt.Errorf("failed to send chunk retrieve request: %v", err)
	}

	// Receive response
	resp := &pb.ChunkRetrieveResponse{}
	if err := c.receiveMessage(conn, resp); err != nil {
		return nil, nil, fmt.Errorf("failed to receive chunk retrieve response: %v", err)
	}

	if resp.Corrupted {
		return resp.Data, resp.Checksum, fmt.Errorf("chunk is corrupted")
	}

	return resp.Data, resp.Checksum, nil
}