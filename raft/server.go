// Package raft provides integration with goraft and ØMQ libraries
package raft

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/goraft/raft"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
)

type Server struct {
	dbPath     string
	raftServer raft.Server
	transport  *ZmqTransporter
}

var logger = log.New(os.Stdout, "[raft-zeromq] ", log.Lmicroseconds)

// Length of node name in bytes
const nodeNameLength = 16

// getNodeName loads or generates and saves node name
func getNodeName(dbPath string) (string, error) {
	var nodeName string

	nodeNamePath := filepath.Join(dbPath, "nodename")

	// Figure out server name
	buf, err := ioutil.ReadFile(nodeNamePath)

	if err == nil {
		nodeName = string(buf)
		logger.Printf("Starting with node name: %s\n", nodeName)
		return nodeName, nil
	}

	if !os.IsNotExist(err) {
		return "", fmt.Errorf("unable to read nodename file %s: %v", nodeNamePath, err)
	}

	rb := make([]byte, nodeNameLength)
	_, err = io.ReadFull(rand.Reader, rb)
	if err != nil {
		return "", fmt.Errorf("unable to generate random data: %v", err)
	}
	nodeName = hex.EncodeToString(rb)
	logger.Printf("Generated node name: %s\n", nodeName)

	err = ioutil.WriteFile(nodeNamePath, []byte(nodeName), 0664)
	if err != nil {
		return "", fmt.Errorf("unable to create nodename file %s: %v", nodeNamePath, err)
	}

	return nodeName, nil
}

// NewServer creates raft server with ØMQ transport
func NewServer(dbPath string, listenAddress string, receiveTimeout time.Duration) (*Server, error) {
	nodeName, err := getNodeName(dbPath)
	if err != nil {
		return nil, err
	}

	transporter, err := NewZmqTransporter(listenAddress, receiveTimeout)
	if err != nil {
		return nil, fmt.Errorf("error creating ZMQ transport: %v", err)
	}

	// TODO: stateMachine
	server, err := raft.NewServer(nodeName, dbPath, transporter, nil, nil, listenAddress)
	if err != nil {
		return nil, fmt.Errorf("error creating Raft server: %v", err)
	}

	return &Server{dbPath: dbPath,
		raftServer: server,
		transport:  transporter,
	}, nil
}

// Start makes raft server & ØMQ transport running, joins existing
// leader or runs on its own
func (s *Server) Start(leader string) error {
	s.transport.Start(s.raftServer)
	err := s.raftServer.Start()
	if err != nil {
		return fmt.Errorf("unable to start Raft server: %v", err)
	}

	if leader != "" {
		// Attempting to join
		if !s.raftServer.IsLogEmpty() {
			return fmt.Errorf("cannot join with non-empty log")
		}
		// TODO: send Join command to leader
	} else if s.raftServer.IsLogEmpty() {
		// TODO: send Join command to myself
	} else {
		// Recovered from log
		logger.Printf("Recovered from log\n")
	}
	return nil
}

// Stop shuts down raft server and transport
func (s *Server) Stop() {
	s.raftServer.Stop()
	s.transport.Shutdown()
}
