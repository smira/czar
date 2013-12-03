// Package raft provides integration with goraft and ØMQ libraries
package raft

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/golang/glog"
	"github.com/goraft/raft"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

// Server is instance of raft server with ØMQ backend
type Server struct {
	dbPath        string
	raftServer    raft.Server
	listenAddress string
	transport     *ZmqTransporter
}

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
		glog.Infof("Starting with node name: %s\n", nodeName)
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
	glog.Infof("Generated node name: %s\n", nodeName)

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

	// Register standard raft commands in ØMQ layer
	transporter.RegisterCommand(func() raft.Command { return &raft.DefaultJoinCommand{} })
	transporter.RegisterCommand(func() raft.Command { return &raft.DefaultLeaveCommand{} })
	transporter.RegisterCommand(func() raft.Command { return &raft.NOPCommand{} })

	return &Server{dbPath: dbPath,
		raftServer:    server,
		transport:     transporter,
		listenAddress: listenAddress,
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

	joinCommand := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.listenAddress,
	}

	if leader != "" {
		// Attempting to join
		if !s.raftServer.IsLogEmpty() {
			return fmt.Errorf("cannot join with non-empty log")
		}

		err = s.transport.SendCommand(leader, joinCommand)
		if err != nil {
			return fmt.Errorf("unable to join leader %s: %v", leader, err)
		}

		glog.Infof("Joined %s as leader\n", leader)

	} else if s.raftServer.IsLogEmpty() {
		// Join myself: empty cluster
		glog.Infof("Starting with empty cluster\n")

		_, err = s.raftServer.Do(joinCommand)

		if err != nil {
			return fmt.Errorf("unable to join itself: %v", err)
		}
	} else {
		// Recovered from log
		glog.Infof("Recovered from log\n")
	}
	return nil
}

// Stop shuts down raft server and transport
func (s *Server) Stop() {
	s.transport.Shutdown()
	s.raftServer.Stop()
}
