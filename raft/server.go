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

var logger = log.New(os.Stdout, "[raft-zeromq] ", log.Lmicroseconds)

// Length of node name in bytes
const nodeNameLength = 16

// getNodeName loads or generates and saves node name
func getNodeName(path string) (string, error) {
	var nodeName string

	nodeNamePath := filepath.Join(path, "nodename")

	// Figure out server name
	buf, err := ioutil.ReadFile(nodeNamePath)

	if err == nil {
		nodeName = string(buf)
		logger.Printf("Starting with node name: %s\n", nodeName)
		return nodeName, nil
	}

	if !os.IsNotExist(err) {
		return "", fmt.Errorf("Unable to read nodename file %s: %v", nodeNamePath, err)
	}

	rb := make([]byte, nodeNameLength)
	_, err = io.ReadFull(rand.Reader, rb)
	if err != nil {
		return "", fmt.Errorf("Unable to generate random data: %v", err)
	}
	nodeName = hex.EncodeToString(rb)
	logger.Printf("Generated node name: %s\n", nodeName)

	err = ioutil.WriteFile(nodeNamePath, []byte(nodeName), 0664)
	if err != nil {
		return "", fmt.Errorf("Unable to create nodename file %s: %v", nodeNamePath, err)
	}

	return nodeName, nil
}

// NewRaftServer creates raft server with ØMQ transport
func NewRaftServer(path string, listenAddress string, receiveTimeout time.Duration) (raft.Server, error) {
	// var nodeName string

	return nil, nil
}
