// Package raft provides integration with goraft and ØMQ libraries
package raft

import (
	"log"
	"os"

//	"github.com/goraft/raft"
)

var logger = log.New(os.Stdout, "[raft-zeromq] ", log.Lmicroseconds)
