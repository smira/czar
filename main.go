package main

import (
	"flag"
	"github.com/smira/czar/raft"
)

func main() {
	var (
		raftAddress = flag.String("raft-address", "localhost:7000", "Address to listen for other servers (inter-cluster communication)")
		dbPath      = flag.String("log-path", ".", "Path to store Czar database")
	)

	flag.Parse()
}
