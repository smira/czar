package raft

import (
	"bytes"
	"github.com/goraft/raft"
	zmq "github.com/pebbe/zmq3"
	"io"
	"reflect"
	"time"
)

// ZmqTransporter mplements raft Transporter interface
type ZmqTransporter struct {
	incoming       *zmq.Socket
	outgoing       map[string]*zmq.Socket
	stop           chan interface{}
	running        bool
	server         raft.Server
	receiveTimeout time.Duration
}

const (
	requestVote             = 0x01
	requestAppendEntries    = 0x02
	requestSnapshot         = 0x03
	requestShapshotRecovery = 0x04
)

type raftReqResp interface {
	Encode(io.Writer) (int, error)
	Decode(io.Reader) (int, error)
}

// NewZmqTransporter creates and initializes transport, starts listening, but doesn't
// start processing incoming requests
func NewZmqTransporter(listenAddress string, receiveTimeout time.Duration) (*ZmqTransporter, error) {
	result := &ZmqTransporter{
		outgoing:       make(map[string]*zmq.Socket),
		stop:           make(chan interface{}),
		receiveTimeout: receiveTimeout,
	}

	incoming, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return nil, err
	}
	incoming.SetLinger(0)

	logger.Printf("Starting to listen at %s\n", listenAddress)
	err = incoming.Bind("tcp://" + listenAddress)
	if err != nil {
		return nil, err
	}

	result.incoming = incoming

	return result, nil
}

// Start processing incoming messages
func (self *ZmqTransporter) Start(server raft.Server) {
	self.server = server
	self.running = true
	go func() {
		reactor := zmq.NewReactor()
		reactor.AddSocket(self.incoming, zmq.POLLIN, func(zmq.State) error { return self.parseIncomingResponse() })
		reactor.AddChannel(self.stop, 1, func(interface{}) error { self.running = false; return nil })

		for self.running {
			reactor.Run(100 * time.Millisecond)
		}
	}()
}

func (self *ZmqTransporter) processResponse(name string, rb *bytes.Buffer, req raftReqResp, handler func() raftReqResp) error {
	_, err := req.Decode(rb)
	if err != nil && err != io.EOF {
		logger.Printf("Unable to decode %s: %v", name, err)
		return err
	}
	resp := handler()
	logger.Printf("resp is %#v, resp == nil ? %#v", resp, resp == nil)
	if resp == nil || !reflect.ValueOf(resp).Elem().IsValid() {
		logger.Printf("Got nil response from raft to %s", name)
		return nil
	}

	var wb bytes.Buffer
	_, err = resp.Encode(&wb)
	if err != nil {
		logger.Printf("Unable to encode response to %s: %v", name, err)
		return err
	}

	_, err = self.incoming.SendBytes(wb.Bytes(), 0)
	if err != nil {
		logger.Printf("Unable to send response back to %s: %v", name, err)
		return err
	}

	return nil
}

func (self *ZmqTransporter) parseIncomingResponse() error {
	request, err := self.incoming.RecvBytes(0)
	if err != nil {
		logger.Printf("Error while receiving incoming request: %v\n", err)
		return nil
	}

	rb := bytes.NewBuffer(request)

	kind, err := rb.ReadByte()
	if err != nil {
		logger.Printf("Unable to read first byte of request: %v", err)
		return nil
	}

	switch kind {
	case requestVote:
		req := &raft.RequestVoteRequest{}
		self.processResponse("request vote", rb, req,
			func() raftReqResp { return self.server.RequestVote(req) })
	case requestAppendEntries:
		req := &raft.AppendEntriesRequest{}
		self.processResponse("append entries request", rb, req,
			func() raftReqResp { return self.server.AppendEntries(req) })
	case requestSnapshot:
		req := &raft.SnapshotRequest{}
		self.processResponse("snapshot request", rb, req,
			func() raftReqResp { return self.server.RequestSnapshot(req) })
	case requestShapshotRecovery:
		req := &raft.SnapshotRecoveryRequest{}
		self.processResponse("snapshot recovery request", rb, req,
			func() raftReqResp { return self.server.SnapshotRecoveryRequest(req) })
	default:
		logger.Printf("Unknown request kind: %v", kind)
	}

	return nil
}

// getSocketFor returns outgoing ZMQ socket for peer, creating when necessary
func (self *ZmqTransporter) getSocketFor(peer *raft.Peer) (*zmq.Socket, error) {
	_, ok := self.outgoing[peer.ConnectionString]
	if !ok {
		socket, err := zmq.NewSocket(zmq.REQ)
		if err != nil {
			return nil, err
		}
		socket.SetLinger(0)

		logger.Printf("Connecting to: %s", peer.ConnectionString)
		err = socket.Connect("tcp://" + peer.ConnectionString)
		if err != nil {
			return nil, err
		}

		err = socket.SetRcvtimeo(self.receiveTimeout)
		if err != nil {
			return nil, err
		}

		self.outgoing[peer.ConnectionString] = socket
	}

	return self.outgoing[peer.ConnectionString], nil
}

func (self *ZmqTransporter) sendRequest(name string, kind byte, peer *raft.Peer, req raftReqResp, resp raftReqResp) bool {
	var b bytes.Buffer
	b.WriteByte(kind)

	if _, err := req.Encode(&b); err != nil {
		logger.Printf("Encoding failed for %s request: %v", name, err)
		return false
	}

	socket, err := self.getSocketFor(peer)
	if err != nil {
		logger.Printf("Unable to create socket for peer %s: %v", peer.ConnectionString, err)
		return false
	}

	_, err = socket.SendBytes(b.Bytes(), 0)
	if err != nil {
		logger.Printf("Unable to send message to peer %s: %v", peer.ConnectionString, err)
		return false
	}

	response, err := socket.RecvBytes(0)
	if err != nil {
		logger.Printf("Unable to receive %s response from peer %s: %v", name, peer.ConnectionString, err)
		return false
	}

	rb := bytes.NewBuffer(response)
	if _, err = resp.Decode(rb); err != nil && err != io.EOF {
		logger.Printf("Unable to decode %s response from peer %s: %v", name, peer.ConnectionString, err)
		return false
	}

	return true
}

func (self *ZmqTransporter) SendVoteRequest(server raft.Server, peer *raft.Peer, req *raft.RequestVoteRequest) *raft.RequestVoteResponse {
	resp := &raft.RequestVoteResponse{}

	if self.sendRequest("request vote", requestVote, peer, req, resp) {
		return resp
	}

	return nil
}

func (self *ZmqTransporter) SendAppendEntriesRequest(server raft.Server, peer *raft.Peer, req *raft.AppendEntriesRequest) *raft.AppendEntriesResponse {
	resp := &raft.AppendEntriesResponse{}

	if self.sendRequest("append entries", requestAppendEntries, peer, req, resp) {
		return resp
	}

	return nil
}

func (self *ZmqTransporter) SendSnapshotRequest(server raft.Server, peer *raft.Peer, req *raft.SnapshotRequest) *raft.SnapshotResponse {
	resp := &raft.SnapshotResponse{}

	if self.sendRequest("snapshot request", requestSnapshot, peer, req, resp) {
		return resp
	}

	return nil
}

func (self *ZmqTransporter) SendSnapshotRecoveryRequest(server raft.Server, peer *raft.Peer, req *raft.SnapshotRecoveryRequest) *raft.SnapshotRecoveryResponse {
	resp := &raft.SnapshotRecoveryResponse{}

	if self.sendRequest("snapshot recovery", requestShapshotRecovery, peer, req, resp) {
		return resp
	}

	return nil
}

func (self *ZmqTransporter) Shutdown() {
	if self.running {
		logger.Printf("Shutting down ZMQ transporter for server %s\n", self.server.Name())
		self.stop <- true
		logger.Printf("Stopped ZMQ transporter for server %s\n", self.server.Name())
	}
	self.incoming.Close()
	for _, socket := range self.outgoing {
		socket.Close()
	}
}
