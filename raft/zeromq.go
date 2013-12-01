package raft

import (
	"bytes"
	"github.com/goraft/raft"
	zmq "github.com/pebbe/zmq3"
	"io"
	"reflect"
	"time"
)

// ZmqTransporter mplements raft.Transporter interface
type ZmqTransporter struct {
	incoming       *zmq.Socket
	outgoing       map[string]*zmq.Socket
	stop           chan interface{}
	running        bool
	server         raft.Server
	receiveTimeout time.Duration
}

// Bytes that are used to distinguish packets on the wire
const (
	requestVote             = 0x01
	requestAppendEntries    = 0x02
	requestSnapshot         = 0x03
	requestShapshotRecovery = 0x04
)

// Interace that covers raft request and response types
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

// Start processing incoming messages from ØMQ
func (transporter *ZmqTransporter) Start(server raft.Server) {
	transporter.server = server
	transporter.running = true
	go func() {
		reactor := zmq.NewReactor()
		reactor.AddSocket(transporter.incoming, zmq.POLLIN, func(zmq.State) error { return transporter.parseIncomingResponse() })
		reactor.AddChannel(transporter.stop, 1, func(interface{}) error { transporter.running = false; return nil })

		for transporter.running {
			reactor.Run(100 * time.Millisecond)
		}
	}()
}

// processResponse passes request to Raft server and gets response back
func (transporter *ZmqTransporter) processResponse(name string, rb *bytes.Buffer, req raftReqResp, handler func() raftReqResp) error {
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

	_, err = transporter.incoming.SendBytes(wb.Bytes(), 0)
	if err != nil {
		logger.Printf("Unable to send response back to %s: %v", name, err)
		return err
	}

	return nil
}

// parseIncomingResponse parses packets on the wire
func (transporter *ZmqTransporter) parseIncomingResponse() error {
	request, err := transporter.incoming.RecvBytes(0)
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
		transporter.processResponse("request vote", rb, req,
			func() raftReqResp { return transporter.server.RequestVote(req) })
	case requestAppendEntries:
		req := &raft.AppendEntriesRequest{}
		transporter.processResponse("append entries request", rb, req,
			func() raftReqResp { return transporter.server.AppendEntries(req) })
	case requestSnapshot:
		req := &raft.SnapshotRequest{}
		transporter.processResponse("snapshot request", rb, req,
			func() raftReqResp { return transporter.server.RequestSnapshot(req) })
	case requestShapshotRecovery:
		req := &raft.SnapshotRecoveryRequest{}
		transporter.processResponse("snapshot recovery request", rb, req,
			func() raftReqResp { return transporter.server.SnapshotRecoveryRequest(req) })
	default:
		logger.Printf("Unknown request kind: %v", kind)
	}

	return nil
}

// getSocketFor returns outgoing ZMQ socket for peer, creating when necessary
func (transporter *ZmqTransporter) getSocketFor(peer *raft.Peer) (*zmq.Socket, error) {
	_, ok := transporter.outgoing[peer.ConnectionString]
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

		err = socket.SetRcvtimeo(transporter.receiveTimeout)
		if err != nil {
			return nil, err
		}

		transporter.outgoing[peer.ConnectionString] = socket
	}

	return transporter.outgoing[peer.ConnectionString], nil
}

// sendRequest sends encodes, sends request and decodes response
func (transporter *ZmqTransporter) sendRequest(name string, kind byte, peer *raft.Peer, req raftReqResp, resp raftReqResp) bool {
	var b bytes.Buffer
	b.WriteByte(kind)

	if _, err := req.Encode(&b); err != nil {
		logger.Printf("Encoding failed for %s request: %v", name, err)
		return false
	}

	socket, err := transporter.getSocketFor(peer)
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

// SendVoteRequest sends vote request to other peer
func (transporter *ZmqTransporter) SendVoteRequest(server raft.Server, peer *raft.Peer, req *raft.RequestVoteRequest) *raft.RequestVoteResponse {
	resp := &raft.RequestVoteResponse{}

	if transporter.sendRequest("request vote", requestVote, peer, req, resp) {
		return resp
	}

	return nil
}

// SendAppendEntriesRequest sends append entries request to other peer
func (transporter *ZmqTransporter) SendAppendEntriesRequest(server raft.Server, peer *raft.Peer, req *raft.AppendEntriesRequest) *raft.AppendEntriesResponse {
	resp := &raft.AppendEntriesResponse{}

	if transporter.sendRequest("append entries", requestAppendEntries, peer, req, resp) {
		return resp
	}

	return nil
}

// SendSnapshotRequest sends snapshot request to other peer
func (transporter *ZmqTransporter) SendSnapshotRequest(server raft.Server, peer *raft.Peer, req *raft.SnapshotRequest) *raft.SnapshotResponse {
	resp := &raft.SnapshotResponse{}

	if transporter.sendRequest("snapshot request", requestSnapshot, peer, req, resp) {
		return resp
	}

	return nil
}

// SendSnapshotRecoveryRequest sends snapshot recovery request to other peer
func (transporter *ZmqTransporter) SendSnapshotRecoveryRequest(server raft.Server, peer *raft.Peer, req *raft.SnapshotRecoveryRequest) *raft.SnapshotRecoveryResponse {
	resp := &raft.SnapshotRecoveryResponse{}

	if transporter.sendRequest("snapshot recovery", requestShapshotRecovery, peer, req, resp) {
		return resp
	}

	return nil
}

// Shutdown stops transporter, processing of incoming packets, closes ØMQ sockets
func (transporter *ZmqTransporter) Shutdown() {
	if transporter.running {
		logger.Printf("Shutting down ZMQ transporter for server %s\n", transporter.server.Name())
		transporter.stop <- true
		logger.Printf("Stopped ZMQ transporter for server %s\n", transporter.server.Name())
	}
	transporter.incoming.Close()
	for _, socket := range transporter.outgoing {
		socket.Close()
	}
}
