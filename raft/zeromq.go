package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/goraft/raft"
	zmq "github.com/pebbe/zmq3"
	"io"
	"reflect"
	"time"
)

// CommandInstanciator is a function that instanciates command instance
type CommandInstanciator func() raft.Command

// ZmqTransporter mplements raft.Transporter interface
type ZmqTransporter struct {
	incoming         *zmq.Socket
	outgoing         map[string]*zmq.Socket
	stop             chan interface{}
	running          bool
	server           raft.Server
	receiveTimeout   time.Duration
	commands         map[string]CommandInstanciator
	ConnectionString string
}

// Bytes that are used to distinguish packets on the wire
const (
	requestVote             = 0x01
	requestAppendEntries    = 0x02
	requestSnapshot         = 0x03
	requestShapshotRecovery = 0x04
	requestCommand          = 0x80
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
		outgoing:         make(map[string]*zmq.Socket),
		stop:             make(chan interface{}),
		receiveTimeout:   receiveTimeout,
		commands:         make(map[string]CommandInstanciator),
		ConnectionString: listenAddress,
	}

	incoming, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return nil, err
	}
	incoming.SetLinger(0)

	glog.Infof("Starting to listen at %s\n", listenAddress)
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
		reactor.AddSocket(transporter.incoming, zmq.POLLIN, func(zmq.State) error { transporter.parseIncomingPackets(); return nil })

		var chId uint64
		chId = reactor.AddChannel(transporter.stop, 1, func(interface{}) error {
			transporter.running = false
			reactor.RemoveChannel(chId)
			reactor.RemoveSocket(transporter.incoming)
			return nil
		})

		for transporter.running {
			glog.V(2).Info("Entering reactor")
			err := reactor.Run(100 * time.Millisecond)
			if err != nil && err.Error() != "No sockets to poll, no channels to read" {
				glog.Infof("Got error from ZMQ reactor: %v", err)
			}
		}
		transporter.stop <- true
	}()
}

// processResponse passes request to Raft server and gets response back
func (transporter *ZmqTransporter) processResponse(name string, rb *bytes.Buffer, req raftReqResp, handler func() raftReqResp) error {
	_, err := req.Decode(rb)
	if err != nil && err != io.EOF {
		glog.Infof("Unable to decode %s: %v", name, err)
		return err
	}
	resp := handler()
	if resp == nil || !reflect.ValueOf(resp).Elem().IsValid() {
		glog.Infof("Got nil response from raft to %s", name)
		return nil
	}

	var wb bytes.Buffer
	_, err = resp.Encode(&wb)
	if err != nil {
		glog.Infof("Unable to encode response to %s: %v", name, err)
		return err
	}

	_, err = transporter.incoming.SendBytes(wb.Bytes(), 0)
	if err != nil {
		glog.Infof("Unable to send response back to %s: %v", name, err)
		return err
	}

	return nil
}

// parseIncomingPackets parses packets on the wire
func (transporter *ZmqTransporter) parseIncomingPackets() {
	for {
		request, err := transporter.incoming.RecvBytes(zmq.DONTWAIT)
		if err != nil {
			if err.Error() == "resource temporarily unavailable" {
				// async receive, no data
				break
			}
			glog.Infof("Error while receiving incoming packet: %v\n", err)
			return
		}

		rb := bytes.NewBuffer(request)

		kind, err := rb.ReadByte()
		if err != nil {
			glog.Infof("Unable to read first byte of packet: %v", err)
			return
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
		case requestCommand:
			decoder := json.NewDecoder(rb)

			var commandName string
			var response error

			err = decoder.Decode(&commandName)
			if err != nil {
				response = fmt.Errorf("unable to decode command name: %v", err)
			} else {
				instanciator, ok := transporter.commands[commandName]

				if !ok {
					response = fmt.Errorf("unknown command name %s", commandName)
				} else {
					command := instanciator()
					err = decoder.Decode(command)
					if err != nil {
						response = fmt.Errorf("unable to decode command %s: %v", commandName, err)
					} else {
						_, err = transporter.server.Do(command)
						if err != nil {
							response = fmt.Errorf("error processing command %s: %v", commandName, err)
						}
					}
				}
			}

			wb := new(bytes.Buffer)
			encoder := json.NewEncoder(wb)
			if response == nil {
				encoder.Encode(nil)
			} else {
				encoder.Encode(response.Error())
			}

			_, err = transporter.incoming.SendBytes(wb.Bytes(), 0)
			if err != nil {
				glog.Infof("Unable to send response back to command %s: %v", commandName, err)
			}
		default:
			glog.Infof("Unknown request kind: %v", kind)
		}

	}
}

// getSocketFor returns outgoing ZMQ socket for peer, creating when necessary
func (transporter *ZmqTransporter) getSocketFor(peer string) (*zmq.Socket, error) {
	_, ok := transporter.outgoing[peer]
	if !ok {
		socket, err := zmq.NewSocket(zmq.REQ)
		if err != nil {
			return nil, err
		}
		socket.SetLinger(0)

		glog.V(1).Infof("Connecting to: %s", peer)
		err = socket.Connect("tcp://" + peer)
		if err != nil {
			return nil, err
		}

		err = socket.SetRcvtimeo(transporter.receiveTimeout)
		if err != nil {
			return nil, err
		}

		transporter.outgoing[peer] = socket
	}

	return transporter.outgoing[peer], nil
}

// sendRequest sends encodes, sends request and decodes response
func (transporter *ZmqTransporter) sendRequest(name string, kind byte, peer *raft.Peer, req raftReqResp, resp raftReqResp) bool {
	var b bytes.Buffer
	b.WriteByte(kind)

	if _, err := req.Encode(&b); err != nil {
		glog.Infof("Encoding failed for %s request: %v", name, err)
		return false
	}

	socket, err := transporter.getSocketFor(peer.ConnectionString)
	if err != nil {
		glog.Infof("Unable to create socket for peer %s: %v", peer.ConnectionString, err)
		return false
	}

	_, err = socket.SendBytes(b.Bytes(), 0)
	if err != nil {
		glog.Infof("Unable to send message to peer %s: %v", peer.ConnectionString, err)
		return false
	}

	response, err := socket.RecvBytes(0)
	if err != nil {
		glog.Infof("Unable to receive %s response from peer %s: %v", name, peer.ConnectionString, err)
		return false
	}

	rb := bytes.NewBuffer(response)
	if _, err = resp.Decode(rb); err != nil && err != io.EOF {
		glog.Infof("Unable to decode %s response from peer %s: %v", name, peer.ConnectionString, err)
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

// RegisterCommand stores information about command, so that it could be decoded on receiving
func (transporter *ZmqTransporter) RegisterCommand(instanciator CommandInstanciator) {
	command := instanciator()
	name := command.CommandName()
	if transporter.commands[name] != nil {
		panic(fmt.Sprintf("Duplicate register for command name %s", name))
	}
	transporter.commands[name] = instanciator
}

// SendCommand sends command to other peer and receives reply
func (transporter *ZmqTransporter) SendCommand(peer string, command raft.Command) error {
	socket, err := transporter.getSocketFor(peer)
	if err != nil {
		return fmt.Errorf("unable to create socket for peer %s: %v", peer, err)
	}

	buf := new(bytes.Buffer)
	buf.WriteByte(requestCommand)

	encoder := json.NewEncoder(buf)
	encoder.Encode(command.CommandName())
	encoder.Encode(command)

	_, err = socket.SendBytes(buf.Bytes(), 0)
	if err != nil {
		return fmt.Errorf("unable to send command %s to peer %s: %v", command.CommandName(), peer, err)
	}

	response, err := socket.RecvBytes(0)
	if err != nil {
		return fmt.Errorf("unable to receive %s response from peer %s: %v", command.CommandName(), peer, err)
	}

	var result interface{}
	err = json.Unmarshal(response, &result)
	if err != nil {
		return fmt.Errorf("unable to decode %s response from peer %s: %v", command.CommandName(), peer, err)
	}

	if result != nil {
		return fmt.Errorf("error response to command %s from peer %s: %v", command.CommandName(), peer, result)
	}

	return nil
}

// Shutdown stops transporter, processing of incoming packets, closes ØMQ sockets
func (transporter *ZmqTransporter) Shutdown() {
	if transporter.running {
		glog.V(2).Infof("Shutting down ZMQ transporter for server %s\n", transporter.server.Name())
		transporter.stop <- true
		<-transporter.stop
		glog.Infof("Stopped ZMQ transporter for server %s\n", transporter.server.Name())
	}
	transporter.incoming.Close()
	for _, socket := range transporter.outgoing {
		socket.Close()
	}
}
