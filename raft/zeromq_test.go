package raft

import (
	"github.com/goraft/raft"
	. "launchpad.net/gocheck"
	"os"
	"testing"
	"time"
)

const (
	testListener1   = "127.0.0.1:7535"
	testListener2   = "127.0.0.1:7536"
	testRecvTimeout = 1 * time.Second
)

// Create new raft test server
func newTestServer(name string, transporter raft.Transporter) raft.Server {
	p := tempDirForDB()
	server, _ := raft.NewServer(name, p, transporter, &MockStateMachine{}, nil, "")
	return server
}

// State machine mock
type MockStateMachine struct{}

func (*MockStateMachine) Save() ([]byte, error) {
	return []byte{}, nil
}

func (*MockStateMachine) Recovery([]byte) error {
	return nil
}

// Launch gocheck tests
func Test(t *testing.T) {
	TestingT(t)
}

type ZmqSuite struct {
	transporter1 *ZmqTransporter
	transporter2 *ZmqTransporter
	server1      raft.Server
	server2      raft.Server
	peer1        *raft.Peer
	peer2        *raft.Peer
}

var _ = Suite(&ZmqSuite{})

func (s *ZmqSuite) SetUpTest(c *C) {
	raft.SetLogLevel(raft.Trace)
	var err error
	s.transporter1, err = NewZmqTransporter(testListener1, testRecvTimeout)
	c.Assert(err, IsNil)
	s.transporter2, err = NewZmqTransporter(testListener2, testRecvTimeout)
	c.Assert(err, IsNil)

	s.server1 = newTestServer("server1", s.transporter1)
	s.server2 = newTestServer("server2", s.transporter1)

	s.peer1 = &raft.Peer{Name: "server1", ConnectionString: testListener1}
	s.peer2 = &raft.Peer{Name: "server2", ConnectionString: testListener2}
}

func (s *ZmqSuite) TearDownTest(c *C) {
	if s.transporter1 != nil {
		s.transporter1.Shutdown()
	}
	if s.transporter2 != nil {
		s.transporter2.Shutdown()
	}

	time.Sleep(100 * time.Millisecond)

	if s.server1 != nil {
		os.RemoveAll(s.server1.Path())
	}
	if s.server2 != nil {
		os.RemoveAll(s.server1.Path())
	}
}

func (s *ZmqSuite) TestStartTransport(c *C) {
	s.transporter1.Start(s.server1)
	s.transporter2.Start(s.server2)
}

func (s *ZmqSuite) TestStartTransportAndServer(c *C) {
	s.transporter1.Start(s.server1)
	s.transporter2.Start(s.server2)

	s.server1.Start()
	defer s.server1.Stop()

	s.server2.Start()
	defer s.server2.Stop()
}

func (s *ZmqSuite) TestVoteRequest(c *C) {
	s.transporter2.Start(s.server2)
	s.server2.Start()
	defer s.server2.Stop()

	resp := s.transporter1.SendVoteRequest(s.server1, s.peer2, &raft.RequestVoteRequest{
		Term:          1,
		LastLogIndex:  1,
		LastLogTerm:   1,
		CandidateName: "server1",
	})
	c.Assert(resp, NotNil)
	c.Assert(resp.VoteGranted, Equals, true)
	c.Assert(resp.Term, Equals, uint64(1))
}

func (s *ZmqSuite) TestAppendEntriesRequest(c *C) {
	s.transporter2.Start(s.server2)
	s.server2.Start()
	defer s.server2.Stop()

	resp := s.transporter1.SendAppendEntriesRequest(s.server1, s.peer2, &raft.AppendEntriesRequest{
		Term:         1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		CommitIndex:  0,
		LeaderName:   "server1",
		Entries:      []*raft.LogEntry{},
	})
	c.Assert(resp, NotNil)
	c.Assert(resp.Success, Equals, true)
	c.Assert(resp.CommitIndex, Equals, uint64(0))
	c.Assert(resp.Term, Equals, uint64(1))
}

func (s *ZmqSuite) TestSnapshotRequest(c *C) {
	s.transporter2.Start(s.server2)
	s.server2.Start()
	defer s.server2.Stop()

	resp := s.transporter1.SendSnapshotRequest(s.server1, s.peer2, &raft.SnapshotRequest{
		LastTerm:   1,
		LastIndex:  0,
		LeaderName: "server1",
	})
	c.Assert(resp, NotNil)
	c.Assert(resp.Success, Equals, true)
}

func (s *ZmqSuite) TestSnapshotRecoveryRequest(c *C) {
	s.transporter2.Start(s.server2)
	s.server2.Start()
	defer s.server2.Stop()

	s.transporter1.SendSnapshotRequest(s.server1, s.peer2, &raft.SnapshotRequest{
		LastTerm:   1,
		LastIndex:  0,
		LeaderName: "server1",
	})

	resp := s.transporter1.SendSnapshotRecoveryRequest(s.server1, s.peer2, &raft.SnapshotRecoveryRequest{
		Peers:      []*raft.Peer{},
		State:      []byte{},
		LastTerm:   1,
		LastIndex:  0,
		LeaderName: "server1",
	})
	c.Assert(resp, NotNil)
	c.Assert(resp.Success, Equals, true)
	c.Assert(resp.Term, Equals, uint64(1))
}

func (s *ZmqSuite) TestNilResponse(c *C) {
	s.transporter2.Start(s.server2)
	s.server2.Start()
	defer s.server2.Stop()

	resp := s.transporter1.SendSnapshotRecoveryRequest(s.server1, s.peer2, &raft.SnapshotRecoveryRequest{
		Peers:      []*raft.Peer{},
		State:      []byte{},
		LastTerm:   1,
		LastIndex:  0,
		LeaderName: "server1",
	})
	c.Assert(resp, IsNil)
}
