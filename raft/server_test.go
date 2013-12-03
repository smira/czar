package raft

import (
	. "launchpad.net/gocheck"
	"time"
)

type ServerSuite struct {
	dbPath string
}

var _ = Suite(&ServerSuite{})

func (s *ServerSuite) SetUpTest(c *C) {
	s.dbPath = c.MkDir()
}

func (s *ServerSuite) TestGetNodeName(c *C) {
	name, err := getNodeName(s.dbPath)
	c.Assert(err, IsNil)
	c.Assert(name, HasLen, 32)

	name2, err := getNodeName(s.dbPath)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, name2)

	_, err = getNodeName("/path/does/not/exist")
	c.Assert(err, NotNil)
}

func (s *ServerSuite) TestStartAsEmptyCluster(c *C) {
	server, err := NewServer(s.dbPath, testListener1, testRecvTimeout)
	c.Assert(err, IsNil)

	err = server.Start("")
	c.Assert(err, IsNil)

	server.Stop()
}

func (s *ServerSuite) TestStartAndJoin(c *C) {
	// Start server2 as leader
	server2, err := NewServer(c.MkDir(), testListener2, testRecvTimeout)
	c.Assert(err, IsNil)

	server2.raftServer.SetHeartbeatTimeout(1 * time.Second)

	err = server2.Start("")
	defer server2.Stop()

	c.Assert(err, IsNil)

	// Server joins server2
	server, err := NewServer(s.dbPath, testListener1, testRecvTimeout)
	c.Assert(err, IsNil)

	err = server.Start(testListener2)
	c.Assert(err, IsNil)

	server.Stop()
}

func (s *ServerSuite) TestStartAndJoinNonLeader(c *C) {
	// Start server2
	server2, err := NewServer(c.MkDir(), testListener2, testRecvTimeout)
	c.Assert(err, IsNil)

	server2.raftServer.SetHeartbeatTimeout(1 * time.Second)

	server2.Start("localhost:7899")
	defer server2.Stop()

	// Server joins server2
	server, err := NewServer(s.dbPath, testListener1, testRecvTimeout)
	c.Assert(err, IsNil)

	err = server.Start(testListener2)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*Not current leader")

	server.Stop()
}
