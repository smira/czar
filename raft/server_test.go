package raft

import (
	. "launchpad.net/gocheck"
	"os"
)

type ServerSuite struct {
	dbPath string
}

var _ = Suite(&ServerSuite{})

func (s *ServerSuite) SetUpTest(c *C) {
	s.dbPath = tempDirForDB()
}

func (s *ServerSuite) TearDownTest(c *C) {
	os.RemoveAll(s.dbPath)
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
