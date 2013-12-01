package raft

import (
	"io/ioutil"
	"os"
)

func tempDirForDB() string {
	p, _ := ioutil.TempDir("", "czar-test-")
	if err := os.MkdirAll(p, 0755); err != nil {
		panic(err.Error())
	}

	return p
}
