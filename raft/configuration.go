package main
import (
	"os"
	"io/ioutil"
	"fmt"
	"encoding/json"
)


type jsonobject struct {
	Raft RaftConfig
}

type RaftConfig struct {
	Leader   LeaderConfig
	Follower FollowerConfig
}

type LeaderConfig struct {
	PeerTimeout int `json:"peer_timeout"`
}

type FollowerConfig struct {
	Timeout int `json:"timeout"`
}

// Reads info from config file
func ReadConfig() *RaftConfig {
	file, e := ioutil.ReadFile("./src/consensus/config.json")
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}
	var jsontype jsonobject
	json.Unmarshal(file, &jsontype)
	return &(jsontype.Raft)
}