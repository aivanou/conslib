package raft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// Config is the main config point
type Config struct {
	Raft  NodeConfig   `json:"raft"`
	Peers []PeerConfig `json:"peers"`
}

// NodeConfig is a raft config data
type NodeConfig struct {
	Leader   LeaderConfig   `json:"leader"`
	Follower FollowerConfig `json:"follower"`
	Host     string         `json:"host"`
	Port     int            `json:"port"`
}

// LeaderConfig is a raft.leader config data
type LeaderConfig struct {
	PeerTimeout int `json:"peer_timeout"`
}

// FollowerConfig is a raft.follower config data
type FollowerConfig struct {
	Timeout int `json:"timeout"`
}

// PeerConfig is a peer config data
type PeerConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// ReadConfig Reads info from config file
func ReadConfig(filePath string) *Config {
	file, e := ioutil.ReadFile(filePath)
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}
	var config Config
	json.Unmarshal(file, &config)
	return &config
}
