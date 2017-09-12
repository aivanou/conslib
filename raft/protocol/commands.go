package protocol

import (
	"fmt"
	// "github.com/tierex/conslib/raft/logstore"
)

type RequestArgs struct {
	Term         uint64
	LastLogIndex uint32
	LastLogTerm  uint64
	CandidateId  string
}

type RequestResult struct {
	Term        uint64
	VoteGranted bool
}

type AppendArgs struct {
	Term     uint64
	LeaderId string
	// PrevLogIndex uint32
	// PrevLogTerm  uint64
	// LeaderCommit uint32
	// Entries      []logstore.LogItem
}

type AppendResult struct {
	Term    uint64
	Success bool
}

type RegisterServerArgs struct {
	Id   string
	Host string
	Port int
}

type RegisterServerResult struct {
	Status string
	Code   int
}

type WriteLogRequest struct {
	Data uint64
}

type WriteLogResponse struct {
	Status uint32
}

type NodeSnapshotRequest struct {
}

type NodeSnapshotResponse struct {
	Id               string
	State            string
	Term             uint64
	CommitIndex      uint32
	LastAppliedIndex uint32
	LastLogIndex     uint32
	LastLogTerm      uint64
	LogSize          uint32
}

func (resp *NodeSnapshotResponse) String() string {
	return fmt.Sprintf(`[id:%s, State:%s, Term:%d, CommitIndex:%d,
						LastAppliedIndex:%d, LastLogIndex:%d,
						LastLogTerm:%d, LogSize:%d]`,
		resp.Id, resp.State, resp.Term, resp.CommitIndex,
		resp.LastAppliedIndex, resp.LastLogIndex, resp.LastLogTerm,
		resp.LogSize)
}
