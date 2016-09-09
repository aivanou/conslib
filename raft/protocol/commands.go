package protocol
import "consensus/raft/logstore"


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
	Term         uint64
	LeaderId     string
	PrevLogIndex uint32
	PrevLogTerm  uint64
	LeaderCommit uint32
	Entries      []logstore.LogItem
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