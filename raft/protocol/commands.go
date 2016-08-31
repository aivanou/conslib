package protocol


type RequestArgs struct {
	Term         uint64
	LastLogIndex uint64
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
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	Entries      []uint64
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