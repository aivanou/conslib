package main
import "sync/atomic"


type RaftState struct {
	Term          uint64
	Log           []uint64
	VotedFor      string
	VotesReceived uint32
}

func (state *RaftState) UpdateTerm(newTerm uint64) {
	termAddr := &state.Term
	atomic.StoreUint64(termAddr, newTerm)
}

func (state *RaftState) IncTerm() {
	termAddr := &state.Term
	atomic.AddUint64(termAddr, 1)
}

func (state *RaftState) IncVotesForTerm() {
	atomic.AddUint32(&state.VotesReceived, 1)
}

func (state *RaftState) ResetVotesReceived() {
	atomic.StoreUint32(&state.VotesReceived, 0)
}