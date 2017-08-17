package main
import (
	"sync/atomic"
	"github.com/tierex/conslib/raft/logstore"
)


type RaftState struct {
	Term             uint64
	VotedFor         string
	VotesReceived    uint32
	CommitIndex      uint32
	LastAppliedIndex uint32
	Store            *logstore.LogStore
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

func (state *RaftState) UpdateCommitIndex(newIndex uint32) {
	atomic.StoreUint32(&state.CommitIndex, newIndex)
}

func (state *RaftState) UpdateLastAppliedIndex(newIndex uint32) {
	atomic.StoreUint32(&state.LastAppliedIndex, newIndex)
}
