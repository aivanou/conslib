package raft

import (
	// "github.com/tierex/conslib/raft/logstore"
	"sync/atomic"
)

// NodeState contains raft node data for particular term
type NodeState struct {
	Term             uint64 // current nodes Term
	VotedFor         string // the node id that this node voted for for term: Term
	VotesReceived    uint32 // amount of votes that voted for this node
	CommitIndex      uint32 // TODO: implement
	LastAppliedIndex uint32 // TODO: implement
	// Store            *logstore.LogStore
}

// UpdateTerm is a thread safe method tha changes the term to the newTerm.
func (state *NodeState) UpdateTerm(newTerm uint64) {
	state.updateTerm(newTerm)
}

// IncTerm is a thread safe method tha increases the current node's term by one
func (state *NodeState) IncTerm() {
	state.updateTerm(state.Term + 1)
}

func (state *NodeState) updateTerm(newTerm uint64) {
	termAddr := &state.Term
	atomic.StoreUint64(termAddr, newTerm)
}

// IncVotesForTerm is a thread safe method that
// increases amount of nodes that voted for this node by one
func (state *NodeState) IncVotesForTerm() {
	atomic.AddUint32(&state.VotesReceived, 1)
}

// ResetVotesReceived resets the amount of votes to zero
func (state *NodeState) ResetVotesReceived() {
	atomic.StoreUint32(&state.VotesReceived, 0)
}

// UpdateCommitIndex updates the commit index
func (state *NodeState) UpdateCommitIndex(newIndex uint32) {
	atomic.StoreUint32(&state.CommitIndex, newIndex)
}

// UpdateLastAppliedIndex updates the last applied index
func (state *NodeState) UpdateLastAppliedIndex(newIndex uint32) {
	atomic.StoreUint32(&state.LastAppliedIndex, newIndex)
}
