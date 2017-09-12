package raft

import (
	"sync"
	"time"
)

// CandidateState bla
type CandidateState struct {
	SmState
	raftServer *Node
}

// OnInit bla
func (state *CandidateState) OnInit(data interface{}, raftConfig NodeConfig) error {
	return nil
}

// OnStateStarted bla
func (state *CandidateState) OnStateStarted() error {
	server := state.raftServer
	server.State.IncTerm()
	// vote for myself
	server.State.VotedFor = server.ID
	server.State.IncVotesForTerm()
	var wg sync.WaitGroup
	wg.Add(len(server.Servers))
	server.SendRequestVotes(&wg)
	go func(rnode *Node) {
		wg.Wait()
		eventLoop := rnode.eventProcessor
		//raftNode.hasMajority()
		if hasMajority(rnode) {
			log.Println(rnode.ID, "Received response from all servers, becoming LEADER for term ", rnode.State.Term)
			eventLoop.Trigger(NewUpdateStateEvent(BecomeLeader, time.Now()))
		} else {
			log.Println(rnode.ID, "Received response from all servers, becoming follower for term ", rnode.State.Term)
			eventLoop.Trigger(NewUpdateStateEvent(BecomeFollower, time.Now()))
		}
	}(server)
	return nil
}

// Process d
func (state *CandidateState) Process(eventID uint16, data interface{}) error {
	return nil
}

// OnStateFinished d
func (state *CandidateState) OnStateFinished() error {
	state.raftServer.State.ResetVotesReceived()
	return nil
}
