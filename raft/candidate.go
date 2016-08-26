package main
import (
	"sync"
	"log"
	"time"
)


type CandidateState struct {
	NodeState
	raftServer *RaftNode
}

func (state *CandidateState) OnInit(data interface{}) error {
	return nil
}

func (state *CandidateState) OnStateStarted() error {
	server := state.raftServer
	server.Term += 1
	server.VotedFor = server.Id
	server.VotesReceived = 1
	var wg sync.WaitGroup
	wg.Add(len(server.Servers))
	state.raftServer.SendRequestVotes(&wg)
	go func(rnode *RaftNode) {
		wg.Wait()
		eventLoop := rnode.eventProcessor
		//raftNode.hasMajority()
		if hasMajority(rnode) {
			log.Println(rnode.Id, "Received response from all servers, becoming LEADER for term ", rnode.Term)
			eventLoop.Trigger(NewUpdateStateEvent(BECOME_LEADER, time.Now()))
		}else {
			log.Println(rnode.Id, "Received response from all servers, becoming follower for term ", rnode.Term)
			eventLoop.Trigger(NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
		}
	}(server)
	return nil
}

func (state *CandidateState) Process(eventId uint16) error {
	return nil
}

func (state *CandidateState) OnStateFinished() error {
	return nil
}