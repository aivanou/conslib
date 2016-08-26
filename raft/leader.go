package main
import (
	"time"
//	"log"
)

func NewLeaderState(nodeState NodeState, raftNode *RaftNode) *LeaderState {
	leaderState := new(LeaderState)
	leaderState.raftNode = raftNode
	leaderState.NodeState = nodeState
	return leaderState
}

type LeaderState struct {
	NodeState
	slaves   []*SlaveServerTimer
	raftNode *RaftNode
}

func (state *LeaderState) OnInit(data interface{}) error {
	slaves := make([]*SlaveServerTimer, 0, len(state.raftNode.Servers))
	for id, _ := range state.raftNode.Servers {
		slave := &SlaveServerTimer{id, 100, nil}
		slaves = append(slaves, slave)
	}
	state.slaves = slaves
	return nil
}


type SlaveServerTimer struct {
	Id       string
	Duration time.Duration
	Ticker   *time.Ticker
}

func (state *LeaderState) Process(eventId uint16) error {
	return nil
}

func (state *LeaderState) OnStateStarted() error {
	for _, slave := range state.slaves {
		slave.Ticker = time.NewTicker(time.Millisecond * slave.Duration)
		go func(server *SlaveServerTimer) {
			for _ = range server.Ticker.C {
				state.raftNode.SendAppend(server.Id)
			}
		}(slave)
	}
	return nil
}

func (state *LeaderState) OnStateFinished() error {
	for _, slave := range state.slaves {
		slave.Ticker.Stop()
	}
	return nil
}