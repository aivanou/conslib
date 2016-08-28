package main
import (
	"time"
	"log"
)

type eventProcessor func() error


type FollowerState struct {
	NodeState
	Duration       time.Duration
	Timer          *time.Timer
	raftServer     *RaftNode
	eventFunctions map[uint16]eventProcessor
}

func (state *FollowerState) OnInit(data interface{}) error {
	state.eventFunctions[RESET_TIMER] = state.resetTimer
	return nil
}


func (state *FollowerState) OnStateStarted() error {
	server := state.raftServer
	log.Println(server.Id, ": Starting follower timer")
	if state.Timer != nil {
		wasActive := state.Timer.Reset(time.Millisecond * state.Duration)
		if !wasActive {
			log.Println(server.Id, "timer was in stop state, recreating timer")
		}
		return nil
	}else {
		state.Timer = time.NewTimer(time.Millisecond * state.Duration)
	}
	go func(cState *FollowerState) {
		<-cState.Timer.C
		eventLoop := cState.raftServer.eventProcessor
		log.Println(cState.raftServer.Id, "triggering BECOME_CANDIDATE event")
		eventLoop.Trigger(NewUpdateStateEvent(BECOME_CANDIDATE, time.Now()))
	}(state)
	return nil
}

func (state *FollowerState) OnStateFinished() error {
	state.Timer.Stop()
	return nil
}

func (state *FollowerState) Process(eventId uint16) error {
	return state.eventFunctions[eventId]()
}

func (state *FollowerState) resetTimer() error {
	if state.Timer != nil {
		log.Println(state.raftServer.Id, "RESETTING TIMER EVENT WAS TRIGGERED: resetting to ", state.Duration)
		wasActive := state.Timer.Reset(time.Millisecond * state.Duration)
		if !wasActive {
			log.Println("Follower Timer is not Active!!")
		}
	}
	return nil
}
