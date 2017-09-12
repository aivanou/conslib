package raft

import (
	"time"
)

// FollowerState contains data that is relevant for active follower state
type FollowerState struct {
	SmState                      // extends the common data
	Duration       time.Duration // if the server
	Timer          *time.Timer
	raftServer     *Node
	eventFunctions map[uint16]eventProcessor
}

// OnInit is the event that occurs when the State Machine is initialized
func (state *FollowerState) OnInit(data interface{}, raftConfig NodeConfig) error {
	state.eventFunctions[ResetTimer] = state.resetTimer
	return nil
}

// OnStateStarted is an event that occurs when the StatMachine switches to the current state
func (state *FollowerState) OnStateStarted() error {
	server := state.raftServer
	log.Println(server.ID, ": Starting follower timer")
	if state.Timer != nil {
		wasActive := state.Timer.Reset(time.Millisecond * state.Duration)
		if !wasActive {
			log.Println(server.ID, "timer was in stop state, recreating timer")
		}
	} else {
		state.Timer = time.NewTimer(time.Millisecond * state.Duration)
	}
	go func(cState *FollowerState) {
		<-cState.Timer.C
		eventLoop := cState.raftServer.eventProcessor
		log.Println(cState.raftServer.ID, "triggering BECOME_CANDIDATE event")
		eventLoop.Trigger(NewUpdateStateEvent(BecomeCandidate, time.Now()))
	}(state)
	return nil
}

// OnStateFinished is an event that occurs
// when the StatMachine switches to the other state from this state
func (state *FollowerState) OnStateFinished() error {
	state.Timer.Stop()
	return nil
}

// Process processes the corresponding eventID
func (state *FollowerState) Process(eventID uint16, data interface{}) error {
	return state.eventFunctions[eventID](data)
}

func (state *FollowerState) resetTimer(data interface{}) error {
	if state.Timer != nil {
		wasActive := state.Timer.Reset(time.Millisecond * state.Duration)
		if !wasActive {
			log.Println("Follower Timer is not Active!!")
		}
	}
	return nil
}
