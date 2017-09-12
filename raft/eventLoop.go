package raft

import (
	"time"
)

// EventLoop is the interface that contains public methods for triggering event processing
type EventLoop interface {
	ProcessEvents() error
	Trigger(event Event) error
}

// EventProcessor is the class that contains implements event loop
type EventProcessor struct {
	server       *Node
	eventChannel chan Event
}

// NewEventProcessor returns the new event loop processor
func NewEventProcessor(server *Node) EventLoop {
	eventChannel := make(chan Event, 100)
	eventProcessor := &EventProcessor{server, eventChannel}
	return eventProcessor
}

// ProcessEvents starts a loop for processing events. Call Trigger for sending events.
func (eventProcessor *EventProcessor) ProcessEvents() error {
	log.Println("Starting event loop for server: ", eventProcessor.server.ID)
	server := eventProcessor.server
	server.stateHandler.GetActiveState().OnStateStarted()
	for {
		event := <-eventProcessor.eventChannel
		eventProcessor.processEvent(event)
	}
}

// Trigger is the function that sends events into the event loop
func (eventProcessor *EventProcessor) Trigger(event Event) error {
	go func(loop *EventProcessor, ev Event) {
		loop.eventChannel <- event
	}(eventProcessor, event)
	return nil
}

// Chain is the function that chains two events, the parent event will be processed first.
func Chain(parent Event, child Event) Event {
	return &ChainedEvent{NodeEvent{
		id:                 ChainEvent,
		eventTriggeredTime: parent.EventTriggeredTime()},
		parent, child}
}

// NewUpdateStateEvent creates a event for updating state machine
func NewUpdateStateEvent(id uint16, time time.Time) Event {
	return &UpdateStateEvent{NodeEvent{id: id, eventTriggeredTime: time}}
}

// NewServerRequestEvent TODO : implement
func NewServerRequestEvent(id uint16, time time.Time, payload interface{}) Event {
	return &ServerRequestEvent{NodeEvent{id: id, eventTriggeredTime: time}, payload}
}

func (eventProcessor *EventProcessor) processChainedEvent(chEvent *ChainedEvent) {
	event := chEvent.Event()
	eventProcessor.processEvent(event)
	if chEvent.NextEvent() != nil {
		go func(loop *EventProcessor, nextEvent Event) {
			loop.eventChannel <- nextEvent
		}(eventProcessor, chEvent.NextEvent())
	}
}

func (eventProcessor *EventProcessor) processEvent(event Event) {
	switch event.(type) {
	case *ChainedEvent:
		eventProcessor.processChainedEvent(event.(*ChainedEvent))
	case *ServerRequestEvent:
		eventProcessor.processServerRequestEvent(event.(*ServerRequestEvent))
	case *UpdateStateEvent:
		eventProcessor.updateStateMachine(event)
	default:
		log.Println(eventProcessor.server.ID, "Unrecognized event: ", event.ID())
	}
}

func (eventProcessor *EventProcessor) processServerRequestEvent(event *ServerRequestEvent) {
	switch {
	case event.ID() == UpdateServer:
		eventProcessor.updateServerParams(event)
	default:
		eventProcessor.server.stateHandler.GetActiveState().Process(event.ID(), event.Payload())
	}
}

func (eventProcessor *EventProcessor) updateStateMachine(event Event) {
	updEvent, ok := event.(*UpdateStateEvent)
	if !ok {
		return
	}
	server := eventProcessor.server
	activeState := server.stateHandler.GetActiveState()
	switch activeState.ID() {
	case FollowerID:
		eventProcessor.processFollower(updEvent)
	case CandidateID:
		eventProcessor.processCandidate(updEvent)
	case LeaderID:
		eventProcessor.processLeader(updEvent)
	}
}

func (eventProcessor *EventProcessor) updateServerParams(event *ServerRequestEvent) {
	payload := event.Payload()
	param, ok := payload.(*UpdateServerPayload)
	if !ok {
		return
	}
	server := eventProcessor.server
	log.Debug(server.ID, "Updating server params: ", server.State.Term, "->", param.Term)
	if server.State.Term < param.Term {
		server.State.UpdateTerm(param.Term)
	}
}

func (eventProcessor *EventProcessor) processFollower(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.ID() {
	case BecomeCandidate:
		log.Println(server.ID, "Becoming candidate")
		server.stateHandler.ChangeState(CandidateID)
	case BecomeFollower:
		server.stateHandler.ChangeState(FollowerID)
	}
}

func (eventProcessor *EventProcessor) processCandidate(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.ID() {
	case BecomeLeader:
		server.stateHandler.ChangeState(LeaderID)
	case BecomeFollower:
		log.Println(server.ID, "EVENT loop Becoming follower")
		server.stateHandler.ChangeState(FollowerID)
	}
}

func (eventProcessor *EventProcessor) processLeader(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.ID() {
	case BecomeFollower:
		log.Println(server.ID, " LEADER -> FOLLOWER transition", server.State.Term)
		server.stateHandler.ChangeState(FollowerID)
	}
}
