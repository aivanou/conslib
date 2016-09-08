package main

import (
	"log"
	"time"
)


const (
	RESET_TIMER = uint16(10)
	BECOME_FOLLOWER = uint16(11)
	BECOME_LEADER = uint16(12)
	BECOME_CANDIDATE = uint16(13)
	REGISTER_SERVER = uint16(14)
	UPDATE_SERVER = uint16(15)
	CHAINED_EVENT = uint16(16)
	WRITE_LOG = uint16(17)
)


type EventLoop interface {
	ProcessEvents() error
	Trigger(event Event) error
}

type EventProcessor struct {
	server       *RaftNode
	eventChannel chan Event
}

func NewEventProcessor(server *RaftNode) EventLoop {
	eventChannel := make(chan Event, 100)
	eventProcessor := &EventProcessor{server, eventChannel}
	return eventProcessor
}

func (eventProcessor *EventProcessor) ProcessEvents() error {
	log.Println("Starting event loop for server: ", eventProcessor.server.Id)
	server := eventProcessor.server
	server.stateHandler.GetActiveState().OnStateStarted()
	for {
		event := <-eventProcessor.eventChannel
		eventProcessor.processEvent(event)
	}
	return nil
}

func (eventProcessor *EventProcessor) Trigger(event Event) error {
	go func(loop *EventProcessor, ev Event) {
		loop.eventChannel <- event
	}(eventProcessor, event)
	return nil
}

func Chain(parent Event, child Event) Event {
	return &ChainedEvent{ServerEvent{CHAINED_EVENT, parent.EventTriggeredTime()}, parent, child}
}

func NewUpdateStateEvent(id uint16, time time.Time) Event {
	return &UpdateStateEvent{ServerEvent{id, time}}
}

func NewUpdateParamsEvent(id uint16, time time.Time, payload interface{}) Event {
	return &ServerRequestEvent{ServerEvent{id, time}, payload}}


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
	switch  event.(type){
	case *ChainedEvent:
		eventProcessor.processChainedEvent(event.(*ChainedEvent))
	case *ServerRequestEvent:
		eventProcessor.processServerRequestEvent(event.(*ServerRequestEvent))
	case *UpdateStateEvent:
		eventProcessor.updateStateMachine(event)
	default:
		log.Println(eventProcessor.server.Id, "Unrecognized event: ", event.Id())
	}
}

func (eventProcessor *EventProcessor) processServerRequestEvent(event *ServerRequestEvent) {
	switch {
	case event.Id() == UPDATE_SERVER:
		eventProcessor.updateServerParams(event)
	default:
		eventProcessor.server.stateHandler.GetActiveState().Process(event.Id(), event.Payload())
	}
}

func (eventProcessor *EventProcessor) updateStateMachine(event Event) {
	//	if !isLaterThan(server.LastChangedStateTime, event.EventTriggeredTime) {
	//		return
	//	}
	updEvent, ok := event.(*UpdateStateEvent)
	if !ok {
		return
	}
	server := eventProcessor.server
	activeState := server.stateHandler.GetActiveState()
	switch  activeState.Id(){
	case FOLLOWER_ID:
		eventProcessor.processFollower(updEvent)
	case CANDIDATE_ID:
		eventProcessor.processCandidate(updEvent)
	case LEADER_ID:
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
	log.Println(server.Id, "Updating server params: ", server.State.Term, "->", param.Term)
	if server.State.Term < param.Term {
		server.State.UpdateTerm(param.Term)
	}
}

func (eventProcessor *EventProcessor) processFollower(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.Id() {
	case BECOME_CANDIDATE:
		log.Println(server.Id, "Becoming candidate")
		server.stateHandler.ChangeState(CANDIDATE_ID)
	case BECOME_FOLLOWER:
		server.stateHandler.ChangeState(FOLLOWER_ID)
	}
}

func (eventProcessor *EventProcessor) processCandidate(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.Id() {
	case BECOME_LEADER:
		server.stateHandler.ChangeState(LEADER_ID)
	case BECOME_FOLLOWER:
		log.Println(server.Id, "EVENT loop Becoming follower")
		server.stateHandler.ChangeState(FOLLOWER_ID)
	}
}

func (eventProcessor *EventProcessor) processLeader(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.Id() {
	case BECOME_FOLLOWER:
		log.Println(server.Id, " LEADER -> FOLLOWER transition", server.State.Term)
		server.stateHandler.ChangeState(FOLLOWER_ID)
	}
}