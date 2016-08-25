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
)


type Event interface {
	Id() uint16
	EventTriggeredTime() time.Time
}

type ServerEvent struct {
	id                 uint16
	eventTriggeredTime time.Time
}

func (event ServerEvent) Id() uint16 {
	return event.id
}

func (event ServerEvent) EventTriggeredTime() time.Time {
	return event.eventTriggeredTime;
}


type UpdateStateEvent struct {
	Event
}

type UpdateParamsEvent struct {
	Event
	payload interface{}
}

func (event *UpdateParamsEvent) Payload() interface{} {
	return event.payload;
}

type ChainedEvent struct {
	ServerEvent
	event Event
	next  Event
}

func (event *ChainedEvent) Event() Event {
	return event.event
}

func (event *ChainedEvent) NextEvent() Event {
	return event.next
}

type EventLoop interface {
	ProcessEvents() error
	NewUpdateStateEvent(id uint16, time time.Time) Event
	NewUpdateParamsEvent(id uint16, time time.Time, payload interface{}) Event
	Chain(parent Event, child Event) Event
	Trigger(event Event) error
}

type EventProcessor struct {
	server       *Info
	eventChannel chan Event
}

func NewEventProcessor(server *Info) EventLoop {
	eventChannel := make(chan Event, 100)
	eventProcessor := &EventProcessor{server, eventChannel}
	return eventProcessor
}

func (eventProcessor *EventProcessor) ProcessEvents() error {
	log.Println("Starting event loop for server: ", eventProcessor.server.Id)
	startFollower(eventProcessor.server)
	for {
		event := <-eventProcessor.eventChannel
		chEvent, ok := event.(*ChainedEvent)
		if ok {
			eventProcessor.processChainedEvent(chEvent)
		} else {
			eventProcessor.processEvent(event)
		}
	}
	return nil
}

func (eventProcessor *EventProcessor) Chain(parent Event, child Event) Event {
	return &ChainedEvent{ServerEvent{1, parent.EventTriggeredTime()}, parent, child}
}

func (eventProcessor *EventProcessor) Trigger(event Event) error {
	go func(loop *EventProcessor, ev Event) {
		loop.eventChannel <- event
	}(eventProcessor, event)
	return nil
}

func (eventProcessor *EventProcessor) NewUpdateStateEvent(id uint16, time time.Time) Event {
	return &UpdateStateEvent{ServerEvent{id, time}}
}

func (eventProcessor *EventProcessor) NewUpdateParamsEvent(id uint16, time time.Time, payload interface{}) Event {
	return &UpdateParamsEvent{ServerEvent{id, time}, payload}}


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
	switch  {
	case event.Id() == UPDATE_SERVER:
		eventProcessor.updateServerParams(event)
	default:
		eventProcessor.updateStateMachine(event)
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
	switch  {
	case server.State == FOLLOWER:
		eventProcessor.processFollower(updEvent)
	case server.State == CANDIDATE:
		eventProcessor.processCandidate(updEvent)
	case server.State == LEADER:
		eventProcessor.processLeader(updEvent)
	}
}

func (eventProcessor *EventProcessor) updateServerParams(event Event) {
	updEvent, ok := event.(*UpdateParamsEvent)
	if !ok {
		return
	}
	payload := updEvent.Payload()
	param, ok := payload.(*UpdateServerPayload)
	if !ok {
		return
	}
	server := eventProcessor.server
	log.Println(server.Id, "Updating server params: ", server.Term, "->", param.Term)
	if server.Term < param.Term {
		server.Term = param.Term
	}
}

func (eventProcessor *EventProcessor) processFollower(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.Id() {
	case RESET_TIMER:
		log.Println(server.Id, "Resetting timer")
		resetTimer(server)
	case BECOME_CANDIDATE:
		server.State = CANDIDATE
		log.Println(server.Id, "Becoming candidate")
		startCandidate(server)
	case BECOME_FOLLOWER:
		server.State = FOLLOWER
		startFollower(server)
	}
}

func (eventProcessor *EventProcessor) processCandidate(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.Id() {
	case BECOME_LEADER:
		server.State = LEADER
		startLeader(server, server.Servers)
	case BECOME_FOLLOWER:
		log.Println(server.Id, "EVENT loop Becoming follower")
		server.State = FOLLOWER
		startFollower(server)
	}
}

func (eventProcessor *EventProcessor) processLeader(event *UpdateStateEvent) {
	server := eventProcessor.server
	switch event.Id() {
	case BECOME_FOLLOWER:
		log.Println(server.Id, " LEADER -> FOLLOWER transition", server.Term)
		server.State = FOLLOWER
		stopLeader(server, nil)
		startFollower(server)
	}
}

type UpdateServerPayload struct {
	Term int
}