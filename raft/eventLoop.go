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

//type Event struct {
//	Id                 uint16
//	EventTriggeredTime time.Time
//	Payload            interface{}
//}

type Event interface {
	Id() uint16
	EventTriggeredTime() time.Time
}


type UpdateStateEvent struct {
	id                 uint16
	eventTriggeredTime time.Time
}

func (event *UpdateStateEvent) Id() uint16 {
	return event.id
}

func (event *UpdateStateEvent) EventTriggeredTime() time.Time {
	return event.eventTriggeredTime;
}

type UpdateParamsEvent struct {
	id                 uint16
	eventTriggeredTime time.Time
	payload            interface{}
}

func (event *UpdateParamsEvent) Id() uint16 {
	return event.id
}

func (event *UpdateParamsEvent) EventTriggeredTime() time.Time {
	return event.eventTriggeredTime;
}

func (event *UpdateParamsEvent) Payload() interface{} {
	return event.payload;
}


type EventLoop interface {
	ProcessEvents()
}

func (server *Info) ProcessEvents() {
	log.Println("Starting event loop for server: ", server.Id)
	startFollower(server)
	for {
		event := <-server.EventChannel
		switch  {
		case event.Id() == UPDATE_SERVER:
			updateServerParams(server, event)
		default:
			updateStateMachine(server, event)
		}
	}
}

func updateStateMachine(server *Info, event Event) {
	//	if !isLaterThan(server.LastChangedStateTime, event.EventTriggeredTime) {
	//		return
	//	}
	updEvent, ok := event.(*UpdateStateEvent)
	if !ok {
		return
	}
	switch  {
	case server.State == FOLLOWER:
		processFollower(server, updEvent)
	case server.State == CANDIDATE:
		processCandidate(server, updEvent)
	case server.State == LEADER:
		processLeader(server, updEvent)
	}
}

func updateServerParams(server *Info, event Event) {
	updEvent, ok := event.(*UpdateParamsEvent)
	if !ok {
		return
	}
	payload := updEvent.Payload()
	param, ok := payload.(*UpdateServerEvent)
	if !ok {
		return
	}
	log.Println(server.Id, "Updating server params: ", server.Term, "->", param.Term)
	if server.Term < param.Term {
		server.Term = param.Term
	}
	if param.StateChangedEvent != nil {
		log.Println(server.Id, "Sending next event: ", param.StateChangedEvent.Id())
		server.EventChannel <- param.StateChangedEvent
	}
}

func processFollower(server *Info, event *UpdateStateEvent) {
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

func processCandidate(server *Info, event *UpdateStateEvent) {
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

func processLeader(server *Info, event *UpdateStateEvent) {
	switch event.Id() {
	case BECOME_FOLLOWER:
		log.Println(server.Id, " LEADER -> FOLLOWER transition", server.Term)
		server.State = FOLLOWER
		stopLeader(server, nil)
		startFollower(server)
	}
}

type UpdateServerEvent struct {
	Term              int
	StateChangedEvent Event
}