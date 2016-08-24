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

type Event struct {
	Id                 uint16
	EventTriggeredTime time.Time
	Payload            interface{}
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
		case event.Id == UPDATE_SERVER:
			updateServerParams(server, event)
		default:
			updateStateMachine(server, event)
		}
	}
}

func updateStateMachine(server *Info, event *Event) {
	//	if !isLaterThan(server.LastChangedStateTime, event.EventTriggeredTime) {
	//		return
	//	}
	switch  {
	case server.State == FOLLOWER:
		processFollower(server, event)
	case server.State == CANDIDATE:
		processCandidate(server, event)
	case server.State == LEADER:
		processLeader(server, event)
	}
}

func updateServerParams(server *Info, event *Event) {
	payload := event.Payload
	param, ok := payload.(*UpdateServerEvent)
	if !ok {
		return
	}
	log.Println(server.Id, "Updating server params: ", server.Term, "->", param.Term)
	if server.Term < param.Term {
		server.Term = param.Term
	}
	if param.StateChangedEvent != nil {
		log.Println(server.Id, "Sending next event: ", param.StateChangedEvent.Id)
		server.EventChannel <- param.StateChangedEvent
	}
}

func processFollower(server *Info, event *Event) {
	switch event.Id {
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

func processCandidate(server *Info, event *Event) {
	switch event.Id {
	case BECOME_LEADER:
		server.State = LEADER
		startLeader(server, server.Servers)
	case BECOME_FOLLOWER:
		log.Println(server.Id, "EVENT loop Becoming follower")
		server.State = FOLLOWER
		startFollower(server)
	}
}

func processLeader(server *Info, event *Event) {
	switch event.Id {
	case BECOME_FOLLOWER:
		log.Println(server.Id, " LEADER -> FOLLOWER transition", server.Term)
		server.State = FOLLOWER
		stopLeader(server, nil)
		startFollower(server)
	}
}

type UpdateServerEvent struct {
	Term              int
	StateChangedEvent *Event
}

type EmptyEvent int

func isLaterThan(oldTime, newTime time.Time) bool {
	cmp := compareTime(newTime, oldTime)
	return cmp >= 0
}

func compareTime(t1, t2 time.Time) int {
	return compareInt64(t1.UnixNano(), t2.UnixNano())
}

func compareInt64(v1, v2 int64) int {
	if v1 == v2 {
		return 0
	} else if v1 > v2 {
		return 1
	}else {
		return 2
	}
}