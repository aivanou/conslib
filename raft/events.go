package main
import "time"

type Event interface {
	Id() uint16
	EventTriggeredTime() time.Time
}

type ServerEvent struct {
	id                 uint16
	eventTriggeredTime time.Time
}

type UpdateServerPayload struct {
	Term uint64
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

type ServerRequestEvent struct {
	Event
	payload interface{}
}

func (event *ServerRequestEvent) Payload() interface{} {
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