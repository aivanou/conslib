package raft

import "time"

// the event ids
const (
	ResetTimer      = uint16(10)
	BecomeFollower  = uint16(11)
	BecomeLeader    = uint16(12)
	BecomeCandidate = uint16(13)
	RegisterServer  = uint16(14)
	UpdateServer    = uint16(15)
	ChainEvent      = uint16(16)
)

// Event is an interface for describe different event pool events
type Event interface {
	ID() uint16
	EventTriggeredTime() time.Time
}

// NodeEvent is a data that is associated with server
type NodeEvent struct {
	id                 uint16
	eventTriggeredTime time.Time
}

// UpdateServerPayload is a payload for processing server state updates(e.g. term update)
type UpdateServerPayload struct {
	Term uint64
}

// ID is a unique event id
func (event NodeEvent) ID() uint16 {
	return event.id
}

// EventTriggeredTime returns the time when the event was triggered
func (event NodeEvent) EventTriggeredTime() time.Time {
	return event.eventTriggeredTime
}

// UpdateStateEvent represents the event that updates state machine
type UpdateStateEvent struct {
	Event // extends the Event interface
}

// ServerRequestEvent represents the event that updates the node state data
type ServerRequestEvent struct {
	Event
	payload interface{}
}

// Payload returns the payload
func (event *ServerRequestEvent) Payload() interface{} {
	return event.payload
}

// ChainedEvent represents the event that contains multiple events
type ChainedEvent struct {
	NodeEvent
	event Event
	next  Event
}

// Event returns the main event
func (event *ChainedEvent) Event() Event {
	return event.event
}

// NextEvent returns the next event in chain
func (event *ChainedEvent) NextEvent() Event {
	return event.next
}
