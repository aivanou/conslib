package main

import (
	"testing"
	"consensus/raft/protocol"
	"fmt"
)

func TestOnAppendEntriesReceived(t *testing.T) {

	s := new(RaftNode)
	s.State = &RaftState{1, make([]uint64, 100), "", 0}
	shandler := &MockStateHandler{&MockState{FOLLOWER_ID, "FOLLOWER", make(map[int]int)}}
	s.stateHandler = shandler
	mockEventLoop := &MockEventLoop{make(map[uint16]int)}
	s.eventProcessor = mockEventLoop
	s.State.Term = 1
	res := new(protocol.AppendResult)
	err := s.OnAppendEntriesReceived(&protocol.AppendArgs{0, "id2", 1, 1, 1, make([]uint64, 10)}, res)
	if err != nil {
		t.Fatal("OnAppendEntriesReceived:", err)
	}
	if res.Success || res.Term != s.State.Term {
		t.Error("Received success when expected to receive fail, server term:", s.State.Term, "result term:", res.Term)
	}
	err = s.OnAppendEntriesReceived(&protocol.AppendArgs{1, "id2", 1, 1, 1, make([]uint64, 10)}, res)
	if err != nil {
		t.Fatal("OnAppendEntriesReceived:", err)
	}
	fmt.Println(mockEventLoop.eventsCount)
	if mockEventLoop.eventsCount[RESET_TIMER] != 1 {
		t.Error("RESET_TIMER wasn't triggered")
	}
	if mockEventLoop.eventsCount[UPDATE_SERVER] != 1 {
		t.Error("UPDATE_SERVER wasn't triggered")
	}
	if (!res.Success) {
		t.Error("Result should be success")
	}
	err = s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 1, 1, 1, make([]uint64, 10)}, res)
	if mockEventLoop.eventsCount[BECOME_FOLLOWER] != 1 {
		t.Error("BECOME_FOLLOWER wasn't triggered")
	}
	if mockEventLoop.eventsCount[UPDATE_SERVER] != 2 {
		t.Error("UPDATE_SERVER wasn't triggered")
	}
	if (!res.Success) {
		t.Error("Result should be success")
	}
}

func TestOnRequestVoteReceived(t *testing.T) {
	s := new(RaftNode)
	s.State = &RaftState{1, make([]uint64, 100), "", 1}
	shandler := &MockStateHandler{&MockState{CANDIDATE_ID, "CANDIDATE", make(map[int]int)}}
	s.stateHandler = shandler
	mockEventLoop := &MockEventLoop{make(map[uint16]int)}
	s.eventProcessor = mockEventLoop
	s.State.Term = 1
	res := new(protocol.RequestResult)
	s.OnRequestVoteReceived(&protocol.RequestArgs{1, 1, 1, ""}, res)
}

type MockState struct {
	id    int
	name  string
	calls map[int]int
}

func (s *MockState) Id() int {
	return s.id
}
func (s *MockState) Name() string {
	return s.name
}
func (s *MockState) OnInit(data interface{}) error {
	return nil
}

func (s *MockState) OnStateStarted() error {
	return nil
}

func (s *MockState) OnStateFinished() error {
	return nil
}

func (s *MockState) Process(eventId uint16) error {
	return nil
}

type MockStateHandler struct {
	as State
}

func (h *MockStateHandler) GetActiveState() State {
	return h.as
}
func (h *MockStateHandler) GetStateById(id int) State {
	return h.as
}

func (h *MockStateHandler) ChangeState(newStateId int) error {
	return nil
}


type MockEventLoop struct {
	eventsCount map[uint16]int
}

func (el *MockEventLoop) Trigger(event Event) error {
	if event.Id() == CHAINED_EVENT {
		chEvent := event.(*ChainedEvent)
		el.Trigger(chEvent.event)
		el.Trigger(chEvent.next)
	}
	_, ok := el.eventsCount[event.Id()]; if !ok {
		el.eventsCount[event.Id()] = 1
	}else {
		el.eventsCount[event.Id()] += 1
	}

	return nil
}

func (el *MockEventLoop) ProcessEvents() error {
	return nil
}