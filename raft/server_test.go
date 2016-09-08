package main

import (
	"testing"
	"consensus/raft/protocol"
	"fmt"
	"consensus/raft/logstore"
	"github.com/stretchr/testify/assert"
)

func TestOnAppendEntriesReceived(t *testing.T) {

	s := new(RaftNode)
	s.State = &RaftState{1, "", 0, 0, 0, logstore.NewLogStore()}
	shandler := &MockStateHandler{&MockState{FOLLOWER_ID, "FOLLOWER", make(map[int]int)}}
	s.stateHandler = shandler
	mockEventLoop := &MockEventLoop{make(map[uint16]int)}
	s.eventProcessor = mockEventLoop
	s.State.Term = 1
	res := new(protocol.AppendResult)
	err := s.OnAppendEntriesReceived(&protocol.AppendArgs{0, "id2", 1, 1, 1, make([]logstore.LogItem, 10)}, res)
	assert.Nil(t, err)
	assert.False(t, res.Success || res.Term != s.State.Term, "Received success when expected to receive fail")
	err = s.OnAppendEntriesReceived(&protocol.AppendArgs{1, "id2", 1, 1, 1, make([]logstore.LogItem, 0, 10)}, res)
	assert.Nil(t, err)
	fmt.Println(mockEventLoop.eventsCount)
	assert.Equal(t, 1, mockEventLoop.eventsCount[RESET_TIMER], "RESET_TIMER wasn't triggered")
	assert.Equal(t, 1, mockEventLoop.eventsCount[UPDATE_SERVER], "UPDATE_SERVER wasn't triggered")
	assert.True(t, res.Success)
	err = s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 1, 1, 1, make([]logstore.LogItem, 0, 10)}, res)
	assert.Equal(t, 1, mockEventLoop.eventsCount[BECOME_FOLLOWER], "BECOME_FOLLOWER wasn't triggered")
	assert.Equal(t, 2, mockEventLoop.eventsCount[UPDATE_SERVER], "UPDATE_SERVER wasn't triggered")
	assert.True(t, res.Success)
}

func TestAppendLogItems(t *testing.T) {
	s := new(RaftNode)
	s.State = &RaftState{1, "", 0, 0, 0, logstore.NewLogStore()}
	shandler := &MockStateHandler{&MockState{FOLLOWER_ID, "FOLLOWER", make(map[int]int)}}
	s.stateHandler = shandler
	mockEventLoop := &MockEventLoop{make(map[uint16]int)}
	s.eventProcessor = mockEventLoop
	s.State.Term = 1
	logSize := 10
	items := GenRandomLogItems(logSize, 1, 1)
	res := new(protocol.AppendResult)
	err := s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 1, 1, 1, items}, res)
	assert.Nil(t, err)
	assert.True(t, res.Success)
	store := s.State.Store
	lastLogItem := store.LastLogItem()
	assert.NotNil(t, lastLogItem)
	assert.Equal(t, uint32(10), lastLogItem.Index, "Last log item error")
	assert.Equal(t, uint32(10), store.Size())
	items = GenRandomLogItems(logSize, 1, 11)
	err = s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 10, 1, 10, items}, res)
	assert.Nil(t, err)
	assert.True(t, res.Success)
	lastLogItem = store.LastLogItem()
	assert.Equal(t, uint32(20), lastLogItem.Index, "Last log item error")
	assert.Equal(t, uint32(20), store.Size())
}

func TestAppendEntriesRPCRemoveLogItems(t *testing.T) {
	s := new(RaftNode)
	s.State = &RaftState{1, "", 0, 0, 0, logstore.NewLogStore()}
	shandler := &MockStateHandler{&MockState{FOLLOWER_ID, "FOLLOWER", make(map[int]int)}}
	s.stateHandler = shandler
	mockEventLoop := &MockEventLoop{make(map[uint16]int)}
	s.eventProcessor = mockEventLoop
	s.State.Term = 1
	store := s.State.Store
	logSize := 10
	items := GenRandomLogItems(logSize, 1, 1)
	res := new(protocol.AppendResult)
	s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 1, 1, 1, items}, res)
	res.Success = false
	for !store.IsEmpty() {
		s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 0, 1, 1, items}, res)
	}
	s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 1, 1, 1, items}, res)
	for !store.IsEmpty() {
		s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 1, 2, 1, items}, res)
	}
	assert.Equal(t, uint32(0), store.Size())
	ind := 10
	for ind > 3 {
		s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 1, 1, 1, items}, res)
		ind -= 1
	}
	items = GenRandomLogItems(logSize, 2, 4)
	s.OnAppendEntriesReceived(&protocol.AppendArgs{2, "id2", 4, 1, 1, items}, res)
	assert.Equal(t, uint32(14), store.Size())
}

func TestOnRequestVoteReceived(t *testing.T) {
	s := new(RaftNode)
	s.State = &RaftState{1, "", 0, 0, 0, logstore.NewLogStore()}
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

func (s *MockState) Process(eventId uint16, data interface{}) error {
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

func GenRandomLogItems(size int, term uint64, startIndex uint32) []logstore.LogItem {
	items := make([]logstore.LogItem, size)
	for i := 0; i < size; i++ {
		items[i] = logstore.LogItem{startIndex + uint32(i), uint64(i), term}
	}
	return items
}