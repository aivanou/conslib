package main

import "math/rand"

type State interface {
	Id() int
	Name() string
	OnInit(data interface{}) error
	OnStateStarted() error
	OnStateFinished() error
	Process(eventId uint16, data interface{}) error
}

type NodeState struct {
	id   int
	name string
}

func (state *NodeState) Id() int {
	return state.id
}

func (state *NodeState) Name() string {
	return state.name
}


func (nodeState *NodeState) String() string {
	return nodeState.name
}

const (
	FOLLOWER_ID = 1
	CANDIDATE_ID = 2
	LEADER_ID = 3
)

type StateHandler interface {
	GetActiveState() State
	GetStateById(id int) State
	ChangeState(newStateId int) error
}

type NodeStateHandler struct {
	states      map[int]State
	activeState State
}

func NewStateHandler(raftNode *RaftNode) StateHandler {
	rand.Seed(int64(hash(raftNode.Id)))
	followerState := &FollowerState{NodeState{FOLLOWER_ID, "FOLLOWER"}, randomDuration(1000), nil, raftNode, make(map[uint16]eventProcessor)}
	candidateState := &CandidateState{NodeState{CANDIDATE_ID, "CANDIDATE"}, raftNode}
	leaderState := NewLeaderState(NodeState{LEADER_ID, "LEADER"}, raftNode)
	states := make(map[int]State)
	states[FOLLOWER_ID] = followerState
	states[CANDIDATE_ID] = candidateState
	states[LEADER_ID] = leaderState
	for _, state := range states {
		state.OnInit(raftNode)
	}
	return &NodeStateHandler{states, followerState}
}

func (handler *NodeStateHandler) GetActiveState() State {
	return handler.activeState;
}

func (handler *NodeStateHandler) GetStateById(id int) State {
	return handler.states[id];
}

func (handler *NodeStateHandler) ChangeState(newStateId int) error {
	newState := handler.GetStateById(newStateId)
	currentState := handler.GetActiveState()
	err := currentState.OnStateFinished()
	if err != nil {
		return err
	}
	err = newState.OnStateStarted()
	if err != nil {
		return err
	}
	handler.activeState = newState
	return nil
}