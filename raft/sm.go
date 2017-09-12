package raft

import "math/rand"

// Events
const (
	FollowerID  = 1
	CandidateID = 2
	LeaderID    = 3
)

// StateHandler s
type StateHandler interface {
	GetActiveState() State
	GetStateByID(id int) State
	ChangeState(newStateID int) error
}

// NodeStateHandler contains parameters relevant to state handler
type NodeStateHandler struct {
	states      map[int]State // contains a map of states
	activeState State         // the active state
}

// NewStateHandler creates a new state handler
func NewStateHandler(raftNode *Node, raftConfig NodeConfig) StateHandler {
	rand.Seed(int64(hash(raftNode.ID)))
	followerState := &FollowerState{SmState{FollowerID, "FOLLOWER"},
		randomDuration(raftConfig.Follower.Timeout), nil, raftNode, make(map[uint16]eventProcessor)}
	candidateState := &CandidateState{SmState{CandidateID, "CANDIDATE"}, raftNode}
	leaderState := NewLeaderState(SmState{LeaderID, "LEADER"}, raftNode)
	states := make(map[int]State)
	states[FollowerID] = followerState
	states[CandidateID] = candidateState
	states[LeaderID] = leaderState
	for _, state := range states {
		state.OnInit(raftNode, raftConfig)
	}
	return &NodeStateHandler{states, followerState}
}

// GetActiveState returns the active state
func (handler *NodeStateHandler) GetActiveState() State {
	return handler.activeState
}

// GetStateByID returns the state based on id
func (handler *NodeStateHandler) GetStateByID(id int) State {
	return handler.states[id]
}

// ChangeState changes a state to the new state that has newStateID
func (handler *NodeStateHandler) ChangeState(newStateID int) error {
	newState := handler.GetStateByID(newStateID)
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
