package raft

type eventProcessor func(data interface{}) error

// State represents the set of methods that should be implemented by the corresponding states
type State interface {
	ID() int
	Name() string
	OnInit(data interface{}, raftConfig NodeConfig) error
	OnStateStarted() error
	OnStateFinished() error
	Process(eventID uint16, data interface{}) error
}

// SmState is abstract class that contains common fields for States(e.g. Leader,Follower,Candidate)
type SmState struct {
	id   int
	name string
}

//ID the method that returns logical ID
func (state *SmState) ID() int {
	return state.id
}

// Name returns the state Name
func (state *SmState) Name() string {
	return state.name
}

// String is toString function
func (state *SmState) String() string {
	return state.name
}
