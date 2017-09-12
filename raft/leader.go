package raft

import (
	"time"

	"github.com/tierex/conslib/raft/protocol"
)

// LeaderState is an active state when the node becomes a leader
type LeaderState struct {
	SmState                               // extends the SmState
	raftNode             *Node            // reference to the raft node
	peers                map[string]*Peer //
	eventFunctions       map[uint16]eventProcessor
	ongoingHeartbeatTask map[string]bool // has true value for peers that have ongoing heartbeat.
	// each peer(follower) contains timer that every {duration} sends the request to the
	// heartbeat channel for sending AppendArgs message to the corresponding server.
	// If the destination sever did not reply in time, the  ongoingHeartbeatTask[peer.id] will have true value.
	// This will prevent from piling up messages for particular peer.
	heartbeat chan string // the channel for sending AppendArgs message
	stop      chan bool   // when the leader state starts, it initiases loop in the
	// background thread. As a retult, in order to switch from leader to another state,
	// the stop message should be sent to the channel for shutting down the process loop.
}

// Peer contains data for sending heartbeat and updating log storage for each follower
type Peer struct {
	ID         string        // ID of a peer, this is the same as Node.ID
	NextIndex  uint32        // TODO: imlement
	MatchIndex uint32        // TODO: imlement
	Duration   time.Duration // the period of time between consecutive heartbeats
	Ticker     *time.Ticker  // the timer for sending heartbeat
}

// NewLeaderState serves as a constructor for for a LeaderState
func NewLeaderState(state SmState, raftNode *Node) *LeaderState {
	leaderState := new(LeaderState)
	leaderState.raftNode = raftNode
	leaderState.SmState = state
	return leaderState
}

// OnInit is the event that occurs when the State Machine is initialized
func (state *LeaderState) OnInit(data interface{}, raftConfig NodeConfig) error {
	slaves := make(map[string]*Peer)
	for id := range state.raftNode.Servers {
		slaves[id] = &Peer{
			ID:         id,
			NextIndex:  1,
			MatchIndex: 0,
			Duration:   time.Duration(raftConfig.Leader.PeerTimeout),
			Ticker:     nil}
	}
	state.eventFunctions = make(map[uint16]eventProcessor)
	state.peers = slaves
	state.heartbeat = make(chan string, 100)
	state.stop = make(chan bool, 100)
	state.ongoingHeartbeatTask = make(map[string]bool)
	return nil
}

// OnStateStarted is an event that occurs when the StatMachine switches to the current state
func (state *LeaderState) OnStateStarted() error {
	go state.processLoop()
	state.startHeartbeat()
	return nil
}

// OnStateFinished is an event that occurs
// when the StatMachine switches to the other state from this state
func (state *LeaderState) OnStateFinished() error {
	state.stopHeartbeat()
	return nil
}

// NodeID returns the node Id
func (state *LeaderState) NodeID() string {
	return state.raftNode.ID
}

// Process processes the corresponding eventID
func (state *LeaderState) Process(eventID uint16, data interface{}) error {
	err := state.eventFunctions[eventID](data)
	return err
}

// Stop is called when the leater state is changed to follower.
func (state *LeaderState) Stop() error {
	state.stop <- true
	return nil
}

func (state *LeaderState) processLoop() {
	server := state.raftNode
	log.Debug(server.ID, " Starting leader process loop")
	for {
		select {
		case peerID := <-state.heartbeat:
			if _, ok := state.ongoingHeartbeatTask[peerID]; !ok {
				state.sendHeartbeat(state.peers[peerID])
			}
		case <-state.stop:
			close(state.stop)
		}
	}
}

func (state *LeaderState) startHeartbeat() {
	for _, peer := range state.peers {
		peer.Ticker = time.NewTicker(time.Millisecond * peer.Duration)
		go func(peer *Peer) {
			for _ = range peer.Ticker.C {
				state.heartbeat <- peer.ID
			}
		}(peer)
	}
}

func (state *LeaderState) stopHeartbeat() {
	for _, peer := range state.peers {
		if peer.Ticker != nil {
			peer.Ticker.Stop()
		}
	}
}

func (state *LeaderState) sendHeartbeat(peer *Peer) error {
	args := &protocol.AppendArgs{
		Term:     1,
		LeaderId: "",
	}
	state.ongoingHeartbeatTask[peer.ID] = true
	reply, err := state.raftNode.SendAppend(peer.ID, args)
	if err != nil {
		log.Println("Received error: ", err)
		return err
	}
	state.processReply(peer.ID, reply)
	return nil
}

func (state *LeaderState) processReply(ID string, reply *protocol.AppendResult) {
	state.ongoingHeartbeatTask[ID] = false
	if reply.Term > state.raftNode.State.Term {
		state.raftNode.eventProcessor.Trigger(NewUpdateStateEvent(BecomeFollower, time.Now()))
	}
}
