package main

import (
	"errors"
	"time"
)

type WriteRequest struct {
	taskID       string
	data         uint64
	responseChan chan uint32
}

type WriteResponse struct {
	success uint32
}

func NewLeaderState(nodeState NodeState, raftNode *RaftNode) *LeaderState {
	leaderState := new(LeaderState)
	leaderState.raftNode = raftNode
	leaderState.NodeState = nodeState
	return leaderState
}

type LeaderState struct {
	NodeState
	peers                map[string]*Peer
	raftNode             *RaftNode
	requestReceiver      chan *WriteRequest
	replicationReceiver  chan *ReplicationResult
	heartbeat            chan string
	stop                 chan bool
	replication          *Replication
	eventFunctions       map[uint16]eventProcessor
	ongoingHeartbeatTask map[string]bool
	activeTasks          map[string]chan uint32
}

// OnInit blabla
func (state *LeaderState) OnInit(data interface{}, raftConfig *RaftConfig) error {
	slaves := make(map[string]*Peer)
	for id := range state.raftNode.Servers {
		logItem := state.raftNode.State.Store.LastLogItem()
		ind := uint32(1)
		if logItem != nil {
			ind = logItem.Index + 1
		}
		slaves[id] = &Peer{id, ind, 0, time.Duration(raftConfig.Leader.PeerTimeout), nil}
	}
	state.eventFunctions = make(map[uint16]eventProcessor)
	state.activeTasks = make(map[string]chan uint32)
	state.peers = slaves
	state.requestReceiver = make(chan *WriteRequest, 100)
	state.replicationReceiver = make(chan *ReplicationResult, 100)
	state.heartbeat = make(chan string, 100)
	state.stop = make(chan bool, 100)
	state.ongoingHeartbeatTask = make(map[string]bool)
	state.eventFunctions[WRITE_LOG] = state.processWriteLogRequest
	state.replication = NewReplication(state)
	go state.processLoop()
	return nil
}

func (state *LeaderState) NodeId() string {
	return state.raftNode.ID
}

func (state *LeaderState) processWriteLogRequest(data interface{}) error {
	req, ok := data.(*WriteRequest)
	if !ok {
		return errors.New("Incorrect input data for Write request")
	}
	go func() { state.requestReceiver <- req }()
	return nil
}

type Peer struct {
	ID         string
	NextIndex  uint32
	MatchIndex uint32
	Duration   time.Duration
	Ticker     *time.Ticker
}

func (state *LeaderState) Process(eventId uint16, data interface{}) error {
	err := state.eventFunctions[eventId](data)
	return err
}

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
				state.replication.ReplicateToPeer(state.peers[peerID])
			}
		case req := <-state.requestReceiver:
			log.Debug(server.ID, "Received request from client: ", req.taskID)
			state.activeTasks[req.taskID] = req.responseChan
			state.raftNode.State.Store.Append(req.data, state.raftNode.State.Term)
			lastLogItem := state.raftNode.State.Store.LastLogItem()
			for _, peer := range state.peers {
				peer.NextIndex = lastLogItem.Index
			}
			state.replication.ReplicateToAll(req.taskID, uint32(len(state.peers)))
		case finishedTask := <-state.replicationReceiver:
			taskID := finishedTask.TaskId
			if _, ok := state.activeTasks[taskID]; !ok {
				delete(state.ongoingHeartbeatTask, finishedTask.TaskId)
			} else {
				lastLogItem := state.raftNode.State.Store.LastLogItem()
				for _, peer := range state.peers {
					peer.NextIndex = lastLogItem.Index + 1
				}
				state.raftNode.State.UpdateCommitIndex(lastLogItem.Index)
				log.Debug(server.ID, "Finished task: ", taskID, " data: ", finishedTask.SuccessCount)
				state.activeTasks[taskID] <- finishedTask.SuccessCount
				delete(state.activeTasks, taskID)
			}
		case <-state.stop:
			state.replication.Stop()
			close(state.replicationReceiver)
			close(state.stop)
			close(state.requestReceiver)
		}
	}
}

func (state *LeaderState) OnStateStarted() error {
	state.startHeartbeat()
	return nil
}

func (state *LeaderState) OnStateFinished() error {
	state.stopHeartbeat()
	return nil
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
