package main
import (
	"time"
	"errors"
)


type WriteRequest struct {
	taskId       uint32
	data         uint64
	responseChan chan uint32
}

func NewLeaderState(nodeState NodeState, raftNode *RaftNode) *LeaderState {
	leaderState := new(LeaderState)
	leaderState.raftNode = raftNode
	leaderState.NodeState = nodeState
	return leaderState
}


type LeaderState struct {
	NodeState
	peers               []*Peer
	raftNode            *RaftNode
	requestReceiver     chan *WriteRequest
	replicationReceiver chan *ReplicationResult
	heartbeat           chan bool
	stop                chan bool
	replication         *Replication
	eventFunctions      map[uint16]eventProcessor
	activeTasks         map[uint32]chan uint32
	Duration            time.Duration
	Ticker              *time.Ticker
}


func (state *LeaderState) OnInit(data interface{}) error {
	slaves := make([]*Peer, 0, len(state.raftNode.Servers))
	for id, _ := range state.raftNode.Servers {
		logItem := state.raftNode.State.Store.LastLogItem()
		ind := uint32(0)
		if logItem != nil {
			ind = logItem.Index + 1
		}
		slave := &Peer{id, 100, nil, ind, 0}
		slaves = append(slaves, slave)
	}
	state.eventFunctions = make(map[uint16]eventProcessor)
	state.activeTasks = make(map[uint32]chan uint32)
	state.peers = slaves
	state.requestReceiver = make(chan *WriteRequest, 100)
	state.replicationReceiver = make(chan *ReplicationResult, 100)
	state.heartbeat = make(chan bool, 100)
	state.stop = make(chan bool, 100)
	state.eventFunctions[WRITE_LOG] = state.processWriteLogRequest
	state.replication = NewReplication(state)
	state.processLoop()
	return nil
}

func (state *LeaderState)processWriteLogRequest(data interface{}) error {
	req, ok := data.(*WriteRequest)
	if !ok {
		return errors.New("Incorrect input data for Write request")
	}
	state.requestReceiver <- req
	return nil
}


type Peer struct {
	Id         string
	Duration   time.Duration
	Ticker     *time.Ticker
	NextIndex  uint32
	MatchIndex uint32
}

func (state *LeaderState) Process(eventId uint16, data interface{}) error {
	state.eventFunctions[eventId](data)
	return nil
}

func (state *LeaderState) Stop() error {
	state.stop <- true
	return nil
}

func (state *LeaderState) processLoop() {
	for {
		select {
		case <- state.heartbeat:
			state.replication.ReplicateToAll(1)
		case req := <-state.requestReceiver:
			state.activeTasks[req.taskId] = req.responseChan
			state.raftNode.State.Store.Append(req.data, state.raftNode.State.Term)
			lastLogItem := state.raftNode.State.Store.LastLogItem()
			for _, peer := range state.peers {
				peer.NextIndex = lastLogItem.Index + 1
			}
			state.replication.ReplicateToAll(req.taskId)
		case finishedTask := <-state.replicationReceiver:
			taskId := finishedTask.TaskId
			_, ok := state.activeTasks[finishedTask.TaskId]
			if !ok {
				//error, log
			}else {
				state.activeTasks[taskId] <- finishedTask.SuccessCount
				delete(state.activeTasks, taskId)
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
	state.Ticker = time.NewTicker(time.Millisecond * state.Duration)
	go func() {
		for _ = range state.Ticker.C {
			state.heartbeat <- true
		}
	}()
}

func (state *LeaderState) stopHeartbeat() {
	state.Ticker.Stop()
}
