package main
import (
	"consensus/raft/protocol"
	"time"
)

type Replication struct {
	replChan         chan *ReplicationTask
	stop             chan int
	responseReceiver chan *ReplicationResult
	peerReplReceiver chan *ReplStatus
	leaderState      *LeaderState
	activeTasks      map[uint32]*ReplicationResult
}

const (
	SUCCESS = 1
	ERROR = 2
)

type ReplStatus struct {
	code   uint8
	taskId uint32
}

type ReplicationTask struct {
	peer   *Peer
	taskId uint32
}

type ReplicationResult struct {
	TaskId             uint32
	TotalCount         uint32
	SuccessCount       uint32
	FailedCount        uint32
	ReplicationTimeout uint32
}

func NewReplication(state *LeaderState) *Replication {
	repl := new(Replication)
	repl.replChan = make(chan *ReplicationTask)
	repl.peerReplReceiver = make(chan *ReplStatus)
	repl.stop = make(chan int)
	repl.responseReceiver = state.replicationReceiver
	repl.leaderState = state
	repl.activeTasks = make(map[uint32]*ReplicationResult)
	repl.startReplicationProcess(uint32(len(repl.leaderState.peers)))
	return repl
}


func (repl *Replication) ReplicateToAll(taskId uint32) {
	//	repl.startReplicationTask(taskId, len(repl.leaderState.peers))
	for _, peer := range repl.leaderState.peers {
		repl.replChan <- &ReplicationTask{peer, taskId}
	}
}

func (repl *Replication) Stop() {
	repl.stop <- 1
}

func (repl *Replication) startReplicationProcess(npeers uint32) {
	for {
		select {
		case replTask := <-repl.replChan:
			_, ok := repl.activeTasks[replTask.taskId]
			if !ok {
				repl.activeTasks[replTask.taskId] = &ReplicationResult{replTask.taskId, 0, 0, 0, 0}
			}
			go repl.replicateToPeer(replTask)
		case peerResult := <-repl.peerReplReceiver:
			task := repl.activeTasks[peerResult.taskId]
			task.TotalCount += 1
			switch peerResult.code {
			case ERROR:
				task.FailedCount += 1
			case SUCCESS:
				task.SuccessCount += 1
			}
			if task.TotalCount == npeers {
				repl.responseReceiver <- task
				delete(repl.activeTasks, peerResult.taskId)
			}
		case <-repl.stop:
			close(repl.peerReplReceiver)
			close(repl.replChan)
			close(repl.stop)
			return
		}
	}
}

func (repl *Replication) replicateToPeer(replTask *ReplicationTask) {
	server := repl.leaderState.raftNode
	args := repl.buildAppendArgs(replTask.peer)
	reply, err := repl.sendToPeer(replTask.peer.Id, args)
	switch  {
	case err != nil:
		repl.peerReplReceiver <- &ReplStatus{ERROR, replTask.taskId}
	case reply.Term > server.State.Term:
		eventLoop := server.eventProcessor
		eventLoop.Trigger(NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
		repl.peerReplReceiver <- &ReplStatus{ERROR, replTask.taskId}
	//TODO: repl.leaderState.stop <- true
	//change to follower
	case !reply.Success:
		replTask.peer.NextIndex -= 1
		repl.replChan <- replTask
	default:
		replTask.peer.MatchIndex = replTask.peer.NextIndex - 1
		repl.peerReplReceiver <- &ReplStatus{SUCCESS, replTask.taskId}
	}
}

func (repl *Replication) buildAppendArgs(peer *Peer) *protocol.AppendArgs {
	state := repl.leaderState
	server := state.raftNode
	args := new(protocol.AppendArgs)
	args.LeaderCommit = server.State.CommitIndex
	args.PrevLogIndex = peer.NextIndex - 1
	args.PrevLogTerm = server.State.Store.FindByIndex(peer.NextIndex - 1).Term
	args.LeaderId = server.Id
	args.Term = server.State.Term
	args.Entries = server.State.Store.GetAllItemsAfter(peer.NextIndex - 1)
	return args
}

func (repl *Replication) sendToPeer(destServerId string, args *protocol.AppendArgs) (*protocol.AppendResult, error) {
	server := repl.leaderState.raftNode
	reply, err := server.SendAppend(destServerId, args)
	return reply, err
}