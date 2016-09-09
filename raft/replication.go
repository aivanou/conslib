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
	activeTasks      map[string]*ReplicationResult
}

const (
	SUCCESS = 1
	ERROR = 2
)

type ReplStatus struct {
	code   uint8
	taskId string
}

type ReplicationTask struct {
	peer   *Peer
	taskId string
	npeers uint32
}

type ReplicationResult struct {
	TaskId             string
	TotalCount         uint32
	ProcessedCount     uint32
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
	repl.activeTasks = make(map[string]*ReplicationResult)
	go repl.startReplicationProcess()
	return repl
}


func (repl *Replication) ReplicateToPeer(peer *Peer) {
	repl.replChan <- &ReplicationTask{peer, peer.Id, 1}
}

func (repl *Replication) ReplicateToAll(taskId string, npeers uint32) {
	//	repl.startReplicationTask(taskId, len(repl.leaderState.peers))
	for _, peer := range repl.leaderState.peers {
		repl.replChan <- &ReplicationTask{peer, taskId, npeers}
	}
}

func (repl *Replication) Stop() {
	repl.stop <- 1
}

func (repl *Replication) startReplicationProcess() {
	for {
		select {
		case replTask := <-repl.replChan:
			if _, ok := repl.activeTasks[replTask.taskId]; !ok {
				repl.activeTasks[replTask.taskId] = &ReplicationResult{replTask.taskId, replTask.npeers, 0, 0, 0, 0}
			}
			go repl.replicateToPeer(replTask)
		case peerResult := <-repl.peerReplReceiver:
			task := repl.activeTasks[peerResult.taskId]
			task.ProcessedCount += 1
			switch peerResult.code {
			case ERROR:
				task.FailedCount += 1
			case SUCCESS:
				task.SuccessCount += 1
			}
			if task.TotalCount == task.ProcessedCount {
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
	server := repl.leaderState.raftNode
	args := new(protocol.AppendArgs)
	args.LeaderCommit = server.State.CommitIndex
	args.PrevLogIndex = peer.NextIndex - 1
	logItem := server.State.Store.FindByIndex(peer.NextIndex - 1)
	if logItem == nil {
		args.PrevLogTerm = 1
	}else {
		args.PrevLogTerm = logItem.Term
	}
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