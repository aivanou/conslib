package protocol

type Receiver interface {
	RegisterServer(args *RegisterServerArgs, result *RegisterServerResult) error
	AppendEntries(args *AppendArgs, result *AppendResult) error
	RequestVote(args *RequestArgs, result *RequestResult) error
	WriteLogRequest(args *WriteLogRequest, result *WriteLogResponse) error
	SnapshotRequest(args *NodeSnapshotRequest, result *NodeSnapshotResponse) error
}

type RPCReceiver struct {
	protocol         Protocol
	raftEventHandler RaftEventHandler
}

func (receiver *RPCReceiver) AppendEntries(args *AppendArgs, result *AppendResult) error {
	err := receiver.raftEventHandler.OnAppendEntriesReceived(args, result)
	return err
}

func (receiver *RPCReceiver) RequestVote(args *RequestArgs, result *RequestResult) error {
	err := receiver.raftEventHandler.OnRequestVoteReceived(args, result)
	return err
}

func (receiver *RPCReceiver) WriteLogRequest(args *WriteLogRequest, result *WriteLogResponse) error {
	err := receiver.raftEventHandler.OnWriteLogRequestReceived(args, result)
	return err
}

func (receiver *RPCReceiver) SnapshotRequest(args *NodeSnapshotRequest, result *NodeSnapshotResponse) error {
	return receiver.raftEventHandler.OnSnapshotRequestReceived(args, result)
}
