package protocol

type Receiver interface {
	RegisterServer(args *RegisterServerArgs, result *RegisterServerResult) error
	AppendEntries(args *AppendArgs, result *AppendResult) error
	RequestVote(args *RequestArgs, result *RequestResult) error
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
