package protocol

import (
	"log"
	"net/rpc"
)

type Sender interface {
	SendAppend(args *AppendArgs) (*AppendResult, error)
	SendRequestVote(args *RequestArgs) (*RequestResult, error)
	SendRegisterServer(id string, host *Host) (*RegisterServerResult, error)
}

type RPCSender struct {
	protocol  *RPCProtocol
	RpcClient *rpc.Client
	destHost  *Host
}

func (sender *RPCSender) SendRequestVote(args *RequestArgs) (*RequestResult, error) {
	var reply RequestResult
	log.Println(args.CandidateId, " :Sending request vote to: ", sender.destHost)
	err := sender.RpcClient.Call("RPCReceiver.RequestVote", args, &reply)
	if err != nil {
		log.Println("Error during Info.RequestVote:", err)
		return nil, err
	}
	return &reply, nil
}

func (sender *RPCSender) SendRegisterServer(id string, host *Host) (*RegisterServerResult, error) {
	var registerServerArgs = &RegisterServerArgs{id, host.Domain, host.Port}
	var reply RegisterServerResult
	err := sender.RpcClient.Call("RPCReceiver.RegisterServer", registerServerArgs, &reply)
	if err != nil {
		log.Println("Error while sending RendRegisterServer towards :", host, err)
		return nil, err
	}
	return &reply, nil
}

func (sender *RPCSender) SendAppend(args *AppendArgs) (*AppendResult, error) {
	var reply AppendResult
	err := sender.RpcClient.Call("RPCReceiver.AppendEntries", args, &reply)
	if err != nil {
		log.Println("SendAppend RPC error:", err)
		return nil, err
	}
	return &reply, nil
}
