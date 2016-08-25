package protocol
import (
	"log"
	"net/rpc"
)

type Sender interface {
	SendAppend(leaderId string, term int) (*AppendResult, error)
	SendRequestVote(id string, term int) (*RequestResult, error)
	SendRegisterServer(id string, host *Host) (*RegisterServerResult, error)
}


type RPCSender struct {
	protocol  *RPCProtocol
	RpcClient *rpc.Client
	destHost  *Host
}

func (sender *RPCSender)SendRequestVote(id string, term int) (*RequestResult, error) {
	var requestArgs = &RequestArgs{term, 0, 0, id}
	var reply RequestResult
	log.Println(id, " :Sending request vote to: ", sender.destHost)
	err := sender.RpcClient.Call("RPCReceiver.RequestVote", requestArgs, &reply)
	if err != nil {
		log.Fatal("Error during Info.RequestVote:", err)
		return nil, err
	}
	return &reply, nil
}


func (sender *RPCSender) SendRegisterServer(id string, host *Host) (*RegisterServerResult, error) {
	var registerServerArgs = &RegisterServerArgs{id, host.Domain, host.Port}
	var reply RegisterServerResult
	err := sender.RpcClient.Call("RPCReceiver.RegisterServer", registerServerArgs, &reply)
	if err != nil {
		log.Fatal("Error while sending RendRegisterServer towards :", host, err)
		return nil, err
	}
	return &reply, nil
}

func (sender *RPCSender) SendAppend(leaderId string, term int) (*AppendResult, error) {
	var appendArgs = &AppendArgs{term, leaderId}
	var reply AppendResult
	log.Println(leaderId, "Sending APPEND RPC")
	err := sender.RpcClient.Call("RPCReceiver.AppendEntries", appendArgs, &reply)
	if err != nil {
		log.Fatal("SendAppend RPC error:", err)
		return nil, err
	}
	return &reply, nil
}