package main
import (
	"os"
	"strconv"
	"net/rpc"
	"log"
	"fmt"
	"consensus/raft/protocol"
)

func main() {
	args := os.Args[1:]
	nservers, _ := strconv.Atoi(args[0])
	sport, _ := strconv.Atoi(args[1])
	servers := make([]string, nservers)
	clients := make([]*rpc.Client, nservers)
	for i := 0; i < nservers; i++ {
		servers[i] = fmt.Sprintf("%s:%d", "localhost", sport + i)
		clients[i] = newRpcClient(servers[i])
	}
	dlen := 5
	for i := 0; i < dlen; i++ {
		sendWriteLog(clients, uint64(i))
	}
	printServersState(clients)
}

func sendWriteLog(clients []*rpc.Client, data uint64) {
	for _, client := range clients {
		sargs := &protocol.WriteLogRequest{data}
		reply := new(protocol.WriteLogResponse)
		client.Call("RPCReceiver.WriteLogRequest", sargs, &reply)
		fmt.Println(reply.Status)
	}
}


func printServersState(clients []*rpc.Client) {
	for _, client := range clients {
		snap := getSnapshot(client)
		fmt.Println(snap)
	}
}

func getSnapshot(client *rpc.Client) *protocol.NodeSnapshotResponse {
	args := new(protocol.NodeSnapshotRequest)
	result := new(protocol.NodeSnapshotResponse)
	err := client.Call("RPCReceiver.SnapshotRequest", args, result)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	return result
}

func newRpcClient(host string) *rpc.Client {
	client, err := rpc.DialHTTP("tcp", host)
	log.Println("Adding new server: ", host)
	if err != nil {
		log.Fatal("Error while initialising a sender:", err)
		return nil
	}
	return client
}
