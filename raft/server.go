package main

import (
	"fmt"
	"net/rpc"
//	"net/http"
	"log"
//	"net"
	"os"
	"time"
	"sync"
//	"sync/atomic"
	"net"
	"net/http"
	"strconv"
	"math/rand"
	"hash/fnv"
)

type NodeState struct {
	StateId   int
	StateName string
}

func (nodeState *NodeState) String() string {
	return nodeState.StateName
}

var FOLLOWER = &NodeState{1, "FOLLOWER"}
var CANDIDATE = &NodeState{2, "CANDIDATE"}
var LEADER = &NodeState{3, "LEADER"}

type Host struct {
	Domain string
	Port   int
}

func (host Host) String() string {
	return fmt.Sprintf("%s:%d", host.Domain, host.Port)
}

type Server struct {
	Id        string
	Address   *Host
	Duration  int
	Ticker    *time.Ticker
	RpcClient *rpc.Client
}

type Info struct {
	Id                   string
	State                *NodeState
	Term                 int
	Log                  []int
	EventChannel         chan Event
	FollowerDuration     time.Duration
	FollowerTimer        *time.Timer
	VotedFor             string
	VotesReceived        int
	Servers              map[string]*Server
	LastChangedStateTime time.Time
}

func resetTimer(server *Info) {
	if server.FollowerTimer != nil {
		wasActive := server.FollowerTimer.Reset(time.Millisecond * server.FollowerDuration)
		if !wasActive {
			log.Println(server.Id, "Follower Timer is not Active!!")
		}
	}
}

func startFollower(server *Info) {
	log.Println(server.Id, ": Starting follower timer")
	if server.FollowerTimer != nil {
		wasActive := server.FollowerTimer.Reset(time.Millisecond * server.FollowerDuration)
		if !wasActive {
			server.FollowerTimer = time.NewTimer(time.Millisecond * server.FollowerDuration)
		}
		return
	}else {
		server.FollowerTimer = time.NewTimer(time.Millisecond * server.FollowerDuration)
	}
	go func(info *Info) {
		<-info.FollowerTimer.C
		sendEvent(info, &UpdateStateEvent{BECOME_CANDIDATE, time.Now()})
	}(server)
}


func startLeader(server *Info, servers map[string]*Server) error {
	for _, sr := range servers {
		if sr.Id == server.Id {
			continue
		}
		sr.Ticker = time.NewTicker(time.Millisecond * 100)
		go sendAppendRPC(server, sr)
	}
	return nil
}

func stopLeader(server *Info, servers map[string]*Server) error {
	for _, sr := range servers {
		if sr.Id == server.Id {
			continue
		}
		sr.Ticker.Stop()
	}
	return nil
}


func sendRegisterServerRpc(server *Info, destServer *Server) {
	var currServer = server.Servers[server.Id]
	var registerServerArgs = &RegisterServerArgs{currServer.Id, currServer.Address.Domain, currServer.Address.Port}
	var reply RegisterServerResult
	err := destServer.RpcClient.Call("Info.RegisterServer", registerServerArgs, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Println("Registered server, response : ", reply.Code, reply.Status)
}

func sendAppendRPC(server *Info, destServer *Server) {
	for _ = range destServer.Ticker.C {
		var appendArgs = &AppendArgs{server.Term, server.Id}
		var reply AppendResult
		log.Println(server.Id, "Sending APPEND RPC")
		err := destServer.RpcClient.Call("Info.AppendEntries", appendArgs, &reply)
		if server.State != LEADER {
			return
		}
		if err != nil {
			log.Fatal("Append RPC error:", err)
		}
		if (reply.Term > server.Term) {
			sendEvent(server, &UpdateStateEvent{BECOME_FOLLOWER, time.Now()})
		}
	}
}

func startCandidate(server *Info) {
	server.Term += 1
	server.VotedFor = server.Id
	server.VotesReceived = 1
	var wg sync.WaitGroup
	wg.Add(len(server.Servers) - 1)
	for id, srv := range server.Servers {
		if id != server.Id {
			go func(info *Info, destServer *Server) {
				sendRequestVoteRPC(info, destServer)
				wg.Done()
			}(server, srv)
		}
	}
	go func() {
		wg.Wait()
		if hasMajority(server)  && server.State == CANDIDATE {
			log.Println(server.Id, "Received response from all servers, becoming LEADER for term ", server.Term)
			sendEvent(server, &UpdateStateEvent{BECOME_LEADER, time.Now()})
		}else {
			log.Println(server.Id, "Received response from all servers, becoming follower for term ", server.Term)
			sendEvent(server, &UpdateStateEvent{BECOME_FOLLOWER, time.Now()})
		}
	}()
}

func hasMajority(server *Info) bool {
	return server.VotesReceived >= (len(server.Servers) / 2 + len(server.Servers) % 2)
}

func sendRequestVoteRPC(server *Info, destServer *Server) error {
	var requestArgs = &RequestArgs{server.Term, 0, 0, server.Id}
	var reply RequestResult
	log.Println(server.Id, " :Sending request vote to: ", destServer.Id)
	err := destServer.RpcClient.Call("Info.RequestVote", requestArgs, &reply)
	if err != nil {
		log.Fatal("Error during Info.RequestVote:", err)
		return err
	}
	if reply.Term > server.Term {
		server.Term = reply.Term
		server.VotesReceived = 0
		sendEvent(server, &UpdateStateEvent{BECOME_FOLLOWER, time.Now()})
	}
	if reply.VoteGranted {
		server.VotesReceived += 1
	}
	return nil
}

func addServer(server*Info, newServer *Server) {
	if _, ok := server.Servers[newServer.Id]; ok {
		return
	}
	client, err := rpc.DialHTTP("tcp", newServer.Address.String())
	log.Println(server.Id, ": Adding new server locally: ", newServer.Id, newServer.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	newServer.RpcClient = client
	server.Servers[newServer.Id] = newServer
}


func main() {
	run()
}


func run() {
	args := os.Args[1:]
	id, servers := parse(args)
	log.Println("Init server: ", id, servers[id])
	rand.Seed(int64(hash(id)))
	ec := make(chan Event, 100)
	var s = newServer(id, servers[id], ec)
	rpc.Register(s)
	rpc.HandleHTTP()
	newRpcServer("localhost", servers[id])
	time.Sleep(5 * time.Second)
	for sid, sport := range servers {
		if sport == servers[id] {
			continue
		}
		addr := &Host{"localhost", sport}
		srv := &Server{sid, addr, 400, nil, nil}
		addServer(s, srv)
	}
	fmt.Println(s.Servers)
	eventLoop := EventLoop(s)
	go eventLoop.ProcessEvents()
	var wg sync.WaitGroup
	wg.Add(3)
	wg.Wait()
}


func parse(args[] string) (string, map[string]int) {
	var id string
	var servers = make(map[string]int)
	for i := 0; i < len(args); i++ {
		if args[i] == "-id" {
			id = args[i + 1]
			i += 1
		}else if args[i] == "-s" {
			i += 1
			number, _ := strconv.Atoi(args[i])
			i += 1
			for j := 0; j < number; j++ {
				sid := args[i + j * 2]
				port, _ := strconv.Atoi(args[i + j * 2 + 1])
				servers[sid] = port
			}
			i += number * 2
		}
	}
	return id, servers
}


func newServer(id string, port int, eventChannel chan Event) *Info {
	var addr = &Host{"localhost", port}
	var s1 = &Server{id, addr, 400, nil, nil}
	var server = &Info{State:FOLLOWER,
		Term:1,
		Id:id,
		Log:make([]int, 0, 100),
		EventChannel:eventChannel,
		FollowerDuration:randomDuration(1000),
	}
	server.Servers = make(map[string]*Server)
	server.Servers[id] = s1
	return server
}

func newRpcServer(host string, port int) {
	fmt.Println(fmt.Sprintf("Listening rpc on port: %d", port))
	l, e := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func randomDuration(base int) time.Duration {
	dr := time.Duration(base + rand.Intn(300))
	fmt.Println(dr)
	return dr
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func getServerById(servers map[string]Server, id string) Server {
	return servers[id]
}

func getServersInv(servers map[string]Server, id string) []Server {
	var cap = len(servers)
	var serverSlice = make([]Server, cap, cap)
	ind := 0
	for k, v := range servers {
		if k == id {
			continue
		}
		serverSlice[ind] = v
	}
	return serverSlice
}