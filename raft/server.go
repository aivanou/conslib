package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"sync"
	"net"
	"net/http"
	"strconv"
	"math/rand"
	"hash/fnv"
	"consensus/raft/protocol"
)


func (server *Info) OnAppendEntriesReceived(args *protocol.AppendArgs, result *protocol.AppendResult) error {
	serverTerm := server.Term
	if serverTerm > args.Term {
		result.Success = false
		result.Term = server.Term
		return nil
	}
	var eventId uint16
	if serverTerm < args.Term {
		eventId = BECOME_FOLLOWER
	} else if server.State == FOLLOWER {
		eventId = RESET_TIMER
	}
	log.Printf("%s, state: %s, eventId: %d Received AppendEntries from %s; My Term: %d, Leader Term: %d", server.Id, server.State.String(), eventId, args.LeaderId, server.Term, args.Term)
	eventLoop := server.eventProcessor
	var serverStateEvent Event = nil
	if eventId != 0 {
		serverStateEvent = eventLoop.NewUpdateStateEvent(eventId, time.Now())
	}
	if serverTerm <= args.Term {
		updateParamsEvent := &UpdateParamsEvent{ServerEvent{UPDATE_SERVER, time.Now()}, &UpdateServerPayload{args.Term}}
		eventLoop.Trigger(eventLoop.Chain(updateParamsEvent, serverStateEvent))
	} else if serverStateEvent != nil {
		eventLoop.Trigger(serverStateEvent)
	}
	result.Success = true
	result.Term = args.Term
	return nil
}

func (server *Info) OnRequestVoteReceived(args *protocol.RequestArgs, result *protocol.RequestResult) error {
	log.Println(server.Id, "Received RequestVote from", args.CandidateId)
	var serverTerm = server.Term
	if serverTerm > args.Term {
		result.VoteGranted = false
		result.Term = serverTerm
		log.Println(server.Id, "Not granting vote to:", args.CandidateId, "because my Term is greater")
		return nil
	} else if serverTerm < args.Term {
		log.Println(server.Id, "Received requestVote from server", args.CandidateId, "that has greater Term, Granting vote")
		eventLoop := server.eventProcessor
		serverChangeStateEvent := eventLoop.NewUpdateStateEvent(BECOME_FOLLOWER, time.Now())
		serverUpdateEvent := eventLoop.NewUpdateParamsEvent(UPDATE_SERVER, time.Now(), &UpdateServerPayload{args.Term})
		eventLoop.Trigger(eventLoop.Chain(serverUpdateEvent, serverChangeStateEvent))
		server.VotedFor = args.CandidateId
		result.VoteGranted = true
		result.Term = args.Term
	} else if server.VotedFor == "" {
		log.Println(server.Id, " Received RequestVote from", args.CandidateId, "Granting vote")
		server.VotedFor = args.CandidateId
		result.VoteGranted = true
		result.Term = args.Term
	}else if server.VotedFor != "" {
		log.Println(server.Id, " Received RequestVote from", args.CandidateId, "Not Granting vote")
		result.VoteGranted = false
		result.Term = server.Term
	}
	return nil
}


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


type Server struct {
	Id       string
	Address  *protocol.Host
	Duration int
	Ticker   *time.Ticker
	sender   protocol.Sender
}

type Info struct {
	Id                   string
	State                *NodeState
	Term                 int
	Log                  []int
	eventProcessor       EventLoop
	protocol             protocol.Protocol
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
		eventLoop := server.eventProcessor
		eventLoop.Trigger(eventLoop.NewUpdateStateEvent(BECOME_CANDIDATE, time.Now()))
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

func sendAppendRPC(server *Info, destServer *Server) {
	for _ = range destServer.Ticker.C {
		log.Println(server.Id, "Sending APPEND RPC")
		reply, err := destServer.sender.SendAppend(server.Id, server.Term)
		if err != nil {
			log.Fatal("Append RPC error:", err)
		}
		if (reply.Term > server.Term) {
			eventLoop := server.eventProcessor
			eventLoop.Trigger(eventLoop.NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
		}
	}
}


func sendRequestVoteRPC(server *Info, destServer *Server) error {
	log.Println(server.Id, " :Sending request vote to: ", destServer.Id)
	reply, err := destServer.sender.SendRequestVote(server.Id, server.Term)
	if err != nil {
		log.Fatal("Error during Info.RequestVote:", err)
		return err
	}
	if reply.Term > server.Term {
		server.Term = reply.Term
		server.VotesReceived = 0
		eventLoop := server.eventProcessor
		eventLoop.Trigger(eventLoop.NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
	}
	if reply.VoteGranted {
		server.VotesReceived += 1
	}
	return nil
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
		eventLoop := server.eventProcessor
		if hasMajority(server)  && server.State == CANDIDATE {
			log.Println(server.Id, "Received response from all servers, becoming LEADER for term ", server.Term)
			eventLoop.Trigger(eventLoop.NewUpdateStateEvent(BECOME_LEADER, time.Now()))
		}else {
			log.Println(server.Id, "Received response from all servers, becoming follower for term ", server.Term)
			eventLoop.Trigger(eventLoop.NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
		}
	}()
}

func hasMajority(server *Info) bool {
	return server.VotesReceived >= (len(server.Servers) / 2 + len(server.Servers) % 2)
}

func addServer(server*Info, newServer *Server) {
	if _, ok := server.Servers[newServer.Id]; ok {
		return
	}
	prot := server.protocol
	sender, err := prot.NewSender(newServer.Address)
	if err != nil {
		log.Fatal("Error while initialising sender for ", newServer.Address.String())
	}
	newServer.sender = sender
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
	var s = newServer(id, servers[id])
	time.Sleep(5 * time.Second)
	for sid, sport := range servers {
		if sport == servers[id] {
			continue
		}
		addr := &protocol.Host{"localhost", sport}
		srv := &Server{sid, addr, 400, nil, nil}
		addServer(s, srv)
	}
	fmt.Println(s.Servers)
	go s.eventProcessor.ProcessEvents()
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


func newServer(id string, port int) *Info {
	var addr = &protocol.Host{"localhost", port}
	var s1 = &Server{id, addr, 400, nil, nil}
	var server = &Info{State:FOLLOWER,
		Term:1,
		Id:id,
		Log:make([]int, 0, 100),
		FollowerDuration:randomDuration(1000),
	}
	server.protocol = protocol.NewProtocol()
	server.protocol.RegisterListener(addr, server)
	var eventLoop = NewEventProcessor(server)
	server.eventProcessor = eventLoop
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