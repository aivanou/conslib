package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"sync"
	"strconv"
	"consensus/raft/protocol"
)


type Server struct {
	Id      string
	Address *protocol.Host
	sender  protocol.Sender
}

type RaftNode struct {
	Id                   string
	Term                 int
	Log                  []int
	VotedFor             string
	VotesReceived        int
	Servers              map[string]*Server
	LastChangedStateTime time.Time
	eventProcessor       EventLoop
	protocol             protocol.Protocol
	stateHandler         StateHandler
}


func hasMajority(server *RaftNode) bool {
	return server.VotesReceived >= (len(server.Servers) / 2 + len(server.Servers) % 2)
}

func addServer(server*RaftNode, newServer *Server) {
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

	var s = newServer(id, servers[id])
	time.Sleep(5 * time.Second)
	for sid, sport := range servers {
		if sport == servers[id] {
			continue
		}
		addr := &protocol.Host{"localhost", sport}
		srv := &Server{sid, addr, nil}
		addServer(s, srv)
	}
	fmt.Println(s.Servers)
	stateHandler := NewStateHandler(s)
	s.stateHandler = stateHandler
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


func newServer(id string, port int) *RaftNode {
	var addr = &protocol.Host{"localhost", port}
	var server = &RaftNode{Term:1,
		Id:id,
		Log:make([]int, 0, 100),
	}
	server.protocol = protocol.NewProtocol()
	server.protocol.RegisterListener(addr, server)
	var eventLoop = NewEventProcessor(server)
	server.eventProcessor = eventLoop
	server.Servers = make(map[string]*Server)
	return server
}

func (server *RaftNode) SendAppend(destServerId string) {
	log.Println(server.Id, "Sending APPEND RPC to", destServerId, "at time: ", time.Now())
	destServer := server.Servers[destServerId]
	reply, err := destServer.sender.SendAppend(server.Id, server.Term)
	if err != nil {
		log.Println("Append RPC error:", err)
		return
	}
	if (reply.Term > server.Term) {
		eventLoop := server.eventProcessor
		eventLoop.Trigger(NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
	}
}

func (server *RaftNode) SendRequestVotes(wg *sync.WaitGroup) {
	for _, srv := range server.Servers {
		go func(rnode *RaftNode, destServer *Server, barrier *sync.WaitGroup) {
			rnode.sendRequestVoteRPC(destServer)
			log.Println(rnode.Id, "Minus barrier")
			barrier.Done()
		}(server, srv, wg)
	}
}

func (server *RaftNode) sendRequestVoteRPC(destServer *Server) error {
	log.Println(server.Id, " :Sending request vote to: ", destServer.Id)
	reply, err := destServer.sender.SendRequestVote(server.Id, server.Term)
	if err != nil {
		log.Println("Error during Info.RequestVote:", err)
		return err
	}
	if reply.Term > server.Term {
		server.Term = reply.Term
		server.VotesReceived = 0
		eventLoop := server.eventProcessor
		eventLoop.Trigger(NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
	}

	if reply.VoteGranted {
		server.VotesReceived += 1
	}
	return nil
}


func (server *RaftNode) OnAppendEntriesReceived(args *protocol.AppendArgs, result *protocol.AppendResult) error {
	serverTerm := server.Term
	if serverTerm > args.Term {
		result.Success = false
		result.Term = server.Term
		return nil
	}
	var eventId uint16
	serverState := server.stateHandler.GetActiveState()
	if serverTerm < args.Term {
		eventId = BECOME_FOLLOWER
	} else if serverState.Id() == FOLLOWER_ID {
		eventId = RESET_TIMER
	}
	log.Printf("%s, state: %s, eventId: %d Received AppendEntries from %s; My Term: %d, Leader Term: %d", server.Id, server.stateHandler.GetActiveState().Name(), eventId, args.LeaderId, server.Term, args.Term)
	eventLoop := server.eventProcessor
	var serverStateEvent Event = nil
	if eventId != 0 {
		serverStateEvent = NewUpdateStateEvent(eventId, time.Now())
	}
	updateParamsEvent := NewUpdateParamsEvent(UPDATE_SERVER, time.Now(), &UpdateServerPayload{args.Term})
	eventLoop.Trigger(Chain(updateParamsEvent, serverStateEvent))
	result.Success = true
	result.Term = args.Term
	return nil
}

func (server *RaftNode) OnRequestVoteReceived(args *protocol.RequestArgs, result *protocol.RequestResult) error {
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
		serverChangeStateEvent := NewUpdateStateEvent(BECOME_FOLLOWER, time.Now())
		serverUpdateEvent := NewUpdateParamsEvent(UPDATE_SERVER, time.Now(), &UpdateServerPayload{args.Term})
		eventLoop.Trigger(Chain(serverUpdateEvent, serverChangeStateEvent))
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