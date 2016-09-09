package main

import (
	"os"
	"time"
	"sync"
	"consensus/raft/protocol"
	"consensus/raft/logstore"
)


type Server struct {
	Id      string
	Address *protocol.Host
	sender  protocol.Sender
}


type RaftNode struct {
	Id                   string
	Servers              map[string]*Server
	State                *RaftState
	LastChangedStateTime time.Time
	eventProcessor       EventLoop
	protocol             protocol.Protocol
	stateHandler         StateHandler
}

func main() {
	run()
}

func run() {
	args := os.Args[1:]
	id, servers := parse(args)

	log.Debug(id, " Starting server on port ", servers[id])

	var s = newServer(id, servers[id])
	time.Sleep(1 * time.Second)
	for sid, sport := range servers {
		if sport == servers[id] {
			continue
		}
		addr := &protocol.Host{"localhost", sport}
		srv := &Server{sid, addr, nil}
		addServer(s, srv)
	}
	stateHandler := NewStateHandler(s)
	s.stateHandler = stateHandler
	go s.eventProcessor.ProcessEvents()
	var wg sync.WaitGroup
	wg.Add(3)
	wg.Wait()
}

func newServer(id string, port int) *RaftNode {
	var addr = &protocol.Host{"localhost", port}
	var server = &RaftNode{
		Id:id,
		State: &RaftState{1, "", 0, 0, 0, logstore.NewLogStore()},
	}
	server.protocol = protocol.NewProtocol()
	server.protocol.RegisterListener(addr, server)
	var eventLoop = NewEventProcessor(server)
	server.eventProcessor = eventLoop
	server.Servers = make(map[string]*Server)
	return server
}

func (server *RaftNode) SendAppend(destServerId string, args *protocol.AppendArgs) (*protocol.AppendResult, error) {
	log.Debug(server.Id, " Sending APPEND_RPC to ", destServerId, " at time: ", time.Now())
	destServer := server.Servers[destServerId]
	reply, err := destServer.sender.SendAppend(args)
	if err != nil {
		log.Println(server.Id, "Append RPC error:", err)
		return nil, err
	}
	return reply, nil
}

func (server *RaftNode) SendRequestVotes(wg *sync.WaitGroup) {
	for _, srv := range server.Servers {
		go func(rnode *RaftNode, destServer *Server, barrier *sync.WaitGroup) {
			rnode.sendRequestVoteRPC(destServer)
			barrier.Done()
		}(server, srv, wg)
	}
}

func (server *RaftNode) sendRequestVoteRPC(destServer *Server) error {
	log.Debug(server.Id, " :Sending REQUEST_VOTE_RPC to: ", destServer.Id)
	reqArgs := &protocol.RequestArgs{server.State.Term, 1, 1, server.Id}
	reply, err := destServer.sender.SendRequestVote(reqArgs)
	if err != nil {
		log.Println(server.Id, " Error during Info.RequestVote: ", err)
		return err
	}
	if reply.Term > server.State.Term {
		server.State.UpdateTerm(reply.Term)
		server.State.ResetVotesReceived()
		eventLoop := server.eventProcessor
		eventLoop.Trigger(NewUpdateStateEvent(BECOME_FOLLOWER, time.Now()))
	}

	if reply.VoteGranted {
		server.State.IncVotesForTerm()
	}
	return nil
}

/**
* This function is triggered every time when another server sends APPEND_ENTRIES RPC request.
*/
func (server *RaftNode) OnAppendEntriesReceived(args *protocol.AppendArgs, result *protocol.AppendResult) error {
	serverTerm := server.State.Term
	if serverTerm > args.Term {
		result.Success = false
		result.Term = server.State.Term
		return nil
	}
	var eventId uint16
	serverState := server.stateHandler.GetActiveState()
	var serverStateEvent Event = nil
	if serverTerm < args.Term {
		eventId = BECOME_FOLLOWER
		serverStateEvent = NewUpdateStateEvent(eventId, time.Now())
	} else if serverState.Id() == FOLLOWER_ID {
		eventId = RESET_TIMER
		serverStateEvent = NewServerRequestEvent(eventId, time.Now(), nil)
	}
	log.Debug(server.Id, " Received APPEND_ENTRIES_RPC. My Term: ", server.State.Term, " My state: ", server.stateHandler.GetActiveState().Name(), " Leader term: ", args.Term, " Leader ID: ", args.LeaderId)
	eventLoop := server.eventProcessor
	updateParamsEvent := NewServerRequestEvent(UPDATE_SERVER, time.Now(), &UpdateServerPayload{args.Term})
	if serverState != nil {
		eventLoop.Trigger(Chain(updateParamsEvent, serverStateEvent))
	}
	result.Success = true
	result.Term = args.Term
	lastLogItem := server.State.Store.LastLogItem()
	if lastLogItem == nil {
		log.Debug(server.Id, " Appending new entries, because I have no entries")
		for _, li := range args.Entries {
			server.State.Store.AppendLogItem(&li)
		}
		if (args.LeaderCommit > server.State.CommitIndex) {
			lastLogItem := server.State.Store.LastLogItem()
			if lastLogItem != nil {
				server.State.UpdateCommitIndex(min(args.LeaderCommit, lastLogItem.Index))
			}
		}
	}else if lastLogItem.Index > args.PrevLogIndex {
		log.Debug(server.Id, "Don't grant entry, because: my LastLogIndex:", lastLogItem.Index, " greater than: ", args.PrevLogIndex)
		// if lastLog in server's store has greater index than PrevLogIndex, remove all entries after lastLog.Index, and reply false
		server.State.Store.RemoveAfterIncl(lastLogItem.Index)
		result.Success = false
	}else if lastLogItem.Index < args.PrevLogIndex {
		result.Success = false
	} else if (lastLogItem.Index == args.PrevLogIndex && lastLogItem.Term != args.PrevLogTerm) {
		server.State.Store.RemoveByIndex(lastLogItem.Index)
		result.Success = false
	} else {
		for _, li := range args.Entries {
			server.State.Store.AppendLogItem(&li)
		}
		if (args.LeaderCommit > server.State.CommitIndex) {
			server.State.UpdateCommitIndex(min(args.LeaderCommit, server.State.Store.LastLogItem().Index))
		}
	}
	return nil
}

/**
* This function is triggered every time when another server sends REQUEST_VOTE RPC request.
*/
func (server *RaftNode) OnRequestVoteReceived(args *protocol.RequestArgs, result *protocol.RequestResult) error {
	log.Println(server.Id, "Received RequestVote from", args.CandidateId)
	var serverTerm = server.State.Term
	if serverTerm > args.Term {
		result.VoteGranted = false
		result.Term = serverTerm
		log.Println(server.Id, "Not granting vote to:", args.CandidateId, "because my Term is greater")
		return nil
	} else if serverTerm < args.Term {
		log.Println(server.Id, "Received requestVote from server", args.CandidateId, "that has greater Term, Granting vote")
		eventLoop := server.eventProcessor
		serverChangeStateEvent := NewUpdateStateEvent(BECOME_FOLLOWER, time.Now())
		serverUpdateEvent := NewServerRequestEvent(UPDATE_SERVER, time.Now(), &UpdateServerPayload{args.Term})
		eventLoop.Trigger(Chain(serverUpdateEvent, serverChangeStateEvent))
		server.State.VotedFor = args.CandidateId
		result.VoteGranted = true
		result.Term = args.Term
	} else if server.State.VotedFor == "" {
		store := server.State.Store
		lastLogItem := store.LastLogItem()
		if lastLogItem == nil || (lastLogItem.Index <= args.LastLogIndex && lastLogItem.Term <= args.LastLogTerm) {
			log.Println(server.Id, " Received RequestVote from", args.CandidateId, "Granting vote")
			server.State.VotedFor = args.CandidateId
			result.VoteGranted = true
			result.Term = args.Term
		}else {
			result.VoteGranted = false
			result.Term = server.State.Term
		}
	}else if server.State.VotedFor != "" {
		log.Debug(server.Id, " Received RequestVote from", args.CandidateId, "Not Granting vote because already granted to: ", server.State.VotedFor)
		result.VoteGranted = false
		result.Term = server.State.Term
	}
	return nil
}


func hasMajority(server *RaftNode) bool {
	return server.State.VotesReceived >= uint32(len(server.Servers) / 2 + 1)
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