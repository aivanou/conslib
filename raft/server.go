package raft

import (
	"sync"
	"time"

	"github.com/tierex/conslib/raft/protocol"
)

// Server bla
type Server struct {
	ID      string
	Address *protocol.Host
	sender  protocol.Sender
}

// Node bla
type Node struct {
	ID                   string
	Servers              map[string]*Server
	State                *NodeState
	LastChangedStateTime time.Time
	eventProcessor       EventLoop
	protocol             protocol.Protocol
	stateHandler         StateHandler
}

// Run is the main function
func Run(config *Config) {
	log.Debug("Starting server on port ", config.Raft.Port)

	var s = newServer(config.Raft)
	for ind := range config.Peers {
		peer := config.Peers[ind]
		addPeer(s, peer)
	}
	s.stateHandler = NewStateHandler(s, config.Raft)
	go s.eventProcessor.ProcessEvents()
	var wg sync.WaitGroup
	wg.Add(3)
	wg.Wait()
}

func newServer(raftConfig NodeConfig) *Node {
	var addr = &protocol.Host{Domain: raftConfig.Host, Port: raftConfig.Port}
	var server = &Node{
		ID: "my_id",
		State: &NodeState{
			Term:             1,
			VotedFor:         "",
			VotesReceived:    0,
			CommitIndex:      0,
			LastAppliedIndex: 0},
	}
	server.protocol = protocol.NewProtocol()
	server.protocol.RegisterListener(addr, server)
	var eventLoop = NewEventProcessor(server)
	server.eventProcessor = eventLoop
	server.Servers = make(map[string]*Server)
	return server
}

// SendAppend sends the SendAppend RPC
func (server *Node) SendAppend(destServerID string, args *protocol.AppendArgs) (*protocol.AppendResult, error) {
	log.Debug(server.ID, " Sending APPEND_RPC to ", destServerID, " at time: ", time.Now())
	destServer := server.Servers[destServerID]
	reply, err := destServer.sender.SendAppend(args)
	if err != nil {
		log.Println(server.ID, "Append RPC error:", err)
		return nil, err
	}
	return reply, nil
}

// SendRequestVotes sends the SendRequestVotes RPC
func (server *Node) SendRequestVotes(wg *sync.WaitGroup) {
	for _, srv := range server.Servers {
		go func(rnode *Node, destServer *Server, barrier *sync.WaitGroup) {
			rnode.sendRequestVoteRPC(destServer)
			barrier.Done()
		}(server, srv, wg)
	}
}

func (server *Node) sendRequestVoteRPC(destServer *Server) error {
	log.Debug(server.ID, " :Sending REQUEST_VOTE_RPC to: ", destServer.ID)
	reqArgs := &protocol.RequestArgs{
		Term:         server.State.Term,
		LastLogIndex: 1,
		LastLogTerm:  1,
		CandidateId:  server.ID}
	// if the sender is nil, the connection was refused, try to reestablish it
	if destServer.sender == nil {
		newSender, _ := server.protocol.NewSender(destServer.Address)
		destServer.sender = newSender
	}
	if destServer.sender == nil {
		log.Println("Warn: Wasn't able to establish connection with: ", destServer.Address)
		return nil
	}
	reply, err := destServer.sender.SendRequestVote(reqArgs)
	if err != nil {
		log.Println(server.ID, " Error during Info.RequestVote: ", err)
		return err
	}
	if reply.Term > server.State.Term {
		server.State.UpdateTerm(reply.Term)
		server.State.ResetVotesReceived()
		eventLoop := server.eventProcessor
		eventLoop.Trigger(NewUpdateStateEvent(BecomeFollower, time.Now()))
	}

	if reply.VoteGranted {
		server.State.IncVotesForTerm()
	}
	return nil
}

// OnAppendEntriesReceived is called when the Append RPC received from the remote server
func (server *Node) OnAppendEntriesReceived(args *protocol.AppendArgs, result *protocol.AppendResult) error {
	serverTerm := server.State.Term
	// if the server term is greater than the observed term the procedure should be ignored
	if serverTerm > args.Term {
		result.Success = false
		result.Term = server.State.Term
		return nil
	}
	var eventID uint16
	serverState := server.stateHandler.GetActiveState()
	var serverStateEvent Event
	if serverTerm < args.Term {
		// if the observed term is greater than the current server term,
		// the server state should be reset to follower
		eventID = BecomeFollower
		serverStateEvent = NewUpdateStateEvent(eventID, time.Now())
	} else if serverState.ID() == FollowerID {
		// reset follower timer
		eventID = ResetTimer
		serverStateEvent = NewServerRequestEvent(eventID, time.Now(), nil)
	}
	log.Debug(server.ID, " Received APPEND_ENTRIES_RPC. My Term: ", server.State.Term,
		" My state: ", server.stateHandler.GetActiveState().Name(),
		" Leader term: ", args.Term, " Leader ID: ", args.LeaderId)
	eventLoop := server.eventProcessor
	updateParamsEvent := NewServerRequestEvent(UpdateServer, time.Now(), &UpdateServerPayload{args.Term})
	eventLoop.Trigger(Chain(updateParamsEvent, serverStateEvent))
	result.Success = true
	result.Term = args.Term
	return nil
}

// OnRequestVoteReceived is called when the RequestVote RPC received from the remote server
func (server *Node) OnRequestVoteReceived(args *protocol.RequestArgs, result *protocol.RequestResult) error {
	log.Println(server.ID, "Received RequestVote from", args.CandidateId)
	var serverTerm = server.State.Term
	if serverTerm > args.Term {
		result.VoteGranted = false
		result.Term = serverTerm
		log.Println(server.ID, "Not granting vote to: ", args.CandidateId,
			"because my Term is greater")
		return nil
	} else if serverTerm < args.Term {
		log.Println(server.ID, "Received requestVote from server", args.CandidateId,
			"that has greater Term, Granting vote")
		eventLoop := server.eventProcessor
		serverChangeStateEvent := NewUpdateStateEvent(BecomeFollower, time.Now())
		serverUpdateEvent := NewServerRequestEvent(UpdateServer, time.Now(),
			&UpdateServerPayload{args.Term})
		eventLoop.Trigger(Chain(serverUpdateEvent, serverChangeStateEvent))
		server.State.VotedFor = args.CandidateId
		result.VoteGranted = true
		result.Term = args.Term
	} else if server.State.VotedFor == "" {
		log.Println(server.ID, " Received RequestVote from", args.CandidateId, "Granting vote")
		server.State.VotedFor = args.CandidateId
		result.VoteGranted = true
		result.Term = args.Term
	} else if server.State.VotedFor != "" {
		log.Debug(server.ID, " Received RequestVote from", args.CandidateId,
			"Not Granting vote because already granted to: ", server.State.VotedFor)
		result.VoteGranted = false
		result.Term = server.State.Term
	}
	return nil
}

// IsLeader returns true if the current server is leader
func (server *Node) IsLeader() bool {
	return server.stateHandler.GetActiveState().ID() == LeaderID
}

func hasMajority(server *Node) bool {
	return server.State.VotesReceived >= uint32(len(server.Servers)/2+1)
}

func addPeer(server *Node, peer PeerConfig) {
	addr := &protocol.Host{Domain: peer.Host, Port: peer.Port}
	srv := &Server{toID(peer.Host, peer.Port), addr, nil}
	if _, ok := server.Servers[srv.ID]; ok {
		return
	}
	sender, err := server.protocol.NewSender(srv.Address)
	if err != nil {
		log.Fatal("Error while initialising sender for ", srv.Address.String())
	}
	srv.sender = sender
	server.Servers[srv.ID] = srv
}
