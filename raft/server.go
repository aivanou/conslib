package main

import (
	"os"
	"sync"
	"time"

	"github.com/tierex/conslib/raft/logstore"
	"github.com/tierex/conslib/raft/protocol"
)

// Server bla
type Server struct {
	ID      string
	Address *protocol.Host
	sender  protocol.Sender
}

// RaftNode bla
type RaftNode struct {
	ID                   string
	Servers              map[string]*Server
	State                *RaftState
	LastChangedStateTime time.Time
	eventProcessor       EventLoop
	protocol             protocol.Protocol
	stateHandler         StateHandler
}

func main() {
	raftConfig := ReadConfig()
	run(raftConfig)
}

func run(raftConfig *RaftConfig) {
	args := os.Args[1:]
	id, servers := parse(args)

	log.Debug(id, " Starting server on port ", servers[id])

	var s = newServer(id, servers[id])
	time.Sleep(1 * time.Second)
	for sid, sport := range servers {
		if sport == servers[id] {
			continue
		}
		addr := &protocol.Host{Domain: "localhost", Port: sport}
		srv := &Server{sid, addr, nil}
		addServer(s, srv)
	}
	s.stateHandler = NewStateHandler(s, raftConfig)
	go s.eventProcessor.ProcessEvents()
	var wg sync.WaitGroup
	wg.Add(3)
	wg.Wait()
}

func newServer(id string, port int) *RaftNode {
	var addr = &protocol.Host{Domain: "localhost", Port: port}
	var server = &RaftNode{
		ID:    id,
		State: &RaftState{1, "", 0, 0, 0, logstore.NewLogStore()},
	}
	server.protocol = protocol.NewProtocol()
	server.protocol.RegisterListener(addr, server)
	var eventLoop = NewEventProcessor(server)
	server.eventProcessor = eventLoop
	server.Servers = make(map[string]*Server)
	return server
}

// SendAppend bla
func (server *RaftNode) SendAppend(destServerId string, args *protocol.AppendArgs) (*protocol.AppendResult, error) {
	log.Debug(server.ID, " Sending APPEND_RPC to ", destServerId, " at time: ", time.Now())
	destServer := server.Servers[destServerId]
	reply, err := destServer.sender.SendAppend(args)
	if err != nil {
		log.Println(server.ID, "Append RPC error:", err)
		return nil, err
	}
	return reply, nil
}

// SendRequestVotes bla
func (server *RaftNode) SendRequestVotes(wg *sync.WaitGroup) {
	for _, srv := range server.Servers {
		go func(rnode *RaftNode, destServer *Server, barrier *sync.WaitGroup) {
			rnode.sendRequestVoteRPC(destServer)
			barrier.Done()
		}(server, srv, wg)
	}
}

func (server *RaftNode) sendRequestVoteRPC(destServer *Server) error {
	log.Debug(server.ID, " :Sending REQUEST_VOTE_RPC to: ", destServer.ID)
	reqArgs := &protocol.RequestArgs{Term: server.State.Term, LastLogIndex: 1, LastLogTerm: 1, CandidateId: server.ID}
	reply, err := destServer.sender.SendRequestVote(reqArgs)
	if err != nil {
		log.Println(server.ID, " Error during Info.RequestVote: ", err)
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

// OnAppendEntriesReceived blab bla
func (server *RaftNode) OnAppendEntriesReceived(args *protocol.AppendArgs, result *protocol.AppendResult) error {
	serverTerm := server.State.Term
	if serverTerm > args.Term {
		result.Success = false
		result.Term = server.State.Term
		return nil
	}
	var eventID uint16
	serverState := server.stateHandler.GetActiveState()
	var serverStateEvent Event
	if serverTerm < args.Term {
		eventID = BECOME_FOLLOWER
		serverStateEvent = NewUpdateStateEvent(eventID, time.Now())
	} else if serverState.Id() == FOLLOWER_ID {
		eventID = RESET_TIMER
		serverStateEvent = NewServerRequestEvent(eventID, time.Now(), nil)
	}
	log.Debug(server.ID, " Received APPEND_ENTRIES_RPC. My Term: ", server.State.Term,
		" My state: ", server.stateHandler.GetActiveState().Name(),
		" Leader term: ", args.Term, " Leader ID: ", args.LeaderId,
		" Logs: ", len(args.Entries), " My store: ", server.State.Store.Size())
	eventLoop := server.eventProcessor
	updateParamsEvent := NewServerRequestEvent(UPDATE_SERVER, time.Now(), &UpdateServerPayload{args.Term})
	if serverState != nil {
		eventLoop.Trigger(Chain(updateParamsEvent, serverStateEvent))
	}
	result.Success = true
	result.Term = args.Term
	var lastLogItem = server.State.Store.LastLogItem()
	if lastLogItem == nil {
		log.Debug(server.ID, " Appending new entries, because I have no entries")
		for _, li := range args.Entries {
			server.State.Store.AppendLogItem(&li)
		}
		if args.LeaderCommit > server.State.CommitIndex {
			lastLogItem = server.State.Store.LastLogItem()
			if lastLogItem != nil {
				server.State.UpdateCommitIndex(min(args.LeaderCommit, lastLogItem.Index))
			}
		}
	} else if lastLogItem.Index > args.PrevLogIndex {
		log.Debug(server.ID, "Don't grant entry, because: my LastLogIndex:", lastLogItem.Index, " greater than: ", args.PrevLogIndex)
		// if lastLog in server's store has greater index than PrevLogIndex, remove all entries after lastLog.Index, and reply false
		server.State.Store.RemoveAfterIncl(lastLogItem.Index)
		result.Success = false
	} else if lastLogItem.Index < args.PrevLogIndex {
		log.Debug(server.ID, " LastLogItem.Index ", lastLogItem.Index, " < ", args.PrevLogIndex, " returning false")
		result.Success = false
	} else if lastLogItem.Index == args.PrevLogIndex && lastLogItem.Term != args.PrevLogTerm {
		log.Debug(server.ID, " lastLogTerm: ", lastLogItem.Term, " != ", args.PrevLogTerm)
		server.State.Store.RemoveByIndex(lastLogItem.Index)
		result.Success = false
	} else {
		for _, li := range args.Entries {
			server.State.Store.AppendLogItem(&li)
		}
		if args.LeaderCommit > server.State.CommitIndex {
			server.State.UpdateCommitIndex(min(args.LeaderCommit, server.State.Store.LastLogItem().Index))
		}
	}
	return nil
}

// OnRequestVoteReceived blab bla
func (server *RaftNode) OnRequestVoteReceived(args *protocol.RequestArgs, result *protocol.RequestResult) error {
	log.Println(server.ID, "Received RequestVote from", args.CandidateId)
	var serverTerm = server.State.Term
	if serverTerm > args.Term {
		result.VoteGranted = false
		result.Term = serverTerm
		log.Println(server.ID, "Not granting vote to:", args.CandidateId, "because my Term is greater")
		return nil
	} else if serverTerm < args.Term {
		log.Println(server.ID, "Received requestVote from server", args.CandidateId, "that has greater Term, Granting vote")
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
			log.Println(server.ID, " Received RequestVote from", args.CandidateId, "Granting vote")
			server.State.VotedFor = args.CandidateId
			result.VoteGranted = true
			result.Term = args.Term
		} else {
			result.VoteGranted = false
			result.Term = server.State.Term
		}
	} else if server.State.VotedFor != "" {
		log.Debug(server.ID, " Received RequestVote from", args.CandidateId, "Not Granting vote because already granted to: ", server.State.VotedFor)
		result.VoteGranted = false
		result.Term = server.State.Term
	}
	return nil
}

// IsLeader bla
func (server *RaftNode) IsLeader() bool {
	return server.stateHandler.GetActiveState().Id() == LEADER_ID
}

// OnWriteLogRequestReceived bla bla
func (server *RaftNode) OnWriteLogRequestReceived(args *protocol.WriteLogRequest, result *protocol.WriteLogResponse) error {
	if !server.IsLeader() {
		result.Status = 0
		return nil
	}
	resp := server.execRequest(args.Data)
	result.Status = resp.success
	return nil
}

// OnSnapshotRequestReceived blabla
func (server *RaftNode) OnSnapshotRequestReceived(args *protocol.NodeSnapshotRequest, result *protocol.NodeSnapshotResponse) error {
	raftState := server.State
	store := raftState.Store
	result.Id = server.ID
	result.Term = raftState.Term
	result.State = server.stateHandler.GetActiveState().Name()
	result.CommitIndex = raftState.CommitIndex
	result.LastAppliedIndex = raftState.LastAppliedIndex
	if item := store.LastLogItem(); item != nil {
		result.LastLogIndex = item.Index
		result.LastLogTerm = item.Term
	}
	result.LogSize = store.Size()
	return nil
}

func (server *RaftNode) execRequest(data uint64) *WriteResponse {
	responseChan := make(chan uint32)
	writeRequest := &WriteRequest{"500", data, responseChan}
	server.eventProcessor.Trigger(NewServerRequestEvent(WRITE_LOG, time.Now(), writeRequest))
	resp := <-responseChan
	return &WriteResponse{resp}
}

func hasMajority(server *RaftNode) bool {
	return server.State.VotesReceived >= uint32(len(server.Servers)/2+1)
}

func addServer(server *RaftNode, newServer *Server) {
	if _, ok := server.Servers[newServer.ID]; ok {
		return
	}
	prot := server.protocol
	sender, err := prot.NewSender(newServer.Address)
	if err != nil {
		log.Fatal("Error while initialising sender for ", newServer.Address.String())
	}
	newServer.sender = sender
	server.Servers[newServer.ID] = newServer
}
