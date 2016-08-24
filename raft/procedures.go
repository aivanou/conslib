package main

import (
	"net/rpc"
	"log"
	"fmt"
	"time"
)


type AppendResult struct {
	Term    int
	Success bool
}

type AppendArgs struct {
	Term     int
	LeaderId string
}

type RegisterServerArgs struct {
	Id   string
	Host string
	Port int
}

type RegisterServerResult struct {
	Status string
	Code   int
}

func (server *Info) RegisterServer(args *RegisterServerArgs, result *RegisterServerResult) error {
	if _, ok := server.Servers[args.Id]; ok {
		result.Code = 300;
		result.Status = fmt.Sprintf("The server with id: %s is already registered", args.Id)
		return nil
	}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", args.Host, args.Port))
	log.Println("Received RegisterServer: ", args.Id, args.Host, args.Port)
	if err != nil {
		log.Fatal("dialing:", err)
		return err
	}
	var addr = &Host{"localhost", args.Port}
	var newServer = &Server{args.Id, addr, 1000, nil, client}
	server.Servers[args.Id] = newServer
	result.Code = 200
	result.Status = "OK"
	return nil
}

func (server *Info) AppendEntries(args *AppendArgs, result *AppendResult) error {
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
	log.Printf("%s, state: %d, eventId: %d Received AppendEntries from %s; My Term: %d, Leader Term: %d", server.Id, server.State.String(), eventId, args.LeaderId, server.Term, args.Term)
	var serverStateEvent *Event = nil
	if eventId != 0 {
		serverStateEvent = &Event{eventId, time.Now(), nil}
	}
	if serverTerm <= args.Term {
		server.EventChannel <- &Event{UPDATE_SERVER, time.Now(), &UpdateServerEvent{args.Term, serverStateEvent}}
	} else if serverStateEvent != nil {
		server.EventChannel <- serverStateEvent
	}
	result.Success = true
	result.Term = args.Term
	return nil
}


type RequestArgs struct {
	Term         int
	LastLogIndex int
	LastLogTerm  int
	CandidateId  string
}

type RequestResult struct {
	Term        int
	VoteGranted bool
}

func (server* Info) RequestVote(args *RequestArgs, result *RequestResult) error {
	log.Println(server.Id, "Received RequestVote from", args.CandidateId)
	var serverTerm = server.Term
	if serverTerm > args.Term {
		result.VoteGranted = false
		result.Term = serverTerm
		log.Println(server.Id, "Not granting vote to:", args.CandidateId, "because my Term is greater")
		return nil
	} else if serverTerm < args.Term {
		log.Println(server.Id, "Received requestVote from server", args.CandidateId, "that has greater Term, Granting vote")
		serverChangeStateEvent := &Event{BECOME_FOLLOWER, time.Now(), nil}
		serverUpdateEvent := &Event{UPDATE_SERVER, time.Now(), &UpdateServerEvent{args.Term, serverChangeStateEvent}}
		server.EventChannel <- serverUpdateEvent
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