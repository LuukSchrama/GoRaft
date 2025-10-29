package node

import (
	"Rafting/raft"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type RaftService struct {
	raft.UnimplementedRaftServiceServer
	node *Node
}

func (n *Node) StartGRPCServer() error {
	_, port, err := net.SplitHostPort(n.Address)
	if err != nil {
		return fmt.Errorf("invalid address %s: %w", n.Address, err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	raft.RegisterRaftServiceServer(s, &RaftService{node: n})

	log.Printf("Node %s is listening on port %v", n.ID, n.Address)
	log.Printf("Node Peers are: %v", n.Peers)
	return s.Serve(lis)
}

func (r *RaftService) RequestVote(ctx context.Context, req *raft.VoteRequest) (*raft.VoteResponse, error) {
	n := r.node
	granted := false

	if req.Term < n.Term {
		return &raft.VoteResponse{Term: n.Term, VoteGranted: false}, nil
	}

	if req.Term > n.Term {
		n.Term = req.Term
		n.State = "Follower"
		n.VotedFor = ""
	}

	if n.VotedFor == "" || n.VotedFor == req.CandidateId {
		n.VotedFor = req.CandidateId
		n.LastHeartbeat = time.Now()
		granted = true
	}

	log.Printf("Node %s received vote request from %s (term %d)", r.node.ID, req.CandidateId, req.Term)
	return &raft.VoteResponse{
		Term:        n.Term,
		VoteGranted: granted,
	}, nil
}

func (r *RaftService) AppendEntries(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	n := r.node

	if req.Term < n.Term {
		return &raft.AppendEntriesResponse{Term: r.node.Term, Success: false}, nil
	}

	r.node.Term = req.Term
	r.node.State = "Follower"
	r.node.LastHeartbeat = time.Now()

	if len(req.Entries) > 0 {
		for _, entry := range req.Entries {
			log.Printf("Node %s appending entry from Leader: %s: %s", n.ID, req.LeaderId, entry.Command)
			n.Log = append(n.Log, entry.Command)
		}
		n.persist()
	} else {
		log.Printf("Node %s recieved heartbeat from Leader: %s (term %d)", n.ID, req.LeaderId, req.Term)
	}

	return &raft.AppendEntriesResponse{Term: n.Term, Success: true}, nil
}
