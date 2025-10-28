package node

import (
	"Rafting/raft"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Node struct {
	ID          string
	Address     string
	Term        int64
	Log         []string
	Peers       []Peer
	PeerClients map[string]raft.RaftServiceClient

	State           string
	VotedFor        string
	ElectionTimeout time.Duration
	LastHeartbeat   time.Time
}

type Peer struct {
	ID      string
	Address string
}

func (n *Node) ConnectToPeers() error {
	if n.PeerClients == nil {
		n.PeerClients = make(map[string]raft.RaftServiceClient)
	}

	for _, peer := range n.Peers {
		conn, err := grpc.NewClient(peer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("could not connect to peer %s: %w", peer.ID, err)
		}

		client := raft.NewRaftServiceClient(conn)
		n.PeerClients[peer.ID] = client
		log.Printf("Node %s connected to peer %s at %s", n.ID, peer.ID, peer.Address)
	}
	return nil
}

func (n *Node) RunElectionTimer() {
	for {
		timeout := n.ElectionTimeout + time.Duration(rand.Intn(2))*time.Second
		time.Sleep(timeout)

		if n.State == "Leader" {
			continue
		}

		if time.Since(n.LastHeartbeat) > timeout {
			go n.StartElection()
		}

	}
}

func (n *Node) StartElection() {
	if n.State == "Candidate" || n.State == "Leader" {
		return
	}

	n.State = "Candidate"
	n.Term += 1
	n.VotedFor = n.ID

	var once sync.Once
	var voteCount int32 = 1

	log.Printf("Node: %v starting election for term: %d", n.ID, n.Term)

	for _, peer := range n.Peers {
		go func(peer Peer) {
			client := n.PeerClients[peer.ID]
			resp, err := client.RequestVote(context.Background(), &raft.VoteRequest{
				Term:        n.Term,
				CandidateId: n.ID,
			})
			if err != nil {
				log.Printf("Error requesting vote from %s: %v", peer.ID, err)
			}

			if resp.VoteGranted {
				count := atomic.AddInt32(&voteCount, 1)
				if count > int32(len(n.Peers)/2) {
					once.Do(func() {
						if n.State == "Candidate" {
							n.BecomeLeader()
						}
					})
				}
			} else if resp.Term > n.Term {
				n.Term = resp.Term
				n.State = "Follower"
				n.VotedFor = ""
			}
		}(peer)
	}
}

func (n *Node) BecomeLeader() {
	n.State = "Leader"
	log.Printf("Node %s became Leader for term %d", n.ID, n.Term)
	go n.SendHeartbeats()
}

func (n *Node) SendHeartbeats() {
	for n.State == "Leader" {
		for _, peer := range n.Peers {
			client := n.PeerClients[peer.ID]

			go func(peer Peer) {
				_, err := client.AppendEntries(context.Background(), &raft.AppendEntriesRequest{
					Term:     n.Term,
					LeaderId: n.ID,
					Entries:  nil,
				})
				if err != nil {
					log.Printf("Error sending Heartbeat to %s: %v", peer.ID, err)
				}
			}(peer)
		}
		time.Sleep(2 * time.Second)
	}
}
