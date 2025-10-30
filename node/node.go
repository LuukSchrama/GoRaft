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
	Peers       []Peer
	PeerClients map[string]raft.RaftServiceClient

	State           string
	VotedFor        string
	ElectionTimeout time.Duration
	LastHeartbeat   time.Time

	Log         []LogEntry
	CommitIndex int64
	LastApplied int64
	commitMu    sync.Mutex
	storage     *Storage

	KV *KVStore
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
			command := fmt.Sprintf("SET X=%d", time.Now().Unix())
			n.AppendCommand(command)
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
				entries := ToProtoEntries(n.Log)
				_, err := client.AppendEntries(context.Background(), &raft.AppendEntriesRequest{
					Term:     n.Term,
					LeaderId: n.ID,
					Entries:  entries,
				})
				if err != nil {
					log.Printf("Error sending Heartbeat to %s: %v", peer.ID, err)
				}
			}(peer)
		}
		time.Sleep(10 * time.Second)
	}
}

func (n *Node) persist() {
	state := &PersistenceState{
		CurrentTerm: n.Term,
		VotedFor:    n.VotedFor,
		Log:         n.Log,
	}
	err := n.storage.SaveState(state)
	if err != nil {
		log.Printf("Error persisting state: %v", err)
	}
}

func (n *Node) AppendCommand(command string) {
	if n.State != "Leader" {
		log.Printf("Node %s is not a leader", n.ID)
		return
	}

	entry := LogEntry{
		Term:    n.Term,
		Command: command,
	}
	n.Log = append(n.Log, entry)
	n.persist()

	newIndex := int64(len(n.Log) - 1)
	entries := ToProtoEntries([]LogEntry{entry})
	var replicated int32 = 1

	for _, peer := range n.Peers {
		go func(peer Peer) {
			client := n.PeerClients[peer.ID]

			prevIndex := newIndex - 1
			var prevTerm int64 = 0
			if prevIndex >= 0 {
				prevTerm = n.Log[prevIndex].Term
			}

			req := &raft.AppendEntriesRequest{
				Term:         n.Term,
				LeaderId:     n.ID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: n.CommitIndex,
			}

			resp, err := client.AppendEntries(context.Background(), req)
			if err != nil {
				log.Printf("Error appending command to %s: %v", peer.ID, err)
			}

			if resp.Success {
				count := atomic.AddInt32(&replicated, 1)
				if count > int32(len(n.Peers)/2) {
					n.commitMu.Lock()
					if newIndex > n.CommitIndex {
						n.CommitIndex = newIndex
						n.commitMu.Unlock()
						go n.applyLogEntries()
					} else {
						n.commitMu.Unlock()
					}
				}
			} else {
				log.Printf("Peer %s rejected AppendEntries", peer.ID)
			}
		}(peer)
	}
}

func (n *Node) applyLogEntries() {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()

	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		entry := n.Log[n.LastApplied]

		log.Printf("Node %s aplied Log Entry %d: %s", n.ID, n.LastApplied, entry.Command)

		if n.KV != nil {
			err := n.KV.Apply(entry.Command)
			if err != nil {
				log.Printf("Node %s failed to apply command to %s: %v", n.ID, entry.Command, err)
			} else {
				err = n.storage.SaveKV(n.KV)
				if err != nil {
					log.Printf("Node %s failed to save KV: %v", n.ID, err)
				}
			}
		}
	}
}
