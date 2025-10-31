package node

import (
	"Rafting/raft"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

func LoadClusterConfig(configPath, selfId string) (*Node, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var conf struct {
		Nodes []Peer `json:"nodes"`
	}

	err = json.Unmarshal(data, &conf)
	if err != nil {
		return nil, err
	}

	var self *Node
	var peers []Peer
	for _, n := range conf.Nodes {
		if n.ID == selfId {
			self = &Node{
				ID:      n.ID,
				Address: n.Address,
				Term:    0,
				Log:     []LogEntry{},
			}
		} else {
			peers = append(peers, n)
		}
	}

	if self == nil {
		return nil, errors.New("no Node found with ID: " + selfId)
	}

	self.Peers = peers
	self.State = "Follower"
	self.VotedFor = ""
	self.LastHeartbeat = time.Now()
	self.ElectionTimeout = time.Duration(5+rand.Intn(10)) * time.Second
	self.PeerClients = make(map[string]raft.RaftServiceClient)

	self.storage = NewStorage(fmt.Sprintf("/data/state_%v.json", selfId))
	log.Println("Created storage")
	state, err := self.storage.LoadState()
	if err != nil {
		return nil, err
	}
	self.Term = state.CurrentTerm
	self.VotedFor = state.VotedFor
	self.Log = state.Log

	self.KV = NewKVStore()
	err = self.storage.LoadKV(self.KV)
	if err != nil {
		return nil, err
	}

	return self, nil
}
