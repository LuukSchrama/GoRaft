package node

import (
	"Rafting/raft"
	"encoding/json"
	"errors"
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
				Log:     []string{},
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
	self.ElectionTimeout = time.Duration(1+rand.Intn(5)) * time.Second
	self.PeerClients = make(map[string]raft.RaftServiceClient)
	
	return self, nil
}
