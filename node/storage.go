package node

import (
	"Rafting/raft"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type PersistenceState struct {
	CurrentTerm int64           `json:"current_term"`
	VotedFor    string          `json:"voted_for"`
	Log         []raft.LogEntry `json:"log"`
}

type Storage struct {
	mu   sync.Mutex
	path string
}

func NewStorage(path string) *Storage {
	return &Storage{path: path}
}

func (s *Storage) SaveState(state *PersistenceState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(state, "", "	")
	if err != nil {
		return err
	}

	return os.WriteFile(s.path, data, 0644)
}

func (s *Storage) LoadState() (*PersistenceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return &PersistenceState{0, "", []raft.LogEntry{}}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("error reading files: %v", err)
	}

	if len(data) == 0 {
		return &PersistenceState{0, "", []raft.LogEntry{}}, nil
	}

	var state PersistenceState
	err = json.Unmarshal(data, &state)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling files: %v", err)
	}

	return &state, nil
}
