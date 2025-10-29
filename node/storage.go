package node

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type PersistanceState struct {
	CurrentTerm int64    `json:"current_term"`
	VotedFor    string   `json:"voted_for"`
	Log         []string `json:"log"`
}

type Storage struct {
	mu   sync.Mutex
	path string
}

func NewStorage(path string) *Storage {
	return &Storage{path: path}
}

func (s *Storage) SaveState(state *PersistanceState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(state, "", "	")
	if err != nil {
		return err
	}

	return os.WriteFile(s.path, data, 0644)
}

func (s *Storage) LoadState() (*PersistanceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return &PersistanceState{0, "", []string{}}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("error reading files: %v", err)
	}

	if len(data) == 0 {
		return &PersistanceState{0, "", []string{}}, nil
	}

	var state PersistanceState
	err = json.Unmarshal(data, &state)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling files: %v", err)
	}

	return &state, nil
}
