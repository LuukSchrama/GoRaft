package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type PersistenceState struct {
	CurrentTerm int64      `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
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
		return &PersistenceState{0, "", []LogEntry{}}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("error reading files: %v", err)
	}

	if len(data) == 0 {
		return &PersistenceState{0, "", []LogEntry{}}, nil
	}

	var state PersistenceState
	err = json.Unmarshal(data, &state)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling files: %v", err)
	}

	return &state, nil
}

func (s *Storage) SaveKV(kv *KVStore) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(kv, "", "	")
	if err != nil {
		return fmt.Errorf("error marshaling kv: %v", err)
	}

	kvPath := s.GetKVPath()
	tmpPath := fmt.Sprintf("%s.tmp", kvPath)
	err = os.WriteFile(tmpPath, data, 0644)
	if err != nil {
		return fmt.Errorf("error writing kv temp file: %v", err)
	}
	err = os.Rename(tmpPath, kvPath)
	if err != nil {
		return fmt.Errorf("error renaming kv file: %v", err)
	}

	return nil
}

func (s *Storage) LoadKV(kv *KVStore) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	kvPath := s.GetKVPath()
	data, err := os.ReadFile(kvPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error reading kv file: %v", err)
	}

	var tmp KVStore
	err = json.Unmarshal(data, &tmp)
	if err != nil {
		return fmt.Errorf("error unmarshaling kv store: %v", err)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Data = tmp.Data

	return nil
}

func (s *Storage) GetKVPath() string {
	base := filepath.Base(s.path)
	nodeID := strings.TrimSuffix(base, filepath.Ext(base))
	nodeID = strings.TrimPrefix(nodeID, "state_")

	dir := filepath.Dir(s.path)
	kvPath := filepath.Join(dir, fmt.Sprintf("kv_%s.json", nodeID))
	return kvPath
}
