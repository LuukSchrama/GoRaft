package node

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
	Data map[string]string `json:"data"`
}

func NewKVStore() *KVStore {
	return &KVStore{
		Data: make(map[string]string),
	}
}

func (kv *KVStore) Apply(command string) error {
	command = strings.TrimSpace(command)
	if command == "" {
		return errors.New("empty command")
	}

	parts := strings.SplitN(command, " ", 2)
	op := strings.ToUpper(parts[0])
	switch op {
	case "SET":
		if len(parts) < 2 {
			return errors.New("invalid SET command:'SET key=value'")
		}
		kvParts := strings.Split(parts[1], "=")
		if len(kvParts) != 2 {
			return errors.New("invalid SET command, requires arg 'key=value'")
		}
		key := strings.TrimSpace(kvParts[0])
		val := kvParts[1]
		kv.mu.Lock()
		kv.Data[key] = val
		kv.mu.Unlock()
		return nil
	case "DELETE":
		if len(parts) < 2 {
			return errors.New("DELETE requires key arg")
		}
		key := strings.TrimSpace(parts[1])
		kv.mu.Lock()
		delete(kv.Data, key)
		kv.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("unsupported command: %s", op)
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.Data[key]
	return val, ok
}
