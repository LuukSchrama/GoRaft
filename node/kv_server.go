package node

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func (n *Node) StartKVHTTPServer(port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			http.Error(w, "key required", http.StatusBadRequest)
			return
		}

		val, ok := n.KV.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Add("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": val,
		})
	})

	mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "invalid method", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req struct {
			Command string `json:"command"`
		}
		err = json.Unmarshal(body, &req)
		if err != nil || strings.TrimSpace(req.Command) == "" {
			http.Error(w, "invalid command, expected JSON {\"command\":\"SET k=v\"}", http.StatusBadRequest)
			return
		}

		if n.State != "Leader" {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}

		n.AppendCommand(req.Command)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "accepted",
		})
	})

	address := fmt.Sprintf(":%d", port)
	log.Printf("Starting KV HTTP server on %s", address)
	if err := http.ListenAndServe(address, mux); err != nil {
		log.Printf("Error starting KV HTTP server on %s: %v", address, err)
	}
}
