package node

import "Rafting/raft"

type LogEntry struct {
	Term    int64  `json:"term"`
	Command string `json:"command"`
}

func ToProtoEntries(entries []LogEntry) []*raft.LogEntry {
	out := make([]*raft.LogEntry, len(entries))
	for i, entry := range entries {
		out[i] = &raft.LogEntry{
			Term:    entry.Term,
			Command: entry.Command,
		}
	}
	return out
}

func FromProtoEntries(entries []*raft.LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, entry := range entries {
		if entry != nil {
			out[i] = LogEntry{
				Term:    entry.Term,
				Command: entry.Command,
			}
		}
	}
	return out
}
