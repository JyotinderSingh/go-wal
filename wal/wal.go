package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type OperationType int

const (
	InsertOperation OperationType = iota
	DeleteOperation
)

// WALEntry represents a log entry in the WAL
type WALEntry struct {
	Key   string        `json:"key"`
	Value []byte        `json:"value,omitempty"` // Changed to string for Base64 encoding
	Op    OperationType `json:"op"`
}

// WAL structure
type WAL struct {
	file *os.File
	lock sync.Mutex
}

// Initialize a new WAL
func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

// WriteEntry writes an entry to the WAL
func (wal *WAL) WriteEntry(entry WALEntry) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = wal.file.Write(append(data, '\n'))
	return err
}

// Close the WAL file
func (wal *WAL) Close() error {
	return wal.file.Close()
}

// Recover from the WAL
func (wal *WAL) Recover() ([]WALEntry, error) {
	file, err := os.Open(wal.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []WALEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}
