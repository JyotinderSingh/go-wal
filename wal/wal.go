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
	sequenceNumber int64         `json:"sn"`
	Key            string        `json:"key"`
	Value          []byte        `json:"value,omitempty"` // Changed to string for Base64 encoding
	Op             OperationType `json:"op"`
}

// WAL structure
type WAL struct {
	file           *os.File
	lock           sync.Mutex
	lastSequenceNo int64
}

// Initialize a new WAL
func OpenWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	// TODO: Read last sequence number from file.
	return &WAL{file: file, lastSequenceNo: 0}, nil
}

// WriteEntry writes an entry to the WAL
func (wal *WAL) WriteEntry(entry WALEntry) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.lastSequenceNo++
	entry.sequenceNumber = wal.lastSequenceNo

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

// Read all entries from the WAL
func (wal *WAL) ReadAll() ([]WALEntry, error) {
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
