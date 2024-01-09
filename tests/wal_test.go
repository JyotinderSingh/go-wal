package tests

import (
	"os"
	"reflect"
	"testing"

	"github.com/JyotinderSingh/go-wal/wal"
)

func TestWAL_WriteAndRecover(t *testing.T) {
	// Setup: Create a temporary file for the WAL
	filePath := "test_wal.log"
	defer os.Remove(filePath) // Cleanup after the test

	walog, err := wal.NewWAL(filePath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer walog.Close()

	// Test data

	entries := []wal.WALEntry{
		{Key: "key1", Value: []byte("value1"), Op: wal.InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: wal.InsertOperation},
		{Key: "key3", Op: wal.DeleteOperation},
	}

	// Write entries to WAL
	for _, entry := range entries {
		if err := walog.WriteEntry(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	// Recover entries from WAL
	recoveredEntries, err := walog.Recover()
	if err != nil {
		t.Fatalf("Failed to recover entries: %v", err)
	}

	// Check if recovered entries match the written entries
	if !reflect.DeepEqual(entries, recoveredEntries) {
		t.Errorf("Recovered entries do not match written entries. Written: %+v, Recovered: %+v", entries, recoveredEntries)
	}
}
