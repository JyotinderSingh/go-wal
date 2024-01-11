package tests

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/JyotinderSingh/go-wal"
)

func TestWAL_WriteAndRecover(t *testing.T) {
	// Setup: Create a temporary file for the WAL
	filePath := "test_wal.log"
	defer os.Remove(filePath) // Cleanup after the test

	walog, err := wal.OpenWAL(filePath, true)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer walog.Close()

	// Test data

	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key3", Op: DeleteOperation},
	}

	// Write entries to WAL
	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("Failed to marshal entry: %v", err)
		}
		if err := walog.WriteEntry(marshaledEntry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	// Recover entries from WAL
	recoveredEntries, err := walog.ReadAll()
	if err != nil {
		t.Fatalf("Failed to recover entries: %v", err)
	}

	// Check if recovered entries match the written entries
	for entryIndex, entry := range recoveredEntries {
		unMarshalledEntry := Record{}
		if err := json.Unmarshal(entry.Data, &unMarshalledEntry); err != nil {
			t.Fatalf("Failed to unmarshal entry: %v", err)
		}
		// Can't use deep equal because of the sequence number
		if unMarshalledEntry.Key != entries[entryIndex].Key ||
			unMarshalledEntry.Op != entries[entryIndex].Op ||
			!reflect.DeepEqual(unMarshalledEntry.Value, entries[entryIndex].Value) {
			t.Fatalf("Recovered entry does not match written entry: %v", entry)
		}
	}
}
