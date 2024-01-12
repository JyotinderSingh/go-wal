package tests

import (
	"encoding/json"
	"log"
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
		log.Printf("LogSequenceNumber: %d", entry.GetLogSequenceNumber())
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

// Test to verify that the log sequence number is incremented correctly
// after reopening the WAL.
func TestWAL_LogSequenceNumber(t *testing.T) {
	filePath := "test_wal.log"
	defer os.Remove(filePath) // Cleanup after the test

	walog, err := wal.OpenWAL(filePath, true)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Test data
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key3", Op: DeleteOperation},
		{Key: "key4", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key5", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key6", Op: DeleteOperation},
	}

	// Write entries to WAL
	for i := 0; i < 3; i++ {
		entry := entries[i]
		marshaledEntry, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("Failed to marshal entry: %v", err)
		}
		if err := walog.WriteEntry(marshaledEntry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	// Close the WAL
	if err := walog.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Reopen the WAL
	walog, err = wal.OpenWAL(filePath, true)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Write entries to WAL
	for i := 3; i < 6; i++ {
		entry := entries[i]
		marshaledEntry, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("Failed to marshal entry: %v", err)
		}
		if err := walog.WriteEntry(marshaledEntry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	// Important to ensure the entries are flushed to the disk.
	walog.Close()

	// Recover entries from WAL
	recoveredEntries, err := walog.ReadAll()
	if err != nil {
		t.Fatalf("Failed to recover entries: %v", err)
	}

	if len(recoveredEntries) != 6 {
		t.Fatalf("Expected 6 entries, got %d", len(recoveredEntries))
	}

	// Check if recovered entries match the written entries
	for entryIndex, entry := range recoveredEntries {
		unMarshalledEntry := Record{}
		// (entryIndex + 1) should be the same as the sequence number.
		// This is because the sequence number starts from 1.
		if entry.GetLogSequenceNumber() != int64(entryIndex+1) {
			t.Fatalf("Log sequence number does not match: %d", entry.GetLogSequenceNumber())
		}

		if err := json.Unmarshal(entry.Data, &unMarshalledEntry); err != nil {
			t.Fatalf("Failed to unmarshal entry: %v", err)
		}
		// Can't use deep equal because of the sequence number
		if unMarshalledEntry.Key != entries[entryIndex].Key ||
			unMarshalledEntry.Op != entries[entryIndex].Op ||
			!reflect.DeepEqual(unMarshalledEntry.Value, entries[entryIndex].Value) {
			t.Fatalf("Recovered entry does not match written entry: %v vs %v", unMarshalledEntry, entries[entryIndex%3])
		}
	}
}
