package tests

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/JyotinderSingh/go-wal"
	"github.com/stretchr/testify/assert"
)

func TestWAL_WriteAndRecover(t *testing.T) {
	// Setup: Create a temporary file for the WAL
	filePath := "test_wal.log"
	defer os.Remove(filePath) // Cleanup after the test

	walog, err := wal.OpenWAL(filePath, true)
	assert.NoError(t, err, "Failed to create WAL")
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
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// Recover entries from WAL
	recoveredEntries, err := walog.ReadAll()
	assert.NoError(t, err, "Failed to recover entries")

	// Check if recovered entries match the written entries
	for entryIndex, entry := range recoveredEntries {
		unMarshalledEntry := Record{}
		log.Printf("LogSequenceNumber: %d", entry.GetLogSequenceNumber())
		assert.NoError(t, json.Unmarshal(entry.Data, &unMarshalledEntry), "Failed to unmarshal entry")

		// Can't use deep equal because of the sequence number
		assert.Equal(t, entries[entryIndex].Key, unMarshalledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, entries[entryIndex].Op, unMarshalledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.True(t, reflect.DeepEqual(entries[entryIndex].Value, unMarshalledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
}

// Test to verify that the log sequence number is incremented correctly
// after reopening the WAL.
func TestWAL_LogSequenceNumber(t *testing.T) {
	filePath := "test_wal.log"
	defer os.Remove(filePath) // Cleanup after the test

	walog, err := wal.OpenWAL(filePath, true)
	assert.NoError(t, err, "Failed to create WAL")

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
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// Close the WAL
	assert.NoError(t, walog.Close(), "Failed to close WAL")

	// Reopen the WAL
	walog, err = wal.OpenWAL(filePath, true)
	assert.NoError(t, err, "Failed to reopen WAL")

	// Write entries to WAL
	for i := 3; i < 6; i++ {
		entry := entries[i]
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// Important to ensure the entries are flushed to the disk.
	assert.NoError(t, walog.Close(), "Failed to close WAL")

	// Recover entries from WAL
	recoveredEntries, err := walog.ReadAll()
	assert.NoError(t, err, "Failed to recover entries")

	assert.Equal(t, 6, len(recoveredEntries), "Expected 6 entries")

	// Check if recovered entries match the written entries
	for entryIndex, entry := range recoveredEntries {
		unMarshalledEntry := Record{}
		// (entryIndex + 1) should be the same as the sequence number.
		// This is because the sequence number starts from 1.
		assert.Equal(t, uint64(entryIndex+1), entry.GetLogSequenceNumber(), "Log sequence number does not match")

		assert.NoError(t, json.Unmarshal(entry.Data, &unMarshalledEntry), "Failed to unmarshal entry")

		// Can't use deep equal because of the sequence number
		assert.Equal(t, entries[entryIndex].Key, unMarshalledEntry.Key, "Recovered entry key does not match written entry")
		assert.Equal(t, entries[entryIndex].Op, unMarshalledEntry.Op, "Recovered entry operation does not match written entry")
		assert.True(t, reflect.DeepEqual(entries[entryIndex].Value, unMarshalledEntry.Value), "Recovered entry value does not match written entry")
	}
}

func TestWAL_WriteRepairRead(t *testing.T) {
	filepath := "test.wal"
	defer os.Remove(filepath)

	// Create a new WAL
	walog, err := wal.OpenWAL(filepath, true)
	assert.NoError(t, err)

	// Write some entries to the WAL
	err = walog.WriteEntry([]byte("entry1"))
	assert.NoError(t, err)
	err = walog.WriteEntry([]byte("entry2"))
	assert.NoError(t, err)

	walog.Close()

	// Corrupt the WAL by writing some random data
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY, 0644)
	assert.NoError(t, err)

	_, err = file.Write([]byte("random data"))
	assert.NoError(t, err)
	file.Close()

	// Repair the WAL
	entries, err := walog.Repair()
	assert.NoError(t, err)

	// Check that the correct entries were recovered
	assert.Equal(t, 2, len(entries))
	assert.Equal(t, "entry1", string(entries[0].Data))
	assert.Equal(t, "entry2", string(entries[1].Data))

	// Check that the WAL is usable
	walog, err = wal.OpenWAL(filepath, true)
	assert.NoError(t, err)

	err = walog.WriteEntry([]byte("entry3"))
	assert.NoError(t, err)

	walog.Close()

	// Check that the correct entries were recovered
	entries, err = walog.ReadAll()
	assert.NoError(t, err)

	assert.Equal(t, 3, len(entries))
	assert.Equal(t, "entry1", string(entries[0].Data))
	assert.Equal(t, "entry2", string(entries[1].Data))
	assert.Equal(t, "entry3", string(entries[2].Data))
}

// Similar to previous function, but with a different corruption pattern
// (corrupting the CRC instead of writing random data).
func TestWAL_WriteRepairRead2(t *testing.T) {
	filepath := "test.wal"

	defer os.Remove(filepath)

	// Create a new WAL
	walog, err := wal.OpenWAL(filepath, true)
	assert.NoError(t, err)

	// Write some entries to the WAL
	err = walog.WriteEntry([]byte("entry1"))
	assert.NoError(t, err)
	err = walog.WriteEntry([]byte("entry2"))
	assert.NoError(t, err)

	walog.Close()

	// Corrupt the WAL by writing some random data
	file, err := os.OpenFile(filepath, os.O_WRONLY, 0644)
	assert.NoError(t, err)

	// Read the last entry
	entries, err := walog.ReadAll()
	assert.NoError(t, err)
	lastEntry := entries[len(entries)-1]

	// Corrupt the CRC
	lastEntry.CRC = 0
	marshaledEntry := wal.MustMarshal(lastEntry)

	// Seek to the last entry
	_, err = file.Seek(-int64(len(marshaledEntry)), io.SeekEnd)
	assert.NoError(t, err)

	_, err = file.Write(marshaledEntry)
	assert.NoError(t, err)

	file.Close()

	// Repair the WAL
	entries, err = walog.Repair()
	assert.NoError(t, err)

	// Check that the correct entries were recovered
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, "entry1", string(entries[0].Data))
}
