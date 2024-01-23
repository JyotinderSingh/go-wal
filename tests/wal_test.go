package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/JyotinderSingh/go-wal"
	"github.com/JyotinderSingh/go-wal/types"
	"github.com/stretchr/testify/assert"
)

const (
	maxSegments = 3
	maxFileSize = 64 * 1000 * 1000 // 64MB
)

func TestWAL_WriteAndRecover(t *testing.T) {
	// Setup: Create a temporary file for the WAL
	dirPath := "test_wal.log"
	defer os.RemoveAll(dirPath) // Cleanup after the test

	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
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
	recoveredEntries, err := walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")

	// Check if recovered entries match the written entries
	for entryIndex, entry := range recoveredEntries {
		unMarshalledEntry := Record{}
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
	dirPath := "test_wal.log"
	defer os.RemoveAll(dirPath) // Cleanup after the test

	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
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
	walog, err = wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
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
	recoveredEntries, err := walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")

	assertCollectionsAreIdentical(t, entries, recoveredEntries)
}

func TestWAL_WriteRepairRead(t *testing.T) {
	dirPath := "test_wal"
	defer os.RemoveAll(dirPath)

	// Create a new WAL
	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err)

	// Write some entries to the WAL
	err = walog.WriteEntry([]byte("entry1"))
	assert.NoError(t, err)
	err = walog.WriteEntry([]byte("entry2"))
	assert.NoError(t, err)

	walog.Close()

	// Corrupt the WAL by writing some random data
	file, err := os.OpenFile(filepath.Join(dirPath, "segment-0"), os.O_APPEND|os.O_WRONLY, 0644)
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
	walog, err = wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err)

	err = walog.WriteEntry([]byte("entry3"))
	assert.NoError(t, err)

	walog.Close()

	// Check that the correct entries were recovered
	entries, err = walog.ReadAll(false)
	assert.NoError(t, err)

	assert.Equal(t, 3, len(entries))
	assert.Equal(t, "entry1", string(entries[0].Data))
	assert.Equal(t, "entry2", string(entries[1].Data))
	assert.Equal(t, "entry3", string(entries[2].Data))
}

// Similar to previous function, but with a different corruption pattern
// (corrupting the CRC instead of writing random data).
func TestWAL_WriteRepairRead2(t *testing.T) {
	dirPath := "test_wal"

	defer os.RemoveAll(dirPath)

	// Create a new WAL
	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err)

	// Write some entries to the WAL
	err = walog.WriteEntry([]byte("entry1"))
	assert.NoError(t, err)
	err = walog.WriteEntry([]byte("entry2"))
	assert.NoError(t, err)

	walog.Close()

	// Corrupt the WAL by writing some random data
	file, err := os.OpenFile(filepath.Join(dirPath, "segment-0"), os.O_WRONLY, 0644)
	assert.NoError(t, err)

	// Read the last entry
	entries, err := walog.ReadAll(false)
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

// Test to verify log segment rotation. Creates very large log files (each file can only go upto 64 mb) to test
// the rotation logic.
func TestWAL_LogSegmentRotation(t *testing.T) {
	dirPath := "test_wal"
	defer os.RemoveAll(dirPath)

	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to create WAL")
	defer walog.Close()

	// Generate test data on the fly
	entries := generateTestData()

	// Write entries to WAL
	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// Recover entries from WAL
	_, err = walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")

	// Validate that only three files should be present inside the directory
	// with names segment-1, segment-2 and segment-3 were created.
	// Each file should be 64 mb in size.
	files, err := os.ReadDir(dirPath)
	assert.NoError(t, err, "Failed to read directory")
	assert.Equal(t, 3, len(files), "Expected 3 files")

	for _, file := range files {
		assert.True(t, strings.HasPrefix(file.Name(), "segment-"), "Unexpected file found")
	}
}

func TestWAL_OldestLogDeletion(t *testing.T) {
	dirPath := "test_wal"
	defer os.RemoveAll(dirPath)

	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, 3)
	assert.NoError(t, err, "Failed to create WAL")
	defer walog.Close()

	// Generate test data on the fly
	entries := generateTestData()

	// Write entries to WAL
	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// Recover entries from WAL (sanity check)
	_, err = walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")

	// Validate that only three files should be present inside the directory
	// with names segment-1, segment-2 and segment-3 were created.
	// Each file should be 64 mb in size.
	files, err := os.ReadDir(dirPath)
	assert.NoError(t, err, "Failed to read directory")
	assert.Equal(t, 3, len(files), "Expected 3 files")

	for idx, file := range files {
		assert.Equal(t, file.Name(), fmt.Sprintf("%s%d", "segment-", idx),
			"Unexpected file found")
	}

	// Write entries to WAL
	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// Recover entries from WAL (sanity check)
	_, err = walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")

	// Validate that the oldest log file was deleted
	files, err = os.ReadDir(dirPath)
	assert.NoError(t, err, "Failed to read directory")
	assert.Equal(t, 3, len(files), "Expected 3 files")

	for idx, file := range files {
		assert.Equal(t, file.Name(), fmt.Sprintf("%s%d", "segment-", idx+2),
			"Unexpected file found")
	}
}

// Writes 10000 entries to the WAL and then reads them back from offset 0.
func TestWAL_ReadFromOffset(t *testing.T) {
	directory := "test_wal"
	defer os.RemoveAll(directory)

	walog, err := wal.OpenWAL(directory, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to create WAL")
	defer walog.Close()

	// Generate test data on the fly
	entries := generateTestData()

	// Write entries to WAL
	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// Recover entries from WAL
	recoveredEntries, err := walog.ReadAllFromOffset(0, false)
	assert.NoError(t, err, "Failed to recover entries")

	// Check if recovered entries match the written entries
	assertCollectionsAreIdentical(t, entries, recoveredEntries)
}

func generateTestData() []Record {
	entries := []Record{}

	// Generate very large strings for the key and value
	keyPrefix := "key"
	valuePrefix := "value"
	keySize := 100000
	valueSize := 1000000

	for i := 0; i < 100; i++ {
		key := keyPrefix + strconv.Itoa(i) + strings.Repeat("x", keySize-len(strconv.Itoa(i))-len(keyPrefix))
		value := valuePrefix + strconv.Itoa(i) + strings.Repeat("x", valueSize-len(strconv.Itoa(i))-len(valuePrefix))

		entries = append(entries, Record{
			Key:   key,
			Value: []byte(value),
			Op:    InsertOperation,
		})
	}

	return entries
}

func assertCollectionsAreIdentical(t *testing.T, expected []Record, actual []*types.WAL_Entry) {
	assert.Equal(t, len(expected), len(actual), "Number of entries do not match")

	for entryIndex, entry := range actual {
		unMarshalledEntry := Record{}
		assert.NoError(t, json.Unmarshal(entry.Data, &unMarshalledEntry), "Failed to unmarshal entry")

		// Can't use deep equal because of the sequence number
		assert.Equal(t, expected[entryIndex].Key, unMarshalledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, expected[entryIndex].Op, unMarshalledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.True(t, reflect.DeepEqual(expected[entryIndex].Value, unMarshalledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
}

// Write entries to the wal, create checkpoint, write more entries. ReadAll should return all entries after the offset.
func TestWAL_Checkpoint(t *testing.T) {
	dirPath := "test_wal.log"
	defer os.RemoveAll(dirPath) // Cleanup after the test

	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
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

	// Create checkpoint
	assert.NoError(t, walog.CreateCheckpoint([]byte("checkpoint info")), "Failed to create checkpoint")

	// Write entries to WAL
	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}
	err = walog.Sync()
	assert.NoError(t, err, "Failed to sync")

	// Recover entries from WAL after the checkpoint
	recoveredEntries, err := walog.ReadAll(true)
	assert.NoError(t, err, "Failed to recover entries")

	assert.Equal(t, 1+len(entries), len(recoveredEntries), "Number of entries do not match")
	// Check if recovered entries match the written entries
	for entryIndex, entry := range recoveredEntries {
		if entryIndex == 0 {
			assert.NotNil(t, entry.IsCheckpoint, "Expected checkpoint entry")
			assert.Equal(t, true, entry.GetIsCheckpoint(), "Expected checkpoint entry")
			assert.Equal(t, []byte("checkpoint info"), entry.GetData(), "Checkpoint info does not match")
			continue
		}
		unMarshalledEntry := Record{}
		assert.NoError(t, json.Unmarshal(entry.Data, &unMarshalledEntry), "Failed to unmarshal entry")

		// Can't use deep equal because of the sequence number
		assert.Equal(t, entries[entryIndex-1].Key, unMarshalledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, entries[entryIndex-1].Op, unMarshalledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.True(t, reflect.DeepEqual(entries[entryIndex-1].Value, unMarshalledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
}

func TestWAL_NoWritesAfterCheckpoint(t *testing.T) {
	dirPath := "test_wal.log"
	defer os.RemoveAll(dirPath) // Cleanup after the test

	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
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

	// Create checkpoint
	assert.NoError(t, walog.CreateCheckpoint([]byte("checkpoint info")), "Failed to create checkpoint")
	err = walog.Sync()
	assert.NoError(t, err, "Failed to sync")

	// Recover entries from WAL after the checkpoint
	recoveredEntries, err := walog.ReadAll(true)
	assert.NoError(t, err, "Failed to recover entries")

	assert.Equal(t, 1, len(recoveredEntries), "Number of entries do not match")
	// The only entry should be the checkpoint entry
	assert.NotNil(t, recoveredEntries[0].IsCheckpoint, "Expected checkpoint entry")
	assert.Equal(t, true, recoveredEntries[0].GetIsCheckpoint(), "Expected checkpoint entry")
	assert.Equal(t, []byte("checkpoint info"), recoveredEntries[0].GetData(), "Checkpoint info does not match")
}
