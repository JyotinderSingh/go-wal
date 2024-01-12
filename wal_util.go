package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	walpb "github.com/JyotinderSingh/go-wal/types"
)

// unmarshalAndVerifyEntry unmarshals the given data into a WAL entry and
// verifies the CRC of the entry. Only returns an error if the CRC is invalid.
func unmarshalAndVerifyEntry(data []byte) (*walpb.WAL_Entry, error) {
	var entry walpb.WAL_Entry
	MustUnmarshal(data, &entry)

	if !verifyCRC(&entry) {
		return nil, fmt.Errorf("CRC mismatch: data may be corrupted")
	}

	return &entry, nil
}

// Validates whether the given entry has a valid CRC.
func verifyCRC(entry *walpb.WAL_Entry) bool {
	expectedCRC := entry.CRC
	// Reset the entry CRC for the verification.
	entry.CRC = 0
	actualCRC := crc32.ChecksumIEEE(entry.GetData())
	entry.CRC = expectedCRC

	return expectedCRC == actualCRC
}

// Finds the last segment ID from the given list of files.
func findLastSegmentIndexinFiles(files []string) (int, error) {
	var lastSegmentID int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentID, err := strconv.Atoi(strings.TrimPrefix(fileName, "segment-"))
		if err != nil {
			return 0, err
		}
		if segmentID > lastSegmentID {
			lastSegmentID = segmentID
		}
	}
	return lastSegmentID, nil
}

// Creates a log segment file with the given segment ID in the given directory.
func createSegmentFile(directory string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("segment-%d", segmentID))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}
