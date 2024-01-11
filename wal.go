package wal

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	walpb "github.com/JyotinderSingh/go-wal/types"
)

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
func (wal *WAL) WriteEntry(data []byte) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.lastSequenceNo++

	entry := walpb.WAL_Entry{
		LogSequenceNumber: wal.lastSequenceNo,
		Data:              data,
	}

	marshaledEntry := MustMarshal(&entry)

	size := int32(len(marshaledEntry))
	if err := binary.Write(wal.file, binary.LittleEndian, size); err != nil {
		return err
	}
	_, err := wal.file.Write(marshaledEntry)

	return err
}

// Close the WAL file
func (wal *WAL) Close() error {
	return wal.file.Close()
}

// Read all entries from the WAL
func (wal *WAL) ReadAll() ([]*walpb.WAL_Entry, error) {
	file, err := os.OpenFile(wal.file.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	var entries []*walpb.WAL_Entry

	for {
		// Read the size of the next entry.
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// End of file reached, return what we have.
				return entries, nil
			}
			return nil, err
		}

		// Read the entry data.
		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, err
		}

		// Deserialize the entry.
		var entry walpb.WAL_Entry
		MustUnmarshal(data, &entry)

		// Add the entry to the slice.
		entries = append(entries, &entry)
	}
}
