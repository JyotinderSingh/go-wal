package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"
	"time"

	walpb "github.com/JyotinderSingh/go-wal/types"
)

const (
	syncInterval = 500 * time.Millisecond
)

// WAL structure
type WAL struct {
	file           *os.File
	lock           sync.Mutex
	lastSequenceNo int64
	bufWriter      *bufio.Writer
	syncTimer      *time.Timer
	shouldFsync    bool
}

// Initialize a new WAL
func OpenWAL(filePath string, enableFsync bool) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	// TODO: Read last sequence number from file.
	wal := &WAL{
		file:           file,
		lastSequenceNo: 0,
		bufWriter:      bufio.NewWriter(file),
		syncTimer:      time.NewTimer(syncInterval), // syncInterval is a predefined duration
	}

	go wal.keepSyncing()

	return wal, nil
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

	// Set the CRC field
	entry.CRC = crc32.ChecksumIEEE(entry.GetData())

	marshaledEntry := MustMarshal(&entry)

	size := int32(len(marshaledEntry))
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, size); err != nil {
		return err
	}
	_, err := wal.bufWriter.Write(marshaledEntry)

	return err
}

// Close the WAL file
func (wal *WAL) Close() error {
	if err := wal.bufWriter.Flush(); err != nil {
		return err
	}
	return wal.file.Close()
}

// Read all entries from the WAL
func (wal *WAL) ReadAll() ([]*walpb.WAL_Entry, error) {
	file, err := os.OpenFile(wal.file.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

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

		// Verify CRC
		expectedCRC := entry.CRC
		entry.CRC = 0 // Reset CRC to compute
		actualCRC := crc32.ChecksumIEEE(entry.GetData())
		if expectedCRC != actualCRC {
			return []*walpb.WAL_Entry{}, fmt.Errorf("CRC mismatch: data may be corrupted")
		}

		// Add the entry to the slice.
		entries = append(entries, &entry)
	}
}

func (wal *WAL) Sync() error {
	if err := wal.bufWriter.Flush(); err != nil {
		return err
	}
	if wal.shouldFsync {
		if err := wal.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// resetTimer resets the synchronization timer.
func (wal *WAL) resetTimer() {
	wal.syncTimer.Reset(syncInterval)
}

func (wal *WAL) keepSyncing() {
	for {
		<-wal.syncTimer.C

		wal.lock.Lock()
		err := wal.Sync()
		wal.lock.Unlock()

		if err != nil {
			log.Printf("Error while performing sync: %v", err)
		}

		wal.resetTimer()
	}
}
