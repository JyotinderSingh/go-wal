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
	lastSequenceNo uint64
	bufWriter      *bufio.Writer
	syncTimer      *time.Timer
	shouldFsync    bool
}

// Initialize a new WAL
func OpenWAL(filePath string, enableFsync bool) (*WAL, error) {
	var err error
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Seek to the end of the file
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	wal := &WAL{
		file:           file,
		lastSequenceNo: 0,
		bufWriter:      bufio.NewWriter(file),
		syncTimer:      time.NewTimer(syncInterval), // syncInterval is a predefined duration
	}

	// We can optimize this by reading the last entry and setting the
	// lastSequenceNo, but for now we'll just read all entries.
	var entries []*walpb.WAL_Entry
	if entries, err = wal.ReadAll(); err != nil {
		return nil, err
	}

	if len(entries) > 0 {
		wal.lastSequenceNo = entries[len(entries)-1].LogSequenceNumber
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

	// Seek to the beginning of the file
	if _, err = file.Seek(0, io.SeekStart); err != nil {
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

// Repair function that repairs a corrupted WAL. It starts scanning the WAL from the start and
// tries to read all entries. If it encounters a corrupted entry, it truncates the file at that
// point and returns the entries that were read before the corruption. It also overwrites the
// existing WAL file with the repaired entries.
// It checks the CRC of each entry to verify if it is corrupted. If the CRC is invalid, it
// truncates the file at that point and returns the entries that were read before the corruption.
// If there are any errors while trying to read the entries, it returns the entries that were
// read before the error, and overwrites the existing WAL file with the repaired entries.
// The function implements a logic similar to ReadAll() to read the entries.
func (wal *WAL) Repair() ([]*walpb.WAL_Entry, error) {
	// Open the file
	file, err := os.OpenFile(wal.file.Name(), os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Seek to the beginning of the file
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	defer file.Close()

	var entries []*walpb.WAL_Entry

	for {
		// Read the size of the next entry.
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// End of file reached, no corruption found.
				return entries, err
			}
			log.Printf("Error while reading entry size: %v", err)
			// Truncate the file at this point.
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return nil, nil
		}

		// Read the entry data.
		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			// Truncate the file at this point
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}

		// Deserialize the entry.
		var entry walpb.WAL_Entry
		MustUnmarshal(data, &entry)

		// Verify CRC
		expectedCRC := entry.CRC
		entry.CRC = 0 // Reset CRC to compute
		actualCRC := crc32.ChecksumIEEE(entry.GetData())
		if expectedCRC != actualCRC {
			log.Printf("CRC mismatch: data may be corrupted")
			// Truncate the file at this point
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}

			return entries, nil
		}
		entry.CRC = expectedCRC

		// Add the entry to the slice.
		entries = append(entries, &entry)
	}
}

// replaceWithFixedFile replaces the existing WAL file with the given entries.
// It first creates a new temporary WAL file and writes the entries to it.
// Then it renames the temporary file to the original file name and replaces the
// existing WAL file.
func (wal *WAL) replaceWithFixedFile(entries []*walpb.WAL_Entry) error {
	// Create a temporary file
	tempFilePath := fmt.Sprintf("%s.tmp", wal.file.Name())
	tempFile, err := os.OpenFile(tempFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// Write the entries to the temporary file
	for _, entry := range entries {
		marshaledEntry := MustMarshal(entry)

		size := int32(len(marshaledEntry))
		if err := binary.Write(tempFile, binary.LittleEndian, size); err != nil {
			return err
		}
		_, err := tempFile.Write(marshaledEntry)

		if err != nil {
			return err
		}
	}

	// Close the temporary file
	if err := tempFile.Close(); err != nil {
		return err
	}

	// Rename the temporary file to the original file name
	if err := os.Rename(tempFilePath, wal.file.Name()); err != nil {
		return err
	}

	return nil
}
