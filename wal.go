package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	walpb "github.com/JyotinderSingh/go-wal/types"
	"google.golang.org/protobuf/proto"
)

const (
	syncInterval = 200 * time.Millisecond
)

// WAL structure
type WAL struct {
	directory      string
	file           *os.File
	lock           sync.Mutex
	lastSequenceNo uint64
	bufWriter      *bufio.Writer
	syncTimer      *time.Timer
	shouldFsync    bool
	maxFileSize    int64
	maxSegments    uint
	currentSegment uint
}

// Initialize a new WAL
func OpenWAL(directory string, enableFsync bool, maxFileSize int64, maxSegments uint) (*WAL, error) {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	// Get the list of log segment files in the directory
	files, err := filepath.Glob(filepath.Join(directory, "segment-*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID uint
	if len(files) > 0 {
		// Find the last segment ID
		lastSegmentID, err = findLastSegmentID(files)
		if err != nil {
			return nil, err
		}
	} else {
		// Create the first log segment
		file, err := createSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}

		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	// Open the last log segment file
	filePath := filepath.Join(directory, fmt.Sprintf("segment-%d", lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Seek to the end of the file
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	wal := &WAL{
		directory:      directory,
		file:           file,
		lastSequenceNo: 0,
		bufWriter:      bufio.NewWriter(file),
		syncTimer:      time.NewTimer(syncInterval), // syncInterval is a predefined duration
		shouldFsync:    enableFsync,
		maxFileSize:    maxFileSize,
		maxSegments:    maxSegments,
		currentSegment: lastSegmentID,
	}

	if wal.lastSequenceNo, err = wal.getLastSequenceNo(); err != nil {
		return nil, err
	}

	go wal.keepSyncing()

	return wal, nil
}

// WriteEntry writes an entry to the WAL
func (wal *WAL) WriteEntry(data []byte) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	entry, err := wal.createEntry(data)
	if err != nil {
		return err
	}

	if err := wal.rotateLogIfNeeded(); err != nil {
		return err
	}

	return wal.writeEntryToBuffer(entry)
}

func (wal *WAL) createEntry(data []byte) (*walpb.WAL_Entry, error) {
	wal.lastSequenceNo++
	entry := &walpb.WAL_Entry{
		LogSequenceNumber: wal.lastSequenceNo,
		Data:              data,
		CRC:               crc32.ChecksumIEEE(data),
	}

	return entry, nil
}

func (wal *WAL) writeEntryToBuffer(entry *walpb.WAL_Entry) error {
	marshaledEntry := MustMarshal(entry)

	size := int32(len(marshaledEntry))
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, size); err != nil {
		return err
	}
	_, err := wal.bufWriter.Write(marshaledEntry)

	return err
}

func (wal *WAL) rotateLogIfNeeded() error {
	fileInfo, err := wal.file.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size() >= wal.maxFileSize {
		if err := wal.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (wal *WAL) rotateLog() error {
	if err := wal.Sync(); err != nil {
		return err
	}

	if err := wal.file.Close(); err != nil {
		return err
	}

	wal.currentSegment++
	if wal.currentSegment >= wal.maxSegments {
		wal.currentSegment = 0
	}

	newFile, err := createSegmentFile(wal.directory, wal.currentSegment)
	if err != nil {
		return err
	}

	wal.file = newFile
	wal.bufWriter = bufio.NewWriter(newFile)

	return nil
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
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				return entries, nil
			}
			return nil, err
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, err
		}

		entry, err := unmarshalAndVerifyEntry(data)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
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

// Repairs a corrupted WAL by scanning the WAL from the start and reading all
// entries until a corrupted entry is encountered, at which point the file is
// truncated. The function returns the entries that were read before the
// corruption and overwrites the existing WAL file with the repaired entries.
// It checks the CRC of each entry to verify if it is corrupted, and if the CRC
// is invalid, the file is truncated at that point.
func (wal *WAL) Repair() ([]*walpb.WAL_Entry, error) {
	files, err := filepath.Glob(filepath.Join(wal.directory, "segment-*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID uint
	if len(files) > 0 {
		// Find the last segment ID
		lastSegmentID, err = findLastSegmentID(files)
		if err != nil {
			return nil, err
		}
	} else {
		log.Fatalf("No log segments found, nothing to repair.")
	}
	// Open the file
	// Open the last log segment file
	filePath := filepath.Join(wal.directory, fmt.Sprintf("segment-%d", lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	// Seek to the beginning of the file
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

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
		if err := proto.Unmarshal(data, &entry); err != nil {
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}

		if !verifyCRC(&entry) {
			log.Printf("CRC mismatch: data may be corrupted")
			// Truncate the file at this point
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}

			return entries, nil
		}

		// Add the entry to the slice.
		entries = append(entries, &entry)
	}
}

// replaceWithFixedFile replaces the existing WAL file with the given entries
// atomically.
func (wal *WAL) replaceWithFixedFile(entries []*walpb.WAL_Entry) error {
	// Create a temporary file to make the operation look atomic.
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

func (wal *WAL) getLastSequenceNo() (uint64, error) {
	entry, err := wal.getLastEntryInLog()
	if err != nil {
		return 0, err
	}

	if entry != nil {
		return entry.GetLogSequenceNumber(), nil
	}

	return 0, nil
}

// getLastEntryInLog iterates through all the entries of the log and returns the
// last entry. It employs an efficient method of scanning the files by reading
// the size of the current entry and directly skipping to the next entry to
// reach the end of the file.
func (wal *WAL) getLastEntryInLog() (*walpb.WAL_Entry, error) {
	file, err := os.OpenFile(wal.file.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var previousSize int32
	var offset int64
	var entry *walpb.WAL_Entry

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// End of file reached, read the last entry at the saved offset.
				if offset == 0 {
					return entry, nil
				}

				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				// Read the entry data.
				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}

				entry, err = unmarshalAndVerifyEntry(data)
				if err != nil {
					return nil, err
				}

				return entry, nil
			}
			return nil, err
		}

		// Get current offset
		offset, err = file.Seek(0, io.SeekCurrent)
		previousSize = size

		if err != nil {
			return nil, err
		}

		// Skip to the next entry.
		if _, err := file.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}
