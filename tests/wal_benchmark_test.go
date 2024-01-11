package tests

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/JyotinderSingh/go-wal"
)

const numEntries = 10000000 // Adjustable parameter for the number of entries

// BenchmarkWriteThroughput measures the throughput for writing operations
func BenchmarkWriteThroughput(b *testing.B) {
	filePath := "benchmark_write.log"
	walog, err := wal.OpenWAL(filePath)
	if err != nil {
		b.Fatal("Failed to prepare WAL:", err)
	}
	defer cleanUpWAL(filePath)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := 0; j < numEntries; j++ {
			if err := writeEntry(walog, j); err != nil {
				b.Error("Write error:", err)
				return
			}
		}
		duration := time.Since(start)

		// Calculating the throughput: number of entries / total time (in seconds)
		throughput := float64(numEntries) / duration.Seconds()
		fmt.Printf("WriteThroughput: %f entries/sec\n", throughput)
	}
}

// BenchmarkReadThroughput measures the throughput for reading operations
func BenchmarkReadThroughput(b *testing.B) {
	filePath := "benchmark_read.log"
	walog, err := wal.OpenWAL(filePath)
	if err != nil {
		b.Fatal("Failed to prepare WAL:", err)
	}
	defer cleanUpWAL(filePath)

	prepopulateWAL(walog, numEntries, b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := walog.ReadAll(); err != nil {
			b.Error("Recovery error:", err)
			return
		}
		duration := time.Since(start)

		// Calculating the throughput: number of entries / total time (in seconds)
		throughput := float64(numEntries) / duration.Seconds()
		fmt.Printf("ReadThroughput: %f entries/sec\n", throughput)
	}
}

// BenchmarkConcurrency measures the performance of concurrent writing operations
func BenchmarkConcurrentWriteThroughPut(b *testing.B) {
	filePath := "benchmark_concurrent.log"
	walog, err := wal.OpenWAL(filePath)
	if err != nil {
		b.Fatal("Failed to prepare WAL:", err)
	}
	defer cleanUpWAL(filePath)

	var wg sync.WaitGroup
	totalEntries := 100 * 10000 // Total entries = number of goroutines * entries per goroutine

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()

		for j := 0; j < 100; j++ { // 100 goroutines
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for k := 0; k < numEntries/100; k++ { // 10000 entries per goroutine
					if err := writeEntry(walog, k); err != nil {
						b.Error("Concurrent write error:", err)
						return
					}
				}
			}(j)
		}
		wg.Wait()

		duration := time.Since(start)
		throughput := float64(totalEntries) / duration.Seconds()
		fmt.Printf("ConcurrentWriteThroughPut: %f entries/sec\n", throughput)
	}
}

func cleanUpWAL(filePath string) {
	if err := os.Remove(filePath); err != nil {
		fmt.Println("Error cleaning up WAL file:", err)
	}
}

func writeEntry(walog *wal.WAL, entryID int) error {
	entry := Record{
		Op:    InsertOperation,
		Key:   "key" + strconv.Itoa(entryID),
		Value: []byte("value" + strconv.Itoa(entryID)),
	}

	marshaledEntry, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	return walog.WriteEntry(marshaledEntry)
}

func prepopulateWAL(walog *wal.WAL, count int, b *testing.B) {
	for i := 0; i < count; i++ {
		if err := writeEntry(walog, i); err != nil {
			b.Fatal("Prepopulate error:", err)
		}
	}
}
