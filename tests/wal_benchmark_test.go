package tests

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/JyotinderSingh/go-wal/wal"
)

const numEntries = 1000000 // Adjustable parameter for the number of entries

// BenchmarkWriteThroughput measures the throughput for writing operations
func BenchmarkWriteThroughput(b *testing.B) {
	filePath := "benchmark_write.log"
	walog, err := wal.NewWAL(filePath)
	if err != nil {
		b.Fatal("Failed to prepare WAL:", err)
	}
	defer cleanUpWAL(filePath)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < numEntries; j++ {
			if err := writeEntry(walog, j); err != nil {
				b.Error("Write error:", err)
			}
		}
	}
}

// BenchmarkReadThroughput measures the throughput for reading operations
func BenchmarkReadThroughput(b *testing.B) {
	filePath := "benchmark_read.log"
	walog, err := wal.NewWAL(filePath)
	if err != nil {
		b.Fatal("Failed to prepare WAL:", err)
	}
	defer cleanUpWAL(filePath)

	prepopulateWAL(walog, numEntries, b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := walog.Recover(); err != nil {
			b.Error("Recovery error:", err)
		}
	}
}

// BenchmarkConcurrency measures the performance of concurrent writing operations
func BenchmarkConcurrency(b *testing.B) {
	filePath := "benchmark_concurrent.log"
	walog, err := wal.NewWAL(filePath)
	if err != nil {
		b.Fatal("Failed to prepare WAL:", err)
	}
	defer cleanUpWAL(filePath)

	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				if err := writeEntry(walog, j); err != nil {
					b.Error("Concurrent write error:", err)
				}
			}
		}(i)
	}
	wg.Wait()
}

// Helper functions

func cleanUpWAL(filePath string) {
	if err := os.Remove(filePath); err != nil {
		fmt.Println("Error cleaning up WAL file:", err)
	}
}

func writeEntry(walog *wal.WAL, entryID int) error {
	entry := wal.WALEntry{
		Op:    wal.InsertOperation,
		Key:   "key" + strconv.Itoa(entryID),
		Value: []byte("value" + strconv.Itoa(entryID)),
	}
	return walog.WriteEntry(entry)
}

func prepopulateWAL(walog *wal.WAL, count int, b *testing.B) {
	for i := 0; i < count; i++ {
		if err := writeEntry(walog, i); err != nil {
			b.Fatal("Prepopulate error:", err)
		}
	}
}
