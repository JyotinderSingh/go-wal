# A Write-Ahead-Log for Go

go-wal is a Write-Ahead Log (WAL) implementation in Go. It has high read and write throughput, and is suitable to be used in high-performance applications.

## Features

- Write entries to the log
- Read all entries from the log
- Log Rotation for efficient startup and recovery.
- Sync entries to disk at regular intervals
- CRC32 checksum for data integrity
- Auto-Repair corrupted WALs

## Usage

### Creating a WAL

You can create a WAL using the `NewWAL` function. This function takes a file path.

```go
wal, err := Open("/wal/directory", enableFsync, maxSegmentSize, maxSegments)
```

### Writing to the WAL

You can write an entry to the WAL using the `Write` method. This method takes a byte slice as data.

```go
err := wal.WriteEntry([]byte("data"))
```

### Reading from the WAL

You can read all entries from the last WAL segment using the `ReadEntries` method.

```go
entries, err := wal.ReadAll()
if err != nil {
    log.Fatalf("Failed to read entries: %v", err)
}
```

### Repairing the WAL

You can repair a corrupted WAL using the `Repair` method. This method returns the repaired entries, and atomically replaces the corrupted WAL file with the repaired one.

```go
entries, err := wal.Repair()
```

### Closing the WAL

You can close the WAL using the `Close` method. Closing the WAL flushes the in-memory buffers and runs a final sync to disk (if enabled).

```go
err := wal.Close()
```

## Testing

This project includes a set of tests. You can run these tests using the `go test ./...` command.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
