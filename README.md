# A Write-Ahead-Log for Go

go-wal is a Write-Ahead Log (WAL) implementation in Go. It has high read and write throughput, and is suitable to be used in high-performance applications.

## Features

- Write entries to the log.
- Read all entries from given log segment offset.
- Read all entries from the last log segment.
- Log Rotation for efficient startup and recovery.
- Auto-Remove old log segments on reaching segment limit.
- Sync entries to disk at regular intervals.
- CRC32 checksum for data integrity.
- Auto-Repair corrupted WALs.
- Supports checkpoints

## Usage

The WAL assumes that at a given time either you are writing to the WAL, or you are reading from it. You cannot write and read from the WAL at the same time.

### Creating a WAL

You can create a WAL using the `NewWAL` function. This function takes a file path.

```go
wal, err := OpenWAL("/wal/directory", enableFsync, maxSegmentSize, maxSegments)
```

### Writing to the WAL

You can write an entry to the WAL using the `Write` method. This method takes a byte slice as data. This method is thread-safe.

```go
err := wal.WriteEntry([]byte("data"))
```

### Checkpointing the WAL

You can checkpoint the WAL using the `Checkpoint` method. This method flushes the in-memory buffers and runs a sync to disk (if enabled).

Also allows the user to store application specific data in the checkpoint.

```go
err := wal.CreateCheckpoint([]byte("checkpoint info"))
```

### Reading from the WAL

You can read all entries from the last WAL segment using the `ReadEntries` method.

```go
// Read all entries from last segment.
entries, err = wal.ReadAll(false)
if err != nil {
    log.Fatalf("Failed to read entries: %v", err)
}

// Read all entries from last segment after the checkpoint
entries, err = wal.ReadAll(true)
if err != nil {
    log.Fatalf("Failed to read entries: %v", err)
}
```

You can also read from a given offset (inclusive) using the `ReadAllFromOffset` method. This method returns all the entries from the WAL starting from given log segment offset.

```go
// Read all entries from a given offset.
entries, err = wal.ReadAllFromOffset(offset, false)

// Read all entries from a given offset after the checkpoint.
entries, err = wal.ReadAllFromOffset(offset, true)
```

### Repairing the WAL

You can repair a corrupted WAL using the `Repair` method. This method returns the repaired entries, and atomically replaces the corrupted WAL file with the repaired one.

The WAL is capable of recovering from corrupted entries, as well as partial damage to the WAL file. However, if the file is completely corrupted, the WAL may not be able to recover from it and would proceed with replacing the file with an empty one.

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
