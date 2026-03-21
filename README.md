# Silo

A high-performance, log-structured key-value store implemented in Go, inspired by the original [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf). 

Further implementation details can be found in the [riak docs](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html#bitcask-implementation-details). 

## Features

- **Low Latency:** Optimized for fast read and write operations.
- **High Throughput:** Designed to handle large volumes of data.
- **Crash Recovery:** The log-structured design ensures data integrity as well as a quick startup when recovering from unexpected shutdowns.
- **Atomic Operations:** Writes are either fully completed or not at all.

## Installation

```bash
go get github.com/portbound/silo
```

## Quick Start

The API is incredibly simple by design. 

```go
package main

import (
	"fmt"
	"log"

	"github.com/portbound/silo"
)

func main() {
	// Connect to a Silo instance
	s, err := silo.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// Put a key-value pair
	k := []byte("key")
	v := []byte("value")
	if err := s.Put(k, v); err != nil {
		log.Fatal(err)
	}

	// Get the value
	val, err := s.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Retrieved: %s", string(val))

	// Delete the key
	if err := s.Delete(k); err != nil {
		log.Fatal(err)
	}
}
```

## Configuration

Silo can be configured using functional options during connection:

- `WithWorkDir(dir string)`: Sets the base directory where Silo will store its metadata and logs. 
- `WithMaxFileSize(size uint64)`: Sets the maximum size a  data file can reach before it is rotated.
- `WithSyncStrategy(strategy SyncStrategy)`: Defines how frequently data is flushed to disk. 
- `WithMergePolicy(policy MergePolicy)`: Configures the strategy and thresholds for reclaiming disk space.

### Default Options 

```go 
var defaultOpts = siloOpts{
	Dir:         ".",
	MaxFileSize: uint64(128 * 1024 * 1024), // 128MB
	MergePolicy: MergePolicy{
		Strategy:          MergeStrategyUnrestricted,
		Interval:          3 * time.Minute,
		WindowStart:       0,
		WindowEnd:         0,
		FragThreshold:     60,
		DeadByteThreshold: uint64(512 * 1024 * 1024), // 512MB
	},
	SyncStrategy: SyncAlways,
}
```

#### Sync Strategy 
- `SyncNone`: relies on the OS to flush data to disk.
- `SyncAlways`: flushes to disk after every write.

#### Merge Policy
- `Strategy`
    - `MergeStrategyUnrestricted`: Merges whenever thresholds are met.
    - `MergeStrategyNever`: Disables automatic merging.
    - `MergeStrategyWindow`: Restricts merging to a specific time window.
- `Interval`:  How often merge thresholds are evaluated.
- `WindowStart`: Hour (0-23)
- `WindowEnd`: Hour (0-23)
- `FragmentationThreshold`: Ratio of dead keys to live keys.
- `DeadByteThreshold`: Max capacity for dead keys occupying storage.

## Error Handling

- `ErrKeyNotFound`: Returned when a key is not found in the database.
- `ErrKeyTooLarge`: Returned when a key exceeds `MaxKeySize` (64 bytes).
- `ErrValueTooLarge`: Returned when a value exceeds `MaxValueSize` (64 KB).
- `ErrDatabaseClosed`: Returned when an operation is attempted on a closed instance.
- `ErrLocked`: Returned when trying to connect to an instance that is already locked by another process.
- `ErrChecksumFailed`: Returned when a record's checksum does not match, e.g. corrupted data.
- `ErrDataCorrupted`: Returned when a data file is detected to be corrupt.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
