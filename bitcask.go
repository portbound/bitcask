package bitcask

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"
)

const (
	MaxKeySize   = 64
	MaxValueSize = 64 * 1024 // 64kb
)

type Bitcask struct {
	lock           *os.File
	mu             sync.RWMutex
	activeDataFile *os.File
	writePosition  uint64
	keys           map[string]*Hint
	opts           bitcaskOpts
	logger         *slog.Logger
	totalBytes     uint64
	deadBytes      uint64
	ctx            context.Context
	cancel         context.CancelFunc
}

// Hint represents the location and metadata of a key in a data file.
type Hint struct {
	FileId         uint64
	ValueSize      uint32
	RecordPosition uint32
	Timestamp      uint32
}

var (
	// ErrKeyTooLarge is returned when a key exceeds MaxKeySize
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge is returned when a value exceeds MaxValueSize
	ErrValueTooLarge = errors.New("value too large")

	// ErrKeyNotFound is returned when a key is not found in the database.
	ErrKeyNotFound = errors.New("key not found")

	// ErrChecksumFailed is returned when a record's checksum does not match.
	ErrChecksumFailed = errors.New("checksum failed")

	// ErrDatabaseClosed is returned when an operation is attempted on a closed database.
	ErrDatabaseClosed = errors.New("database is closed")

	// ErrDataCorrupted is returned when a data file is detected to be corrupt.
	ErrDataCorrupted = errors.New("data corrupted")

	// ErrLocked is returned when trying to open a database that is already locked.
	ErrLocked = errors.New("bitcask is locked by another process")
)

// Connect opens a Bitcask database for the given options.
// If a database does not exist at the specified path, it will be created.
func Connect(opts ...Option) (*Bitcask, error) {
	b := Bitcask{
		mu:   sync.RWMutex{},
		keys: make(map[string]*Hint),
		opts: defaultOpts,
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(&b)
	}

	err := b.reconnect()
	if err == nil {
		return &b, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reconnect: %w", err)
	}

	if err := b.new(); err != nil {
		return nil, fmt.Errorf("initialize new bitcask: %w", err)
	}
	return &b, nil
}

// Put stores a key and value in the database. If the key already exists, its
// value will be overwritten.
func (b *Bitcask) Put(k, v []byte) error {
	if len(k) > MaxKeySize {
		return fmt.Errorf("%w: %d > %d", ErrKeyTooLarge, len(k), MaxKeySize)
	}

	if len(v) > MaxValueSize {
		return fmt.Errorf("%w: %d > %d", ErrValueTooLarge, len(v), MaxValueSize)
	}

	timestamp := uint32(time.Now().Unix())
	record := encodeRecord(k, v, timestamp)

	b.mu.Lock()
	defer b.mu.Unlock()

	fileId, err := parseFileId(b.activeDataFile)
	if err != nil {
		return fmt.Errorf("parse file id: %w", err)
	}

	n, err := b.writeFile(b.activeDataFile, record)
	if err != nil {
		return fmt.Errorf("write record to %s: %w", b.activeDataFile.Name(), err)
	}

	hint := Hint{
		FileId:         fileId,
		ValueSize:      uint32(len(v)),
		RecordPosition: uint32(b.writePosition),
		Timestamp:      timestamp,
	}

	// if we're overwriting a record, increment the deadBytes counter
	if ptr, ok := b.keys[string(k)]; ok {
		b.deadBytes += uint64((16 + len(k) + int(ptr.ValueSize)*1024*1024))
	}

	b.totalBytes += uint64(len(record) * 1024 * 1024)
	b.writePosition += uint64(n)
	b.keys[string(k)] = &hint

	return nil
}

// Get retrieves the value for a given key. It returns ErrKeyNotFound if the key
// is not in the database. An error is returned if a disk read fails, or if
// the data is found to be corrupted.
func (b *Bitcask) Get(k []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	hint, ok := b.keys[string(k)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, string(k))
	}

	dataFilePath := filepath.Join(b.opts.DataDir, fmt.Sprintf("%d.dat", hint.FileId))
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("open data file %s: %w", dataFilePath, err)
	}
	defer dataFile.Close()

	if _, err = dataFile.Seek(int64(hint.RecordPosition), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to record at %d in %s: %w", hint.RecordPosition, dataFilePath, err)
	}

	record := make([]byte, 16+len(k)+int(hint.ValueSize))
	if _, err := io.ReadFull(dataFile, record); err != nil {
		return nil, fmt.Errorf("%w: failed to read record from %s: %w", ErrDataCorrupted, dataFilePath, err)
	}

	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.ChecksumIEEE(record[4:]))
	if !slices.Equal(record[0:4], crc) {
		return nil, ErrChecksumFailed
	}

	return record[16+len(k):], nil
}

// Delete removes a key from the database. It does this by writing a special
// "tombstone" value for the provided key. This marks the record for deletion during a future merge.
func (b *Bitcask) Delete(k []byte) error {
	// using an empty slice for tombstone value
	var v []byte
	return b.Put(k, v)
}

// Close gracefully closes the database by syncing all data to disk, releasing file
// handles, and unlocking the Bitcask for future connections.
func (b *Bitcask) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.cancel()
	err := errors.Join(b.activeDataFile.Close(), b.lock.Close())
	if err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	return nil
}

// new initializes a new Bitcask instance on disk. It creates the necessary
// directory structure, prepares the initial active data file, and acquires a
// file lock to ensure exclusive access. It returns an error if any of these
// steps fail.
func (b *Bitcask) new() error {
	workDir := filepath.Join(b.opts.WorkDir, "bitcask")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("create directory %s: %w", workDir, err)
	}
	b.opts.WorkDir = workDir

	dataDir := filepath.Join(b.opts.WorkDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("create directory %s: %w", dataDir, err)
	}
	b.opts.DataDir = dataDir

	dataFile, err := b.newDataFile()
	if err != nil {
		return fmt.Errorf("create initial data file: %w", err)
	}
	b.activeDataFile = dataFile

	logPath := filepath.Join(b.opts.WorkDir, "error.log")
	errorLog, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", logPath, err)
	}
	defer errorLog.Close()
	b.logger = slog.New(slog.NewJSONHandler(errorLog, nil))

	lockPath := filepath.Join(workDir, ".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("create lock file %s: %w", lockPath, err)
	}
	b.lock = lock

	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return fmt.Errorf("acquire file lock on %s: %w", lockPath, err)
	}

	if b.opts.MergePolicy.Strategy != MergeStrategyNever {
		go b.mergeWorker(b.ctx)
	}

	return nil
}

// reconnect attempts to resurrect an existing Bitcask. It acquires the file lock
// and rebuilds the key index from hint files. It returns os.ErrNotExist if the
// lockfile is not found, signaling the caller to create a new instance.
func (b *Bitcask) reconnect() error {
	lockFilePath := filepath.Join(b.opts.WorkDir, ".lock")
	_, err := os.Stat(lockFilePath)
	if err != nil {
		return fmt.Errorf("stat lock file %s: %w", lockFilePath, err)
	}

	lock, err := os.Open(lockFilePath)
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrLocked
		}
		return fmt.Errorf("acquire file lock on %s: %w", lockFilePath, err)
	}
	b.lock = lock

	if err := b.rebuildKeys(); err != nil {
		return fmt.Errorf("rebuild keys: %w", err)
	}

	if b.opts.MergePolicy.Strategy != MergeStrategyNever {
		go b.mergeWorker(b.ctx)
	}

	return nil
}

// rebuildKeys scans all hint files in the data directory to rebuild the in-memory key index when reconnect is called.
func (b *Bitcask) rebuildKeys() error {
	entries, err := os.ReadDir(b.opts.DataDir)
	if err != nil {
		return fmt.Errorf("list data directory %s: %w", b.opts.DataDir, err)
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".hint" {
			continue
		}

		hintFilePath := filepath.Join(b.opts.DataDir, entry.Name())
		hintFile, err := os.Open(hintFilePath)
		if err != nil {
			return fmt.Errorf("open hint file %s: %w", hintFilePath, err)
		}
		defer hintFile.Close()

		for {
			reader := bufio.NewReader(hintFile)
			metadata := make([]byte, 0, 16)
			fileId, err := parseFileId(hintFile)
			if err != nil {
				return fmt.Errorf("parse file id from %s: %w", hintFile.Name(), err)
			}

			if _, err := io.ReadFull(reader, metadata); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("%w: read metadata from hint file %s: %w", ErrDataCorrupted, hintFilePath, err)
			}

			timestamp := binary.BigEndian.Uint32(metadata[0:4])
			keySize := binary.BigEndian.Uint32(metadata[4:8])
			valueSize := binary.BigEndian.Uint32(metadata[8:12])
			recordPosition := binary.BigEndian.Uint32(metadata[12:16])

			key := make([]byte, keySize)
			if _, err := io.ReadFull(reader, key); err != nil {
				return fmt.Errorf("%w: read key from hint file %s: %w", ErrDataCorrupted, hintFilePath, err)
			}

			b.keys[string(key)] = &Hint{
				FileId:         fileId,
				ValueSize:      valueSize,
				RecordPosition: recordPosition,
				Timestamp:      timestamp,
			}
		}
	}

	return nil
}

// newDataFile creates a new dataFile
func (b *Bitcask) newDataFile() (*os.File, error) {
	id := time.Now().UnixNano()
	path := filepath.Join(b.opts.DataDir, fmt.Sprintf("%d.dat", id))
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
}

// rotateDataFile calls newDataFile and updates b.activeDataFile to use the new file handle.
func (b *Bitcask) rotateDataFile() error {
	dataFile, err := b.newDataFile()
	if err != nil {
		return err
	}

	os.Chmod(b.activeDataFile.Name(), 0444)
	b.activeDataFile.Close()
	b.activeDataFile = dataFile
	b.writePosition = 0

	return nil
}

// writeFile wraps the generic Write method. It first ensures that the b.activeDataFile has enough room for the incoming record. After the record is written, it forces a sync to disk based on the specified b.opts.SyncStrategy.
func (b *Bitcask) writeFile(file *os.File, record []byte) (int, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat file %s: %w", file.Name(), err)
	}

	if uint64(stat.Size()+int64(len(record))) > b.opts.MaxFileSize {
		err := b.rotateDataFile()
		if err != nil {
			return 0, fmt.Errorf("rotate data file: %w", err)
		}
	}

	n, err := file.Write(record)
	if err != nil {
		return 0, fmt.Errorf("write to data file %q: %w", file.Name(), err)
	}

	if b.opts.SyncStrategy != SyncNone {
		// TODO: need to add logic to handle Always vs Interval
		if err := b.activeDataFile.Sync(); err != nil {
			return 0, fmt.Errorf("sync data file %q: %w", b.activeDataFile.Name(), err)
		}
	}

	return n, nil
}

// mergeWorker handles merge
func (b *Bitcask) mergeWorker(ctx context.Context) {
	ticker := time.NewTicker(b.opts.MergePolicy.Interval)
	defer ticker.Stop()

	for {
		if b.opts.MergePolicy.Strategy == MergeStrategyWindow {
			now := time.Now()
			if b.opts.MergePolicy.WindowStart > now.Hour() || b.opts.MergePolicy.WindowEnd < now.Hour() {
				target := time.Date(now.Year(), now.Month(), now.Day(), b.opts.MergePolicy.WindowStart, 0, 0, 0, now.Location())
				if target.Before(now) {
					target = target.Add(24 * time.Hour)
				}
				time.Sleep(time.Until(target))
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// if b.deadBytes >= b.opts.MergePolicy.DeadByteThreshold {
			// }
			//
			// if (uint8(b.deadBytes) / uint8(b.totalBytes)) >= b.opts.MergePolicy.FragThreshold {
			// }

			// create merge file and hintfile
			// mergeFile, err := os.OpenFile(b.dataFilePath(1), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
			// if err != nil {
			//
			// }

			mergefile, err := b.newDataFile()
			if err != nil {
				// TODO: log error
			}
			var mergeFileOffset int

			id, err := parseFileId(mergefile)
			if err != nil {
				// TODO: log error
			}

			hintFilePath := filepath.Join(b.opts.DataDir, fmt.Sprintf("%d.hint", id))
			hintFile, err := os.OpenFile(hintFilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
			if err != nil {
			}

			entries, err := os.ReadDir(b.opts.DataDir)
			if err != nil {
				errMsg := fmt.Sprintf("mergeWorker() unexpected error listing dir entries: %v", err)
				b.logger.Error(errMsg)
			}

			for _, entry := range entries {
				// check to see if this is the correct file type
				if filepath.Ext(entry.Name()) != ".dat" {
					continue
				}

				func() { // extract into a function called ReadRecord
					path := filepath.Join(b.opts.DataDir, entry.Name())
					file, err := os.Open(path)
					if err != nil {
						errMsg := fmt.Sprintf("mergeWorker() unexpected error opening '%s' for reading: %v", entry.Name(), err)
						b.logger.Error(errMsg)
					}
					defer file.Close()

					// using this because it's more optimal to pull larger chunks into memory than to make multiple syscalls per request to the file
					reader := bufio.NewReader(file)
					metadata := make([]byte, 0, 16)
					id, err := parseFileId(file)
					if err != nil {
						// TODO: failed conversion
					}

					for {
						if _, err := io.ReadFull(reader, metadata); err != nil {
							// reached end EOF, return
							if errors.Is(err, io.EOF) {
								return
							}
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading META from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						keySize := binary.BigEndian.Uint32(metadata[8:12])
						valueSize := binary.BigEndian.Uint32(metadata[12:16])

						key := make([]byte, keySize)
						if _, err := io.ReadFull(reader, key); err != nil {
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading KEY from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						valuePtr, ok := b.keys[string(key)]
						if !ok || valuePtr.FileId != id {
							reader.Discard(int(valueSize))
							continue
						}

						value := make([]byte, valueSize)
						if _, err := io.ReadFull(reader, value); err != nil {
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading VALUE from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						// try to append to mergeFile (rotate if necessary)
						record := make([]byte, 16+len(key)+len(value))
						copy(record, metadata)
						copy(record, key)
						copy(record, value)

						n, err := b.writeFile(mergefile, record)
						if err != nil {

						}

						hint := make([]byte, 16+len(key))
						copy(hint, record[4:16])
						copy(hint, key)

						_, err = b.writeFile(hintFile, hint)
						if err != nil {
						}

						// update keyMap with new mergeFile fileId and record position
						id, err := parseFileId(mergefile)
						if err != nil {

						}

						valuePtr.FileId = id
						valuePtr.RecordPosition = uint32(mergeFileOffset)

						// finally update mergefile offset
						mergeFileOffset += n
					}
				}()
			}
		}
	}
}

// TODO: check b.CLose
