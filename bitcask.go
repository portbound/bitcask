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
	lock            *os.File
	mu              sync.RWMutex
	dataDir         string
	activeDataFile  *os.File
	activeMergeFile *os.File
	activeHintFile  *os.File
	dataFileOffset  int64
	mergeFileOffset int64
	keys            map[string]*hint
	opts            bitcaskOpts
	logger          *slog.Logger
	totalBytes      uint64
	deadBytes       uint64
	ctx             context.Context
	cancel          context.CancelFunc
}

// hint represents the location and metadata of a key in a data file.
type hint struct {
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
		keys: make(map[string]*hint),
		opts: defaultOpts,
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(&b)
	}

	workDir := filepath.Join(b.opts.Dir, "bitcask")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, fmt.Errorf("create parent dir %s: %w", workDir, err)
	}

	lockPath := filepath.Join(workDir, ".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("create lock file %s: %w", lockPath, err)
	}
	b.lock = lock

	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("acquire file lock on %s: %w", lockPath, ErrLocked)
		}
		return nil, fmt.Errorf("acquire file lock on %s: %w", lockPath, err)
	}

	dataDir := filepath.Join(workDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", workDir, err)
	}
	b.dataDir = dataDir

	dataFile, err := b.newDataFile()
	if err != nil {
		return nil, fmt.Errorf("create initial data file: %w", err)
	}
	b.activeDataFile = dataFile

	logPath := filepath.Join(workDir, "mergeWorker.log")
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}
	defer log.Close()
	b.logger = slog.New(slog.NewJSONHandler(log, nil))

	if err := b.rebuildKeys(); err != nil {
		return nil, fmt.Errorf("rebuild keys: %w", err)
	}

	if b.opts.MergePolicy.Strategy != MergeStrategyNever {
		go b.mergeWorker(b.ctx)
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

	stat, err := b.activeDataFile.Stat()
	if err != nil {
		return fmt.Errorf("stat file %s: %w", b.activeDataFile.Name(), err)
	}

	if uint64(stat.Size()+int64(len(record))) > b.opts.MaxFileSize {
		err := b.rotateDataFile()
		if err != nil {
			return fmt.Errorf("rotate data file: %w", err)
		}
	}

	fileId, err := parseFileId(b.activeDataFile)
	if err != nil {
		return fmt.Errorf("parse file id: %w", err)
	}

	n, err := b.activeDataFile.Write(record)
	if err != nil {
		if errors.Is(err, os.ErrClosed) {
			return fmt.Errorf("write to data file %q: %w", b.activeDataFile.Name(), ErrDatabaseClosed)
		}
		return fmt.Errorf("write to data file %q: %w", b.activeDataFile.Name(), err)
	}

	if b.opts.SyncStrategy != SyncNone {
		if err := b.activeDataFile.Sync(); err != nil {
			return fmt.Errorf("sync data file %q: %w", b.activeDataFile.Name(), err)
		}
	}

	hint := hint{
		FileId:         fileId,
		ValueSize:      uint32(len(v)),
		RecordPosition: uint32(b.dataFileOffset),
		Timestamp:      timestamp,
	}

	// if we're overwriting a record, increment the deadBytes counter
	if ptr, ok := b.keys[string(k)]; ok {
		b.deadBytes += uint64((16 + len(k) + int(ptr.ValueSize)))
	}

	b.totalBytes += uint64(len(record))
	b.dataFileOffset += n
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

	if hint.ValueSize == 0 {
		return nil, ErrKeyNotFound
	}

	dataFilePath := filepath.Join(b.dataDir, fmt.Sprintf("%d.dat", hint.FileId))
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
		if errors.Is(err, os.ErrClosed) {
			return nil, fmt.Errorf("read record from %s: %w", b.activeDataFile.Name(), ErrDatabaseClosed)
		}
		return nil, fmt.Errorf("%w: read record from %s: %w", ErrDataCorrupted, dataFilePath, err)
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

	var errs []error
	if b.activeDataFile != nil {
		errs = append(errs, b.activeDataFile.Close())
	}
	if b.activeMergeFile != nil {
		errs = append(errs, b.activeMergeFile.Close())
	}
	if b.activeHintFile != nil {
		errs = append(errs, b.activeHintFile.Close())
	}
	if b.lock != nil {
		errs = append(errs, b.lock.Close())
	}

	return errors.Join(errs...)
}

// rebuildKeys scans all hint files in the data directory to rebuild the in-memory key index when reconnect is called.
func (b *Bitcask) rebuildKeys() error {
	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		return fmt.Errorf("list data directory %s: %w", b.dataDir, err)
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".hint" {
			continue
		}

		hintFilePath := filepath.Join(b.dataDir, entry.Name())
		hintFile, err := os.Open(hintFilePath)
		if err != nil {
			return fmt.Errorf("open hint file %s: %w", hintFilePath, err)
		}
		defer hintFile.Close()

		reader := bufio.NewReader(hintFile)
		metadata := make([]byte, 16)
		fileId, err := parseFileId(hintFile)
		if err != nil {
			return fmt.Errorf("parse file id from %s: %w", hintFile.Name(), err)
		}

		for {
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

			b.keys[string(key)] = &hint{
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
	path := filepath.Join(b.dataDir, fmt.Sprintf("%d.dat", id))
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
}

// rotateDataFile calls newDataFile and updates b with the new file handle.
func (b *Bitcask) rotateDataFile() error {
	file, err := b.newDataFile()
	if err != nil {
		return err
	}

	os.Chmod(b.activeDataFile.Name(), 0444)
	b.activeDataFile.Close()
	b.activeDataFile = file
	b.dataFileOffset = 0

	return nil
}

// rotateMergeFile calls newDataFile and creates a new hintFile before updating b with the new file handles.
func (b *Bitcask) rotateMergeFile() error {
	mergeFile, err := b.newDataFile()
	if err != nil {
		return fmt.Errorf("create mergeFile: %w", err)
	}

	id, err := parseFileId(mergeFile)
	if err != nil {
		return fmt.Errorf("parse file id: %w", err)
	}

	hintFilePath := filepath.Join(b.dataDir, fmt.Sprintf("%d.hint", id))
	hintFile, err := os.OpenFile(hintFilePath, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		os.Remove(mergeFile.Name())
		return fmt.Errorf("create hint file: %w", err)
	}

	if b.activeMergeFile != nil {
		os.Chmod(b.activeMergeFile.Name(), 0444)
		b.activeMergeFile.Close()

		os.Chmod(b.activeHintFile.Name(), 0444)
		b.activeHintFile.Close()
	}

	b.activeMergeFile = mergeFile
	b.mergeFileOffset = 0
	b.activeHintFile = hintFile

	return nil
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
			deadByteThresholdExceeded := b.deadBytes >= b.opts.MergePolicy.DeadByteThreshold
			fragThresholdExceeded := (b.deadBytes*100)/b.totalBytes >= uint64(b.opts.MergePolicy.FragThreshold)

			if !deadByteThresholdExceeded && !fragThresholdExceeded {
				continue
			}

			entries, err := os.ReadDir(b.dataDir)
			if err != nil {
				b.logger.Error("list data directory", "error", err)
			}

			for _, entry := range entries {
				if filepath.Ext(entry.Name()) != ".dat" {
					continue
				}

				dataFilePath := filepath.Join(b.dataDir, entry.Name())
				if err := b.merge(dataFilePath); err != nil {
					b.logger.Error("merge", "error", err)
				}

				if err := os.Remove(dataFilePath); err != nil {
					b.logger.Error("remove data file", "error", err)
				}
			}
		}
	}
}

func (b *Bitcask) merge(dataFilePath string) error {
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return fmt.Errorf("open dataFile: %w", err)
	}
	defer dataFile.Close()

	if b.activeMergeFile == nil {
		if err := b.rotateMergeFile(); err != nil {
			return fmt.Errorf("initalize merge file: %w", err)
		}
	}

	reader := bufio.NewReader(dataFile)
	metadata := make([]byte, 16)
	dataFileId, err := parseFileId(dataFile)
	if err != nil {
		return fmt.Errorf("%q: parse file id: %w", dataFile.Name(), err)
	}

	for {
		if _, err := io.ReadFull(reader, metadata); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read metadata: %w", err)
		}

		keySize := (binary.BigEndian.Uint32(metadata[8:12]))
		valueSize := (binary.BigEndian.Uint32(metadata[12:16]))

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return fmt.Errorf("read key: %w", err)
		}

		hint, ok := b.keys[string(key)]
		if !ok || hint.FileId != dataFileId {
			reader.Discard(int(valueSize))
			continue
		}

		stat, err := b.activeMergeFile.Stat()
		if err != nil {
			return fmt.Errorf("stat file %s: %w", b.activeMergeFile.Name(), err)
		}

		if uint64(stat.Size()+int64(len(metadata)+int(keySize+valueSize))) > b.opts.MaxFileSize {
			err := b.rotateMergeFile()
			if err != nil {
				return fmt.Errorf("rotate merge file: %w", err)
			}
		}

		if _, err := b.activeMergeFile.Write(metadata); err != nil {
			return fmt.Errorf("%q: write metadata: %w", b.activeMergeFile.Name(), err)
		}

		if _, err := b.activeMergeFile.Write(key); err != nil {
			return fmt.Errorf("%q: write key: %w", b.activeMergeFile.Name(), err)
		}

		if _, err := io.CopyN(b.activeMergeFile, reader, int64(valueSize)); err != nil {
			return fmt.Errorf("%q: copy value: %w", b.activeMergeFile.Name(), err)
		}

		if b.opts.SyncStrategy != SyncNone {
			if err := b.activeMergeFile.Sync(); err != nil {
				return fmt.Errorf("%q: sync: %w", b.activeMergeFile.Name(), err)
			}
		}

		hintRecord := make([]byte, 16+len(key))
		binary.BigEndian.PutUint32(hintRecord, hint.Timestamp)
		offset := 4
		binary.BigEndian.PutUint32(hintRecord[offset:], keySize)
		offset += 4
		binary.BigEndian.PutUint32(hintRecord[offset:], valueSize)
		offset += 4
		binary.BigEndian.PutUint32(hintRecord[offset:], uint32(b.mergeFileOffset))
		offset += 4
		copy(hintRecord[offset:], key)

		if _, err = b.activeHintFile.Write(hintRecord); err != nil {
			return fmt.Errorf("%q: write: %w", b.activeHintFile.Name(), err)
		}

		if b.opts.SyncStrategy != SyncNone {
			if err := b.activeHintFile.Sync(); err != nil {
				return fmt.Errorf("%q: sync: %w", b.activeHintFile.Name(), err)
			}
		}

		mergeFileId, err := parseFileId(b.activeMergeFile)
		if err != nil {
			return fmt.Errorf("%q: parse file id: %w", dataFile.Name(), err)
		}
		hint.FileId = mergeFileId
		hint.RecordPosition = uint32(b.mergeFileOffset)
		b.mergeFileOffset += 16 + int(keySize+valueSize)

	}
}
